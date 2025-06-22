// Copyright 2017 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/khushmanvar/hyperfork/internal/base"
	"github.com/khushmanvar/hyperfork/vfs"
	"github.com/khushmanvar/hyperfork/record"
	"github.com/khushmanvar/hyperfork/sstable"
	"github.com/khushmanvar/hyperfork/manifest"
)

// Reader is the interface for reading data from a DB.
type Reader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but
	// it is safe to modify the contents of the argument after Get returns.
	Get(key []byte) (value []byte, closer io.Closer, err error)

	// NewIter returns an iterator that is unpositioned (or positioned relative
	// to an optional bound). The iterator must be closed after use.
	NewIter(o *IterOptions) *Iterator

	// Close closes the Reader. It may or may not close the underlying DB
	// instance.
	Close() error
}

// Writer is the interface for writing data to a DB.
type Writer interface {
	// Apply applies a batch of mutations atomically.
	Apply(batch *Batch, o *WriteOptions) error

	// Delete deletes the value for the given key.
	Delete(key []byte, o *WriteOptions) error

	// LogData adds the specified data to the WAL. The data is returned by WAL
	// readers, but is not otherwise interpreted by Pebble.
	LogData(data []byte, o *WriteOptions) error

	// Set sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	Set(key, value []byte, o *WriteOptions) error
}

type tableCache struct {
	db    *DB
	cache *Cache
}

func (c *tableCache) newIter(
	f *manifest.FileMetadata,
) (base.InternalIterator, error) {
	// Check the cache.
	if r, ok := c.cache.Get(f.FileNum).(*sstable.Reader); ok {
		return r.NewIter(nil), nil
	}

	// Open the sstable.
	file, err := c.db.vfs.Open(fmt.Sprintf("%s/%06d.sst", c.db.dir, f.FileNum))
	if err != nil {
		return nil, err
	}
	r, err := sstable.NewReader(file, *c.db.opts)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Cache the reader.
	c.cache.Set(f.FileNum, r)

	return r.NewIter(nil), nil
}

func (c *tableCache) evict(fileNum uint64) {
	if r, ok := c.cache.Get(fileNum).(*sstable.Reader); ok {
		r.Close()
		c.cache.Set(fileNum, nil)
	}
}

// DB is a key-value store.
//
// DB is safe for concurrent access from multiple goroutines.
type DB struct {
	dir       string
	opts      *Options
	cache     *Cache
	vfs       vfs.FS
	mu        sync.Mutex
	logSeqNum uint64
	mem       *memTable
	imm       *memTable
	versions  *versionSet
	wal       *record.LogWriter
	lock      io.Closer
	// Background work state.
	bgCond     sync.Cond
	bgErr      error
	flushing   bool
	compacting bool
	compactionc chan struct{}
	closec      chan struct{}
	logNum      uint64
	logFile     vfs.File
	flock       io.Closer
	tableCache  *tableCache
}

var _ Reader = (*DB)(nil)
var _ Writer = (*DB)(nil)
var _ io.Closer = (*DB)(nil)

// Open opens a DB.
func Open(dir string, o *Options) (*DB, error) {
	o.EnsureDefaults()
	d := &DB{
		dir:       dir,
		opts:      o,
		vfs:       o.FS,
		mem:       newMemTable(o),
		closec:    make(chan struct{}),
		compactionc: make(chan struct{}, 1),
	}
	d.bgCond.L = &d.mu
	d.tableCache = &tableCache{
		db:    d,
		cache: NewCache(o.MaxOpenFiles),
	}

	if err := d.vfs.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Lock the database directory.
	var err error
	d.flock, err = d.vfs.Lock(dir + "/LOCK")
	if err != nil {
		return nil, err
	}

	vs, err := newVersionSet(dir, o)
	if err != nil {
		return nil, err
	}
	d.versions = vs
	d.logSeqNum = vs.current.lastSeqNum

	// Open the WAL.
	wal, err := o.FS.Create(dir + "/WAL-" + fmt.Sprintf("%06d", vs.current.logNum))
	if err != nil {
		return nil, err
	}
	d.wal = record.NewLogWriter(wal, vs.current.logNum)

	// Replay the WAL.
	walPath := dir + "/WAL"
	wal, err = o.FS.Open(walPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if wal != nil {
		defer wal.Close()
		r := record.NewReader(wal, 0)
		for {
			rr, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			b, err := ioutil.ReadAll(rr)
			if err != nil {
				return nil, err
			}
			batch := &Batch{data: b, db: d}
			if err := d.mem.apply(batch); err != nil {
				return nil, err
			}
		}
	}

	go d.bgCompaction()

	// Create the manifest file if it doesn't exist.
	if _, err := vfs.Stat(dir + "/MANIFEST"); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	return d, nil
}

// Apply applies a batch of mutations atomically.
func (d *DB) Apply(b *Batch, o *WriteOptions) error {
	if o == nil {
		o = DefaultWriteOptions
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.makeRoomForWrite(b); err != nil {
		return err
	}

	// Write the batch to the WAL.
	if err := d.wal.WriteRecord(b.data); err != nil {
		return err
	}
	if o.Sync {
		if err := d.wal.Sync(); err != nil {
			return err
		}
	}

	// Apply the batch to the memtable.
	if err := d.mem.apply(b); err != nil {
		return err
	}

	// Update the sequence number.
	d.logSeqNum += uint64(b.Count())

	return nil
}

// Set sets the value for the given key.
func (d *DB) Set(key, value []byte, o *WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	b.Set(key, value, o)
	return d.Apply(b, o)
}

// Delete deletes the value for the given key.
func (d *DB) Delete(key []byte, o *WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	b.Delete(key, o)
	return d.Apply(b, o)
}

// LogData adds the specified data to the WAL.
func (d *DB) LogData(data []byte, o *WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	b.LogData(data, o)
	return d.Apply(b, o)
}

func (d *DB) makeRoomForWrite(b *Batch) error {
	if d.mem.size+uint32(len(b.data)) < uint32(d.opts.MemTableSize) {
		return nil
	}
	return d.flush()
}

func (d *DB) flush() error {
	// Wait for any ongoing flush to complete.
	for d.flushing {
		d.bgCond.Wait()
	}

	// Rotate the memtable.
	d.imm = d.mem
	d.mem = newMemTable(d.opts)

	// Rotate the WAL.
	newLogNum := d.versions.current.nextFileNum
	d.versions.current.nextFileNum++
	wal, err := d.vfs.Create(d.dir + "/WAL-" + fmt.Sprintf("%06d", newLogNum))
	if err != nil {
		return err
	}
	d.wal = record.NewLogWriter(wal, newLogNum)

	// Create a version edit to reflect the new WAL.
	ve := &manifest.VersionEdit{
		LogNumber: newLogNum,
	}
	return d.versions.logAndApply(ve, d)
}

func (d *DB) bgFlush() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for {
		if d.closec == nil {
			return
		}
		if d.imm == nil {
			d.bgCond.Wait()
			continue
		}

		d.flushing = true
		d.mu.Unlock()
		d.doFlush()
		d.mu.Lock()
		d.flushing = false
		d.bgCond.Broadcast()
	}
}

func (d *DB) doFlush() error {
	mem := d.imm
	d.imm = nil

	newSSTFileNum := d.versions.current.nextFileNum
	d.versions.current.nextFileNum++
	sst, err := d.vfs.Create(d.dir + "/" + fmt.Sprintf("%06d.sst", newSSTFileNum))
	if err != nil {
		return err
	}

	w := sstable.NewWriter(sst, d.opts, d.opts.Levels[0])
	iter := mem.newIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		ikey, ok := base.ParseInternalKey(iter.Key())
		if !ok {
			return base.ErrCorruption
		}
		if err := w.Add(ikey, iter.Value()); err != nil {
			w.Close()
			return err
		}
	}
	if err := iter.Close(); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	stat, err := sst.Stat()
	if err != nil {
		return err
	}

	ve := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetadata{
					FileNum:  newSSTFileNum,
					Size:     uint64(stat.Size()),
					Smallest: w.Smallest(),
					Largest:  w.Largest(),
				},
			},
		},
	}
	return d.versions.logAndApply(ve, d)
}

func (d *DB) Get(key []byte) (value []byte, closer io.Closer, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.get(key, d.logSeqNum)
}

func (d *DB) get(key []byte, seq uint64) (value []byte, closer io.Closer, err error) {
	ikey := base.MakeInternalKey(key, seq, base.InternalKeyKindMax)

	// Look in the memtable.
	if v, err := d.mem.get(ikey); err == nil {
		return v, nil, nil
	} else if err != base.ErrNotFound {
		return nil, nil, err
	}

	// Look in the immutable memtable.
	if d.imm != nil {
		if v, err := d.imm.get(ikey); err == nil {
			return v, nil, nil
		} else if err != base.ErrNotFound {
			return nil, nil, err
		}
	}

	// Look in the SSTables.
	v, err := d.versions.get(ikey)
	if err != nil {
		return nil, nil, err
	}
	return v, nil, nil
}

func (d *DB) NewIter(o *IterOptions) *Iterator {
	if o == nil {
		o = &IterOptions{}
	}
	seq := o.seq
	if seq == 0 {
		seq = d.logSeqNum
	}
	return newIterator(d, newMergingIter(d, d.opts.Comparer, d.mem, d.imm, d.versions.current, seq), o)
}

func (d *DB) bgCompaction() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for {
		if d.closec == nil {
			return
		}

		var c *compaction
		picker := &compactionPicker{v: d.versions.current}
		c = picker.pick()

		if c == nil {
			d.bgCond.Wait()
			continue
		}

		d.compacting = true
		d.mu.Unlock()
		ve, err := runCompaction(d.versions, c, d.opts)
		if err != nil {
			d.bgErr = err
		} else {
			d.versions.logAndApply(ve, d)
		}
		d.mu.Lock()
		d.compacting = false
		d.bgCond.Broadcast()
	}
}

func (d *DB) newSSTableIter(f *manifest.FileMetadata) (base.InternalIterator, error) {
	return d.tableCache.newIter(f)
}

// Close closes the database.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closedo.Do(func() {
		close(d.closec)
	})
	if d.flock != nil {
		d.flock.Close()
	}
	return d.wal.Close()
}

// NewSnapshot returns a new snapshot.
func (d *DB) NewSnapshot() *Snapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	return &Snapshot{
		seqNum: d.versions.lastSeqNum,
	}
}
