// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/khushmanvar/hyperfork/internal/base"
	"github.com/khushmanvar/hyperfork/internal/manifest"
	"github.com/khushmanvar/hyperfork/record"
	"github.com/khushmanvar/hyperfork/sstable"
	"github.com/khushmanvar/hyperfork/vfs"
)

const numLevels = 7

// version is a snapshot of the database state.
type version struct {
	// files is a list of files in each level.
	files [numLevels][]*manifest.FileMetadata
	// The next file number to use.
	nextFileNum uint64
	// The last sequence number.
	lastSeqNum uint64
	// The log number of the WAL.
	logNum uint64
	// The previous log number of the WAL.
	prevLogNum uint64

	// The database this version belongs to.
	db *DB
}

// versionSet manages the set of versions.
type versionSet struct {
	// mu protects the fields below.
	mu sync.Mutex
	// The current version.
	current *version
	// The manifest file.
	manifestFile vfs.File
	// The options.
	opts      *Options
	snapshots map[uint64]int
}

func (vs *versionSet) listSnapshots() []uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	s := make([]uint64, 0, len(vs.snapshots))
	for seqNum := range vs.snapshots {
		s = append(s, seqNum)
	}
	return s
}

func newVersionSet(dir string, o *Options) (*versionSet, error) {
	vs := &versionSet{
		opts:      o,
		snapshots: make(map[uint64]int),
	}

	manifestPath := filepath.Join(dir, "MANIFEST")
	f, err := o.FS.Open(manifestPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		// Create the manifest file.
		f, err = o.FS.Create(manifestPath)
		if err != nil {
			return nil, err
		}
		ve := &manifest.VersionEdit{
			Comparator: o.Comparer.Name,
		}
		w := record.NewLogWriter(f, 0)
		b := ve.Encode(nil)
		if err := w.WriteRecord(b); err != nil {
			return nil, err
		}
		if err := w.Sync(); err != nil {
			return nil, err
		}
	}
	vs.manifestFile = f

	// Read the version edits from the manifest.
	r := record.NewReader(f, 0)
	for {
		rr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var ve manifest.VersionEdit
		b, err := ioutil.ReadAll(rr)
		if err != nil {
			return nil, err
		}
		if err := ve.Decode(b); err != nil {
			return nil, err
		}
		if err := vs.logAndApply(&ve, nil); err != nil {
			return nil, err
		}
	}
	return vs, nil
}

// logAndApply applies a version edit.
func (vs *versionSet) logAndApply(ve *manifest.VersionEdit, d *DB) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Write the version edit to the manifest.
	w := record.NewLogWriter(vs.manifestFile, 0)
	b := ve.Encode(nil)
	if err := w.WriteRecord(b); err != nil {
		return err
	}
	if err := w.Sync(); err != nil {
		return err
	}

	// Create a new version.
	nv := &version{
		db: d,
	}
	if vs.current != nil {
		*nv = *vs.current
	}

	nv.logNum = ve.LogNumber
	nv.prevLogNum = ve.PrevLogNumber
	nv.lastSeqNum = ve.LastSequence
	nv.nextFileNum = ve.NextFileNumber

	// Add new files.
	for _, f := range ve.NewFiles {
		nv.files[f.Level] = append(nv.files[f.Level], f.Meta)
	}
	// Delete old files.
	for level, fileNum := range ve.DeletedFiles {
		var files []*manifest.FileMetadata
		for _, f := range nv.files[level] {
			if f.FileNum != fileNum {
				files = append(files, f)
			}
		}
		nv.files[level] = files
	}

	vs.current = nv
	return nil
}

func (v *version) get(ikey base.InternalKey) ([]byte, error) {
	// Search L0.
	for i := len(v.files[0]) - 1; i >= 0; i-- {
		f := v.files[0][i]
		if v.db.opts.Comparer.Compare(ikey.UserKey, f.Smallest.UserKey) < 0 {
			continue
		}
		if v.db.opts.Comparer.Compare(ikey.UserKey, f.Largest.UserKey) > 0 {
			continue
		}

		sst, err := v.db.vfs.Open(v.db.dir + "/" + fmt.Sprintf("%06d.sst", f.FileNum))
		if err != nil {
			return nil, err
		}
		r, err := sstable.NewReader(sst, *v.db.opts)
		if err != nil {
			sst.Close()
			return nil, err
		}
		iter := r.NewIter(nil)
		iter.SeekGE(ikey.UserKey)
		if iter.Valid() {
			ik := base.ParseInternalKey(iter.Key())
			if v.db.opts.Comparer.Equal(ik.UserKey, ikey.UserKey) {
				if ik.Kind() == base.InternalKeyKindDelete {
					iter.Close()
					return nil, base.ErrNotFound
				}
				val := iter.Value()
				iter.Close()
				return val, nil
			}
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
	}

	// Search L1+.
	for level := 1; level < numLevels; level++ {
		for _, f := range v.files[level] {
			if v.db.opts.Comparer.Compare(ikey.UserKey, f.Smallest.UserKey) < 0 {
				continue
			}
			if v.db.opts.Comparer.Compare(ikey.UserKey, f.Largest.UserKey) > 0 {
				continue
			}

			sst, err := v.db.vfs.Open(v.db.dir + "/" + fmt.Sprintf("%06d.sst", f.FileNum))
			if err != nil {
				return nil, err
			}
			r, err := sstable.NewReader(sst, *v.db.opts)
			if err != nil {
				sst.Close()
				return nil, err
			}
			iter := r.NewIter(nil)
			iter.SeekGE(ikey.UserKey)
			if iter.Valid() {
				ik := base.ParseInternalKey(iter.Key())
				if v.db.opts.Comparer.Equal(ik.UserKey, ikey.UserKey) {
					if ik.Kind() == base.InternalKeyKindDelete {
						iter.Close()
						return nil, base.ErrNotFound
					}
					val := iter.Value()
					iter.Close()
					return val, nil
				}
			}
			if err := iter.Close(); err != nil {
				return nil, err
			}
		}
	}

	return nil, base.ErrNotFound
}

func (vs *versionSet) get(ikey base.InternalKey) ([]byte, error) {
	return vs.current.get(ikey)
}

func (v *version) newIter(o *Options) base.InternalIterator {
	l0Files := v.files[0]
	l0Iters := make([]base.InternalIterator, len(l0Files))
	for i := range l0Files {
		f := l0Files[i]
		sst, err := v.db.vfs.Open(v.db.dir + "/" + fmt.Sprintf("%06d.sst", f.FileNum))
		if err != nil {
			return &base.ErrIterator{Err: err}
		}
		r, err := sstable.NewReader(sst, *o)
		if err != nil {
			return &base.ErrIterator{Err: err}
		}
		l0Iters[i] = r.NewIter(nil)
	}

	iters := make([]base.InternalIterator, 0, numLevels)
	iters = append(iters, &mergingIter{iters: l0Iters})
	for level := 1; level < numLevels; level++ {
		if len(v.files[level]) == 0 {
			continue
		}
		iters = append(iters, newLevelIter(o, v.db, v.files[level]))
	}
	return &mergingIter{iters: iters}
} 