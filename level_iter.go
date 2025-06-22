package pebble

import (
	"fmt"
	"sort"

	"github.com/khushmanvar/hyperfork/internal/base"
	"github.com/khushmanvar/hyperfork/internal/manifest"
	"github.com/khushmanvar/hyperfork/internal/sstable"
)

// levelIter provides a merged view of the sstables in a level.
type levelIter struct {
	opts  *Options
	files []*manifest.FileMetadata
	iter  base.InternalIterator
	err   error
	index int
	key   []byte
}

func newLevelIter(opts *Options, files []*manifest.FileMetadata) *levelIter {
	return &levelIter{
		opts:  opts,
		files: files,
		index: -1,
	}
}

func (l *levelIter) findFile(key []byte) int {
	// Binary search for the first file whose largest key is >= key.
	i := sort.Search(len(l.files), func(i int) bool {
		return l.opts.Comparer.Compare(key, l.files[i].Largest.UserKey) <= 0
	})
	if i < len(l.files) {
		return i
	}
	return len(l.files)
}

func (l *levelIter) openFile(index int) error {
	if l.iter != nil {
		if err := l.iter.Close(); err != nil {
			return err
		}
		l.iter = nil
	}
	if index >= len(l.files) {
		return nil
	}

	f := l.files[index]
	sst, err := l.db.vfs.Open(l.db.dirname + "/" + f.Filename)
	if err != nil {
		return err
	}
	r, err := sstable.NewReader(sst, *l.opts)
	if err != nil {
		sst.Close()
		return err
	}
	l.iter = r.NewIter(nil)
	return nil
}

func (l *levelIter) SeekGE(key []byte) {
	l.key = key
	l.index = l.findFile(key)
	if err := l.openFile(l.index); err != nil {
		l.err = err
		return
	}
	if l.iter != nil {
		l.iter.SeekGE(key)
	}
}

func (l *levelIter) Valid() bool {
	if l.err != nil {
		return false
	}
	return l.iter != nil && l.iter.Valid()
}

func (l *levelIter) Key() base.InternalKey {
	return l.iter.Key()
}

func (l *levelIter) Value() []byte {
	return l.iter.Value()
}

func (l *levelIter) Next() {
	l.iter.Next()
	for l.iter != nil && !l.iter.Valid() {
		l.index++
		if err := l.openFile(l.index); err != nil {
			l.err = err
			return
		}
		if l.iter != nil {
			l.iter.SeekGE(l.key)
		}
	}
}

func (l *levelIter) Close() error {
	if l.iter != nil {
		if err := l.iter.Close(); err != nil {
			return err
		}
		l.iter = nil
	}
	return l.err
} 