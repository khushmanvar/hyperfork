// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"github.com/khushmanvar/hyperfork/internal/base"
	"github.com/khushmanvar/hyperfork/internal/manifest"
	"github.com/khushmanvar/hyperfork/sstable"
)

// compaction is a collection of files that are compacted together.
type compaction struct {
	// level is the level that is being compacted.
	level int
	// inputs are the input files for the compaction.
	inputs [2][]*manifest.FileMetadata
}

// runCompaction runs a compaction.
func runCompaction(vs *versionSet, c *compaction, o *Options) (ve *manifest.VersionEdit, err error) {
	iters := make([]base.InternalIterator, 0, len(c.inputs[0])+1)
	for _, f := range c.inputs[0] {
		iter, err := vs.current.db.newSSTableIter(f)
		if err != nil {
			return nil, err
		}
		iters = append(iters, iter)
	}
	for _, f := range c.inputs[1] {
		iter, err := vs.current.db.newSSTableIter(f)
		if err != nil {
			return nil, err
		}
		iters = append(iters, iter)
	}

	merger := newMergingIter(vs.current.db.opts.Comparer, iters...)
	ve = &manifest.VersionEdit{}
	defer merger.Close()

	var writer *sstable.Writer
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()

	snapshots := vs.listSnapshots()
	elideTombstone := func(key []byte, seqNum uint64) bool {
		// Can we elide this tombstone?
		//
		// We can elide a tombstone if it's not the newest version of the key and
		// there are no snapshots that see the tombstone.
		for _, s := range snapshots {
			if s <= seqNum {
				return false
			}
		}

		for level := c.level + 1; level < numLevels; level++ {
			for _, f := range vs.current.files[level] {
				if o.Comparer.Compare(key, f.Smallest.UserKey) >= 0 &&
					o.Comparer.Compare(key, f.Largest.UserKey) <= 0 {
					return false
				}
			}
		}
		return true
	}

	for merger.First(); merger.Valid(); merger.Next() {
		// Handle tombstones.
		if merger.Key().Kind() == base.InternalKeyKindDelete {
			if elideTombstone(merger.Key().UserKey, merger.Key().SeqNum()) {
				continue
			}
		}

		if writer == nil {
			fileNum := vs.nextFileNum
			vs.nextFileNum++
			f, err := vs.current.db.vfs.Create(vs.current.db.dir + "/" + fmt.Sprintf("%06d.sst", fileNum))
			if err != nil {
				return nil, err
			}
			writer = sstable.NewWriter(f, *o)
		}

		if err := writer.Add(merger.Key(), merger.Value()); err != nil {
			return nil, err
		}

		if writer.EstimatedSize() > o.Levels[c.level+1].TargetFileSize {
			meta, err := writer.Metadata()
			if err != nil {
				return nil, err
			}
			ve.NewFiles = append(ve.NewFiles, manifest.NewFileEntry{
				Level: c.level + 1,
				Meta:  meta,
			})
			if err := writer.Close(); err != nil {
				return nil, err
			}
			writer = nil
		}
	}

	if writer != nil {
		meta, err := writer.Metadata()
		if err != nil {
			return nil, err
		}
		ve.NewFiles = append(ve.NewFiles, manifest.NewFileEntry{
			Level: c.level + 1,
			Meta:  meta,
		})
		if err := writer.Close(); err != nil {
			return nil, err
		}
	}

	for i := range c.inputs {
		for _, f := range c.inputs[i] {
			ve.DeletedFiles[f.FileNum] = c.level + i
		}
	}

	return ve, nil
} 