// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/khushmanvar/hyperfork/internal/manifest"
)

// compactionPicker is used to pick a compaction.
type compactionPicker struct {
	// The version of the database.
	v *version
}

// pick selects the best compaction.
func (p *compactionPicker) pick() (c *compaction) {
	// L0 is compacted based on the number of files.
	if len(p.v.files[0]) >= p.v.db.opts.L0CompactionThreshold {
		return &compaction{
			level:  0,
			inputs: [2][]*manifest.FileMetadata{p.v.files[0]},
		}
	}

	// Other levels are compacted based on their size.
	for level := 1; level < numLevels; level++ {
		if p.v.files[level] == nil {
			continue
		}
		totalSize := int64(0)
		for _, f := range p.v.files[level] {
			totalSize += int64(f.Size)
		}
		if totalSize > p.v.db.opts.LBaseMaxBytes {
			// Find the file that overlaps with the most files in the next level.
			var bestFile *manifest.FileMetadata
			var bestOverlap int
			for _, f := range p.v.files[level] {
				overlap := 0
				for _, f2 := range p.v.files[level+1] {
					if p.v.db.opts.Comparer.Compare(f.Largest.UserKey, f2.Smallest.UserKey) >= 0 &&
						p.v.db.opts.Comparer.Compare(f.Smallest.UserKey, f2.Largest.UserKey) <= 0 {
						overlap++
					}
				}
				if overlap > bestOverlap {
					bestOverlap = overlap
					bestFile = f
				}
			}
			if bestFile == nil {
				bestFile = p.v.files[level][0]
			}
			return &compaction{
				level:  level,
				inputs: [2][]*manifest.FileMetadata{{bestFile}},
			}
		}
	}

	return nil
}