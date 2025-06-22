// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import "github.com/khushmanvar/hyperfork/internal/base"

// FileMetadata holds metadata for an sstable.
type FileMetadata struct {
	// FileNum is the file number.
	FileNum uint64
	// Size is the size of the file.
	Size uint64
	// Smallest is the smallest internal key in the table.
	Smallest base.InternalKey
	// Largest is the largest internal key in the table.
	Largest base.InternalKey
	// SmallestSeqNum is the smallest sequence number in the table.
	SmallestSeqNum uint64
	// LargestSeqNum is the largest sequence number in the table.
	LargestSeqNum uint64
}
