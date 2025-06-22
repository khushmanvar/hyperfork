// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"encoding/binary"
	"fmt"
	"github.com/khushmanvar/hyperfork/internal/base"
)

const numLevels = 7

// VersionEdit represents a change to the database state.
type VersionEdit struct {
	// Comparator is the name of the comparer.
	Comparator string
	// LogNumber is the file number of the WAL.
	LogNumber uint64
	// PrevLogNumber is the file number of the previous WAL.
	PrevLogNumber uint64
	// NextFileNumber is the next file number to use.
	NextFileNumber uint64
	// LastSequence is the last sequence number.
	LastSequence uint64
	// NewFiles is a list of new files.
	NewFiles []NewFileEntry
	// DeletedFiles is a list of deleted files.
	DeletedFiles map[uint64]int
}

// NewFileEntry is an entry for a new file.
type NewFileEntry struct {
	// Level is the level of the new file.
	Level int
	// Meta is the metadata for the new file.
	Meta *FileMetadata
}

// NewFile creates a new file entry.
func (ve *VersionEdit) NewFile(fileNum uint64) *NewFileEntry {
	f := &NewFileEntry{
		Meta: &FileMetadata{
			FileNum: fileNum,
		},
	}
	ve.NewFiles = append(ve.NewFiles, *f)
	return f
}

// Filename returns the filename for a file.
func (f *FileMetadata) Filename() string {
	return fmt.Sprintf("%06d.sst", f.FileNum)
}

// Encode encodes a VersionEdit to a byte slice.
func (ve *VersionEdit) Encode(b []byte) []byte {
	b = encodeString(b, ve.Comparator)
	b = encodeUvarint(b, ve.LogNumber)
	b = encodeUvarint(b, ve.PrevLogNumber)
	b = encodeUvarint(b, ve.NextFileNumber)
	b = encodeUvarint(b, ve.LastSequence)
	for _, f := range ve.NewFiles {
		b = encodeUvarint(b, uint64(f.Level))
		b = encodeUvarint(b, f.Meta.FileNum)
		b = encodeUvarint(b, f.Meta.Size)
		b = encodeString(b, f.Meta.Smallest.UserKey)
		b = encodeString(b, f.Meta.Largest.UserKey)
	}
	for level, fileNum := range ve.DeletedFiles {
		b = encodeUvarint(b, uint64(level))
		b = encodeUvarint(b, fileNum)
	}
	return b
}

func encodeUvarint(b []byte, x uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], x)
	return append(b, buf[:n]...)
}

func encodeString(b []byte, s string) []byte {
	b = encodeUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// Decode decodes a VersionEdit from a byte slice.
func (ve *VersionEdit) Decode(b []byte) error {
	var ok bool
	b, ve.Comparator, ok = decodeString(b)
	if !ok {
		return base.ErrCorruption
	}
	b, ve.LogNumber, ok = decodeUvarint(b)
	if !ok {
		return base.ErrCorruption
	}
	b, ve.PrevLogNumber, ok = decodeUvarint(b)
	if !ok {
		return base.ErrCorruption
	}
	b, ve.NextFileNumber, ok = decodeUvarint(b)
	if !ok {
		return base.ErrCorruption
	}
	b, ve.LastSequence, ok = decodeUvarint(b)
	if !ok {
		return base.ErrCorruption
	}

	for len(b) > 0 {
		var level, fileNum, size uint64
		var smallest, largest []byte
		b, level, ok = decodeUvarint(b)
		if !ok {
			return base.ErrCorruption
		}
		if level < numLevels {
			b, fileNum, ok = decodeUvarint(b)
			if !ok {
				return base.ErrCorruption
			}
			b, size, ok = decodeUvarint(b)
			if !ok {
				return base.ErrCorruption
			}
			b, smallest, ok = decodeString(b)
			if !ok {
				return base.ErrCorruption
			}
			b, largest, ok = decodeString(b)
			if !ok {
				return base.ErrCorruption
			}
			ve.NewFiles = append(ve.NewFiles, NewFileEntry{
				Level: int(level),
				Meta: &FileMetadata{
					FileNum:  fileNum,
					Size:     size,
					Smallest: base.MakeInternalKey(smallest, 0, 0),
					Largest:  base.MakeInternalKey(largest, 0, 0),
				},
			})
		} else {
			b, fileNum, ok = decodeUvarint(b)
			if !ok {
				return base.ErrCorruption
			}
			if ve.DeletedFiles == nil {
				ve.DeletedFiles = make(map[uint64]int)
			}
			ve.DeletedFiles[fileNum] = int(level) - numLevels
		}
	}
	return nil
}

func decodeUvarint(b []byte) ([]byte, uint64, bool) {
	x, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, 0, false
	}
	return b[n:], x, true
}

func decodeString(b []byte) ([]byte, string, bool) {
	b, n, ok := decodeUvarint(b)
	if !ok {
		return nil, "", false
	}
	return b[n:], string(b[:n]), true
}
