// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/khushmanvar/hyperfork/internal/base"
)

// TableMagic is the magic number for sstable files.
const TableMagic = 0x574f4c46424c50db

// FooterSize is the size of the sstable footer.
const FooterSize = 48

// blockTrailerLen is the length of the block trailer.
const blockTrailerLen = 5

var (
	// ErrNotAnSSTable is returned if the file is not an sstable.
	ErrNotAnSSTable = errors.New("pebble/sstable: not an sstable")
)

// BlockHandle is the location of a block.
type BlockHandle struct {
	Offset uint64
	Length uint64
}

// Encode encodes a BlockHandle.
func (h BlockHandle) Encode(buf []byte) int {
	n := binary.PutUvarint(buf, h.Offset)
	n += binary.PutUvarint(buf[n:], h.Length)
	return n
}

// Decode decodes a BlockHandle.
func (h *BlockHandle) Decode(buf []byte) int {
	var n1, n2 int
	h.Offset, n1 = binary.Uvarint(buf)
	h.Length, n2 = binary.Uvarint(buf[n1:])
	return n1 + n2
}

// ReadFooter reads the footer from the given reader.
func ReadFooter(
	r interface {
		io.ReaderAt
		Size() int64
	},
) (metaindexBH, indexBH BlockHandle, err error) {
	var footer [FooterSize]byte
	size := r.Size()
	if size < FooterSize {
		return BlockHandle{}, BlockHandle{}, ErrNotAnSSTable
	}
	_, err = r.ReadAt(footer[:], size-FooterSize)
	if err != nil {
		return BlockHandle{}, BlockHandle{}, err
	}
	if magic := binary.LittleEndian.Uint64(footer[FooterSize-8:]); magic != TableMagic {
		return BlockHandle{}, BlockHandle{}, ErrNotAnSSTable
	}

	var n int
	n += metaindexBH.Decode(footer[0:])
	indexBH.Decode(footer[n:])
	return metaindexBH, indexBH, nil
}
