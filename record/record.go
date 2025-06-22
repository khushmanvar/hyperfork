// record.go

// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"encoding/binary"
	"errors"
	"io"
	"hash/crc32"
)

const (
	// BlockSize is the size of a block.
	BlockSize = 32 * 1024
	// HeaderSize is the size of a record header.
	HeaderSize = 7
)

// Record types.
const (
	// Full is a record that contains all of the data for a key-value pair.
	Full uint8 = 1
	// First is a record that contains the first chunk of a key-value pair.
	First uint8 = 2
	// Middle is a record that contains a middle chunk of a key-value pair.
	Middle uint8 = 3
	// Last is a record that contains the last chunk of a key-value pair.
	Last uint8 = 4
)

var (
	// ErrNotAnIOSeeker is returned if the underlying io.Reader does not implement
	// io.Seeker.
	ErrNotAnIOSeeker = errors.New("leveldb/record: reader does not implement io.Seeker")
	// ErrCorrupt is returned if the log is corrupt.
	ErrCorrupt = errors.New("leveldb/record: corrupt log")
)

// Reader reads records from a log file.
type Reader struct {
	// r is the underlying reader.
	r io.Reader
	// seq is the sequence number of the current record.
	seq int64
	// buf is the buffer for reading records.
	buf [BlockSize]byte
	// last is true if the last read returned a full block.
	last bool
	// err is the last error that occurred.
	err error
}

// NewReader creates a new reader.
func NewReader(r io.Reader, seq int64) *Reader {
	return &Reader{
		r:   r,
		seq: seq,
	}
}

// Next returns the next record.
func (r *Reader) Next() (io.Reader, error) {
	if r.err != nil {
		return nil, r.err
	}

	for {
		// Read the header.
		_, r.err = io.ReadFull(r.r, r.buf[:HeaderSize])
		if r.err != nil {
			return nil, r.err
		}

		// Decode the header.
		checksum := binary.LittleEndian.Uint32(r.buf[:4])
		length := binary.LittleEndian.Uint16(r.buf[4:6])
		kind := r.buf[6]

		// Read the payload.
		_, r.err = io.ReadFull(r.r, r.buf[:length])
		if r.err != nil {
			return nil, r.err
		}

		// Check the checksum.
		if crc32.ChecksumIEEE(r.buf[:length]) != checksum {
			r.err = ErrCorrupt
			return nil, r.err
		}

		switch kind {
		case Full:
			return &memReader{r.buf[:length]}, nil
		case First:
			var b []byte
			b = append(b, r.buf[:length]...)
			for {
				_, r.err = io.ReadFull(r.r, r.buf[:HeaderSize])
				if r.err != nil {
					return nil, r.err
				}
				checksum = binary.LittleEndian.Uint32(r.buf[:4])
				length = binary.LittleEndian.Uint16(r.buf[4:6])
				kind = r.buf[6]
				_, r.err = io.ReadFull(r.r, r.buf[:length])
				if r.err != nil {
					return nil, r.err
				}
				if crc32.ChecksumIEEE(r.buf[:length]) != checksum {
					r.err = ErrCorrupt
					return nil, r.err
				}
				b = append(b, r.buf[:length]...)
				if kind == Last {
					return &memReader{b}, nil
				}
			}
		default:
			r.err = ErrCorrupt
			return nil, r.err
		}
	}
}

type memReader struct {
	buf []byte
}

func (m *memReader) Read(p []byte) (int, error) {
	if len(m.buf) == 0 {
		return 0, io.EOF
	}
	n := copy(p, m.buf)
	m.buf = m.buf[n:]
	return n, nil
}
