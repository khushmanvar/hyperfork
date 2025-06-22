// writer.go

// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/khushmanvar/hyperfork"
	"github.com/khushmanvar/hyperfork/internal/base"
	"github.com/khushmanvar/hyperfork/vfs"
	"github.com/golang/snappy"
)

const (
	noCompression   = 0
	snappyCompression = 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Writer is an sstable writer.
type Writer struct {
	// w is the underlying writer.
	w io.Writer
	// buf is a buffered writer.
	buf *bufio.Writer
	// err is the first error that occurred.
	err error
	// seqNum is the sequence number of the keys being written.
	seqNum uint64
	// block is the current block being written.
	block blockWriter
	// index is the index block.
	index blockWriter
	// filter is the filter block.
	filter filterWriter
	// props are the table properties.
	props TableProperties
	// My own tracking
	smallest base.InternalKey
	largest  base.InternalKey
	closed   bool
	f        *bufio.Writer
	file     vfs.File
	opts     *pebble.Options
	comparer *base.Comparer
	offset   int64
}

// NewWriter creates a new sstable writer.
func NewWriter(f vfs.File, opts *pebble.Options, lo pebble.LevelOptions) *Writer {
	w := &Writer{
		file:     f,
		w:        f,
		buf:      bufio.NewWriter(f),
		opts:     opts,
		comparer: &opts.Comparer,
	}
	w.block.restartInterval = lo.BlockRestartInterval
	w.index.restartInterval = 1
	if lo.FilterPolicy != nil {
		w.filter.policy = lo.FilterPolicy
		w.filter.writer = &w.block
	}
	return w
}

// Add adds a key-value pair to the sstable.
func (w *Writer) Add(key base.InternalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.props.NumEntries == 0 {
		w.smallest = key
	}
	w.largest = key
	w.props.NumEntries++
	w.props.RawKeySize += uint64(len(key.UserKey))
	w.props.RawValueSize += uint64(len(value))

	if w.filter.policy != nil {
		w.filter.add(key.UserKey)
	}
	if w.block.empty() {
		w.index.add(key, nil)
	}
	w.block.add(key, value)
	if w.block.size() >= w.opts.BlockSize {
		w.finishBlock()
	}
	return nil
}

// Close finishes writing the sstable and closes the writer.
func (w *Writer) Close() (err error) {
	if w.err != nil {
		return w.err
	}
	defer func() {
		if w.err == nil {
			w.err = err
		}
		w.closed = true
	}()

	w.finishBlock()

	// Write the filter block.
	var filterBH BlockHandle
	if w.filter.policy != nil {
		var b []byte
		b = w.filter.finish(b)
		filterBH, w.err = w.writeRawBlock(b, w.opts.Compression)
		if w.err != nil {
			return w.err
		}
	}

	// Write the properties block.
	var propsBH BlockHandle
	var propsBytes []byte
	propsBytes = w.props.marshal(propsBytes)
	propsBH, w.err = w.writeRawBlock(propsBytes, pebble.NoCompression)
	if w.err != nil {
		return w.err
	}

	// Write the metaindex block.
	var metaindexBH BlockHandle
	metaindexWriter := &blockWriter{
		restartInterval: 1,
	}
	if w.filter.policy != nil {
		metaindexWriter.add(base.InternalKey{UserKey: []byte("filter." + w.filter.policy.Name())}, filterBH.Encode(nil))
	}
	metaindexWriter.add(base.InternalKey{UserKey: []byte("pebble.table.properties")}, propsBH.Encode(nil))
	metaindexBH, w.err = w.writeBlock(metaindexWriter)
	if w.err != nil {
		return w.err
	}

	// Write the index block.
	var indexBH BlockHandle
	indexBH, w.err = w.writeBlock(&w.index)
	if w.err != nil {
		return w.err
	}

	// Write the footer.
	var footer [FooterSize]byte
	n := metaindexBH.Encode(footer[:])
	n += indexBH.Encode(footer[n:])
	binary.LittleEndian.PutUint64(footer[FooterSize-8:], TableMagic)
	if _, err := w.buf.Write(footer[:]); err != nil {
		w.err = err
		return w.err
	}

	return w.buf.Flush()
}

// Smallest returns the smallest key in the sstable.
func (w *Writer) Smallest() base.InternalKey {
	return w.smallest
}

// Largest returns the largest key in the sstable.
func (w *Writer) Largest() base.InternalKey {
	return w.largest
}

func (w *Writer) writeRawBlock(b []byte, compression pebble.Compression) (BlockHandle, error) {
	blockType := byte(noCompression)
	if compression != pebble.NoCompression {
		blockType = snappyCompression
		b = snappy.Encode(nil, b)
	}

	// Calculate the checksum.
	h := crc32.New(crcTable)
	h.Write(b)
	h.Write([]byte{blockType})
	checksum := h.Sum32()

	// Write the block and the checksum.
	if _, err := w.buf.Write(b); err != nil {
		return BlockHandle{}, err
	}
	var trailer [blockTrailerLen]byte
	trailer[0] = blockType
	binary.LittleEndian.PutUint32(trailer[1:], checksum)
	if _, err := w.buf.Write(trailer[:]); err != nil {
		return BlockHandle{}, err
	}
	return BlockHandle{Offset: uint64(w.offset), Length: uint64(len(b) + blockTrailerLen)}, nil
}

func (w *Writer) writeBlock(b *blockWriter) (BlockHandle, error) {
	return w.writeRawBlock(b.finish(), w.opts.Compression)
}

func (w *Writer) finishBlock() {
	if w.block.empty() {
		return
	}
	bh, err := w.writeBlock(&w.block)
	if err != nil {
		w.err = err
		return
	}

	// Add the block handle to the index.
	var buf [2 * binary.MaxVarintLen64]byte
	n := bh.Encode(buf[:])
	w.index.add(base.InternalKey{UserKey: w.block.lastKey()}, buf[:n])

	w.block.reset()
}

// blockWriter writes a block.
type blockWriter struct {
	// restartInterval is the number of keys between restarts.
	restartInterval int
	// blockSize is the size of the block.
	blockSize int
	// buf is the buffer for the block.
	buf []byte
	// restarts is the list of restart points.
	restarts []uint32
	// nEntries is the number of entries in the block.
	nEntries int
	// lastKey is the last key in the block.
	curKey  base.InternalKey
	prevKey base.InternalKey
}

func (w *blockWriter) add(key base.InternalKey, value []byte) {
	if w.nEntries%w.restartInterval == 0 {
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	}

	var shared int
	if w.nEntries > 0 {
		shared = base.SharedPrefixLen(w.prevKey.UserKey, key.UserKey)
	}

	w.curKey.UserKey = append(w.curKey.UserKey[:0], key.UserKey...)
	w.prevKey, w.curKey = w.curKey, w.prevKey

	// <shared><unshared><value_len>
	var varBuf [3 * binary.MaxVarintLen32]byte
	n := binary.PutUvarint(varBuf[:], uint64(shared))
	n += binary.PutUvarint(varBuf[n:], uint64(len(key.UserKey)-shared))
	n += binary.PutUvarint(varBuf[n:], uint64(len(value)))
	w.buf = append(w.buf, varBuf[:n]...)
	w.buf = append(w.buf, key.UserKey[shared:]...)
	w.buf = append(w.buf, value...)
	w.nEntries++
}

func (w *blockWriter) size() int {
	return len(w.buf)
}

func (w *blockWriter) empty() bool {
	return len(w.buf) == 0
}

func (w *blockWriter) reset() {
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	w.nEntries = 0
	w.curKey.UserKey = w.curKey.UserKey[:0]
	w.prevKey.UserKey = w.prevKey.UserKey[:0]
}

func (w *blockWriter) lastKey() base.InternalKey {
	return w.curKey
}

// filterWriter writes a filter block.
type filterWriter struct {
	policy base.FilterPolicy
	writer *blockWriter
	keys   [][]byte
}

func (w *filterWriter) add(key []byte) {
	w.keys = append(w.keys, key)
}

func (w *filterWriter) finish(buf []byte) []byte {
	// Generate the filter data.
	filter := w.policy.CreateFilter(w.keys)

	// Append the filter data to the buffer.
	buf = append(buf, filter...)
	// Append the offset of the filter data.
	var trailer [5]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(len(filter)))
	// Append the filter policy index.
	trailer[4] = 0
	buf = append(buf, trailer[:]...)
	return buf
}

// TableProperties are the properties of an sstable.
type TableProperties struct {
	// NumEntries is the number of entries in the table.
	NumEntries uint64
	// RawKeySize is the total size of the keys.
	RawKeySize uint64
	// RawValueSize is the total size of the values.
	RawValueSize uint64
}

func (p *TableProperties) marshal(b []byte) []byte {
	b = append(b, make([]byte, 4*binary.MaxVarintLen64)...)
	n := binary.PutUvarint(b[len(b):], p.NumEntries)
	n += binary.PutUvarint(b[len(b)+n:], p.RawKeySize)
	n += binary.PutUvarint(b[len(b)+n:], p.RawValueSize)
	return b[:len(b)+n]
}

func (p *TableProperties) unmarshal(b []byte) error {
	var n int
	p.NumEntries, n = binary.Uvarint(b)
	b = b[n:]
	p.RawKeySize, n = binary.Uvarint(b)
	b = b[n:]
	p.RawValueSize, _ = binary.Uvarint(b)
	return nil
}

func (w *blockWriter) finish() []byte {
	// Augment the trailer with the restart points.
	for i := range w.restarts {
		binary.LittleEndian.PutUint32(w.buf[len(w.buf):cap(w.buf)], w.restarts[i])
		w.buf = w.buf[:len(w.buf)+4]
	}
	binary.LittleEndian.PutUint32(w.buf[len(w.buf):cap(w.buf)], uint32(len(w.restarts)))
	w.buf = w.buf[:len(w.buf)+4]
	return w.buf
}
