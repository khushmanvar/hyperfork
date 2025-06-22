// reader.go

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/khushmanvar/hyperfork"
	"github.com/khushmanvar/hyperfork/internal/base"
	"github.com/khushmanvar/hyperfork/vfs"
	"github.com/khushmanvar/hyperfork/internal/crc"
	"github.com/golang/snappy"
)

const (
	noCompression   = 0
	snappyCompression = 1
)

// Reader is a reader for an sstable.
type Reader struct {
	// file is the underlying file.
	file vfs.File
	// cache is the cache for the sstable.
	cache *pebble.Cache
	// err is the first error that occurred.
	err error
	// index is the index block.
	index block
	// filter is the filter block.
	filter filterReader
	// props are the table properties.
	props TableProperties
	// opts are the table options.
	opts *pebble.Options
	// cmp is the comparator for the table.
	cmp base.Compare
}

// NewReader creates a new reader.
func NewReader(f vfs.File, o pebble.Options) (*Reader, error) {
	r := &Reader{
		file:  f,
		cache: o.Cache,
		opts:  &o,
		cmp:   o.Comparer.Compare,
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	metaindexBH, indexBH, err := ReadFooter(f, stat.Size())
	if err != nil {
		return nil, err
	}

	metaindex, err := r.readBlock(metaindexBH)
	if err != nil {
		return nil, err
	}
	metaIter, err := newBlockIter(r.cmp, &metaindex)
	if err != nil {
		return nil, err
	}

	if metaIter.SeekGE([]byte("filter.")) {
		filterBH, err := decodeBlockHandle(metaIter.Value())
		if err != nil {
			return nil, err
		}
		filterBlock, err := r.readBlock(filterBH)
		if err != nil {
			return nil, err
		}
		r.filter = newFilterReader(filterBlock, o.FilterPolicy)
	}

	if metaIter.SeekGE([]byte("pebble.table.properties")) {
		propsBH, err := decodeBlockHandle(metaIter.Value())
		if err != nil {
			return nil, err
		}
		propsBlock, err := r.readBlock(propsBH)
		if err != nil {
			return nil, err
		}
		if err := r.props.unmarshal(propsBlock.data); err != nil {
			return nil, err
		}
	}

	r.index, err = r.readBlock(indexBH)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// readBlock reads a block from the sstable.
func (r *Reader) readBlock(bh BlockHandle) (block, error) {
	b := make([]byte, bh.Length+blockTrailerLen)
	if _, err := r.file.ReadAt(b, int64(bh.Offset)); err != nil {
		return block{}, err
	}
	checksum := binary.LittleEndian.Uint32(b[bh.Length : bh.Length+4])
	if crc.New(b[:bh.Length+1]).Value() != checksum {
		return block{}, base.ErrCorruption
	}
	switch b[bh.Length] {
	case noCompression:
		return newBlock(b[:bh.Length])
	case snappyCompression:
		decoded, err := snappy.Decode(nil, b[:bh.Length])
		if err != nil {
			return block{}, err
		}
		return newBlock(decoded)
	default:
		return block{}, base.ErrCorruption
	}
}

// NewIter returns an iterator for the sstable.
func (r *Reader) NewIter(o *pebble.IterOptions) *pebble.Iterator {
	return &iterator{
		r:     r,
		lower: o.LowerBound,
		upper: o.UpperBound,
	}
}

// Close closes the reader.
func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	return r.file.Close()
}

// block is a reader for a block.
type block struct {
	// data is the block data.
	data []byte
	// restarts is the list of restart points.
	restarts []uint32
	// nEntries is the number of entries in the block.
	nEntries int
}

type iterator struct {
	r     *Reader
	index blockIter
	data  blockIter
	lower []byte
	upper []byte
	err   error
}

type blockIter struct {
	// block is the block being iterated over.
	block *block
	// offset is the offset of the current entry in the block.
	offset int
	// key is the key of the current entry.
	key []byte
	// value is the value of the current entry.
	value []byte
	// err is the first error that occurred.
	err error
	// restarts is the list of restart points.
	restarts []uint32
	cmp      base.Compare
}

var _ pebble.IteratorInterface = (*iterator)(nil)

func (i *iterator) SeekGE(key []byte) *pebble.Iterator {
	dataBlock, err := i.findBlock(key)
	if err != nil {
		i.err = err
		return i
	}
	i.data, err = newBlockIter(i.r.cmp, &dataBlock)
	if err != nil {
		i.err = err
		return i
	}
	i.data.SeekGE(key)
	return i
}

func (i *iterator) SeekLT(key []byte) *pebble.Iterator {
	dataBlock, err := i.findBlock(key)
	if err != nil {
		i.err = err
		return i
	}
	i.data, err = newBlockIter(i.r.cmp, &dataBlock)
	if err != nil {
		i.err = err
		return i
	}
	i.data.SeekLT(key)
	return i
}

func (i *iterator) First() *pebble.Iterator {
	i.index.First()
	if !i.index.Valid() {
		return i
	}
	bh, err := decodeBlockHandle(i.index.Value())
	if err != nil {
		i.err = err
		return i
	}
	dataBlock, err := i.r.readBlock(bh)
	if err != nil {
		i.err = err
		return i
	}
	i.data, err = newBlockIter(i.r.cmp, &dataBlock)
	if err != nil {
		i.err = err
		return i
	}
	i.data.First()
	return i
}

func (i *iterator) Last() *pebble.Iterator {
	i.index.Last()
	if !i.index.Valid() {
		return i
	}
	bh, err := decodeBlockHandle(i.index.Value())
	if err != nil {
		i.err = err
		return i
	}
	dataBlock, err := i.r.readBlock(bh)
	if err != nil {
		i.err = err
		return i
	}
	i.data, err = newBlockIter(i.r.cmp, &dataBlock)
	if err != nil {
		i.err = err
		return i
	}
	i.data.Last()
	return i
}

func (i *iterator) Next() *pebble.Iterator {
	if i.data.Next() {
		return i
	}
	if i.data.err != nil {
		i.err = i.data.err
		return i
	}

	// Current data block is exhausted. Move to the next one.
	if !i.index.Next() {
		// No more data blocks.
		i.err = i.index.Error()
		return i
	}

	// Load the next data block.
	blockHandle, err := decodeBlockHandle(i.index.Value())
	if err != nil {
		i.err = err
		return i
	}
	dataBlock, err := i.r.readBlock(blockHandle)
	if err != nil {
		i.err = err
		return i
	}
	i.data, err = newBlockIter(i.r.cmp, &dataBlock)
	if err != nil {
		i.err = err
		return i
	}
	i.data.First()
	return i
}

func (i *iterator) Prev() *pebble.Iterator {
	if i.data.Prev() {
		return i
	}
	if i.data.err != nil {
		i.err = i.data.err
		return i
	}

	// Current data block is exhausted. Move to the previous one.
	if !i.index.Prev() {
		// No more data blocks.
		i.err = i.index.Error()
		return i
	}

	// Load the previous data block.
	blockHandle, err := decodeBlockHandle(i.index.Value())
	if err != nil {
		i.err = err
		return i
	}
	dataBlock, err := i.r.readBlock(blockHandle)
	if err != nil {
		i.err = err
		return i
	}
	i.data, err = newBlockIter(i.r.cmp, &dataBlock)
	if err != nil {
		i.err = err
		return i
	}
	i.data.Last()
	return i
}

func (i *iterator) Key() []byte {
	return i.data.key
}

func (i *iterator) Value() []byte {
	return i.data.value
}

func (i *iterator) Valid() bool {
	return i.data.key != nil
}

func (i *iterator) Error() error {
	if i.err != nil {
		return i.err
	}
	return i.data.err
}

func (i *iterator) Close() error {
	if i.data != nil {
		i.data.Close()
	}
	return nil
}

func (i *iterator) findBlock(key []byte) (block, error) {
	indexIter, err := newBlockIter(i.r.cmp, &i.r.index)
	if err != nil {
		return block{}, err
	}
	indexIter.SeekGE(key)
	if !indexIter.Valid() {
		return block{}, indexIter.Error()
	}
	blockHandle, err := decodeBlockHandle(indexIter.Value())
	if err != nil {
		return block{}, err
	}
	return i.r.readBlock(blockHandle)
}

func newBlockIter(cmp base.Compare, b *block) (*blockIter, error) {
	i := &blockIter{
		block: b,
		cmp:   cmp,
	}
	numRestarts := binary.LittleEndian.Uint32(i.block.data[len(i.block.data)-4:])
	i.restarts = make([]uint32, numRestarts)
	for j := range i.restarts {
		i.restarts[j] = binary.LittleEndian.Uint32(i.block.data[len(i.block.data)-4-4*int(numRestarts)+4*j:])
	}
	i.data = i.block.data[:len(i.block.data)-4-4*int(numRestarts)]
	return i, nil
}

func (i *blockIter) SeekGE(key []byte) {
	i.offset = 0
	i.key = i.key[:0]
	i.value = i.value[:0]

	// Find the first restart point >= key.
	j := sort.Search(len(i.restarts), func(j int) bool {
		offset := int(i.restarts[j])
		_, k, _, _ := i.decodeEntry(offset)
		return i.cmp(k, key) >= 0
	})
	if j > 0 {
		j--
	}
	i.offset = int(i.restarts[j])

	// Now scan from that restart point to find the actual key.
	for i.Next() {
		if i.cmp(i.key, key) >= 0 {
			return
		}
	}
}

func (i *blockIter) SeekLT(key []byte) {
	i.offset = 0
	i.key = i.key[:0]
	i.value = i.value[:0]

	// Find the first restart point > key.
	j := sort.Search(len(i.restarts), func(j int) bool {
		offset := int(i.restarts[j])
		_, k, _, _ := i.decodeEntry(offset)
		return i.cmp(k, key) > 0
	})
	if j == 0 {
		return
	}
	// The target key is in the restart interval `i.restarts[j-1]`.
	i.offset = int(i.restarts[j-1])

	// Now scan from that restart point to find the actual key.
	for i.Next() {
		if i.cmp(i.key, key) >= 0 {
			i.Prev()
			return
		}
	}
}

func (i *blockIter) First() {
	i.offset = 0
	i.key = i.key[:0]
	i.value = i.value[:0]
	i.Next()
}

func (i *blockIter) Last() {
	i.offset = int(i.restarts[len(i.restarts)-1])
	i.key = i.key[:0]
	i.value = i.value[:0]
	i.Next()

	for {
		offset, _, _, err := i.decodeEntry(i.offset)
		if err != nil || offset >= len(i.data) {
			break
		}
		if !i.Next() {
			break
		}
	}
}

func (i *blockIter) Next() bool {
	if i.offset >= len(i.data) {
		i.key = nil
		i.value = nil
		return false
	}
	offset, key, value, err := i.decodeEntry(i.offset)
	if err != nil {
		i.err = err
		return false
	}
	i.offset = offset
	i.key = key
	i.value = value
	return true
}

func (i *blockIter) Prev() bool {
	if i.offset == 0 {
		i.key = nil
		i.value = nil
		return false
	}

	// Find the restart point that contains the previous entry.
	j := sort.Search(len(i.restarts), func(j int) bool {
		return i.offset <= int(i.restarts[j])
	})
	if j > 0 {
		j--
	}

	// Now scan from that restart point to find the previous entry.
	prevOffset := -1
	for offset := int(i.restarts[j]); offset < i.offset; {
		prevOffset = offset
		offset, _, _, _ = i.decodeEntry(offset)
	}
	i.offset = prevOffset
	return i.Next()
}

func (i *blockIter) decodeEntry(offset int) (nextOffset int, key, value []byte, err error) {
	var shared, unshared, valueLen uint64
	var n int
	shared, n = binary.Uvarint(i.data[offset:])
	offset += n
	unshared, n = binary.Uvarint(i.data[offset:])
	offset += n
	valueLen, n = binary.Uvarint(i.data[offset:])
	offset += n

	key = append(i.key[:shared], i.data[offset:offset+int(unshared)]...)
	offset += int(unshared)
	value = i.data[offset : offset+int(valueLen)]
	nextOffset = offset + int(valueLen)
	return nextOffset, key, value, nil
}

func decodeBlockHandle(data []byte) (BlockHandle, error) {
	var bh BlockHandle
	var n int
	bh.Offset, n = binary.Uvarint(data)
	bh.Length, _ = binary.Uvarint(data[n:])
	return bh, nil
}

type filterReader struct {
	data   []byte
	policy base.FilterPolicy
}

func newFilterReader(b block, policy base.FilterPolicy) filterReader {
	return filterReader{
		data:   b.data,
		policy: policy,
	}
}

func (r filterReader) mayContain(key []byte) bool {
	if r.policy == nil {
		return true
	}
	return r.policy.MayContain(r.data, key)
}

