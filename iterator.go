// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"io"

	"github.com/khushmanvar/hyperfork/internal/base"
)

// IterOptions contains options for creating an iterator.
type IterOptions struct {
	// LowerBound specifies the smallest key that the iterator will return. If the
	// iterator is seeked to a key smaller than LowerBound, it will be positioned
	// at the first key >= LowerBound. The default value is nil.
	LowerBound []byte
	// UpperBound specifies the largest key that the iterator will return. If the
	// iterator is seeked to a key >= UpperBound, it will be positioned at the
	// first key < UpperBound. The default value is nil.
	UpperBound []byte
}

// IteratorInterface is an interface for iterating over a key-value store.
type IteratorInterface interface {
	// SeekGE moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key.
	SeekGE(key []byte) *Iterator
	// SeekLT moves the iterator to the last key/value pair whose key is less than
	// the given key.
	SeekLT(key []byte) *Iterator
	// First moves the iterator to the first key/value pair.
	First() *Iterator
	// Last moves the iterator to the last key/value pair.
	Last() *Iterator
	// Next moves the iterator to the next key/value pair.
	Next() *Iterator
	// Prev moves the iterator to the previous key/value pair.
	Prev() *Iterator
	// Key returns the key of the current key/value pair, or nil if done.
	Key() []byte
	// Value returns the value of the current key/value pair, or nil if done.
	Value() []byte
	// Valid returns true if the iterator is positioned at a valid key/value pair.
	Valid() bool
	// Error returns any accumulated error.
	Error() error
	// Close closes the iterator and returns any accumulated error.
	io.Closer
}

// Iterator iterates over a DB's key/value pairs in key order.
//
// An iterator must be closed after use, but it is not necessary to read an
// iterator until exhaustion.
//
// An iterator is not necessarily goroutine-safe, but it is safe to use
// multiple iterators concurrently, with each in its own goroutine.
type Iterator struct {
	iter base.InternalIterator
}

// SeekGE moves the iterator to the first key/value pair whose key is greater
// than or equal to the given key.
func (i *Iterator) SeekGE(key []byte) {
	i.iter.SeekGE(key)
}

// SeekLT moves the iterator to the last key/value pair whose key is less than
// the given key.
func (i *Iterator) SeekLT(key []byte) {
	i.iter.SeekLT(key)
}

// First moves the iterator to the first key/value pair.
func (i *Iterator) First() {
	i.iter.First()
}

// Last moves the iterator to the last key/value pair.
func (i *Iterator) Last() {
	i.iter.Last()
}

// Next moves the iterator to the next key/value pair.
func (i *Iterator) Next() {
	i.iter.Next()
}

// Prev moves the iterator to the previous key/value pair.
func (i *Iterator) Prev() {
	i.iter.Prev()
}

// Valid returns true if the iterator is positioned at a valid key/value pair.
func (i *Iterator) Valid() bool {
	return i.iter.Valid()
}

// Key returns the key of the current key/value pair.
func (i *Iterator) Key() []byte {
	return i.iter.Key().UserKey
}

// Value returns the value of the current key/value pair.
func (i *Iterator) Value() []byte {
	return i.iter.Value()
}

// Close closes the iterator and returns any accumulated error.
func (i *Iterator) Close() error {
	return i.iter.Close()
}
