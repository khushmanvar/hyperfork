// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "bytes"

// Comparer defines a total ordering over a set of keys.
type Comparer struct {
	// Compare returns -1, 0, or +1 depending on whether a is 'less than',
	// 'equal to' or 'greater than' b.
	Compare func(a, b []byte) int

	// Equal returns true if a and b are equivalent.
	Equal func(a, b []byte) bool

	// Name is the name of the comparer.
	Name string

	// Separator is a key that is greater than any key in a but less than any
	// key in b, for any a < b.
	Separator func(dst, a, b []byte) []byte

	// Successor is a key that is greater than key.
	Successor func(dst, key []byte) []byte
}

// DefaultComparer is the default comparer. It uses bytes.Compare to compare
// keys.
var DefaultComparer = &Comparer{
	Compare: bytes.Compare,
	Equal:   bytes.Equal,
	Name:    "leveldb.BytewiseComparator",

	Separator: func(dst, a, b []byte) []byte {
		i, n := 0, len(a)
		if n > len(b) {
			n = len(b)
		}
		for ; i < n && a[i] == b[i]; i++ {
		}
		if i >= n {
			// Do not shorten if one key is a prefix of the other.
		} else if c := a[i]; c < 0xff && c+1 < b[i] {
			dst = append(dst, a[:i+1]...)
			dst[len(dst)-1]++
			return dst
		}
		return append(dst, a...)
	},

	Successor: func(dst, key []byte) []byte {
		for i, c := range key {
			if c != 0xff {
				dst = append(dst, key[:i+1]...)
				dst[len(dst)-1]++
				return dst
			}
		}
		return append(dst, key...)
	},
}
