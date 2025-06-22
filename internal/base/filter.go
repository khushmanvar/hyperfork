// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// FilterPolicy is an interface for creating and checking filters.
type FilterPolicy interface {
	// Name returns the name of the filter policy.
	Name() string

	// AppendFilter appends a filter for the given keys to dst and returns the
	// updated slice.
	AppendFilter(dst []byte, keys [][]byte) []byte

	// MayContain returns true if the filter may contain the key.
	MayContain(filter, key []byte) bool
} 