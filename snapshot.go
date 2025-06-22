// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "io"

// Snapshot provides a consistent view of the database at a particular
// sequence number.
type Snapshot struct {
	// The sequence number of the snapshot.
	seqNum uint64
	db     *DB
}

var _ Reader = (*Snapshot)(nil)
var _ io.Closer = (*Snapshot)(nil)

// Get gets the value for the given key. It returns ErrNotFound if the DB
// does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (s *Snapshot) Get(key []byte) (value []byte, closer io.Closer, err error) {
	return s.db.get(key, s.seqNum)
}

// NewIter returns an iterator that is unpositioned (or positioned relative
// to an optional bound). The iterator must be closed after use.
func (s *Snapshot) NewIter(o *IterOptions) *Iterator {
	if o == nil {
		o = &IterOptions{}
	}
	return s.db.NewIter(&IterOptions{
		LowerBound: o.LowerBound,
		UpperBound: o.UpperBound,
		seq:        s.seqNum,
	})
}

// Close closes the snapshot.
func (s *Snapshot) Close() error {
	return nil
}
