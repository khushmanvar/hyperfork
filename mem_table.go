// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/khushmanvar/hyperfork/internal/arenaskl"
	"github.com/khushmanvar/hyperfork/internal/base"
)

// memTable is an in-memory table of key-value pairs.
type memTable struct {
	// Arena-backed skiplist.
	skl    *arenaskl.Skiplist
	// The size of the memtable in bytes.
	size   uint32
	// The comparer to use.
	cmp    base.Comparer
	// The options for the memtable.
	opts   *Options
}

func (m *memTable) get(key base.InternalKey) ([]byte, error) {
	it := m.skl.NewIter(nil, nil)
	it.SeekGE(key.Encode(nil))
	if !it.Valid() {
		return nil, base.ErrNotFound
	}
	ik, ok := base.ParseInternalKey(it.Key())
	if !ok {
		return nil, base.ErrCorruption
	}
	if m.cmp.Compare(ik.UserKey, key.UserKey) == 0 {
		if ik.Kind() == base.InternalKeyKindDelete {
			return nil, base.ErrNotFound
		}
		return it.Value(), nil
	}
	return nil, base.ErrNotFound
}

// newMemTable creates a new memtable.
func newMemTable(o *Options) *memTable {
	return &memTable{
		skl:  arenaskl.NewSkiplist(o.Comparer.Compare),
		cmp:  o.Comparer,
		opts: o,
	}
}

// prepare handles the prepare phase of a two-phase commit.
func (m *memTable) prepare(b *Batch) error {
	return nil
}

// apply handles the apply phase of a two-phase commit.
func (m *memTable) apply(b *Batch) error {
	var seq uint64
	if b.db != nil {
		seq = b.db.logSeqNum
	}
	for i := b.iter(); ; {
		kind, ukey, value, ok := i.Next()
		if !ok {
			break
		}
		ikey := base.MakeInternalKey(ukey, seq, kind)
		if err := m.skl.Add(ikey.Encode(nil), value); err != nil {
			return err
		}
		m.size += uint32(len(ikey.UserKey) + 8 + len(value))
		seq++
	}
	return nil
}

// newIter returns an iterator for the memtable.
func (m *memTable) newIter(o *IterOptions) base.InternalIterator {
	return m.skl.NewIter(o.LowerBound, o.UpperBound)
}

// empty returns true if the memtable is empty.
func (m *memTable) empty() bool {
	return m.size == 0
} 