// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package arenaskl

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

// Skiplist is a skiplist implementation.
type Skiplist struct {
	head   *Node
	height uint32
	arena  *Arena
	cmp    Compare
}

// Node is a node in the skiplist.
type Node struct {
	// key is the key.
	key []byte
	// value is the value.
	value []byte
	// next is the next node at each level.
	next [1]unsafe.Pointer
}

// Compare is a comparator for skiplist keys.
type Compare func(a, b []byte) int

// Arena is a memory arena for allocating nodes.
type Arena struct {
	buf []byte
	off int
}

// NewArena creates a new memory arena.
func NewArena(n int) *Arena {
	return &Arena{
		buf: make([]byte, n),
	}
}

// Alloc allocates a block of memory from the arena.
func (a *Arena) Alloc(size, align int) int {
	// Align the offset.
	a.off = (a.off + align - 1) &^ (align - 1)
	offset := a.off
	a.off += size
	if a.off > len(a.buf) {
		panic("out of memory")
	}
	return offset
}

// NewSkiplist creates a new skiplist.
func NewSkiplist(arena *Arena, cmp Compare) *Skiplist {
	head := &Node{}
	s := &Skiplist{
		head:   head,
		height: 1,
		arena:  arena,
		cmp:    cmp,
	}
	return s
}

// Add adds a key-value pair to the skiplist.
func (s *Skiplist) Add(key, value []byte) {
	var prev [maxHeight]unsafe.Pointer
	var next [maxHeight]unsafe.Pointer
	var found bool

	for h := int(atomic.LoadUint32(&s.height)) - 1; h >= 0; h-- {
		prev[h], next[h], found = s.findSplice(key, h)
		if found {
			// Key already exists. Update the value.
			// Note: This is not thread-safe.
			n := (*Node)(next[h])
			n.value = value
			return
		}
	}

	// Create a new node.
	height := s.randomHeight()
	if height > int(atomic.LoadUint32(&s.height)) {
		atomic.StoreUint32(&s.height, uint32(height))
	}
	n := s.newNode(key, value, height)

	// Insert the new node into the skiplist.
	for h := 0; h < height; h++ {
		if prev[h] == nil {
			prev[h] = unsafe.Pointer(s.head)
		}
		n.next[h] = next[h]
		atomic.StorePointer(&((*Node)(prev[h])).next[h], unsafe.Pointer(n))
	}
}

const maxHeight = 12

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Intn(4) == 0 {
		h++
	}
	return h
}

func (s *Skiplist) newNode(key, value []byte, height int) *Node {
	offset := s.arena.Alloc(int(unsafe.Sizeof(Node{})-unsafe.Sizeof(unsafe.Pointer(nil)))+height*int(unsafe.Sizeof(unsafe.Pointer(nil))), int(unsafe.Alignof(Node{})))
	n := (*Node)(unsafe.Pointer(&s.arena.buf[offset]))
	n.key = key
	n.value = value
	return n
}

func (s *Skiplist) findSplice(key []byte, h int) (prev, next unsafe.Pointer, found bool) {
	prev = unsafe.Pointer(s.head)
	for {
		next = atomic.LoadPointer(&((*Node)(prev)).next[h])
		if next == nil {
			return prev, next, false
		}
		n := (*Node)(next)
		cmp := s.cmp(n.key, key)
		if cmp >= 0 {
			return prev, next, cmp == 0
		}
		prev = next
	}
}
