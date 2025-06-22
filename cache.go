// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"

	"github.com/khushmanvar/hyperfork/internal/base"
	"github.com/khushmanvar/hyperfork/internal/manifest"
	"github.com/khushmanvar/hyperfork/internal/sstable"
)

// Cache is a cache for sstable blocks.
type Cache struct {
	mu   sync.Mutex
	data map[uint64]interface{}
	size int
	max  int
}

// NewCache creates a new cache of the given size.
func NewCache(max int) *Cache {
	return &Cache{
		data: make(map[uint64]interface{}),
		max:  max,
	}
}

// Get gets the value for the given key.
func (c *Cache) Get(key uint64) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[key]
}

// Set sets the value for the given key.
func (c *Cache) Set(key uint64, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
}

type tableCache struct {
	cache *Cache
	db    *DB
}

func newTableCache(db *DB, size int) *tableCache {
	return &tableCache{
		cache: NewCache(size),
		db:    db,
	}
}

func (c *tableCache) newIter(
	f *manifest.FileMetadata,
) (base.InternalIterator, error) {
	// Check the cache.
	if r, ok := c.cache.Get(f.FileNum).(*sstable.Reader); ok {
		return r.NewIter(nil), nil
	}

	// Open the sstable.
	file, err := c.db.vfs.Open(c.db.dir + "/" + f.Filename())
	if err != nil {
		return nil, err
	}
	r, err := sstable.NewReader(file, *c.db.opts)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Cache the reader.
	c.cache.Set(f.FileNum, r)

	return r.NewIter(nil), nil
}

func (c *tableCache) evict(fileNum uint64) {
	if r, ok := c.cache.Get(fileNum).(*sstable.Reader); ok {
		r.Close()
		c.cache.Set(fileNum, nil)
	}
} 