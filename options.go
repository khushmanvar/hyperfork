// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"log"
	"runtime"

	"github.com/khushmanvar/hyperfork/internal/base"
	"github.com/khushmanvar/hyperfork/vfs"
)

// Options provide a way to control the behavior of a DB.
type Options struct {
	// Cache is used to cache data. If nil, a new cache will be created.
	Cache *Cache
	// Comparer is used to compare keys, arrange keys in order, and also to
	// generate comparators for use in testing.
	Comparer *Comparer
	// EventListener provides hooks for listening to significant DB events.
	EventListener EventListener
	// FS provides an interface to the filesystem.
	FS vfs.FS
	// Logger is a logger.
	Logger Logger
	// MaxOpenFiles is a soft limit on the number of open files.
	MaxOpenFiles int
	// MemTableSize is the size of the memtable.
	MemTableSize int
	// MemTableStopWritesThreshold is the number of memtables at which writes are
	// stopped.
	MemTableStopWritesThreshold int
	// L0CompactionThreshold is the number of L0 files that triggers a
	// compaction.
	L0CompactionThreshold int
	// L0StopWritesThreshold is the number of L0 files that triggers a write
	// stall.
	L0StopWritesThreshold int
	// LBaseMaxBytes is the max total size of LBase files.
	LBaseMaxBytes int64
	// Levels is the configuration for each level of the LSM.
	Levels [7]LevelOptions
}

// EnsureDefaults finalizes the options, setting default values for any
// unspecified options.
func (o *Options) EnsureDefaults() {
	if o.Cache == nil {
		o.Cache = NewCache(8 << 20) // 8 MB
	}
	if o.Comparer == nil {
		o.Comparer = base.DefaultComparer
	}
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.Logger == nil {
		o.Logger = &DefaultLogger{}
	}
	if o.MaxOpenFiles == 0 {
		o.MaxOpenFiles = 1000
	}
	if o.MemTableSize == 0 {
		o.MemTableSize = 4 << 20 // 4 MB
	}
	if o.MemTableStopWritesThreshold == 0 {
		o.MemTableStopWritesThreshold = 2
	}
	if o.L0CompactionThreshold == 0 {
		o.L0CompactionThreshold = 2
	}
	if o.L0StopWritesThreshold == 0 {
		o.L0StopWritesThreshold = 8
	}
	if o.LBaseMaxBytes == 0 {
		o.LBaseMaxBytes = 64 << 20 // 64 MB
	}

	for i := range o.Levels {
		if o.Levels[i].BlockRestartInterval == 0 {
			o.Levels[i].BlockRestartInterval = 16
		}
		if o.Levels[i].BlockSize == 0 {
			o.Levels[i].BlockSize = 4096
		}
		if o.Levels[i].IndexBlockSize == 0 {
			o.Levels[i].IndexBlockSize = 256 << 10
		}
		if o.Levels[i].FilterPolicy == nil {
			o.Levels[i].FilterPolicy = nil
		}
		if o.Levels[i].FilterType == 0 {
			o.Levels[i].FilterType = TableFilter
		}
		if o.Levels[i].TargetFileSize == 0 {
			o.Levels[i].TargetFileSize = 2 << 20
		}
	}
}

// LevelOptions holds options for a single level of the LSM.
type LevelOptions struct {
	// BlockRestartInterval is the number of keys between restart points.
	BlockRestartInterval int
	// BlockSize is the size of data blocks.
	BlockSize int
	// IndexBlockSize is the size of index blocks.
	IndexBlockSize int
	// FilterPolicy is the filter policy to use for this level.
	FilterPolicy base.FilterPolicy
	// FilterType is the type of filter to use for this level.
	FilterType FilterType
	// TargetFileSize is the target size of SSTables in this level.
	TargetFileSize int64
}

// FilterType defines the type of filter to use.
type FilterType int

const (
	// TableFilter is a filter that is applied to the whole table.
	TableFilter FilterType = iota
	// BlockFilter is a filter that is applied to each block.
	BlockFilter
)

// EventListener provides hooks for listening to significant DB events.
type EventListener struct {
	// CompactionBegin is called when a compaction begins.
	CompactionBegin func(CompactionInfo)
	// CompactionEnd is called when a compaction ends.
	CompactionEnd func(CompactionInfo)
	// FlushBegin is called when a flush begins.
	FlushBegin func(FlushInfo)
	// FlushEnd is called when a flush ends.
	FlushEnd func(FlushInfo)
	// WALCreated is called when a WAL is created.
	WALCreated func(WALInfo)
	// WALDeleted is called when a WAL is deleted.
	WALDeleted func(WALInfo)
}

// CompactionInfo contains information about a compaction.
type CompactionInfo struct {
	// JobID is the ID of the compaction job.
	JobID int
	// Reason is the reason for the compaction.
	Reason string
	// Input is the input levels and files for the compaction.
	Input []LevelInfo
	// Output is the output level for the compaction.
	Output LevelInfo
	// Err is the error that occurred during the compaction, if any.
	Err error
}

// LevelInfo contains information about a level.
type LevelInfo struct {
	// Level is the level number.
	Level int
	// NumFiles is the number of files in the level.
	NumFiles int
	// Size is the total size of the files in the level.
	Size int64
}

// FlushInfo contains information about a flush.
type FlushInfo struct {
	// JobID is the ID of the flush job.
	JobID int
	// Reason is the reason for the flush.
	Reason string
	// Output is the output file for the flush.
	Output FileInfo
	// Err is the error that occurred during the flush, if any.
	Err error
}

// FileInfo contains information about a file.
type FileInfo struct {
	// Path is the path to the file.
	Path string
	// Size is the size of the file.
	Size int64
}

// WALInfo contains information about a WAL.
type WALInfo struct {
	// JobID is the ID of the WAL job.
	JobID int

	// Path is the path to the WAL.
	Path string
	// Err is the error that occurred during the WAL operation, if any.
	Err error
}

// IterOptions are options for iterators.
type IterOptions struct {
	// LowerBound is a lower bound on the keys to be iterated over.
	LowerBound []byte
	// UpperBound is an upper bound on the keys to be iterated over.
	UpperBound []byte
	// seq is the sequence number for the iterator.
	seq uint64
}

// WriteOptions are options for write operations.
type WriteOptions struct {
	// Sync is a flag that requests the write to be flushed to stable storage
	// before returning. The default is false.
	Sync bool
}

// DefaultWriteOptions is the default options for write operations.
var DefaultWriteOptions = &WriteOptions{}

// Logger is a logger.
type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// DefaultLogger is a default logger.
type DefaultLogger struct{}

// Infof logs an info message.
func (l *DefaultLogger) Infof(format string, args ...interface{}) {
	log.Printf(format, args...)
}

// Errorf logs an error message.
func (l *DefaultLogger) Errorf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
