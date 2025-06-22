// vfs.go

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"os"
	"time"
)

// FS is a virtual file system.
type FS interface {
	// Create creates a new file.
	Create(name string) (File, error)
	// Open opens an existing file.
	Open(name string) (File, error)
	// Remove removes a file.
	Remove(name string) error
	// Rename renames a file.
	Rename(oldname, newname string) error
	// List lists the files in a directory.
	List(dir string) ([]string, error)
	// MkdirAll creates a directory.
	MkdirAll(dir string, perm os.FileMode) error
	// Lock locks a file.
	Lock(name string) (io.Closer, error)
	// GetDiskUsage gets the disk usage for a directory.
	GetDiskUsage(dir string) (uint64, error)
	// Stat returns a FileInfo describing the named file.
	Stat(name string) (os.FileInfo, error)
}

// File is a file.
type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	// Stat returns the file's stats.
	Stat() (os.FileInfo, error)
	// Sync flushes the file to stable storage.
	Sync() error
}

// A VFS is a virtual filesystem.
type VFS interface {
	// Create creates the named file for writing, truncating it if it already
	// exists.
	Create(name string) (File, error)

	// Open opens the named file for reading.
	Open(name string) (File, error)

	// Remove removes the named file or directory.
	Remove(name string) error

	// Stat returns a FileInfo describing the named file.
	Stat(name string) (os.FileInfo, error)

	// Lock locks the named file.
	Lock(name string) (io.Closer, error)
}

// defaultFS is the default implementation of VFS.
type defaultFS struct{}

// Create creates the named file for writing, truncating it if it already
// exists.
func (defaultFS) Create(name string) (File, error) {
	return os.Create(name)
}

// Open opens the named file for reading.
func (defaultFS) Open(name string) (File, error) {
	return os.Open(name)
}

// Remove removes the named file or directory.
func (defaultFS) Remove(name string) error {
	return os.Remove(name)
}

// Stat returns a FileInfo describing the named file.
func (defaultFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// Lock locks the named file.
func (defaultFS) Lock(name string) (io.Closer, error) {
	return os.OpenFile(name, os.O_CREATE|os.O_RDONLY, 0644)
}

// Default is the default VFS.
var Default = defaultFS{}

type nilCloser struct{}

func (c *nilCloser) Close() error {
	return nil
}
