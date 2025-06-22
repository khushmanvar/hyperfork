// log_writer.go

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"encoding/binary"
	"io"
	"sync"
	"hash/crc32"
)

// LogWriter writes records to a log file.
type LogWriter struct {
	// w is the underlying writer.
	w io.Writer
	// block is the current block being written.
	block [BlockSize]byte
	// n is the number of bytes written to the current block.
	n int
	// seq is the sequence number of the current record.
	seq int64

	// syncQueue is a queue of channels for waiting for syncs.
	syncQueue chan chan error
	// syncRunning is true if a sync is in progress.
	syncRunning bool
	// mu is a mutex for protecting the fields above.
	mu sync.Mutex
}

// NewLogWriter creates a new log writer.
func NewLogWriter(w io.Writer, seq int64) *LogWriter {
	lw := &LogWriter{
		w:         w,
		seq:       seq,
		syncQueue: make(chan chan error, 16),
	}
	go lw.flushLoop()
	return lw
}

// WriteRecord writes a record.
func (lw *LogWriter) WriteRecord(p []byte) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	var first = true
	for {
		space := BlockSize - lw.n
		if space < HeaderSize {
			// Not enough space for a header. Fill the rest of the block with zeros.
			for i := lw.n; i < BlockSize; i++ {
				lw.block[i] = 0
			}
			if err := lw.flushBlock(); err != nil {
				return err
			}
			first = false
			continue
		}

		var header [HeaderSize]byte
		var kind uint8
		var n int
		if len(p) <= space-HeaderSize {
			if first {
				kind = Full
			} else {
				kind = Last
			}
			n = len(p)
		} else {
			if first {
				kind = First
			} else {
				kind = Middle
			}
			n = space - HeaderSize
		}

		binary.LittleEndian.PutUint32(header[:4], crc32.ChecksumIEEE(p[:n]))
		binary.LittleEndian.PutUint16(header[4:6], uint16(n))
		header[6] = kind

		copy(lw.block[lw.n:], header[:])
		copy(lw.block[lw.n+HeaderSize:], p[:n])
		lw.n += HeaderSize + n
		p = p[n:]

		if len(p) == 0 {
			break
		}

		if err := lw.flushBlock(); err != nil {
			return err
		}
		first = false
	}
	return nil
}

// Sync flushes the writer and waits for the data to be synced to disk.
func (lw *LogWriter) Sync() error {
	c := make(chan error, 1)
	lw.syncQueue <- c
	return <-c
}

func (lw *LogWriter) flushLoop() {
	for c := range lw.syncQueue {
		lw.mu.Lock()
		if lw.n > 0 {
			if err := lw.flushBlock(); err != nil {
				lw.mu.Unlock()
				c <- err
				continue
			}
		}
		lw.mu.Unlock()

		if f, ok := lw.w.(interface {
			Sync() error
		}); ok {
			c <- f.Sync()
		} else {
			c <- nil
		}
	}
}

func (lw *LogWriter) flushBlock() error {
	_, err := lw.w.Write(lw.block[:lw.n])
	if err != nil {
		return err
	}
	lw.n = 0
	return nil
}
