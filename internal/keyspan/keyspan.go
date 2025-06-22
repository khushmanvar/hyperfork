// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/khushmanvar/hyperfork/internal/base"
)

// Span represents a key span.
type Span struct {
	// Start is the inclusive start of the key span.
	Start []byte
	// End is the exclusive end of the key span.
	End []byte
	// Keys are the keys associated with this span.
	Keys []Key
}

// Key represents a key associated with a span.
type Key struct {
	// Trailer is the key trailer.
	Trailer uint64
	// Value is the key value.
	Value []byte
}

// Fragmenter fragments a set of spans into non-overlapping spans.
type Fragmenter struct {
	Cmp    base.Compare
	Format base.FormatKey
	Spans  []Span
	buf    []byte
}

// Add adds a span to the fragmenter.
func (f *Fragmenter) Add(s Span) {
	f.Spans = append(f.Spans, s)
}

// Finalize finalizes the fragmentation and returns the fragmented spans.
func (f *Fragmenter) Finalize() []Span {
	if len(f.Spans) == 0 {
		return nil
	}

	// Sort the spans by start key.
	sort.Slice(f.Spans, func(i, j int) bool {
		return f.Cmp(f.Spans[i].Start, f.Spans[j].Start) < 0
	})

	var result []Span
	var current Span
	current.Start = f.Spans[0].Start
	current.End = f.Spans[0].End
	current.Keys = append(current.Keys, f.Spans[0].Keys...)

	for i := 1; i < len(f.Spans); i++ {
		s := f.Spans[i]
		if f.Cmp(s.Start, current.End) >= 0 {
			// The new span starts after the current one ends.
			result = append(result, current)
			current = s
			continue
		}

		if f.Cmp(s.End, current.End) > 0 {
			// The new span extends the current one.
			current.End = s.End
		}
		current.Keys = append(current.Keys, s.Keys...)
	}
	result = append(result, current)
	return result
}
