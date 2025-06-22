// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"encoding/binary"
	"fmt"
)

// InternalKeyKind defines the kind of key.
type InternalKeyKind int

// These constants are part of the file format and should not be changed.
const (
	// InternalKeyKindDelete is a deletion marker.
	InternalKeyKindDelete InternalKeyKind = iota
	// InternalKeyKindSet is a key-value pair.
	InternalKeyKindSet
	// InternalKeyKindMerge is a merge operation.
	InternalKeyKindMerge
	// InternalKeyKindLogData is a record of data that was logged to the WAL.
	InternalKeyKindLogData
	// InternalKeyKindMax is the maximum kind value.
	InternalKeyKindMax
)

// String returns a string representation of the key kind.
func (k InternalKeyKind) String() string {
	switch k {
	case InternalKeyKindDelete:
		return "DELETE"
	case InternalKeyKindSet:
		return "SET"
	case InternalKeyKindMerge:
		return "MERGE"
	case InternalKeyKindLogData:
		return "LOGDATA"
	}
	return fmt.Sprintf("UNKNOWN:%d", k)
}

// InternalKey is a key used internally by the database.
type InternalKey struct {
	UserKey []byte
	Trailer uint64
}

// MakeInternalKey creates an internal key from a user key, sequence number, and
// kind.
func MakeInternalKey(userKey []byte, seqNum uint64, kind InternalKeyKind) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (seqNum << 8) | uint64(kind),
	}
}

// Kind returns the kind of the internal key.
func (k InternalKey) Kind() InternalKeyKind {
	return InternalKeyKind(k.Trailer & 0xff)
}

// SeqNum returns the sequence number of the internal key.
func (k InternalKey) SeqNum() uint64 {
	return k.Trailer >> 8
}

// ParseInternalKey parses an internal key from a byte slice.
func ParseInternalKey(b []byte) (InternalKey, bool) {
	if len(b) < 8 {
		return InternalKey{}, false
	}
	n := len(b) - 8
	return InternalKey{
		UserKey: b[:n],
		Trailer: binary.LittleEndian.Uint64(b[n:]),
	}, true
}

// Encode encodes the internal key to a byte slice.
func (k InternalKey) Encode(dst []byte) []byte {
	dst = append(dst, k.UserKey...)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], k.Trailer)
	return append(dst, buf[:]...)
}
