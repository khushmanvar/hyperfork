package pebble

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/khushmanvar/hyperfork/internal/base"
)

const (
	batchMagic = 0x4261746368
)

var (
	errBatchClosed = errors.New("pebble: batch closed")
)

// Batch is a sequence of Sets and Deletes that are applied atomically.
type Batch struct {
	// Data is the wire format of a batch's log entry. The entry consists of a
	// 12-byte header, followed by a sequence of records.
	//
	// The 12-byte header is:
	//   - a 4-byte magic number
	//   - a 4-byte count of the number of records in the batch
	//   - a 4-byte reserved field
	//
	// Each record has a 1-byte kind tag, followed by 1 or 2 length-prefixed
	// strings (varstring):
	//   - Set:   <1-byte kind><varstring key><varstring value>
	//   - Delete: <1-byte kind><varstring key>
	//
	// A varstring is a varint32 followed by that many bytes.
	data []byte

	// The db to which the batch will be applied.
	db *DB

	// An optional skiplist for indexing the batch.
	memTable *memTable

	// The options for the batch.
	opts *Options

	// The flushable batch for the commit pipeline.
	flushable *Batch
}

// iter is an iterator over a batch.
type iter struct {
	batch *Batch
	pos   int
}

// Next returns the next record.
func (i *iter) Next() (kind base.InternalKeyKind, ukey, value []byte, ok bool) {
	if i.pos >= len(i.batch.data) {
		return 0, nil, nil, false
	}
	kind = base.InternalKeyKind(i.batch.data[i.pos])
	i.pos++
	var n int
	ukey, n = getVarstring(i.batch.data[i.pos:])
	i.pos += n
	if kind == base.InternalKeyKindSet || kind == base.InternalKeyKindMerge {
		value, n = getVarstring(i.batch.data[i.pos:])
		i.pos += n
	}
	return kind, ukey, value, true
}

func getVarstring(data []byte) ([]byte, int) {
	l, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, 0
	}
	return data[n : n+int(l)], n + int(l)
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return &Batch{}
	},
}

func newBatch(db *DB) *Batch {
	b := batchPool.Get().(*Batch)
	b.db = db
	return b
}

func (b *Batch) release() {
	// If the batch has a memtable, release it.
	if b.memTable != nil {
		b.memTable = nil
	}
	b.db.batchPool.Put(b)
}

// Set adds a Set record to the batch.
func (b *Batch) Set(key, value []byte, _ *WriteOptions) error {
	if len(b.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen32)
	}
	return b.set(key, value)
}

// Delete adds a Delete record to the batch.
func (b *Batch) Delete(key []byte, _ *WriteOptions) error {
	if len(b.data) == 0 {
		b.init(len(key) + binary.MaxVarintLen32)
	}
	return b.delete(key)
}

// Merge adds a Merge record to the batch.
func (b *Batch) Merge(key, value []byte, _ *WriteOptions) error {
	if len(b.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen32)
	}
	return b.merge(key, value)
}

// Count returns the number of records in the batch.
func (b *Batch) Count() uint32 {
	if len(b.data) < 12 {
		return 0
	}
	return binary.LittleEndian.Uint32(b.data[4:8])
}

// LogData adds a LogData record to the batch.
func (b *Batch) LogData(data []byte, _ *WriteOptions) error {
	if len(b.data) == 0 {
		b.init(len(data) + binary.MaxVarintLen32)
	}
	return b.logData(data)
}

func (b *Batch) init(cap int) {
	const maxBatchSize = 1 << 20 // 1MB
	if cap > maxBatchSize {
		cap = maxBatchSize
	}
	n := 12
	b.data = make([]byte, n, n+cap)
	binary.LittleEndian.PutUint32(b.data[0:4], uint32(batchMagic))
	binary.LittleEndian.PutUint32(b.data[4:8], 0)
	binary.LittleEndian.PutUint32(b.data[8:12], 0)
}

func (b *Batch) set(key, value []byte) error {
	if b.db == nil {
		return errBatchClosed
	}
	count := b.Count()
	binary.LittleEndian.PutUint32(b.data[4:8], count+1)
	b.data = append(b.data, byte(base.InternalKeyKindSet))
	b.data = appendVarstring(b.data, key)
	b.data = appendVarstring(b.data, value)
	return nil
}

func (b *Batch) delete(key []byte) error {
	if b.db == nil {
		return errBatchClosed
	}
	count := b.Count()
	binary.LittleEndian.PutUint32(b.data[4:8], count+1)
	b.data = append(b.data, byte(base.InternalKeyKindDelete))
	b.data = appendVarstring(b.data, key)
	return nil
}

func (b *Batch) merge(key, value []byte) error {
	if b.db == nil {
		return errBatchClosed
	}
	count := b.Count()
	binary.LittleEndian.PutUint32(b.data[4:8], count+1)
	b.data = append(b.data, byte(base.InternalKeyKindMerge))
	b.data = appendVarstring(b.data, key)
	b.data = appendVarstring(b.data, value)
	return nil
}

func (b *Batch) logData(data []byte) error {
	if b.db == nil {
		return errBatchClosed
	}
	count := b.Count()
	binary.LittleEndian.PutUint32(b.data[4:8], count+1)
	b.data = append(b.data, byte(base.InternalKeyKindLogData))
	b.data = appendVarstring(b.data, data)
	return nil
}

func appendVarstring(dst []byte, s []byte) []byte {
	var buf [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(buf[:], uint64(len(s)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, s...)
	return dst
}

func (b *Batch) setRepr(key, value []byte) {
	b.data = append(b.data, byte(len(key)>>8), byte(len(key)))
	b.data = append(b.data, key...)
	b.data = append(b.data, byte(len(value)>>8), byte(len(value)))
	b.data = append(b.data, value...)
}
