package pebble

import (
	"container/heap"
	"github.com/khushmanvar/hyperfork/internal/base"
)

// mergingIter provides a merged view of multiple iterators.
type mergingIter struct {
	iters    []base.InternalIterator
	err      error
	heap     mergingIterHeap
	repaired bool
}

func (m *mergingIter) SeekGE(key []byte) {
	for i := range m.iters {
		m.iters[i].SeekGE(key)
	}
	m.repaired = false
}

func (m *mergingIter) Valid() bool {
	if m.err != nil {
		return false
	}
	if !m.repaired {
		m.repair()
	}
	return len(m.heap.items) > 0
}

func (m *mergingIter) Key() base.InternalKey {
	return m.heap.items[0].iter.Key()
}

func (m *mergingIter) Value() []byte {
	return m.heap.items[0].iter.Value()
}

func (m *mergingIter) Next() {
	m.heap.items[0].iter.Next()
	if m.heap.items[0].iter.Valid() {
		heap.Fix(&m.heap, 0)
	} else {
		heap.Pop(&m.heap)
	}
}

func (m *mergingIter) Close() error {
	for i := range m.iters {
		if err := m.iters[i].Close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	return m.err
}

func (m *mergingIter) repair() {
	m.heap.items = m.heap.items[:0]
	for i := range m.iters {
		if m.iters[i].Valid() {
			m.heap.items = append(m.heap.items, mergingIterItem{
				iter: m.iters[i],
			})
		}
	}
	heap.Init(&m.heap)
	m.repaired = true
}

type mergingIterItem struct {
	iter base.InternalIterator
}

type mergingIterHeap struct {
	items []mergingIterItem
}

func (h *mergingIterHeap) Len() int {
	return len(h.items)
}

func (h *mergingIterHeap) Less(i, j int) bool {
	return base.InternalCompare(h.items[i].iter.Key(), h.items[j].iter.Key()) < 0
}

func (h *mergingIterHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergingIterHeap) Push(x interface{}) {
	h.items = append(h.items, x.(mergingIterItem))
}

func (h *mergingIterHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}
