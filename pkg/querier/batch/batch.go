package batch

import (
	"fmt"

	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/cortex/pkg/chunk"
)

const batchSize = 128

// batch is a sort set of (timestamp, value) pairs.  They are intended to be
// small, and passed by value.
type batch struct {
	timestamps [batchSize]int64
	values     [batchSize]float64
	index      int
	length     int
}

func (b batch) Print() {
	fmt.Println("   ", b.timestamps, b.index, b.length)
}

// batchIterator iterate over batches.
type batchIterator interface {
	Seek(t int64) bool
	Next() bool
	AtTime() int64
	Batch() batch
	Err() error
}

// NewChunkMergeIterator returns a storage.SeriesIterator that merges chunks together.
func NewChunkMergeIterator(chunks []chunk.Chunk) storage.SeriesIterator {
	iter := newBatchMergeIterator(chunks)
	return newBatchIteratorAdapter(iter)
}

// batchIteratorAdapter turns a batchIterator into a storage.SeriesIterator.
type batchIteratorAdapter struct {
	curr       batch
	underlying batchIterator
}

func newBatchIteratorAdapter(underlying batchIterator) storage.SeriesIterator {
	return &batchIteratorAdapter{
		underlying: underlying,
	}
}

// Seek implements storage.SeriesIterator.
func (a *batchIteratorAdapter) Seek(t int64) bool {
	a.curr.length = -1
	return a.underlying.Seek(t)
}

// Next implements storage.SeriesIterator.
func (a *batchIteratorAdapter) Next() bool {
	a.curr.index++

	for a.curr.index >= a.curr.length && a.underlying.Next() {
		a.curr = a.underlying.Batch()
	}

	return a.curr.index < a.curr.length
}

// At implements storage.SeriesIterator.
func (a *batchIteratorAdapter) At() (int64, float64) {
	return a.curr.timestamps[a.curr.index], a.curr.values[a.curr.index]
}

// Err implements storage.SeriesIterator.
func (a *batchIteratorAdapter) Err() error {
	return nil
}
