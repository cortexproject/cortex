package batch

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

// batchIterator iterate over batches.
type batchIterator interface {
	Seek(t int64) bool
	Next() bool
	AtTime() int64
	Batch() promchunk.Batch
	Err() error
}

// NewChunkMergeIterator returns a storage.SeriesIterator that merges chunks together.
func NewChunkMergeIterator(chunks []chunk.Chunk) storage.SeriesIterator {
	iter := newBatchMergeIterator(chunks)
	return newBatchIteratorAdapter(iter)
}

// batchIteratorAdapter turns a batchIterator into a storage.SeriesIterator.
type batchIteratorAdapter struct {
	curr       promchunk.Batch
	underlying batchIterator
}

func newBatchIteratorAdapter(underlying batchIterator) storage.SeriesIterator {
	return &batchIteratorAdapter{
		underlying: underlying,
	}
}

// Seek implements storage.SeriesIterator.
func (a *batchIteratorAdapter) Seek(t int64) bool {
	a.curr.Length = -1
	return a.underlying.Seek(t)
}

// Next implements storage.SeriesIterator.
func (a *batchIteratorAdapter) Next() bool {
	a.curr.Index++

	for a.curr.Index >= a.curr.Length && a.underlying.Next() {
		a.curr = a.underlying.Batch()
	}

	return a.curr.Index < a.curr.Length
}

// At implements storage.SeriesIterator.
func (a *batchIteratorAdapter) At() (int64, float64) {
	return a.curr.Timestamps[a.curr.Index], a.curr.Values[a.curr.Index]
}

// Err implements storage.SeriesIterator.
func (a *batchIteratorAdapter) Err() error {
	return nil
}
