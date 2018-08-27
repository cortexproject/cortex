package batch

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

// batchIterator iterate over batches.
type batchIterator interface {
	// Seek to the batch at (or after) time t.
	Seek(t int64) bool

	// Next moves to the next batch.
	Next() bool

	// AtTime returns the start time of the next batch.  Must only be called after
	// Seek or Next have returned true.
	AtTime() int64

	// Batch returns the current batch.  Must only be called after Seek or Next
	// have returned true.  Must return a full batch if possible; if a partial
	// batch is returned, subsequent Next calls must return false.
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
	if a.underlying.Seek(t) {
		a.curr = a.underlying.Batch()
		return a.curr.Index < a.curr.Length
	}
	return false
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
