package batch

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

// iterator iterates over batches.
type iterator interface {
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
	iter := newMergeIterator(chunks)
	return newIteratorAdapter(iter)
}

// iteratorAdapter turns a batchIterator into a storage.SeriesIterator.
type iteratorAdapter struct {
	curr       promchunk.Batch
	underlying iterator
}

func newIteratorAdapter(underlying iterator) storage.SeriesIterator {
	return &iteratorAdapter{
		underlying: underlying,
	}
}

// Seek implements storage.SeriesIterator.
func (a *iteratorAdapter) Seek(t int64) bool {
	a.curr.Length = -1
	if a.underlying.Seek(t) {
		a.curr = a.underlying.Batch()
		return a.curr.Index < a.curr.Length
	}
	return false
}

// Next implements storage.SeriesIterator.
func (a *iteratorAdapter) Next() bool {
	a.curr.Index++
	for a.curr.Index >= a.curr.Length && a.underlying.Next() {
		a.curr = a.underlying.Batch()
	}
	return a.curr.Index < a.curr.Length
}

// At implements storage.SeriesIterator.
func (a *iteratorAdapter) At() (int64, float64) {
	return a.curr.Timestamps[a.curr.Index], a.curr.Values[a.curr.Index]
}

// Err implements storage.SeriesIterator.
func (a *iteratorAdapter) Err() error {
	return nil
}
