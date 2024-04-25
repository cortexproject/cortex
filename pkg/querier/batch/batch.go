package batch

import (
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/querier/iterators"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// GenericChunk is a generic chunk used by the batch iterator, in order to make the batch
// iterator general purpose.
type GenericChunk struct {
	MinTime int64
	MaxTime int64

	iterator func(reuse encoding.Iterator) encoding.Iterator
}

func NewGenericChunk(minTime, maxTime int64, iterator func(reuse encoding.Iterator) encoding.Iterator) GenericChunk {
	return GenericChunk{
		MinTime:  minTime,
		MaxTime:  maxTime,
		iterator: iterator,
	}
}

func (c GenericChunk) Iterator(reuse encoding.Iterator) encoding.Iterator {
	return c.iterator(reuse)
}

// iterator iterates over batches.
type iterator interface {
	// Seek to the batch at (or after) time t.
	Seek(t int64, size int) bool

	// Next moves to the next batch.
	Next(size int) bool

	// AtTime returns the start time of the next batch.  Must only be called after
	// Seek or Next have returned true.
	AtTime() int64

	// MaxCurrentChunkTime returns the max time on the current chunk.
	MaxCurrentChunkTime() int64

	// Batch returns the current batch.  Must only be called after Seek or Next
	// have returned true.
	Batch() encoding.Batch

	Err() error
}

// NewChunkMergeIterator returns a chunkenc.Iterator that merges Cortex chunks together.
func NewChunkMergeIterator(chunks []chunk.Chunk, _, _ model.Time) chunkenc.Iterator {
	converted := make([]GenericChunk, len(chunks))
	for i, c := range chunks {
		converted[i] = NewGenericChunk(int64(c.From), int64(c.Through), c.Data.NewIterator)
	}

	return NewGenericChunkMergeIterator(converted)
}

// NewGenericChunkMergeIterator returns a chunkenc.Iterator that merges generic chunks together.
func NewGenericChunkMergeIterator(chunks []GenericChunk) chunkenc.Iterator {
	iter := newMergeIterator(chunks)
	return newIteratorAdapter(iter)
}

// iteratorAdapter turns a batchIterator into a chunkenc.Iterator.
// It fetches ever increasing batchSizes (up to promchunk.BatchSize) on each
// call to Next; on calls to Seek, resets batch size to 1.
type iteratorAdapter struct {
	batchSize  int
	curr       encoding.Batch
	underlying iterator
}

func newIteratorAdapter(underlying iterator) chunkenc.Iterator {
	return iterators.NewCompatibleChunksIterator(&iteratorAdapter{
		batchSize:  1,
		underlying: underlying,
	})
}

// Seek implements chunkenc.Iterator.
func (a *iteratorAdapter) Seek(t int64) bool {

	// Optimisation: fulfill the seek using current batch if possible.
	if a.curr.Length > 0 && a.curr.Index < a.curr.Length {
		if t <= a.curr.Timestamps[a.curr.Index] {
			//In this case, the interface's requirement is met, so state of this
			//iterator does not need any change.
			return true
		} else if t <= a.curr.Timestamps[a.curr.Length-1] {
			//In this case, some timestamp between current sample and end of batch can fulfill
			//the seek. Let's find it.
			for a.curr.Index < a.curr.Length && t > a.curr.Timestamps[a.curr.Index] {
				a.curr.Index++
			}
			return true
		} else if t <= a.underlying.MaxCurrentChunkTime() {
			// In this case, some timestamp inside the current underlying chunk can fulfill the seek.
			// In this case we will call next until we find the sample as it will be faster than calling
			// `a.underlying.Seek` directly as this would cause the iterator to start from the beginning of the chunk.
			// See: https://github.com/cortexproject/cortex/blob/f69452975877c67ac307709e5f60b8d20477764c/pkg/querier/batch/chunk.go#L26-L45
			//      https://github.com/cortexproject/cortex/blob/f69452975877c67ac307709e5f60b8d20477764c/pkg/chunk/encoding/prometheus_chunk.go#L90-L95
			for a.Next() {
				if t <= a.curr.Timestamps[a.curr.Index] {
					return true
				}
			}
		}
	}

	a.curr.Length = -1
	a.batchSize = 1
	if a.underlying.Seek(t, a.batchSize) {
		a.curr = a.underlying.Batch()
		return a.curr.Index < a.curr.Length
	}
	return false
}

// Next implements chunkenc.Iterator.
func (a *iteratorAdapter) Next() bool {
	a.curr.Index++
	for a.curr.Index >= a.curr.Length && a.underlying.Next(a.batchSize) {
		a.curr = a.underlying.Batch()
		a.batchSize = a.batchSize * 2
		if a.batchSize > encoding.BatchSize {
			a.batchSize = encoding.BatchSize
		}
	}
	return a.curr.Index < a.curr.Length
}

// At implements chunkenc.Iterator.
func (a *iteratorAdapter) At() (int64, float64) {
	return a.curr.Timestamps[a.curr.Index], a.curr.Values[a.curr.Index]
}

// Err implements chunkenc.Iterator.
func (a *iteratorAdapter) Err() error {
	return nil
}
