package batch

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/cortexproject/cortex/pkg/chunk"
)

// GenericChunk is a generic chunk used by the batch iterator, in order to make the batch
// iterator general purpose.
type GenericChunk struct {
	MinTime int64
	MaxTime int64

	iterator func(reuse chunk.Iterator) chunk.Iterator
}

func NewGenericChunk(minTime, maxTime int64, iterator func(reuse chunk.Iterator) chunk.Iterator) GenericChunk {
	return GenericChunk{
		MinTime:  minTime,
		MaxTime:  maxTime,
		iterator: iterator,
	}
}

func (c GenericChunk) Iterator(reuse chunk.Iterator) chunk.Iterator {
	return c.iterator(reuse)
}

// iterator iterates over batches.
type iterator interface {
	// Seek to the batch at (or after) time t and returns chunk value type.
	Seek(t int64, size int) chunkenc.ValueType

	// Next moves to the next batch and returns chunk value type.
	Next(size int) chunkenc.ValueType

	// AtTime returns the start time of the next batch.  Must only be called after
	// Seek or Next have returned true.
	AtTime() int64

	// MaxCurrentChunkTime returns the max time on the current chunk.
	MaxCurrentChunkTime() int64

	// Batch returns the current batch. Must only be called after Seek or Next
	// have returned true.
	Batch() chunk.Batch

	Err() error
}

// NewChunkMergeIterator returns a chunkenc.Iterator that merges Cortex chunks together.
func NewChunkMergeIterator(chunks []chunk.Chunk, _, _ model.Time) chunkenc.Iterator {
	converted := make([]GenericChunk, len(chunks))
	for i, c := range chunks {
		c := c
		converted[i] = NewGenericChunk(int64(c.From), int64(c.Through), c.NewIterator)
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
	curr       chunk.Batch
	underlying iterator
}

func newIteratorAdapter(underlying iterator) chunkenc.Iterator {
	return &iteratorAdapter{
		batchSize:  1,
		underlying: underlying,
	}
}

// Seek implements chunkenc.Iterator.
func (a *iteratorAdapter) Seek(t int64) chunkenc.ValueType {

	// Optimisation: fulfill the seek using current batch if possible.
	if a.curr.Length > 0 && a.curr.Index < a.curr.Length {
		if t <= a.curr.Timestamps[a.curr.Index] {
			//In this case, the interface's requirement is met, so state of this
			//iterator does not need any change.
			return a.curr.ValType
		} else if t <= a.curr.Timestamps[a.curr.Length-1] {
			//In this case, some timestamp between current sample and end of batch can fulfill
			//the seek. Let's find it.
			for a.curr.Index < a.curr.Length && t > a.curr.Timestamps[a.curr.Index] {
				a.curr.Index++
			}
			return a.curr.ValType
		} else if t <= a.underlying.MaxCurrentChunkTime() {
			// In this case, some timestamp inside the current underlying chunk can fulfill the seek.
			// In this case we will call next until we find the sample as it will be faster than calling
			// `a.underlying.Seek` directly as this would cause the iterator to start from the beginning of the chunk.
			// See: https://github.com/cortexproject/cortex/blob/f69452975877c67ac307709e5f60b8d20477764c/pkg/querier/batch/chunk.go#L26-L45
			//      https://github.com/cortexproject/cortex/blob/f69452975877c67ac307709e5f60b8d20477764c/pkg/chunk/encoding/prometheus_chunk.go#L90-L95
			for {
				valType := a.Next()
				if valType == chunkenc.ValNone {
					break
				}
				if t <= a.curr.Timestamps[a.curr.Index] {
					return valType
				}
			}
		}
	}

	a.curr.Length = -1
	a.batchSize = 1
	if valType := a.underlying.Seek(t, a.batchSize); valType != chunkenc.ValNone {
		a.curr = a.underlying.Batch()
		if a.curr.Index < a.curr.Length {
			return a.curr.ValType
		}
	}
	return chunkenc.ValNone
}

// Next implements chunkenc.Iterator.
func (a *iteratorAdapter) Next() chunkenc.ValueType {
	a.curr.Index++
	for a.curr.Index >= a.curr.Length && a.underlying.Next(a.batchSize) != chunkenc.ValNone {
		a.curr = a.underlying.Batch()
		a.batchSize = a.batchSize * 2
		if a.batchSize > chunk.BatchSize {
			a.batchSize = chunk.BatchSize
		}
	}
	if a.curr.Index < a.curr.Length {
		return a.curr.ValType
	}
	return chunkenc.ValNone
}

// At implements chunkenc.Iterator.
func (a *iteratorAdapter) At() (int64, float64) {
	return a.curr.Timestamps[a.curr.Index], a.curr.Values[a.curr.Index]
}

// Err implements chunkenc.Iterator.
func (a *iteratorAdapter) Err() error {
	return nil
}

// AtHistogram implements chunkenc.Iterator.
func (a *iteratorAdapter) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return a.curr.Timestamps[a.curr.Index], a.curr.Histograms[a.curr.Index]
}

// AtFloatHistogram implements chunkenc.Iterator.
func (a *iteratorAdapter) AtFloatHistogram(h *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	// PromQL engine always selects float histogram in its implementation so might call AtFloatHistogram
	// even if it is a histogram. https://github.com/prometheus/prometheus/blob/v2.53.0/promql/engine.go#L2276
	if a.curr.ValType == chunkenc.ValHistogram {
		return a.curr.Timestamps[a.curr.Index], a.curr.Histograms[a.curr.Index].ToFloat(h)
	}
	return a.curr.Timestamps[a.curr.Index], a.curr.FloatHistograms[a.curr.Index]
}

// AtT implements chunkenc.Iterator.
func (a *iteratorAdapter) AtT() int64 {
	return a.curr.Timestamps[a.curr.Index]
}
