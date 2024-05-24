package batch

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	promchunk "github.com/cortexproject/cortex/pkg/chunk"
)

// batchStream deals with iteratoring through multiple, non-overlapping batches,
// and building new slices of non-overlapping batches.  Designed to be used
// without allocations.
type batchStream []promchunk.Batch

// reset, hasNext, next, atTime etc are all inlined in go1.11.

func (bs *batchStream) reset() {
	for i := range *bs {
		(*bs)[i].Index = 0
	}
}

func (bs *batchStream) hasNext() chunkenc.ValueType {
	if len(*bs) > 0 {
		return (*bs)[0].ValType
	}
	return chunkenc.ValNone
}

func (bs *batchStream) next() {
	(*bs)[0].Index++
	if (*bs)[0].Index >= (*bs)[0].Length {
		*bs = (*bs)[1:]
	}
}

func (bs *batchStream) atHistogram() (int64, *histogram.Histogram) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.Histograms[b.Index]
}

func (bs *batchStream) atFloatHistogram() (int64, *histogram.FloatHistogram) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.FloatHistograms[b.Index]
}

func (bs *batchStream) atTime() int64 {
	return (*bs)[0].Timestamps[(*bs)[0].Index]
}

func (bs *batchStream) at() (int64, float64) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.Values[b.Index]
}

func mergeStreams(left, right batchStream, result batchStream, size int) batchStream {
	// Reset the Index and Length of existing batches.
	for i := range result {
		result[i].Index = 0
		result[i].Length = 0
	}
	resultLen := 1 // Number of batches in the final result.
	b := &result[0]

	// This function adds a new batch to the result.
	nextBatch := func(valueType chunkenc.ValueType) {
		// The Index is the place at which new sample
		// has to be appended, hence it tells the length.
		b.Length = b.Index
		resultLen++
		if resultLen > len(result) {
			// It is possible that result can grow longer
			// then the one provided.
			result = append(result, promchunk.Batch{})
		}
		b = &result[resultLen-1]
		b.ValType = valueType
	}

	populateVal := func(bs batchStream, valueType chunkenc.ValueType) {
		if b.Index == 0 {
			b.ValType = valueType
		} else if b.Index == size || b.ValType != valueType {
			// The batch reached it intended size or a new value type is used.
			// Add another batch to the result and use it for further appending.
			nextBatch(valueType)
		}
		switch valueType {
		case chunkenc.ValFloat:
			b.Timestamps[b.Index], b.Values[b.Index] = bs.at()
		case chunkenc.ValHistogram:
			b.Timestamps[b.Index], b.Histograms[b.Index] = bs.atHistogram()
		case chunkenc.ValFloatHistogram:
			b.Timestamps[b.Index], b.FloatHistograms[b.Index] = bs.atFloatHistogram()
		default:
			panic("unsupported value type")
		}
		b.Index++
	}

	for leftValueType, rightValueType := left.hasNext(), right.hasNext(); leftValueType != chunkenc.ValNone && rightValueType != chunkenc.ValNone; {
		t1, t2 := left.atTime(), right.atTime()
		if t1 < t2 {
			populateVal(left, leftValueType)
			left.next()
			leftValueType = left.hasNext()
		} else if t1 > t2 {
			populateVal(right, rightValueType)
			right.next()
			rightValueType = right.hasNext()
		} else {
			populateVal(left, leftValueType)
			left.next()
			leftValueType = left.hasNext()
			right.next()
			rightValueType = right.hasNext()
		}
	}

	// This function adds all the samples from the provided
	// batchStream into the result in the same order.
	addToResult := func(bs batchStream) {
		for valueType := bs.hasNext(); valueType != chunkenc.ValNone; valueType = bs.hasNext() {
			populateVal(bs, valueType)
			bs.next()
		}
	}

	addToResult(left)
	addToResult(right)

	// The Index is the place at which new sample
	// has to be appended, hence it tells the length.
	b.Length = b.Index

	// The provided 'result' slice might be bigger
	// than the actual result, hence return the subslice.
	result = result[:resultLen]
	result.reset()
	return result
}
