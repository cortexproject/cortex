package batch

import (
	"fmt"

	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
)

// batchStream deals with iteratoring through multiple, non-overlapping batches,
// and building new slices of non-overlapping batches.  Designed to be used
// without allocations.
type batchStream []promchunk.Batch

func (bs batchStream) print() {
	fmt.Println("[")
	for _, b := range bs {
		print(b)
	}
	fmt.Println("]")
}

// reset, hasNext, next, atTime etc are all inlined in go1.11.

func (bs *batchStream) reset() {
	for i := range *bs {
		(*bs)[i].Index = 0
	}
}

func (bs *batchStream) hasNext() bool {
	return len(*bs) > 0
}

func (bs *batchStream) next() {
	(*bs)[0].Index++
	if (*bs)[0].Index >= (*bs)[0].Length {
		*bs = (*bs)[1:]
	}
}

func (bs *batchStream) atTime() int64 {
	return (*bs)[0].Timestamps[(*bs)[0].Index]
}

func (bs *batchStream) at() (int64, float64) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.Values[b.Index]
}

// mergeBatches assumes the contents of batches are overlapping and unsorted.
// Merge them together into a sorted, non-overlapping stream in result.
// Caller must guarantee result is big enough.  Return value will always be a
// slice pointing to the same underly array as result, allowing mergeBatches
// to call itself recursively.
func mergeBatches(batches batchStream, result batchStream, size int) batchStream {
	switch len(batches) {
	case 0:
		return nil
	case 1:
		copy(result[:1], batches)
		return result[:1]
	case 2:
		return mergeStreams(batches[0:1], batches[1:2], result, size)
	default:
		n := len(batches) / 2
		left := mergeBatches(batches[n:], result[n:], size)
		right := mergeBatches(batches[:n], result[:n], size)

		batches = mergeStreams(left, right, batches, size)
		result = result[:len(batches)]
		copy(result, batches)

		return result[:len(batches)]
	}
}

func mergeStreams(left, right batchStream, result batchStream, size int) batchStream {
	// Ensure that 'result' has enough capacity of left and right added together.
	if cap(result) >= len(left)+len(right) {
		for i := range result {
			result[i].Index = 0
			result[i].Length = 0
		}
		for len(result) < len(left)+len(right) {
			result = append(result, promchunk.Batch{})
		}
	} else {
		result = make([]promchunk.Batch, len(left)+len(right))
	}
	resultLen := 1
	b := &result[0]

	for left.hasNext() && right.hasNext() {
		if b.Index == size {
			b.Length = b.Index
			resultLen++
			if resultLen > len(result) {
				result = append(result, promchunk.Batch{})
			}
			b = &result[resultLen-1]
		}
		t1, t2 := left.atTime(), right.atTime()
		if t1 < t2 {
			b.Timestamps[b.Index], b.Values[b.Index] = left.at()
			left.next()
		} else if t1 > t2 {
			b.Timestamps[b.Index], b.Values[b.Index] = right.at()
			right.next()
		} else {
			b.Timestamps[b.Index], b.Values[b.Index] = left.at()
			left.next()
			right.next()
		}
		b.Index++
	}

	for ; left.hasNext(); left.next() {
		if b.Index == size {
			b.Length = b.Index
			resultLen++
			if resultLen > len(result) {
				result = append(result, promchunk.Batch{})
			}
			b = &result[resultLen-1]
		}
		b.Timestamps[b.Index], b.Values[b.Index] = left.at()
		b.Index++
		b.Length++
	}

	for ; right.hasNext(); right.next() {
		if b.Index == size {
			b.Length = b.Index
			resultLen++
			if resultLen > len(result) {
				result = append(result, promchunk.Batch{})
			}
			b = &result[resultLen-1]
		}
		b.Timestamps[b.Index], b.Values[b.Index] = right.at()
		b.Index++
		b.Length++
	}
	b.Length = b.Index

	result = result[:resultLen]
	result.reset()
	return result
}

func mergeBatchTimes(a, b promchunk.Batch, size int) promchunk.Batch {
	i, j, k := 0, 0, 0
	var result promchunk.Batch
	for i < a.Length && j < b.Length && k < size {
		t1, t2 := a.Timestamps[i], b.Timestamps[j]
		if t1 < t2 {
			result.Timestamps[k] = t1
			i++
			k++
		} else if t1 > t2 {
			result.Timestamps[k] = t2
			j++
			k++
		} else {
			result.Timestamps[k] = t2
			i++
			j++
			k++
		}
	}
	for i < a.Length && k < size {
		result.Timestamps[k] = a.Timestamps[i]
		i++
		k++
	}
	for j < b.Length && k < size {
		result.Timestamps[k] = b.Timestamps[j]
		j++
		k++
	}
	result.Length = k
	return result
}
