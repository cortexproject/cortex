package batch

import (
	"fmt"

	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
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

// append, reset, hasNext, next, atTime etc are all inlined in go1.11.
// append isn't a pointer receiver as that was causing bs to escape to the heap.
func (bs batchStream) append(t int64, v float64, size int) batchStream {
	l := len(bs)
	if l == 0 || bs[l-1].Index == size {
		bs = append(bs, promchunk.Batch{})
		l++
	}
	b := &bs[l-1]
	b.Timestamps[b.Index] = t
	b.Values[b.Index] = v
	b.Index++
	b.Length++
	return bs
}

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

// mergeBatches assumes the contents of batches are overlapping and unstorted.
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
	result.reset()
	result = result[:0]
	for left.hasNext() && right.hasNext() {
		t1, t2 := left.atTime(), right.atTime()
		if t1 < t2 {
			t, v := left.at()
			result = result.append(t, v, size)
			left.next()
		} else if t1 > t2 {
			t, v := right.at()
			result = result.append(t, v, size)
			right.next()
		} else {
			t, v := left.at()
			result = result.append(t, v, size)
			left.next()
			right.next()
		}
	}
	for ; left.hasNext(); left.next() {
		t, v := left.at()
		result = result.append(t, v, size)
	}
	for ; right.hasNext(); right.next() {
		t, v := right.at()
		result = result.append(t, v, size)
	}
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
