package batch

import "fmt"

// batchStream deals with iteratoring through multiple, non-overlapping batches,
// and building new slices of non-overlapping batches.
type batchStream []batch

func (bs batchStream) Print() {
	fmt.Println("[")
	for _, b := range bs {
		b.Print()
	}
	fmt.Println("]")
}

func (bs *batchStream) append(t int64, v float64) {
	if len(*bs) == 0 || (*bs)[len(*bs)-1].index == batchSize {
		*bs = append(*bs, batch{})
	}
	b := &(*bs)[len(*bs)-1]
	b.timestamps[b.index] = t
	b.values[b.index] = v
	b.index++
	b.length++
}

func (bs *batchStream) reset() {
	for i := range *bs {
		(*bs)[i].index = 0
	}
}

func (bs *batchStream) hasNext() bool {
	return len(*bs) > 0
}

func (bs *batchStream) next() {
	(*bs)[0].index++
	if (*bs)[0].index >= (*bs)[0].length {
		*bs = (*bs)[1:]
	}
}

// destructively iterate through the stream of batches.
func (bs *batchStream) at() (int64, float64) {
	b := &(*bs)[0]
	return b.timestamps[b.index], b.values[b.index]
}

func (bs batchStream) len() int {
	result := 0
	for i := range bs {
		result += (bs)[i].length
	}
	return result
}

func mergeBatches(batches batchStream, result batchStream) batchStream {
	switch len(batches) {
	case 0:
		return nil
	case 1:
		copy(result[:1], batches)
		return result[:1]
	case 2:
		return mergeStreams(batches[0:1], batches[1:2], result)
	default:
		n := len(batches) / 2
		left := mergeBatches(batches[n:], result[n:])
		right := mergeBatches(batches[:n], result[:n])

		batches = mergeStreams(left, right, batches)
		result = result[:len(batches)]
		copy(result, batches)

		return result[:len(batches)]
	}
}

func mergeStreams(left, right batchStream, result batchStream) batchStream {
	result.reset()
	result = result[:0]

	for left.hasNext() && right.hasNext() {
		t1, v1 := left.at()
		t2, v2 := right.at()
		if t1 < t2 {
			result.append(t1, v1)
			left.next()
		} else if t1 > t2 {
			result.append(t2, v2)
			right.next()
		} else {
			result.append(t1, v1)
			left.next()
			right.next()
		}
	}
	for ; left.hasNext(); left.next() {
		result.append(left.at())
	}
	for ; right.hasNext(); right.next() {
		result.append(right.at())
	}
	result.reset()
	return result
}
