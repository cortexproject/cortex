package batch

import (
	"github.com/weaveworks/cortex/pkg/chunk"
)

type nonOverlappingIterator struct {
	curr   int
	chunks []*chunkIterator
	input  batchStream
	output batchStream
}

// newNonOverlappingIterator returns a single iterator over an slice of sorted,
// non-overlapping iterators.
func newNonOverlappingIterator(chunks []chunk.Chunk) *nonOverlappingIterator {
	its := make([]*chunkIterator, 0, len(chunks))
	for _, c := range chunks {
		its = append(its, newChunkIterator(c))
	}

	return &nonOverlappingIterator{
		chunks: its,
		input:  make(batchStream, 0, 2),
		output: make(batchStream, 0, 2),
	}
}

func (it *nonOverlappingIterator) Seek(t int64) bool {
	for ; it.curr < len(it.chunks); it.curr++ {
		if it.chunks[it.curr].Seek(t) {
			return true
		} else if it.chunks[it.curr].Err() != nil {
			return false
		}
	}

	return false
}

func (it *nonOverlappingIterator) Next() bool {
	switch len(it.input) {
	case 1:
		it.input = it.input[:0]
	case 2:
		it.input[0] = it.input[1]
		it.input = it.input[:1]
	}
	it.buildNextBatch()
	return len(it.input) > 0
}

func (it *nonOverlappingIterator) buildNextBatch() {
	for len(it.input) < 2 && it.curr < len(it.chunks) {
		if it.chunks[it.curr].Next() {
			it.input = append(it.input, it.chunks[it.curr].Batch())
		} else if it.chunks[it.curr].Err() != nil {
			return
		} else {
			it.curr++
		}
	}
	it.output = mergeBatches(it.input, it.output[:2])
	copy(it.input[:len(it.output)], it.output)
	it.input = it.input[:len(it.output)]
}

func (it *nonOverlappingIterator) AtTime() int64 {
	return it.input[0].timestamps[0]
}

func (it *nonOverlappingIterator) Batch() batch {
	return it.input[0]
}

func (it *nonOverlappingIterator) Err() error {
	if it.curr < len(it.chunks) {
		return it.chunks[it.curr].Err()
	}
	return nil
}
