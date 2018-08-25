package batch

import (
	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

type nonOverlappingIterator struct {
	curr   int
	chunks []chunk.Chunk
	iter   chunkIterator
	input  batchStream
	output batchStream
}

// newNonOverlappingIterator returns a single iterator over an slice of sorted,
// non-overlapping iterators.
func newNonOverlappingIterator(chunks []chunk.Chunk) *nonOverlappingIterator {
	i := &nonOverlappingIterator{
		chunks: chunks,
		input:  make(batchStream, 0, 2),
		output: make(batchStream, 0, 2),
	}
	i.iter.reset(chunks[0])
	return i
}

func (it *nonOverlappingIterator) next() {
	it.curr++
	if it.curr < len(it.chunks) {
		it.iter.reset(it.chunks[it.curr])
	}
}

func (it *nonOverlappingIterator) Seek(t int64) bool {
	for it.curr < len(it.chunks) {
		if it.iter.Seek(t) {
			return true
		} else if it.iter.Err() != nil {
			return false
		} else {
			it.next()
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
	// We have to return full batches, otherwise the implementation of merge iterator
	// becomes harder & slower.  But in most cases, we can skip it.

	for len(it.input) < 2 && it.curr < len(it.chunks) {
		if it.iter.Next() {
			it.input = append(it.input, it.iter.Batch())
		} else if it.iter.Err() != nil {
			return
		} else {
			it.next()
		}
		if len(it.input) > 0 && it.input[0].Length == promchunk.BatchSize {
			return
		}
	}

	if len(it.input) > 1 {
		it.output = mergeBatches(it.input, it.output[:2])
		copy(it.input[:len(it.output)], it.output)
		it.input = it.input[:len(it.output)]
	}
}

func (it *nonOverlappingIterator) AtTime() int64 {
	return it.input[0].Timestamps[0]
}

func (it *nonOverlappingIterator) Batch() promchunk.Batch {
	return it.input[0]
}

func (it *nonOverlappingIterator) Err() error {
	if it.curr < len(it.chunks) {
		return it.iter.Err()
	}
	return nil
}
