package batch

import (
	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

const bufferBatches = 4

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
	it := &nonOverlappingIterator{
		chunks: chunks,
		input:  make(batchStream, 0, bufferBatches),
		output: make(batchStream, 0, bufferBatches),
	}
	it.iter.reset(it.chunks[0])
	return it
}

func (it *nonOverlappingIterator) Seek(t int64) bool {
	it.input = it.input[:0]
	it.curr = 0
	it.iter.reset(it.chunks[0])

	for ; it.curr < len(it.chunks); it.next() {
		if it.iter.Seek(t) {
			return it.buildNextBatch()
		} else if it.iter.Err() != nil {
			return false
		}
	}

	return false
}

func (it *nonOverlappingIterator) Next() bool {
	// Drop the first cached batch in the queue.
	if len(it.input) > 0 {
		copy(it.input, it.input[1:])
		it.input = it.input[:len(it.input)-1]
	}

	for ; it.curr < len(it.chunks); it.next() {
		if it.iter.Next() {
			return it.buildNextBatch()
		} else if it.iter.Err() != nil {
			return false
		}
	}

	return len(it.input) > 0
}

func (it *nonOverlappingIterator) buildNextBatch() bool {
	// When this function is called the iterator has already been next'd.
	// We need to leave the iterator un-next'd, so it can be next'd or seek'd again.

	// Need to ensure we build a batch size worth of samples which may mean we
	// have to consume many batches, for instance if we hit chunks with only a
	// few samples in them.
	required := promchunk.BatchSize
	for _, batch := range it.input {
		required -= batch.Length
	}

	for required > 0 && it.curr < len(it.chunks) {
		batch := it.iter.Batch()
		required -= batch.Length
		it.input = append(it.input, batch)

		// If we have all the samples we need then leave the current iterator
		// un-next'd to preserver the invariants at the top of this function.
		if required <= 0 {
			break
		}

		for !it.iter.Next() && it.curr < len(it.chunks) {
			if it.iter.Err() != nil {
				return false
			}
			it.next()
		}
	}

	// We have to return full batches, otherwise the implementation of merge iterator
	// becomes harder & slower.  But in some cases we can skip it.
	if len(it.input) > 0 && it.input[0].Length == promchunk.BatchSize {
		return true
	}

	it.output = mergeBatches(it.input, it.output[:bufferBatches])
	copy(it.input[:len(it.output)], it.output)
	it.input = it.input[:len(it.output)]
	return len(it.input) > 0
}

func (it *nonOverlappingIterator) next() {
	it.curr++
	if it.curr < len(it.chunks) {
		it.iter.reset(it.chunks[it.curr])
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
