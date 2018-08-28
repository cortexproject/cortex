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
	}
	it.iter.reset(it.chunks[0])
	return it
}

func (it *nonOverlappingIterator) Seek(t int64, size int) bool {
	for {
		if it.iter.Seek(t, size) {
			return true
		} else if it.iter.Err() != nil {
			return false
		} else if !it.next() {
			return false
		}
	}
}

func (it *nonOverlappingIterator) Next(size int) bool {
	for {
		if it.iter.Next(size) {
			return true
		} else if it.iter.Err() != nil {
			return false
		} else if !it.next() {
			return false
		}
	}
}

func (it *nonOverlappingIterator) next() bool {
	it.curr++
	if it.curr < len(it.chunks) {
		it.iter.reset(it.chunks[it.curr])
	}
	return it.curr < len(it.chunks)
}

func (it *nonOverlappingIterator) AtTime() int64 {
	return it.iter.AtTime()
}

func (it *nonOverlappingIterator) Batch() promchunk.Batch {
	return it.iter.Batch()
}

func (it *nonOverlappingIterator) Err() error {
	if it.curr < len(it.chunks) {
		return it.iter.Err()
	}
	return nil
}
