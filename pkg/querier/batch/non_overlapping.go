package batch

import (
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	promchunk "github.com/cortexproject/cortex/pkg/chunk"
)

type nonOverlappingIterator struct {
	curr   int
	chunks []GenericChunk
	iter   chunkIterator
}

// newNonOverlappingIterator returns a single iterator over an slice of sorted,
// non-overlapping iterators.
func newNonOverlappingIterator(chunks []GenericChunk) *nonOverlappingIterator {
	it := &nonOverlappingIterator{
		chunks: chunks,
	}
	it.iter.reset(it.chunks[0])
	return it
}

func (it *nonOverlappingIterator) Seek(t int64, size int) chunkenc.ValueType {
	for {
		if valType := it.iter.Seek(t, size); valType != chunkenc.ValNone {
			return valType
		} else if it.iter.Err() != nil {
			return chunkenc.ValNone
		} else if !it.next() {
			return chunkenc.ValNone
		}
	}
}

func (it *nonOverlappingIterator) MaxCurrentChunkTime() int64 {
	return it.iter.MaxCurrentChunkTime()
}

func (it *nonOverlappingIterator) Next(size int) chunkenc.ValueType {
	for {
		if valType := it.iter.Next(size); valType != chunkenc.ValNone {
			return valType
		} else if it.iter.Err() != nil {
			return chunkenc.ValNone
		} else if !it.next() {
			return chunkenc.ValNone
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
