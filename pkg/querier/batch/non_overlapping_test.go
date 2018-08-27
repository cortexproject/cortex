package batch

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/cortex/pkg/chunk"
)

func TestNonOverlappingIter(t *testing.T) {
	cs := []chunk.Chunk{}
	for i := int64(0); i < 100; i++ {
		cs = append(cs, mkChunk(t, model.TimeFromUnix(i*10), 10))
	}
	testIter(t, 10*100, newIteratorAdapter(newNonOverlappingIterator(cs)))
	testSeek(t, 10*100, newIteratorAdapter(newNonOverlappingIterator(cs)))
}

func TestNonOverlappingIterSparse(t *testing.T) {
	cs := []chunk.Chunk{
		mkChunk(t, model.TimeFromUnix(0), 1),
		mkChunk(t, model.TimeFromUnix(1), 3),
		mkChunk(t, model.TimeFromUnix(4), 1),
		mkChunk(t, model.TimeFromUnix(5), 90),
		mkChunk(t, model.TimeFromUnix(95), 1),
		mkChunk(t, model.TimeFromUnix(96), 4),
	}
	testIter(t, 100, newIteratorAdapter(newNonOverlappingIterator(cs)))
	testSeek(t, 100, newIteratorAdapter(newNonOverlappingIterator(cs)))
}
