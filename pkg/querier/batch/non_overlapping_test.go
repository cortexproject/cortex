package batch

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/cortex/pkg/chunk"
)

func TestNonOverlappingIter(t *testing.T) {
	cs := []chunk.Chunk{}
	for i := int64(0); i < 100; i++ {
		cs = append(cs, mkChunk(t, model.TimeFromUnix(i*100), 100))
	}
	iter := newNonOverlappingIterator(cs)
	testIter(t, 0, 100*100, newBatchIteratorAdapter(iter))
}
