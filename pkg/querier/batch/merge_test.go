package batch

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/cortex/pkg/chunk"
)

func TestMergeIter(t *testing.T) {
	chunk1 := mkChunk(t, 0, 100)
	chunk2 := mkChunk(t, model.TimeFromUnix(25), 100)
	chunk3 := mkChunk(t, model.TimeFromUnix(50), 100)
	chunk4 := mkChunk(t, model.TimeFromUnix(75), 100)
	chunk5 := mkChunk(t, model.TimeFromUnix(100), 100)
	iter := newBatchMergeIterator([]chunk.Chunk{chunk1, chunk2, chunk3, chunk4, chunk5})
	testIter(t, 0, 200, newBatchIteratorAdapter(iter))
}
