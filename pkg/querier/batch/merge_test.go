package batch

import (
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/prometheus/common/model"
)

func TestMergeIter(t *testing.T) {
	forEncodings(t, func(t *testing.T, enc encoding.Encoding) {
		chunk1 := mkChunk(t, 0, 100, enc)
		chunk2 := mkChunk(t, model.TimeFromUnix(25), 100, enc)
		chunk3 := mkChunk(t, model.TimeFromUnix(50), 100, enc)
		chunk4 := mkChunk(t, model.TimeFromUnix(75), 100, enc)
		chunk5 := mkChunk(t, model.TimeFromUnix(100), 100, enc)
		iter := newMergeIterator([]chunk.Chunk{chunk1, chunk2, chunk3, chunk4, chunk5})
		testIter(t, 200, newIteratorAdapter(iter))
		testSeek(t, 200, newIteratorAdapter(iter))
	})
}

func TestMergeHarder(t *testing.T) {
	forEncodings(t, func(t *testing.T, enc encoding.Encoding) {
		var (
			numChunks = 24 * 15
			chunks    = make([]chunk.Chunk, 0)
			from      = model.Time(0)
			offset    = 30
			samples   = 100
		)
		for i := 0; i < numChunks; i++ {
			chunks = append(chunks, mkChunk(t, from, samples, enc))
			from = from.Add(time.Duration(offset) * time.Second)
		}
		iter := newMergeIterator(chunks)
		testIter(t, offset*numChunks+samples-offset, newIteratorAdapter(iter))
		testSeek(t, offset*numChunks+samples-offset, newIteratorAdapter(iter))
	})
}
