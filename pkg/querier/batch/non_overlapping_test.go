package batch

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/prometheus/common/model"
)

func TestNonOverlappingIter(t *testing.T) {
	forEncodings(t, func(t *testing.T, enc encoding.Encoding) {
		cs := []chunk.Chunk{}
		for i := int64(0); i < 100; i++ {
			cs = append(cs, mkChunk(t, model.TimeFromUnix(i*10), 10, enc))
		}
		testIter(t, 10*100, newIteratorAdapter(newNonOverlappingIterator(cs)))
		testSeek(t, 10*100, newIteratorAdapter(newNonOverlappingIterator(cs)))
	})
}

func TestNonOverlappingIterSparse(t *testing.T) {
	forEncodings(t, func(t *testing.T, enc encoding.Encoding) {
		cs := []chunk.Chunk{
			mkChunk(t, model.TimeFromUnix(0), 1, enc),
			mkChunk(t, model.TimeFromUnix(1), 3, enc),
			mkChunk(t, model.TimeFromUnix(4), 1, enc),
			mkChunk(t, model.TimeFromUnix(5), 90, enc),
			mkChunk(t, model.TimeFromUnix(95), 1, enc),
			mkChunk(t, model.TimeFromUnix(96), 4, enc),
		}
		testIter(t, 100, newIteratorAdapter(newNonOverlappingIterator(cs)))
		testSeek(t, 100, newIteratorAdapter(newNonOverlappingIterator(cs)))
	})
}
