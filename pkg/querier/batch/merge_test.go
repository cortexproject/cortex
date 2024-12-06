package batch

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
)

func TestMergeIter(t *testing.T) {
	t.Parallel()
	forEncodings(t, func(t *testing.T, enc encoding.Encoding) {
		chunk1 := mkGenericChunk(t, 0, 100, enc)
		chunk2 := mkGenericChunk(t, model.TimeFromUnix(25), 100, enc)
		chunk3 := mkGenericChunk(t, model.TimeFromUnix(50), 100, enc)
		chunk4 := mkGenericChunk(t, model.TimeFromUnix(75), 100, enc)
		chunk5 := mkGenericChunk(t, model.TimeFromUnix(100), 100, enc)

		iter := newMergeIterator(nil, []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
		testIter(t, 200, newIteratorAdapter(iter), enc)

		iter = newMergeIterator(iter, []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
		testSeek(t, 200, newIteratorAdapter(iter), enc)
	})
}

func BenchmarkMergeIterator(b *testing.B) {
	chunks := make([]GenericChunk, 0, 10)
	for i := 0; i < 10; i++ {
		chunks = append(chunks, mkGenericChunk(b, model.Time(i*25), 120, encoding.PrometheusXorChunk))
	}
	iter := newMergeIterator(nil, chunks)

	for _, r := range []bool{true, false} {
		b.Run(fmt.Sprintf("reuse-%t", r), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if r {
					iter = newMergeIterator(iter, chunks)
				} else {
					iter = newMergeIterator(nil, chunks)
				}
				a := newIteratorAdapter(iter)
				for a.Next() != chunkenc.ValNone {

				}
			}
		})
	}
}

func TestMergeHarder(t *testing.T) {
	t.Parallel()
	forEncodings(t, func(t *testing.T, enc encoding.Encoding) {
		var (
			numChunks = 24 * 15
			chunks    = make([]GenericChunk, 0, numChunks)
			from      = model.Time(0)
			offset    = 30
			samples   = 100
		)
		for i := 0; i < numChunks; i++ {
			chunks = append(chunks, mkGenericChunk(t, from, samples, enc))
			from = from.Add(time.Duration(offset) * time.Second)
		}
		iter := newMergeIterator(nil, chunks)
		testIter(t, offset*numChunks+samples-offset, newIteratorAdapter(iter), enc)

		iter = newMergeIterator(iter, chunks)
		testSeek(t, offset*numChunks+samples-offset, newIteratorAdapter(iter), enc)
	})
}
