package batch

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	histogram_util "github.com/cortexproject/cortex/pkg/util/histogram"
)

func BenchmarkNewChunkMergeIterator_CreateAndIterate(b *testing.B) {
	scenarios := []struct {
		numChunks          int
		numSamplesPerChunk int
		duplicationFactor  int
		enc                promchunk.Encoding
	}{
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusXorChunk},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusXorChunk},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusHistogramChunk},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusHistogramChunk},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusHistogramChunk},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusHistogramChunk},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusHistogramChunk},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusHistogramChunk},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusFloatHistogramChunk},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusFloatHistogramChunk},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusFloatHistogramChunk},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusFloatHistogramChunk},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 1, enc: promchunk.PrometheusFloatHistogramChunk},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 3, enc: promchunk.PrometheusFloatHistogramChunk},
	}

	for _, scenario := range scenarios {
		name := fmt.Sprintf("chunks: %d samples per chunk: %d duplication factor: %d encoding: %s",
			scenario.numChunks,
			scenario.numSamplesPerChunk,
			scenario.duplicationFactor,
			scenario.enc.String())

		chunks := createChunks(b, step, scenario.numChunks, scenario.numSamplesPerChunk, scenario.duplicationFactor, scenario.enc)

		b.ResetTimer()
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for n := 0; n < b.N; n++ {
				it := NewChunkMergeIterator(chunks, 0, 0)
				for it.Next() != chunkenc.ValNone {
					it.At()
				}

				// Ensure no error occurred.
				if it.Err() != nil {
					b.Fatal(it.Err().Error())
				}
			}
		})
	}
}

func BenchmarkNewChunkMergeIterator_Seek(b *testing.B) {
	scenarios := []struct {
		numChunks          int
		numSamplesPerChunk int
		duplicationFactor  int
		seekStep           time.Duration
		scrapeInterval     time.Duration
		enc                promchunk.Encoding
	}{
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 30 * time.Second, seekStep: 30 * time.Second / 2, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 30 * time.Second, seekStep: 30 * time.Second, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 30 * time.Second, seekStep: 30 * time.Second * 2, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 30 * time.Second, seekStep: 30 * time.Second * 10, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 30 * time.Second, seekStep: 30 * time.Second * 30, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 30 * time.Second, seekStep: 30 * time.Second * 50, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 30 * time.Second, seekStep: 30 * time.Second * 100, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 30 * time.Second, seekStep: 30 * time.Second * 200, enc: promchunk.PrometheusXorChunk},

		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 10 * time.Second, seekStep: 10 * time.Second / 2, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 10 * time.Second, seekStep: 10 * time.Second, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 10 * time.Second, seekStep: 10 * time.Second * 2, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 10 * time.Second, seekStep: 10 * time.Second * 10, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 10 * time.Second, seekStep: 10 * time.Second * 30, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 10 * time.Second, seekStep: 10 * time.Second * 50, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 10 * time.Second, seekStep: 10 * time.Second * 100, enc: promchunk.PrometheusXorChunk},
		{numChunks: 1000, numSamplesPerChunk: 120, duplicationFactor: 3, scrapeInterval: 10 * time.Second, seekStep: 10 * time.Second * 200, enc: promchunk.PrometheusXorChunk},
	}

	for _, scenario := range scenarios {
		name := fmt.Sprintf("scrapeInterval %vs seekStep: %vs",
			scenario.scrapeInterval.Seconds(),
			scenario.seekStep.Seconds())

		chunks := createChunks(b, scenario.scrapeInterval, scenario.numChunks, scenario.numSamplesPerChunk, scenario.duplicationFactor, scenario.enc)

		b.ResetTimer()
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for n := 0; n < b.N; n++ {
				it := NewChunkMergeIterator(chunks, 0, 0)
				i := int64(0)
				for it.Seek(i*scenario.seekStep.Milliseconds()) != chunkenc.ValNone {
					i++
				}
			}
		})
	}
}

func TestSeekCorrectlyDealWithSinglePointChunks(t *testing.T) {
	histograms := histogram_util.GenerateTestHistograms(1000, 1000, 1, 5, 20)
	for _, enc := range []promchunk.Encoding{
		promchunk.PrometheusXorChunk,
		promchunk.PrometheusHistogramChunk,
		promchunk.PrometheusFloatHistogramChunk,
	} {
		valType := enc.ChunkValueType()
		chunkOne := mkChunk(t, step, model.Time(1*step/time.Millisecond), 1, enc)
		chunkTwo := mkChunk(t, step, model.Time(10*step/time.Millisecond), 1, enc)
		chunks := []chunk.Chunk{chunkOne, chunkTwo}

		sut := NewChunkMergeIterator(chunks, 0, 0)

		// Following calls mimics Prometheus's query engine behaviour for VectorSelector.
		require.Equal(t, valType, sut.Next())
		require.Equal(t, valType, sut.Seek(0))

		switch enc {
		case promchunk.PrometheusXorChunk:
			actual, val := sut.At()
			require.Equal(t, float64(1*time.Second/time.Millisecond), val) // since mkChunk use ts as value.
			require.Equal(t, int64(1*time.Second/time.Millisecond), actual)
		case promchunk.PrometheusHistogramChunk:
			actual, val := sut.AtHistogram(nil)
			require.Equal(t, histograms[0], val)
			require.Equal(t, int64(1*time.Second/time.Millisecond), actual)
		case promchunk.PrometheusFloatHistogramChunk:
			actual, val := sut.AtFloatHistogram(nil)
			require.Equal(t, histograms[0].ToFloat(nil), val)
			require.Equal(t, int64(1*time.Second/time.Millisecond), actual)
		}
	}
}

func createChunks(b *testing.B, step time.Duration, numChunks, numSamplesPerChunk, duplicationFactor int, enc promchunk.Encoding) []chunk.Chunk {
	result := make([]chunk.Chunk, 0, numChunks)

	for d := 0; d < duplicationFactor; d++ {
		for c := 0; c < numChunks; c++ {
			minTime := step * time.Duration(c*numSamplesPerChunk)
			result = append(result, mkChunk(b, step, model.Time(minTime.Milliseconds()), numSamplesPerChunk, enc))
		}
	}

	return result
}
