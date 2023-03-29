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
	}

	for _, scenario := range scenarios {
		name := fmt.Sprintf("chunks: %d samples per chunk: %d duplication factor: %d encoding: %s",
			scenario.numChunks,
			scenario.numSamplesPerChunk,
			scenario.duplicationFactor,
			scenario.enc.String())

		chunks := createChunks(b, step, scenario.numChunks, scenario.numSamplesPerChunk, scenario.duplicationFactor, scenario.enc)

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
	t.Parallel()
	chunkOne := mkChunk(t, step, model.Time(1*step/time.Millisecond), 1, promchunk.PrometheusXorChunk)
	chunkTwo := mkChunk(t, step, model.Time(10*step/time.Millisecond), 1, promchunk.PrometheusXorChunk)
	chunks := []chunk.Chunk{chunkOne, chunkTwo}

	sut := NewChunkMergeIterator(chunks, 0, 0)

	// Following calls mimics Prometheus's query engine behaviour for VectorSelector.
	require.True(t, sut.Next() != chunkenc.ValNone)
	require.True(t, sut.Seek(0) != chunkenc.ValNone)

	actual, val := sut.At()
	require.Equal(t, float64(1*time.Second/time.Millisecond), val) // since mkChunk use ts as value.
	require.Equal(t, int64(1*time.Second/time.Millisecond), actual)
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
