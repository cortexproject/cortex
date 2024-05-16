package client

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/util"
)

func TestSamplesCount(t *testing.T) {
	floatChk := util.GenerateChunk(t, time.Second, model.Time(0), 100, encoding.PrometheusXorChunk)
	histogramChk := util.GenerateChunk(t, time.Second, model.Time(0), 300, encoding.PrometheusHistogramChunk)
	floatHistogramChk := util.GenerateChunk(t, time.Second, model.Time(0), 500, encoding.PrometheusFloatHistogramChunk)
	for _, tc := range []struct {
		name          string
		chunkSeries   []TimeSeriesChunk
		expectedCount int
	}{
		{
			name: "single float chunk",
			chunkSeries: []TimeSeriesChunk{
				{
					Chunks: []Chunk{
						{Encoding: int32(encoding.PrometheusXorChunk), Data: floatChk.Data.Bytes()},
					},
				},
			},
			expectedCount: 100,
		},
		{
			name: "single histogram chunk",
			chunkSeries: []TimeSeriesChunk{
				{
					Chunks: []Chunk{
						{Encoding: int32(encoding.PrometheusHistogramChunk), Data: histogramChk.Data.Bytes()},
					},
				},
			},
			expectedCount: 300,
		},
		{
			name: "single float histogram chunk",
			chunkSeries: []TimeSeriesChunk{
				{
					Chunks: []Chunk{
						{Encoding: int32(encoding.PrometheusFloatHistogramChunk), Data: floatHistogramChk.Data.Bytes()},
					},
				},
			},
			expectedCount: 500,
		},
		{
			name: "all chunks",
			chunkSeries: []TimeSeriesChunk{
				{
					Chunks: []Chunk{
						{Encoding: int32(encoding.PrometheusXorChunk), Data: floatChk.Data.Bytes()},
						{Encoding: int32(encoding.PrometheusHistogramChunk), Data: histogramChk.Data.Bytes()},
						{Encoding: int32(encoding.PrometheusFloatHistogramChunk), Data: floatHistogramChk.Data.Bytes()},
					},
				},
			},
			expectedCount: 900,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := (&QueryStreamResponse{Chunkseries: tc.chunkSeries}).SamplesCount()
			require.Equal(t, tc.expectedCount, c)
		})
	}
}
