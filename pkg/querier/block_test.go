package querier

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func Test_seriesToChunks(t *testing.T) {
	t.Parallel()

	// Init some test fixtures
	minTimestamp := time.Unix(1, 0)
	maxTimestamp := time.Unix(10, 0)

	tests := map[string]struct {
		series          *storepb.Series
		expectedMetric  labels.Labels
		expectedSamples []model.SamplePair
		expectedErr     string
	}{
		"empty series": {
			series:         &storepb.Series{},
			expectedMetric: labels.Labels{},
			expectedErr:    "no chunks",
		},
		"should remove the external label added by the shipper": {
			series: &storepb.Series{
				Labels: []storepb.Label{
					{Name: tsdb.TenantIDExternalLabel, Value: "test"},
					{Name: "foo", Value: "bar"},
				},
				Chunks: []storepb.AggrChunk{
					{MinTime: minTimestamp.Unix() * 1000, MaxTime: maxTimestamp.Unix() * 1000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: mockTSDBChunkData()}},
				},
			},
			expectedMetric: labels.Labels{
				{Name: "foo", Value: "bar"},
			},
			expectedSamples: []model.SamplePair{
				{Timestamp: model.TimeFromUnixNano(time.Unix(1, 0).UnixNano()), Value: model.SampleValue(1)},
				{Timestamp: model.TimeFromUnixNano(time.Unix(2, 0).UnixNano()), Value: model.SampleValue(2)},
			},
		},
		"should return error on failure while reading encoded chunk data": {
			series: &storepb.Series{
				Labels: []storepb.Label{{Name: "foo", Value: "bar"}},
				Chunks: []storepb.AggrChunk{
					{MinTime: minTimestamp.Unix() * 1000, MaxTime: maxTimestamp.Unix() * 1000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: []byte{0, 1}}},
				},
			},
			expectedMetric: labels.Labels{labels.Label{Name: "foo", Value: "bar"}},
			expectedErr:    `cannot iterate chunk for series: {foo="bar"} min time: 1000 max time: 10000: EOF`,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			series := newBlockQuerierSeries(testData.series.Labels, testData.series.Chunks)

			assert.Equal(t, testData.expectedMetric, series.Labels())

			sampleIx := 0

			it := series.Iterator()
			for it.Next() {
				ts, val := it.At()
				require.True(t, sampleIx < len(testData.expectedSamples))
				assert.Equal(t, int64(testData.expectedSamples[sampleIx].Timestamp), ts)
				assert.Equal(t, float64(testData.expectedSamples[sampleIx].Value), val)
				sampleIx++
			}

			if testData.expectedErr != "" {
				require.EqualError(t, it.Err(), testData.expectedErr)
			} else {
				require.NoError(t, it.Err())
			}
		})
	}
}

func mockTSDBChunkData() []byte {
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	appender.Append(time.Unix(1, 0).Unix()*1000, 1)
	appender.Append(time.Unix(2, 0).Unix()*1000, 2)

	return chunk.Bytes()
}

func mockTSDBChunkDataWithInvalidTimestampOrder() []byte {
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	appender.Append(time.Unix(1, 0).Unix()*1000, 1)
	appender.Append(time.Unix(0, 0).Unix()*1000, 2)

	return chunk.Bytes()
}
