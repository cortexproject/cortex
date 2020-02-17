package querier

import (
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
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

func TestBlockQuerierSeriesSet(t *testing.T) {
	now := time.Now()

	bss := &blockQuerierSeriesSet{
		seriesClient: &mockSeriesClient{responses: []*storepb.SeriesResponse{
			storepb.NewSeriesResponse(&storepb.Series{
				Labels: mkLabels("__name__", "first"),
				Chunks: []storepb.AggrChunk{
					createChunkWithSineSamples(now, now.Add(100*time.Second), 3*time.Millisecond), // ceil(100 / 0.003) samples (= 33334)
				},
			}),

			// warning series response is ignored
			storepb.NewWarnSeriesResponse(errors.New("test error")),

			// continuation of previous series
			storepb.NewSeriesResponse(&storepb.Series{
				Labels: mkLabels("__name__", "first"),
				Chunks: []storepb.AggrChunk{
					createChunkWithSineSamples(now.Add(100*time.Second), now.Add(200*time.Second), 3*time.Millisecond), // ceil(100 / 0.003) samples more, 66668 in total
				},
			}),

			// new series
			storepb.NewSeriesResponse(&storepb.Series{
				Labels: mkLabels("__name__", "second"),
				Chunks: []storepb.AggrChunk{
					createChunkWithSineSamples(now, now.Add(200*time.Second), 5*time.Millisecond), // 200 / 0.005 (= 40000 samples)
				},
			}),
		}},
	}

	verifyNextSeries(t, bss, labels.FromStrings("__name__", "first"), 66668)
	verifyNextSeries(t, bss, labels.FromStrings("__name__", "second"), 40000)
	require.False(t, bss.Next())
}

func verifyNextSeries(t *testing.T, ss storage.SeriesSet, labels labels.Labels, samples int) {
	require.True(t, ss.Next())

	s := ss.At()
	require.Equal(t, labels, s.Labels())

	count := 0
	for it := s.Iterator(); it.Next(); {
		count++
		ts, v := it.At()
		require.Equal(t, math.Sin(float64(ts)), v)
	}

	require.Equal(t, samples, count)
}

type mockSeriesClient struct {
	responses []*storepb.SeriesResponse

	ix int
}

func (m *mockSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if m.ix < len(m.responses) {
		ix := m.ix
		m.ix++
		return m.responses[ix], nil
	}
	return nil, io.EOF
}

func createChunkWithSineSamples(minTime, maxTime time.Time, step time.Duration) storepb.AggrChunk {
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	minT := minTime.Unix() * 1000
	maxT := maxTime.Unix() * 1000
	stepMillis := step.Milliseconds()

	for t := minT; t < maxT; t += stepMillis {
		appender.Append(t, math.Sin(float64(t)))
	}

	return storepb.AggrChunk{
		MinTime: minT,
		MaxTime: maxT,
		Raw: &storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: chunk.Bytes(),
		},
	}
}

func mkLabels(s ...string) []storepb.Label {
	result := []storepb.Label{}

	for i := 0; i+1 < len(s); i = i + 2 {
		result = append(result, storepb.Label{
			Name:  s[i],
			Value: s[i+1],
		})
	}

	return result
}
