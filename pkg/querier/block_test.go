package querier

import (
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func Test_seriesToChunks(t *testing.T) {
	t.Parallel()

	// Define a struct used to hold the expected chunk data we wanna assert on
	type expectedChunk struct {
		from    model.Time
		through model.Time
		metric  labels.Labels
		samples []model.SamplePair
	}

	// Init some test fixtures
	minTimestamp := time.Unix(1, 0)
	maxTimestamp := time.Unix(10, 0)

	tests := map[string]struct {
		series         *storepb.Series
		expectedChunks []expectedChunk
	}{
		"empty series": {
			series:         &storepb.Series{},
			expectedChunks: []expectedChunk{},
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
			expectedChunks: []expectedChunk{
				{
					from:    model.TimeFromUnixNano(minTimestamp.UnixNano()),
					through: model.TimeFromUnixNano(maxTimestamp.UnixNano()),
					metric: labels.Labels{
						{Name: "foo", Value: "bar"},
					},
					samples: []model.SamplePair{
						{Timestamp: model.TimeFromUnixNano(time.Unix(1, 0).UnixNano()), Value: model.SampleValue(1)},
						{Timestamp: model.TimeFromUnixNano(time.Unix(2, 0).UnixNano()), Value: model.SampleValue(2)},
					},
				},
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actualChunks := seriesToChunks("test", testData.series)
			require.Equal(t, len(testData.expectedChunks), len(actualChunks))

			for i, actual := range actualChunks {
				expected := testData.expectedChunks[i]

				assert.Equal(t, "test", actual.UserID)
				assert.Equal(t, expected.from, actual.From)
				assert.Equal(t, expected.through, actual.Through)
				assert.Equal(t, expected.metric, actual.Metric)

				samples, err := actual.Samples(actual.From, actual.Through)
				require.NoError(t, err)
				assert.Equal(t, expected.samples, samples)
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
