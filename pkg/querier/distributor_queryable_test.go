package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	mint, maxt = 0, 10
)

func TestDistributorQuerier_SelectShouldHonorQueryIngestersWithin(t *testing.T) {

	now := time.Now()

	tests := map[string]struct {
		querySeries          bool
		queryStoreForLabels  bool
		queryIngestersWithin time.Duration
		queryMinT            int64
		queryMaxT            int64
		expectedMinT         int64
		expectedMaxT         int64
	}{
		"should not manipulate query time range if queryIngestersWithin is disabled": {
			queryIngestersWithin: 0,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should not manipulate query time range if queryIngestersWithin is enabled but query min time is newer": {
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-50 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-50 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should manipulate query time range if queryIngestersWithin is enabled and query min time is older": {
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-60 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should skip the query if the query max time is older than queryIngestersWithin": {
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-90 * time.Minute)),
			expectedMinT:         0,
			expectedMaxT:         0,
		},
		"should not manipulate query time range if queryIngestersWithin is enabled and query max time is older, but the query is for /series": {
			querySeries:          true,
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-90 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-90 * time.Minute)),
		},
		"should manipulate query time range if queryIngestersWithin is enabled and queryStoreForLabels is enabled": {
			querySeries:          true,
			queryStoreForLabels:  true,
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-60 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
	}

	for _, streamingMetadataEnabled := range []bool{false, true} {
		for testName, testData := range tests {
			testData := testData
			t.Run(fmt.Sprintf("%s (streaming metadata enabled: %t)", testName, streamingMetadataEnabled), func(t *testing.T) {
				t.Parallel()

				distributor := &MockDistributor{}
				distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)
				distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]model.Metric{}, nil)
				distributor.On("MetricsForLabelMatchersStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]model.Metric{}, nil)

				ctx := user.InjectOrgID(context.Background(), "test")
				queryable := newDistributorQueryable(distributor, streamingMetadataEnabled, nil, testData.queryIngestersWithin, testData.queryStoreForLabels)
				querier, err := queryable.Querier(testData.queryMinT, testData.queryMaxT)
				require.NoError(t, err)

				limits := DefaultLimitsConfig()
				overrides, err := validation.NewOverrides(limits, nil)
				require.NoError(t, err)

				start, end, err := validateQueryTimeRange(ctx, "test", testData.queryMinT, testData.queryMaxT, overrides, 0)
				require.NoError(t, err)
				// Select hints are passed by Prometheus when querying /series.
				var hints *storage.SelectHints
				if testData.querySeries {
					hints = &storage.SelectHints{
						Start: start,
						End:   end,
						Func:  "series",
					}
				}

				seriesSet := querier.Select(ctx, true, hints)
				require.NoError(t, seriesSet.Err())

				if testData.expectedMinT == 0 && testData.expectedMaxT == 0 {
					assert.Len(t, distributor.Calls, 0)
				} else {
					require.Len(t, distributor.Calls, 1)
					assert.InDelta(t, testData.expectedMinT, int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), float64(5*time.Second.Milliseconds()))
					assert.Equal(t, testData.expectedMaxT, int64(distributor.Calls[0].Arguments.Get(2).(model.Time)))
				}
			})
		}
	}
}

func TestDistributorQueryableFilter(t *testing.T) {
	t.Parallel()

	d := &MockDistributor{}
	dq := newDistributorQueryable(d, false, nil, 1*time.Hour, true)

	now := time.Now()

	queryMinT := util.TimeToMillis(now.Add(-5 * time.Minute))
	queryMaxT := util.TimeToMillis(now)

	require.True(t, dq.UseQueryable(now, queryMinT, queryMaxT))
	require.True(t, dq.UseQueryable(now.Add(time.Hour), queryMinT, queryMaxT))

	// Same query, hour+1ms later, is not sent to ingesters.
	require.False(t, dq.UseQueryable(now.Add(time.Hour).Add(1*time.Millisecond), queryMinT, queryMaxT))
}

func TestIngesterStreaming(t *testing.T) {
	t.Parallel()

	// We need to make sure that there is at least one chunk present,
	// else no series will be selected.
	promChunk := chunkenc.NewXORChunk()
	appender, err := promChunk.Appender()
	require.NoError(t, err)
	appender.Append(int64(model.ZeroSamplePair.Timestamp), float64(model.ZeroSamplePair.Value))

	clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{
		chunk.NewChunk(nil, promChunk, model.Earliest, model.Earliest),
	})
	require.NoError(t, err)

	d := &MockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: []cortexpb.LabelAdapter{
						{Name: "bar", Value: "baz"},
					},
					Chunks: clientChunks,
				},
				{
					Labels: []cortexpb.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Chunks: clientChunks,
				},
			},
		},
		nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := newDistributorQueryable(d, true, batch.NewChunkMergeIterator, 0, true)
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt})
	require.NoError(t, seriesSet.Err())

	require.True(t, seriesSet.Next())
	series := seriesSet.At()
	require.Equal(t, labels.Labels{{Name: "bar", Value: "baz"}}, series.Labels())

	require.True(t, seriesSet.Next())
	series = seriesSet.At()
	require.Equal(t, labels.Labels{{Name: "foo", Value: "bar"}}, series.Labels())

	require.False(t, seriesSet.Next())
	require.NoError(t, seriesSet.Err())
}

func TestIngesterStreamingMixedResults(t *testing.T) {
	t.Parallel()

	const (
		mint = 0
		maxt = 10000
	)
	s1 := []cortexpb.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2, TimestampMs: 2000},
		{Value: 3, TimestampMs: 3000},
		{Value: 4, TimestampMs: 4000},
		{Value: 5, TimestampMs: 5000},
	}
	s2 := []cortexpb.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2.5, TimestampMs: 2500},
		{Value: 3, TimestampMs: 3000},
		{Value: 5.5, TimestampMs: 5500},
	}

	mergedSamplesS1S2 := []cortexpb.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2, TimestampMs: 2000},
		{Value: 2.5, TimestampMs: 2500},
		{Value: 3, TimestampMs: 3000},
		{Value: 4, TimestampMs: 4000},
		{Value: 5, TimestampMs: 5000},
		{Value: 5.5, TimestampMs: 5500},
	}

	d := &MockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, s1),
				},
				{
					Labels: []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Chunks: convertToChunks(t, s1),
				},
			},

			Timeseries: []cortexpb.TimeSeries{
				{
					Labels:  []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Samples: s2,
				},
				{
					Labels:  []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "three"}},
					Samples: s1,
				},
			},
		},
		nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := newDistributorQueryable(d, true, batch.NewChunkMergeIterator, 0, true)
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt}, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"))
	require.NoError(t, seriesSet.Err())

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.Labels{{Name: labels.MetricName, Value: "one"}}, s1)

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.Labels{{Name: labels.MetricName, Value: "three"}}, s1)

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.Labels{{Name: labels.MetricName, Value: "two"}}, mergedSamplesS1S2)

	require.False(t, seriesSet.Next())
	require.NoError(t, seriesSet.Err())
}

func verifySeries(t *testing.T, series storage.Series, l labels.Labels, samples []cortexpb.Sample) {
	require.Equal(t, l, series.Labels())

	it := series.Iterator(nil)
	for _, s := range samples {
		require.NotEqual(t, it.Next(), chunkenc.ValNone)
		require.Nil(t, it.Err())
		ts, v := it.At()
		require.Equal(t, s.Value, v)
		require.Equal(t, s.TimestampMs, ts)
	}
	require.Equal(t, it.Next(), chunkenc.ValNone)
	require.Nil(t, it.Err())
}
func TestDistributorQuerier_LabelNames(t *testing.T) {
	t.Parallel()

	someMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	labelNames := []string{"foo", "job"}

	for _, streamingEnabled := range []bool{false, true} {
		streamingEnabled := streamingEnabled
		t.Run("with matchers", func(t *testing.T) {
			t.Parallel()

			metrics := []model.Metric{
				{"foo": "bar"},
				{"job": "baz"},
				{"job": "baz", "foo": "boom"},
			}
			d := &MockDistributor{}
			d.On("MetricsForLabelMatchers", mock.Anything, model.Time(mint), model.Time(maxt), someMatchers).
				Return(metrics, nil)
			d.On("MetricsForLabelMatchersStream", mock.Anything, model.Time(mint), model.Time(maxt), someMatchers).
				Return(metrics, nil)

			queryable := newDistributorQueryable(d, streamingEnabled, nil, 0, true)
			querier, err := queryable.Querier(mint, maxt)
			require.NoError(t, err)

			ctx := context.Background()
			names, warnings, err := querier.LabelNames(ctx, someMatchers...)
			require.NoError(t, err)
			assert.Empty(t, warnings)
			assert.Equal(t, labelNames, names)
		})
	}
}

func convertToChunks(t *testing.T, samples []cortexpb.Sample) []client.Chunk {
	// We need to make sure that there is at least one chunk present,
	// else no series will be selected.
	chk := chunkenc.NewXORChunk()
	appender, err := chk.Appender()
	require.NoError(t, err)

	for _, s := range samples {
		appender.Append(s.TimestampMs, s.Value)
	}

	c := chunk.NewChunk(nil, chk, model.Time(samples[0].TimestampMs), model.Time(samples[len(samples)-1].TimestampMs))
	clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{c})
	require.NoError(t, err)

	return clientChunks
}
