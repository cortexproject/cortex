package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/querier/partialdata"
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
		"should manipulate query time range if queryIngestersWithin is enabled": {
			querySeries:          true,
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
				distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]labels.Labels{}, nil)
				distributor.On("MetricsForLabelMatchersStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]labels.Labels{}, nil)

				ctx := user.InjectOrgID(context.Background(), "test")
				queryable := newDistributorQueryable(distributor, streamingMetadataEnabled, true, nil, testData.queryIngestersWithin, nil)
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
	dq := newDistributorQueryable(d, false, true, nil, 1*time.Hour, nil)

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

	now := time.Now()

	for _, enc := range encodings {
		for _, partialDataEnabled := range []bool{false, true} {
			promChunk := util.GenerateChunk(t, time.Second, model.TimeFromUnix(now.Unix()), 10, enc)
			clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{promChunk})
			require.NoError(t, err)

			d := &MockDistributor{}
			queryResponse := &client.QueryStreamResponse{
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
			}
			var partialDataErr error
			if partialDataEnabled {
				partialDataErr = partialdata.ErrPartialData
			}
			d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(queryResponse, partialDataErr)

			ctx := user.InjectOrgID(context.Background(), "0")

			queryable := newDistributorQueryable(d, true, true, batch.NewChunkMergeIterator, 0, func(string) bool {
				return partialDataEnabled
			})
			querier, err := queryable.Querier(mint, maxt)
			require.NoError(t, err)

			seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt})
			require.NoError(t, seriesSet.Err())

			require.True(t, seriesSet.Next())
			series := seriesSet.At()
			require.Equal(t, labels.Labels{{Name: "bar", Value: "baz"}}, series.Labels())
			chkIter := series.Iterator(nil)
			require.Equal(t, enc.ChunkValueType(), chkIter.Next())

			require.True(t, seriesSet.Next())
			series = seriesSet.At()
			require.Equal(t, labels.Labels{{Name: "foo", Value: "bar"}}, series.Labels())
			chkIter = series.Iterator(chkIter)
			require.Equal(t, enc.ChunkValueType(), chkIter.Next())

			require.False(t, seriesSet.Next())
			require.NoError(t, seriesSet.Err())

			if partialDataEnabled {
				require.Contains(t, seriesSet.Warnings(), partialdata.ErrPartialData.Error())
			}
		}
	}
}

func TestDistributorQuerier_LabelNames(t *testing.T) {
	t.Parallel()

	someMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	labelNames := []string{"foo", "job"}

	for _, labelNamesWithMatchers := range []bool{false, true} {
		for _, streamingEnabled := range []bool{false, true} {
			for _, partialDataEnabled := range []bool{false, true} {
				streamingEnabled := streamingEnabled
				labelNamesWithMatchers := labelNamesWithMatchers
				t.Run("with matchers", func(t *testing.T) {
					t.Parallel()

					metrics := []labels.Labels{
						labels.FromStrings("foo", "bar"),
						labels.FromStrings("job", "baz"),
						labels.FromStrings("job", "baz", "foo", "boom"),
					}
					d := &MockDistributor{}

					var partialDataErr error
					if partialDataEnabled {
						partialDataErr = partialdata.ErrPartialData
					}
					if labelNamesWithMatchers {
						d.On("LabelNames", mock.Anything, model.Time(mint), model.Time(maxt), mock.Anything, someMatchers).
							Return(labelNames, partialDataErr)
						d.On("LabelNamesStream", mock.Anything, model.Time(mint), model.Time(maxt), mock.Anything, someMatchers).
							Return(labelNames, partialDataErr)
					} else {
						d.On("MetricsForLabelMatchers", mock.Anything, model.Time(mint), model.Time(maxt), mock.Anything, someMatchers).
							Return(metrics, partialDataErr)
						d.On("MetricsForLabelMatchersStream", mock.Anything, model.Time(mint), model.Time(maxt), mock.Anything, someMatchers).
							Return(metrics, partialDataErr)
					}

					queryable := newDistributorQueryable(d, streamingEnabled, labelNamesWithMatchers, nil, 0, func(string) bool {
						return partialDataEnabled
					})
					querier, err := queryable.Querier(mint, maxt)
					require.NoError(t, err)

					ctx := context.Background()
					names, warnings, err := querier.LabelNames(ctx, nil, someMatchers...)
					require.NoError(t, err)
					if partialDataEnabled {
						assert.Contains(t, warnings, partialdata.ErrPartialData.Error())
					} else {
						assert.Empty(t, warnings)
					}
					assert.Equal(t, labelNames, names)
				})
			}
		}
	}
}
