package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
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
				queryable := newDistributorQueryable(distributor, streamingMetadataEnabled, true, nil, testData.queryIngestersWithin, nil, 1)
				querier, err := queryable.Querier(testData.queryMinT, testData.queryMaxT)
				require.NoError(t, err)

				limits := DefaultLimitsConfig()
				overrides := validation.NewOverrides(limits, nil)

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
	dq := newDistributorQueryable(d, false, true, nil, 1*time.Hour, nil, 1)

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
			}, 1)
			querier, err := queryable.Querier(mint, maxt)
			require.NoError(t, err)

			seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt})
			require.NoError(t, seriesSet.Err())

			require.True(t, seriesSet.Next())
			series := seriesSet.At()
			require.Equal(t, labels.FromStrings("bar", "baz"), series.Labels())
			chkIter := series.Iterator(nil)
			require.Equal(t, enc.ChunkValueType(), chkIter.Next())

			require.True(t, seriesSet.Next())
			series = seriesSet.At()
			require.Equal(t, labels.FromStrings("foo", "bar"), series.Labels())
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

func TestDistributorQuerier_Retry(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "0")

	tests := map[string]struct {
		api           string
		errors        []error
		isPartialData bool
		isError       bool
	}{
		"Select - should retry": {
			api: "Select",
			errors: []error{
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
				nil,
			},
			isError:       false,
			isPartialData: false,
		},
		"Select - should return partial data after all retries": {
			api: "Select",
			errors: []error{
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
			},
			isError:       false,
			isPartialData: true,
		},
		"Select - should not retry on other error": {
			api: "Select",
			errors: []error{
				fmt.Errorf("new error"),
				partialdata.ErrPartialData,
			},
			isError:       true,
			isPartialData: false,
		},
		"LabelNames - should retry": {
			api: "LabelNames",
			errors: []error{
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
				nil,
			},
			isError:       false,
			isPartialData: false,
		},
		"LabelNames - should return partial data after all retries": {
			api: "LabelNames",
			errors: []error{
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
			},
			isError:       false,
			isPartialData: true,
		},
		"LabelNames - should not retry on other error": {
			api: "LabelNames",
			errors: []error{
				fmt.Errorf("new error"),
				partialdata.ErrPartialData,
			},
			isError:       true,
			isPartialData: false,
		},
		"LabelValues - should retry": {
			api: "LabelValues",
			errors: []error{
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
				nil,
			},
			isError:       false,
			isPartialData: false,
		},
		"LabelValues - should return partial data after all retries": {
			api: "LabelValues",
			errors: []error{
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
				partialdata.ErrPartialData,
			},
			isError:       false,
			isPartialData: true,
		},
		"LabelValues - should not retry on other error": {
			api: "LabelValues",
			errors: []error{
				fmt.Errorf("new error"),
				partialdata.ErrPartialData,
			},
			isError:       true,
			isPartialData: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			d := &MockDistributor{}

			switch tc.api {
			case "Select":
				promChunk := util.GenerateChunk(t, time.Second, model.TimeFromUnix(time.Now().Unix()), 10, promchunk.PrometheusXorChunk)
				clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{promChunk})
				require.NoError(t, err)

				for _, err := range tc.errors {
					d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{
						Chunkseries: []client.TimeSeriesChunk{
							{
								Labels: []cortexpb.LabelAdapter{
									{Name: "foo", Value: "bar"},
								},
								Chunks: clientChunks,
							},
						},
					}, err).Once()
				}
			case "LabelNames":
				res := []string{"foo"}
				for _, err := range tc.errors {
					d.On("LabelNamesStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(res, err).Once()
					d.On("LabelNames", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(res, err).Once()
				}
			case "LabelValues":
				res := []string{"foo"}
				for _, err := range tc.errors {
					d.On("LabelValuesForLabelNameStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(res, err).Once()
					d.On("LabelValuesForLabelName", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(res, err).Once()
				}
			}

			ingesterQueryMaxAttempts := 3
			queryable := newDistributorQueryable(d, true, true, batch.NewChunkMergeIterator, 0, func(string) bool {
				return true
			}, ingesterQueryMaxAttempts)
			querier, err := queryable.Querier(mint, maxt)
			require.NoError(t, err)

			if tc.api == "Select" {
				seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt})
				if tc.isError {
					require.Error(t, seriesSet.Err())
					return
				}
				require.NoError(t, seriesSet.Err())

				if tc.isPartialData {
					require.Contains(t, seriesSet.Warnings(), partialdata.ErrPartialData.Error())
				}
			} else {
				var annots annotations.Annotations
				var err error
				switch tc.api {
				case "LabelNames":
					_, annots, err = querier.LabelNames(ctx, nil, labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"))
				case "LabelValues":
					_, annots, err = querier.LabelValues(ctx, "foo", nil, labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"))
				}

				if tc.isError {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				if tc.isPartialData {
					warnings, _ := annots.AsStrings("", 1, 0)
					require.Contains(t, warnings, partialdata.ErrPartialData.Error())
				}
			}
		})
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
					}, 1)
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
