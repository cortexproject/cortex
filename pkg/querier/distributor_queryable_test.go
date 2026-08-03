package querier

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/encoding"

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
			t.Run(fmt.Sprintf("%s (streaming metadata enabled: %t)", testName, streamingMetadataEnabled), func(t *testing.T) {
				t.Parallel()

				distributor := &MockDistributor{}
				distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)
				distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]labels.Labels{}, nil)
				distributor.On("MetricsForLabelMatchersStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]labels.Labels{}, nil)

				ctx := user.InjectOrgID(context.Background(), "test")

				limits := DefaultLimitsConfig()
				limits.QueryIngestersWithin = model.Duration(testData.queryIngestersWithin)
				overrides := validation.NewOverrides(limits, nil)

				queryable := newDistributorQueryable(distributor, streamingMetadataEnabled, true, nil, nil, 1, overrides, nil)
				querier, err := queryable.Querier(testData.queryMinT, testData.queryMaxT)
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
					assert.InDelta(t, testData.expectedMinT, int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), float64(15*time.Second.Milliseconds()))
					assert.Equal(t, testData.expectedMaxT, int64(distributor.Calls[0].Arguments.Get(2).(model.Time)))
				}
			})
		}
	}
}

func TestDistributorQueryableFilter(t *testing.T) {
	t.Parallel()

	d := &MockDistributor{}

	limits := DefaultLimitsConfig()
	limits.QueryIngestersWithin = model.Duration(1 * time.Hour)
	overrides := validation.NewOverrides(limits, nil)

	dq := newDistributorQueryable(d, false, true, nil, nil, 1, overrides, nil)

	now := time.Now()

	queryMinT := util.TimeToMillis(now.Add(-5 * time.Minute))
	queryMaxT := util.TimeToMillis(now)

	require.True(t, dq.UseQueryable(now, "test", queryMinT, queryMaxT))
	require.True(t, dq.UseQueryable(now.Add(time.Hour), "test", queryMinT, queryMaxT))

	// Same query, hour+1ms later, is not sent to ingesters.
	require.False(t, dq.UseQueryable(now.Add(time.Hour).Add(1*time.Millisecond), "test", queryMinT, queryMaxT))
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

			limits := DefaultLimitsConfig()
			limits.QueryIngestersWithin = model.Duration(0) // Disable time filtering for this test
			overrides := validation.NewOverrides(limits, nil)

			queryable := newDistributorQueryable(d, true, true, batch.NewChunkMergeIterator, func(string) bool {
				return partialDataEnabled
			}, 1, overrides, nil)
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

			limits := DefaultLimitsConfig()
			limits.QueryIngestersWithin = model.Duration(0)
			overrides := validation.NewOverrides(limits, nil)

			queryable := newDistributorQueryable(d, true, true, batch.NewChunkMergeIterator, func(string) bool {
				return true
			}, ingesterQueryMaxAttempts, overrides, nil)
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

// TestDistributorQuerier_Select_CancelledContext_NoRetry verifies that with
// ingesterQueryMaxAttempts=1, a cancelled context does not panic because the
// direct code path (no retry loop) is used.
func TestDistributorQuerier_Select_CancelledContext_NoRetry(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "0")
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	d := &MockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, context.Canceled)

	ingesterQueryMaxAttempts := 1
	limits := DefaultLimitsConfig()
	overrides := validation.NewOverrides(limits, nil)
	queryable := newDistributorQueryable(d, true, true, batch.NewChunkMergeIterator, func(string) bool {
		return true
	}, ingesterQueryMaxAttempts, overrides, nil)
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt})
		_ = seriesSet.Err()
	})
}

// TestDistributorQuerier_Select_CancelledContext reproduces the panic described
// in https://github.com/cortexproject/cortex/issues/7364.
//
// When ingesterQueryMaxAttempts > 1 and the context is cancelled before the
// retry loop starts (e.g. query timeout or another querier goroutine failing),
// backoff.Ongoing() returns false immediately. queryWithRetry now propagates
// ctx.Err() so callers always see a non-nil error.
func TestDistributorQuerier_Select_CancelledContext(t *testing.T) {
	t.Parallel()

	// Create a context that is already cancelled.
	ctx := user.InjectOrgID(context.Background(), "0")
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	d := &MockDistributor{}
	// No mock expectations needed — QueryStream should never be called
	// because the context is already cancelled.

	ingesterQueryMaxAttempts := 2
	limits := DefaultLimitsConfig()
	overrides := validation.NewOverrides(limits, nil)
	queryable := newDistributorQueryable(d, true, true, batch.NewChunkMergeIterator, func(string) bool {
		return true
	}, ingesterQueryMaxAttempts, overrides, nil)
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt})
	require.ErrorIs(t, seriesSet.Err(), context.Canceled)
}

// TestDistributorQuerier_Labels_CancelledContext verifies that labelsWithRetry
// propagates ctx.Err() when the context is cancelled before the retry loop
// executes.
func TestDistributorQuerier_Labels_CancelledContext(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "0")
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	d := &MockDistributor{}

	ingesterQueryMaxAttempts := 2
	limits := DefaultLimitsConfig()
	overrides := validation.NewOverrides(limits, nil)
	queryable := newDistributorQueryable(d, true, true, batch.NewChunkMergeIterator, func(string) bool {
		return true
	}, ingesterQueryMaxAttempts, overrides, nil)
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)

	t.Run("LabelNames", func(t *testing.T) {
		_, _, err := querier.LabelNames(ctx, nil)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("LabelValues", func(t *testing.T) {
		_, _, err := querier.LabelValues(ctx, "foo", nil)
		require.ErrorIs(t, err, context.Canceled)
	})
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

					limits := DefaultLimitsConfig()
					overrides := validation.NewOverrides(limits, nil)

					queryable := newDistributorQueryable(d, streamingEnabled, labelNamesWithMatchers, nil, func(string) bool {
						return partialDataEnabled
					}, 1, overrides, nil)
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
func TestDistributorQuerier_QueryIngestersWithinBoundary(t *testing.T) {
	t.Parallel()

	now := time.Now()
	lookback := 1 * time.Hour

	tests := map[string]struct {
		queryMinT    int64
		queryMaxT    int64
		expectedMinT int64
		expectedMaxT int64
		description  string
	}{
		"query exactly at lookback boundary": {
			queryMinT:    util.TimeToMillis(now.Add(-lookback)),
			queryMaxT:    util.TimeToMillis(now),
			expectedMinT: util.TimeToMillis(now.Add(-lookback)),
			expectedMaxT: util.TimeToMillis(now),
			description:  "should not manipulate when minT is exactly at boundary",
		},
		"query 1ms before lookback boundary": {
			queryMinT:    util.TimeToMillis(now.Add(-lookback - time.Millisecond)),
			queryMaxT:    util.TimeToMillis(now),
			expectedMinT: util.TimeToMillis(now.Add(-lookback)),
			expectedMaxT: util.TimeToMillis(now),
			description:  "should manipulate when minT is 1ms before boundary",
		},
		"query 1ms after lookback boundary": {
			queryMinT:    util.TimeToMillis(now.Add(-lookback + time.Millisecond)),
			queryMaxT:    util.TimeToMillis(now),
			expectedMinT: util.TimeToMillis(now.Add(-lookback + time.Millisecond)),
			expectedMaxT: util.TimeToMillis(now),
			description:  "should not manipulate when minT is 1ms after boundary",
		},
		"maxT well before lookback boundary": {
			queryMinT:    util.TimeToMillis(now.Add(-2 * lookback)),
			queryMaxT:    util.TimeToMillis(now.Add(-lookback - 10*time.Second)),
			expectedMinT: 0,
			expectedMaxT: 0,
			description:  "should skip query when maxT is well before boundary",
		},
		"maxT 1ms before lookback boundary": {
			queryMinT:    util.TimeToMillis(now.Add(-2 * lookback)),
			queryMaxT:    util.TimeToMillis(now.Add(-lookback - time.Millisecond)),
			expectedMinT: 0,
			expectedMaxT: 0,
			description:  "should skip query when maxT is before boundary",
		},
		"maxT well after lookback boundary": {
			queryMinT:    util.TimeToMillis(now.Add(-2 * lookback)),
			queryMaxT:    util.TimeToMillis(now.Add(-lookback + 10*time.Second)),
			expectedMinT: util.TimeToMillis(now.Add(-lookback)),
			expectedMaxT: util.TimeToMillis(now.Add(-lookback + 10*time.Second)),
			description:  "should manipulate when maxT is well after boundary",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			distributor := &MockDistributor{}
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)

			ctx := user.InjectOrgID(context.Background(), "test")

			limits := DefaultLimitsConfig()
			limits.QueryIngestersWithin = model.Duration(lookback)
			overrides := validation.NewOverrides(limits, nil)

			queryable := newDistributorQueryable(distributor, false, true, nil, nil, 1, overrides, func() time.Time { return now })
			querier, err := queryable.Querier(testData.queryMinT, testData.queryMaxT)
			require.NoError(t, err)

			seriesSet := querier.Select(ctx, true, nil)
			require.NoError(t, seriesSet.Err())

			if testData.expectedMinT == 0 && testData.expectedMaxT == 0 {
				assert.Len(t, distributor.Calls, 0, testData.description)
			} else {
				require.Len(t, distributor.Calls, 1, testData.description)
				assert.Equal(t, testData.expectedMinT, int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), testData.description)
				assert.Equal(t, testData.expectedMaxT, int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), testData.description)
			}
		})
	}
}

// BenchmarkIngesterStreamingSelect quantifies the cost of the chunk detach
// copy removed for issue #7732, on an input with the shape a real gRPC
// Unmarshal produces for chunks: each chunk's Data is its own independently
// allocated slice (Chunk.Unmarshal appends into the nil Data slice of a
// fresh per-Recv message, so it never aliases the receive buffer or any
// other chunk). Label memory layout is irrelevant to what the two arms
// compare -- both run the identical label copy -- so labels are ordinary
// independent strings here. This replaces the original #7670 benchmark,
// which constructed Chunk.Data as sub-slices of one shared buffer, a shape
// a real gRPC unmarshal never produces.
func BenchmarkIngesterStreamingSelect(b *testing.B) {
	const numSeries = 100
	const chunkDataSize = 1024

	buildResponse := func() *client.QueryStreamResponse {
		series := make([]client.TimeSeriesChunk, numSeries)
		for i := range series {
			// Chunk data independently allocated, as a real Chunk.Unmarshal
			// produces -- never a sub-slice of a shared buffer.
			chunkData := make([]byte, chunkDataSize)
			for j := range chunkData {
				chunkData[j] = byte(j % 256)
			}

			series[i] = client.TimeSeriesChunk{
				Labels: []cortexpb.LabelAdapter{
					{Name: "__name__", Value: fmt.Sprintf("metric_%d", i)},
					{Name: "instance", Value: fmt.Sprintf("instance-%d", i)},
				},
				Chunks: []client.Chunk{
					{
						StartTimestampMs: 0,
						EndTimestampMs:   1000,
						Data:             chunkData,
					},
				},
			}
		}
		return &client.QueryStreamResponse{Chunkseries: series}
	}

	// after_fix mirrors the current streamingSelect body: copy labels out of
	// the receive buffer, pass chunks through untouched.
	b.Run("after_fix_no_redundant_copy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			resp := buildResponse()
			for _, result := range resp.Chunkseries {
				_ = cortexpb.FromLabelAdaptersToLabelsWithCopy(result.Labels)
				_ = result.Chunks
			}
		}
	})

	// before_fix reproduces the removed detachChunksFromBuffer behavior inline
	// (the helper itself is gone from the production code) to quantify the
	// allocation/byte cost of copying data that Unmarshal already allocated
	// separately, even under this realistic input shape.
	b.Run("before_fix_redundant_copy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			resp := buildResponse()
			for _, result := range resp.Chunkseries {
				_ = cortexpb.FromLabelAdaptersToLabelsWithCopy(result.Labels)
				copied := make([]client.Chunk, len(result.Chunks))
				for ci, c := range result.Chunks {
					copied[ci] = c
					if len(c.Data) > 0 {
						copied[ci].Data = append([]byte(nil), c.Data...)
					}
				}
				_ = copied
			}
		}
	})
}

// newAliasingProbeQueryStreamResponse returns a QueryStreamResponse with one
// series carrying a label and a chunk whose payload bytes are individually
// distinguishable (0x00, 0x01, 0x02, ...), so that accidental aliasing can be
// detected either by pointer-range overlap or by mutating the source buffer
// and observing whether the decoded bytes change. It also returns an
// independent copy of the chunk payload to compare against after the wire
// buffer has been mutated, since wire-format field ordering is an
// implementation detail we don't want the test to depend on.
//
// chunkDataSize is a parameter (not a constant) because callers going through
// the registered cortexCodec need a marshaled message large enough to cross
// mem.IsBelowBufferPoolingThreshold (1KiB): below that threshold, Marshal
// returns a plain mem.SliceBuffer whose Ref/Free are no-ops, which would
// silently skip exercising the real pool-backed mem.Buffer code path.
func newAliasingProbeQueryStreamResponse(chunkDataSize int) (resp *client.QueryStreamResponse, wantChunkData []byte) {
	chunkData := make([]byte, chunkDataSize)
	for i := range chunkData {
		chunkData[i] = byte(i)
	}
	resp = &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: []cortexpb.LabelAdapter{
					{Name: "__name__", Value: "some_metric_name"},
					{Name: "instance", Value: "instance-aliasing-probe-01"},
				},
				Chunks: []client.Chunk{
					{
						StartTimestampMs: 1000,
						EndTimestampMs:   2000,
						Encoding:         3,
						Data:             chunkData,
					},
				},
			},
		},
	}
	return resp, append([]byte(nil), chunkData...)
}

// overlaps reports whether byte slices a and b share any backing memory.
//
// This compares raw addresses as uintptrs, which is only valid because Go's
// garbage collector does not move heap-allocated objects (unlike goroutine
// stacks, which can be copied). If a future Go runtime introduces a moving
// GC for the heap, this address-range comparison would need to be redone
// (e.g. by writing sentinel bytes and checking for their presence/absence
// instead of comparing pointers). runtime.KeepAlive calls below only guard
// against the compiler treating a/b as dead before the address is taken;
// they say nothing about GC movement.
func overlaps(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	aStart := uintptr(unsafe.Pointer(&a[0]))
	aEnd := aStart + uintptr(len(a))
	bStart := uintptr(unsafe.Pointer(&b[0]))
	bEnd := bStart + uintptr(len(b))
	result := aStart < bEnd && bStart < aEnd
	runtime.KeepAlive(a)
	runtime.KeepAlive(b)
	return result
}

// unsafeBytesFromString exposes a string's backing bytes without copying, so
// that yoloString-produced strings can be checked for aliasing with the
// buffer they were decoded from.
func unsafeBytesFromString(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// TestChunkDataDoesNotAliasWireBuffer_ButLabelValueDoes pins the invariant
// behind the fix for #7732: a gogo Marshal -> Unmarshal round trip of
// client.QueryStreamResponse must leave Chunk.Data as an independent
// allocation (never aliasing the wire buffer), while LabelAdapter.Value must
// still alias it via yoloString. If a future change to the gogo-generated
// Chunk.Unmarshal (or a replacement decoder) ever starts aliasing Data, this
// test must fail loudly -- both the pointer-overlap check AND the mutation
// check below have to be defeated for that regression to slip through.
func TestChunkDataDoesNotAliasWireBuffer_ButLabelValueDoes(t *testing.T) {
	// Size is irrelevant here: this test calls gogo's Marshal/Unmarshal
	// directly, never going through cortexCodec's buffer-pooling threshold.
	orig, wantChunkData := newAliasingProbeQueryStreamResponse(256)

	wireBytes, err := orig.Marshal()
	require.NoError(t, err)

	got := &client.QueryStreamResponse{}
	require.NoError(t, got.Unmarshal(wireBytes))
	require.Len(t, got.Chunkseries, 1)
	require.Len(t, got.Chunkseries[0].Chunks, 1)
	require.Len(t, got.Chunkseries[0].Labels, 2)

	chunkData := got.Chunkseries[0].Chunks[0].Data
	labelValue := got.Chunkseries[0].Labels[1].Value
	require.Equal(t, "instance-aliasing-probe-01", labelValue)

	// --- Pointer-range checks ---
	assert.False(t, overlaps(chunkData, wireBytes),
		"invariant broken: Chunk.Data now aliases the wire buffer -- if Unmarshal changed "+
			"to make this true, a detach/copy step must be reinstated in streamingSelect")
	assert.True(t, overlaps(unsafeBytesFromString(labelValue), wireBytes),
		"expected LabelAdapter.Value to still alias the wire buffer via yoloString; if this "+
			"is now false, FromLabelAdaptersToLabelsWithCopy may no longer be load-bearing "+
			"(but should still be kept -- do not remove without re-verifying this test)")

	// --- Behavioral (mutation) checks: corrupt the wire buffer in place and
	// confirm the chunk is unaffected while the label (known to alias) is. ---
	for i := range wireBytes {
		wireBytes[i] = 0xFF
	}
	assert.Equal(t, wantChunkData, chunkData,
		"Chunk.Data must be a private copy: it should be unaffected by mutating the wire buffer")
	assert.Equal(t, bytes.Repeat([]byte{0xFF}, len(labelValue)), unsafeBytesFromString(labelValue),
		"sanity check failed: LabelAdapter.Value should have observably changed after mutating "+
			"the wire buffer it aliases -- if this assertion itself fails, the test's mutation "+
			"methodology is broken and the non-aliasing assertion above cannot be trusted")
}

// TestChunkDataDoesNotAliasWireBuffer_ViaRegisteredCodec re-runs the same
// invariant through the actual gRPC codec registered for Cortex's ingester
// connections (pkg/cortexpb.codec.go's cortexCodec, registered under the
// "proto" content-subtype via encoding.RegisterCodecV2). That codec uses
// grpc's mem.BufferSlice/mem.Buffer machinery. The payload is sized above
// mem.IsBelowBufferPoolingThreshold (1KiB) so that cortexCodec.Marshal takes
// its real pool.Get/mem.NewBuffer branch -- a smaller payload would silently
// fall back to a plain mem.SliceBuffer, whose Ref/Free are no-ops, and this
// test would then never actually touch a pool-backed buffer.
//
// This test confirms that, even through that pool-backed buffer, decoding
// still routes to the classic gogo Marshal/Unmarshal methods (via the
// protobuf-go legacy-message shim) so Chunk.Data is still never an alias of
// the wire buffer -- this is the real path used by QueryStream, not just the
// pb.go method in isolation.
//
// What this does NOT cover: a message split across multiple transport reads
// (mem.BufferSlice with len > 1), which drives MaterializeToBuffer's other
// branch (pool.Get + CopyTo instead of the single-buffer Ref fast path). A
// same-process Marshal->Unmarshal round trip can't produce that shape --
// reaching it would need an actual (or bufconn-based) gRPC transport with a
// large enough message to span multiple HTTP/2 DATA frames, which is
// integration-test territory and out of scope here.
func TestChunkDataDoesNotAliasWireBuffer_ViaRegisteredCodec(t *testing.T) {
	codec := encoding.GetCodecV2(cortexpb.Name)
	require.NotNil(t, codec, "expected a CodecV2 to be registered under name %q", cortexpb.Name)

	// 4096 bytes of chunk data comfortably clears the 1KiB pooling threshold
	// once labels and protobuf framing overhead are added.
	orig, _ := newAliasingProbeQueryStreamResponse(4096)

	wireData, err := codec.Marshal(orig)
	require.NoError(t, err)
	require.NotEmpty(t, wireData)
	require.Greater(t, wireData.Len(), 1024,
		"test payload must exceed the buffer-pooling threshold or this test silently stops "+
			"exercising the pool-backed mem.Buffer code path (see mem.IsBelowBufferPoolingThreshold)")

	got := &client.QueryStreamResponse{}
	require.NoError(t, codec.Unmarshal(wireData, got))
	require.Len(t, got.Chunkseries, 1)
	require.Len(t, got.Chunkseries[0].Chunks, 1)
	require.Len(t, got.Chunkseries[0].Labels, 2)

	chunkData := got.Chunkseries[0].Chunks[0].Data
	labelValue := got.Chunkseries[0].Labels[1].Value
	require.Equal(t, "instance-aliasing-probe-01", labelValue)

	labelBytes := unsafeBytesFromString(labelValue)
	chunkAliasesWire := false
	labelAliasesWire := false
	for _, b := range wireData {
		wireSegment := b.ReadOnlyData()
		if overlaps(chunkData, wireSegment) {
			chunkAliasesWire = true
		}
		if overlaps(labelBytes, wireSegment) {
			labelAliasesWire = true
		}
	}

	assert.False(t, chunkAliasesWire,
		"invariant broken: through the registered gRPC codec, Chunk.Data aliases a wire "+
			"mem.Buffer segment -- this would mean a zero-copy/pooled decode path now exists "+
			"and detachChunksFromBuffer (or equivalent) must be reinstated")
	assert.True(t, labelAliasesWire,
		"expected LabelAdapter.Value to alias a wire mem.Buffer segment via yoloString even "+
			"through the registered codec; if false, re-verify FromLabelAdaptersToLabelsWithCopy "+
			"is still necessary before considering it dead weight")
}
