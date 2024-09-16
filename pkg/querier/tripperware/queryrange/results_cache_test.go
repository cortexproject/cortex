package queryrange

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	query                    = "/api/v1/query_range?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&stats=all&step=120"
	queryWithWarnings        = "/api/v1/query_range?end=1536716898&query=sumsum%28warnings%29&start=1536673680&stats=all&step=120&warnings=true"
	queryWithInfos           = "/api/v1/query_range?end=1536716898&query=rate%28go_gc_gogc_percent%5B5m%5D%29&start=1536673680&stats=all&step=120"
	responseBody             = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
	responseBodyWithWarnings = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]},"warnings":["test-warn"]}`
	responseBodyWithInfos    = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]},"infos":["PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: \"go_gc_gogc_percent\" (1:6)"]}`
)

var (
	parsedRequest = &tripperware.PrometheusRequest{
		Path:  "/api/v1/query_range",
		Start: 1536673680 * 1e3,
		End:   1536716898 * 1e3,
		Step:  120 * 1e3,
		Query: "sum(container_memory_rss) by (namespace)",
		Stats: "all",
	}
	reqHeaders = http.Header(map[string][]string{"Test-Header": {"test"}})

	noCacheRequest = &tripperware.PrometheusRequest{
		Path:           "/api/v1/query_range",
		Start:          1536673680 * 1e3,
		End:            1536716898 * 1e3,
		Step:           120 * 1e3,
		Query:          "sum(container_memory_rss) by (namespace)",
		CachingOptions: tripperware.CachingOptions{Disabled: true},
	}
	noCacheRequestWithStats = &tripperware.PrometheusRequest{
		Path:           "/api/v1/query_range",
		Start:          1536673680 * 1e3,
		End:            1536716898 * 1e3,
		Step:           120 * 1e3,
		Stats:          "all",
		Query:          "sum(container_memory_rss) by (namespace)",
		CachingOptions: tripperware.CachingOptions{Disabled: true},
	}
	respHeaders = []*tripperware.PrometheusResponseHeader{
		{
			Name:   "Content-Type",
			Values: []string{"application/json"},
		},
	}
	parsedResponse = &tripperware.PrometheusResponse{
		Status: "success",
		Data: tripperware.PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: tripperware.PrometheusQueryResult{
				Result: &tripperware.PrometheusQueryResult_Matrix{
					Matrix: &tripperware.Matrix{
						SampleStreams: []tripperware.SampleStream{
							{
								Labels: []cortexpb.LabelAdapter{
									{Name: "foo", Value: "bar"},
								},
								Samples: []cortexpb.Sample{
									{Value: 137, TimestampMs: 1536673680000},
									{Value: 137, TimestampMs: 1536673780000},
								},
							},
						},
					},
				},
			},
		},
	}

	parsedResponseWithWarnings = &tripperware.PrometheusResponse{
		Status:   "success",
		Warnings: []string{"test-warn"},
		Data: tripperware.PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: tripperware.PrometheusQueryResult{
				Result: &tripperware.PrometheusQueryResult_Matrix{
					Matrix: &tripperware.Matrix{
						SampleStreams: []tripperware.SampleStream{
							{
								Labels: []cortexpb.LabelAdapter{
									{Name: "foo", Value: "bar"},
								},
								Samples: []cortexpb.Sample{
									{Value: 137, TimestampMs: 1536673680000},
									{Value: 137, TimestampMs: 1536673780000},
								},
							},
						},
					},
				},
			},
		},
	}
	parsedResponseWithInfos = &tripperware.PrometheusResponse{
		Status: "success",
		Infos:  []string{"PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: \"go_gc_gogc_percent\" (1:6)"},
		Data: tripperware.PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: tripperware.PrometheusQueryResult{
				Result: &tripperware.PrometheusQueryResult_Matrix{
					Matrix: &tripperware.Matrix{
						SampleStreams: []tripperware.SampleStream{
							{
								Labels: []cortexpb.LabelAdapter{
									{Name: "foo", Value: "bar"},
								},
								Samples: []cortexpb.Sample{
									{Value: 137, TimestampMs: 1536673680000},
									{Value: 137, TimestampMs: 1536673780000},
								},
							},
						},
					},
				},
			},
		},
	}
)

func mkAPIResponse(start, end, step int64) tripperware.Response {
	return mkAPIResponseWithStats(start, end, step, false, false)
}

func mkAPIResponseWithStats(start, end, step int64, withStats bool, oldFormat bool) tripperware.Response {
	var samples []cortexpb.Sample
	var stats *tripperware.PrometheusResponseStats
	if withStats {
		stats = &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{}}
	}
	for i := start; i <= end; i += step {
		samples = append(samples, cortexpb.Sample{
			TimestampMs: int64(i),
			Value:       float64(i),
		})

		if withStats {
			stats.Samples.TotalQueryableSamplesPerStep = append(stats.Samples.TotalQueryableSamplesPerStep, &tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
				TimestampMs: i,
				Value:       i,
			})

			stats.Samples.TotalQueryableSamples += i
		}
	}

	if oldFormat {
		return &PrometheusResponse{
			Status: StatusSuccess,
			Data: PrometheusData{
				ResultType: matrix,
				Stats:      stats,
				Result: []tripperware.SampleStream{
					{
						Labels: []cortexpb.LabelAdapter{
							{Name: "foo", Value: "bar"},
						},
						Samples: samples,
					},
				},
			},
		}
	}

	return &tripperware.PrometheusResponse{
		Status: StatusSuccess,
		Data: tripperware.PrometheusData{
			ResultType: matrix,
			Stats:      stats,
			Result: tripperware.PrometheusQueryResult{
				Result: &tripperware.PrometheusQueryResult_Matrix{
					Matrix: &tripperware.Matrix{
						SampleStreams: []tripperware.SampleStream{
							{
								Labels: []cortexpb.LabelAdapter{
									{Name: "foo", Value: "bar"},
								},
								Samples: samples,
							},
						},
					},
				},
			},
		},
	}
}

func mkExtentWithStats(start, end int64) tripperware.Extent {
	return mkExtentWithStepWithStatsWithFormat(start, end, 10, true, false)
}

func mkExtent(start, end int64) tripperware.Extent {
	return mkExtentWithStepWithStatsWithFormat(start, end, 10, false, false)
}

func mkExtentWithOldFormat(start, end int64) tripperware.Extent {
	return mkExtentWithStepWithStatsWithFormat(start, end, 10, false, true)
}

func mkExtentWithStep(start, end, step int64) tripperware.Extent {
	return mkExtentWithStepWithStatsWithFormat(start, end, step, false, false)
}

func mkExtentWithStepAndFormat(start, end, step int64, oldFormat bool) tripperware.Extent {
	return mkExtentWithStepWithStatsWithFormat(start, end, step, false, false)
}

func mkExtentWithStepWithStatsWithFormat(start, end, step int64, withStats bool, oldFormat bool) tripperware.Extent {
	res := mkAPIResponseWithStats(start, end, step, withStats, oldFormat)
	any, err := types.MarshalAny(res)
	if err != nil {
		panic(err)
	}
	return tripperware.Extent{
		Start:    start,
		End:      end,
		Response: any,
	}
}

func TestStatsCacheQuerySamples(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                       string
		cacheQueryableSamplesStats bool
		err                        error
		req                        tripperware.Request
		upstreamResponse           tripperware.Response
		expectedResponse           tripperware.Response
	}{
		{
			name:                       "should return error",
			cacheQueryableSamplesStats: true,
			req:                        noCacheRequest,
			err:                        fmt.Errorf("error"),
		},
		{
			name:                       "should return response with stats",
			cacheQueryableSamplesStats: true,
			req:                        noCacheRequestWithStats,
			upstreamResponse:           mkAPIResponseWithStats(0, 100, 10, true, false),
			expectedResponse:           mkAPIResponseWithStats(0, 100, 10, true, false),
		},
		{
			name:                       "should return response strip stats if not requested",
			cacheQueryableSamplesStats: true,
			req:                        noCacheRequest,
			upstreamResponse:           mkAPIResponseWithStats(0, 100, 10, false, false),
			expectedResponse:           mkAPIResponseWithStats(0, 100, 10, false, false),
		},
		{
			name:                       "should not ask stats is cacheQueryableSamplesStats is disabled",
			cacheQueryableSamplesStats: false,
			req:                        noCacheRequest,
			upstreamResponse:           mkAPIResponseWithStats(0, 100, 10, false, false),
			expectedResponse:           mkAPIResponseWithStats(0, 100, 10, false, false),
		},
		{
			name:                       "should not forward stats when cacheQueryableSamplesStats is disabled",
			cacheQueryableSamplesStats: false,
			req:                        noCacheRequestWithStats,
			upstreamResponse:           mkAPIResponseWithStats(0, 100, 10, true, false),
			expectedResponse:           mkAPIResponseWithStats(0, 100, 10, false, false),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := ResultsCacheConfig{
				CacheConfig: cache.Config{
					Cache: cache.NewMockCache(),
				},
				CacheQueryableSamplesStats: tc.cacheQueryableSamplesStats,
			}
			rcm, _, err := NewResultsCacheMiddleware(
				log.NewNopLogger(),
				cfg,
				constSplitter(day),
				mockLimits{},
				PrometheusCodec,
				PrometheusResponseExtractor{},
				nil,
				nil,
			)
			require.NoError(t, err)

			rc := rcm.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
				if tc.cacheQueryableSamplesStats {
					require.Equal(t, "all", req.GetStats())
				} else {
					require.Equal(t, "", req.GetStats())
				}
				return tc.upstreamResponse, tc.err
			}))
			ctx := user.InjectOrgID(context.Background(), "1")
			r, err := rc.Do(ctx, tc.req)
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.expectedResponse, r)
		})
	}
}

func TestShouldCache(t *testing.T) {
	t.Parallel()
	maxCacheTime := int64(150 * 1000)
	c := &resultsCache{logger: log.NewNopLogger()}
	for _, tc := range []struct {
		name     string
		request  tripperware.Request
		input    tripperware.Response
		expected bool
	}{
		// Tests only for cacheControlHeader
		{
			name:    "does not contain the cacheControl header",
			request: &tripperware.PrometheusRequest{Query: "metric"},
			input: tripperware.Response(&tripperware.PrometheusResponse{
				Headers: []*tripperware.PrometheusResponseHeader{
					{
						Name:   "meaninglessheader",
						Values: []string{},
					},
				},
			}),
			expected: true,
		},
		{
			name:    "does contain the cacheControl header which has the value",
			request: &tripperware.PrometheusRequest{Query: "metric"},
			input: tripperware.Response(&tripperware.PrometheusResponse{
				Headers: []*tripperware.PrometheusResponseHeader{
					{
						Name:   cacheControlHeader,
						Values: []string{noStoreValue},
					},
				},
			}),
			expected: false,
		},
		{
			name:    "cacheControl header contains extra values but still good",
			request: &tripperware.PrometheusRequest{Query: "metric"},
			input: tripperware.Response(&tripperware.PrometheusResponse{
				Headers: []*tripperware.PrometheusResponseHeader{
					{
						Name:   cacheControlHeader,
						Values: []string{"foo", noStoreValue},
					},
				},
			}),
			expected: false,
		},
		{
			name:     "broken response",
			request:  &tripperware.PrometheusRequest{Query: "metric"},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:    "nil headers",
			request: &tripperware.PrometheusRequest{Query: "metric"},
			input: tripperware.Response(&tripperware.PrometheusResponse{
				Headers: []*tripperware.PrometheusResponseHeader{nil},
			}),
			expected: true,
		},
		{
			name:    "had cacheControl header but no values",
			request: &tripperware.PrometheusRequest{Query: "metric"},
			input: tripperware.Response(&tripperware.PrometheusResponse{
				Headers: []*tripperware.PrometheusResponseHeader{{Name: cacheControlHeader}},
			}),
			expected: true,
		},
		// @ modifier on vector selectors.
		{
			name:     "@ modifier on vector selector, before end, before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "metric @ 123", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "@ modifier on vector selector, after end, before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "metric @ 127", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on vector selector, before end, after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "metric @ 151", End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on vector selector, after end, after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "metric @ 151", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on vector selector with start() before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "metric @ start()", Start: 100000, End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "@ modifier on vector selector with end() after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "metric @ end()", Start: 100000, End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		// @ modifier on matrix selectors.
		{
			name:     "@ modifier on matrix selector, before end, before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "rate(metric[5m] @ 123)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "@ modifier on matrix selector, after end, before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "rate(metric[5m] @ 127)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on matrix selector, before end, after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "rate(metric[5m] @ 151)", End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on matrix selector, after end, after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "rate(metric[5m] @ 151)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on matrix selector with start() before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "rate(metric[5m] @ start())", Start: 100000, End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "@ modifier on matrix selector with end() after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "rate(metric[5m] @ end())", Start: 100000, End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		// @ modifier on subqueries.
		{
			name:     "@ modifier on subqueries, before end, before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ 123)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "@ modifier on subqueries, after end, before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ 127)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on subqueries, before end, after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ 151)", End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on subqueries, after end, after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ 151)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		{
			name:     "@ modifier on subqueries with start() before maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ start())", Start: 100000, End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "@ modifier on subqueries with end() after maxCacheTime",
			request:  &tripperware.PrometheusRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ end())", Start: 100000, End: 200000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		// offset on vector selectors.
		{
			name:     "positive offset on vector selector",
			request:  &tripperware.PrometheusRequest{Query: "metric offset 10ms", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "negative offset on vector selector",
			request:  &tripperware.PrometheusRequest{Query: "metric offset -10ms", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		// offset on matrix selectors.
		{
			name:     "positive offset on matrix selector",
			request:  &tripperware.PrometheusRequest{Query: "rate(metric[5m] offset 10ms)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "negative offset on matrix selector",
			request:  &tripperware.PrometheusRequest{Query: "rate(metric[5m] offset -10ms)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
		// offset on subqueries.
		{
			name:     "positive offset on subqueries",
			request:  &tripperware.PrometheusRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] offset 10ms)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: true,
		},
		{
			name:     "negative offset on subqueries",
			request:  &tripperware.PrometheusRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] offset -10ms)", End: 125000},
			input:    tripperware.Response(&tripperware.PrometheusResponse{}),
			expected: false,
		},
	} {
		{
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ret := c.shouldCacheResponse(context.Background(), tc.request, tc.input, maxCacheTime)
				require.Equal(t, tc.expected, ret)
			})
		}
	}
}

func TestPartition(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                   string
		input                  tripperware.Request
		prevCachedResponse     []tripperware.Extent
		expectedRequests       []tripperware.Request
		expectedCachedResponse []tripperware.Response
	}{
		{
			name: "Test a complete hit.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtent(0, 100),
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(0, 100, 10),
			},
		},

		{
			name: "Test a complete hit with old format.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithOldFormat(0, 100),
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(0, 100, 10),
			},
		},

		{
			name: "Test with a complete miss.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtent(110, 210),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   100,
				}},
		},
		{
			name: "Test a partial hit.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtent(50, 100),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   50,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(50, 100, 10),
			},
		},
		{
			name: "Test a partial hit with old format.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithOldFormat(50, 100),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   50,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(50, 100, 10),
			},
		},
		{
			name: "Test multiple partial hits.",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   200,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtent(50, 120),
				mkExtent(160, 250),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 120,
					End:   160,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(100, 120, 10),
				mkAPIResponse(160, 200, 10),
			},
		},
		{
			name: "Test multiple partial hits with old format.",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   200,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithOldFormat(50, 120),
				mkExtentWithOldFormat(160, 250),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 120,
					End:   160,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(100, 120, 10),
				mkAPIResponse(160, 200, 10),
			},
		},
		{
			name: "Partial hits with tiny gap.",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   160,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtent(50, 120),
				mkExtent(122, 130),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 120,
					End:   160,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(100, 120, 10),
			},
		},
		{
			name: "Partial hits with tiny gap with old format.",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   160,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithOldFormat(50, 120),
				mkExtentWithOldFormat(122, 130),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 120,
					End:   160,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(100, 120, 10),
			},
		},
		{
			name: "Extent is outside the range and the request has a single step (same start and end).",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtent(50, 90),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 100,
					End:   100,
				},
			},
		},
		{
			name: "Test when hit has a large step and only a single sample extent.",
			// If there is a only a single sample in the split interval, start and end will be the same.
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtent(100, 100),
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(100, 105, 10),
			},
		},
		{
			name: "Test when hit has a large step and only a single sample extent with old format.",
			// If there is a only a single sample in the split interval, start and end will be the same.
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithOldFormat(100, 100),
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponse(100, 105, 10),
			},
		},
		{
			name: "[Stats] Test a complete hit.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStats(0, 100),
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponseWithStats(0, 100, 10, true, false),
			},
		},

		{
			name: "[Stats] Test a complete hit with old format.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStepWithStatsWithFormat(0, 100, 10, true, true),
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponseWithStats(0, 100, 10, true, false),
			},
		},

		{
			name: "[Stats] Test with a complete miss.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStats(110, 210),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   100,
				}},
		},
		{
			name: "[Stats] Test with a complete miss with old format.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStepWithStatsWithFormat(110, 210, 10, true, true),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   100,
				}},
		},
		{
			name: "[stats] Test a partial hit.",
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStats(50, 100),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   50,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponseWithStats(50, 100, 10, true, false),
			},
		},
		{
			name: "[stats] Test multiple partial hits.",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   200,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStats(50, 120),
				mkExtentWithStats(160, 250),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 120,
					End:   160,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponseWithStats(100, 120, 10, true, false),
				mkAPIResponseWithStats(160, 200, 10, true, false),
			},
		},
		{
			name: "[stats] Partial hits with tiny gap.",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   160,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStats(50, 120),
				mkExtentWithStats(122, 130),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 120,
					End:   160,
				},
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponseWithStats(100, 120, 10, true, false),
			},
		},
		{
			name: "[stats] Extent is outside the range and the request has a single step (same start and end).",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStats(50, 90),
			},
			expectedRequests: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 100,
					End:   100,
				},
			},
		},
		{
			name: "[stats] Test when hit has a large step and only a single sample extent.",
			// If there is a only a single sample in the split interval, start and end will be the same.
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStats(100, 100),
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponseWithStats(100, 105, 10, true, false),
			},
		},
		{
			name: "[stats] Test when hit has a large step and only a single sample extent with old format.",
			// If there is a only a single sample in the split interval, start and end will be the same.
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   100,
			},
			prevCachedResponse: []tripperware.Extent{
				mkExtentWithStepWithStatsWithFormat(100, 100, 10, true, true),
			},
			expectedCachedResponse: []tripperware.Response{
				mkAPIResponseWithStats(100, 105, 10, true, false),
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := resultsCache{
				extractor:      PrometheusResponseExtractor{},
				minCacheExtent: 10,
			}
			reqs, resps, err := s.partition(tc.input, tc.prevCachedResponse)
			require.Nil(t, err)
			require.Equal(t, tc.expectedRequests, reqs)
			require.Equal(t, tc.expectedCachedResponse, resps)
		})
	}
}

func TestHandleHit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                       string
		input                      tripperware.Request
		cachedEntry                []tripperware.Extent
		expectedUpdatedCachedEntry []tripperware.Extent
	}{
		{
			name: "Should drop tiny extent that overlaps with non-tiny request only",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   120,
				Step:  5,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 105, 5),
				mkExtentWithStep(110, 150, 5),
				mkExtentWithStep(160, 165, 5),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 150, 5),
				mkExtentWithStep(160, 165, 5),
			},
		},
		{
			name: "Should replace tiny extents that are cover by bigger request",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   200,
				Step:  5,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 105, 5),
				mkExtentWithStep(110, 115, 5),
				mkExtentWithStep(120, 125, 5),
				mkExtentWithStep(220, 225, 5),
				mkExtentWithStep(240, 250, 5),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 200, 5),
				mkExtentWithStep(220, 225, 5),
				mkExtentWithStep(240, 250, 5),
			},
		},
		{
			name: "Should not drop tiny extent that completely overlaps with tiny request",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   105,
				Step:  5,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 105, 5),
				mkExtentWithStep(160, 165, 5),
			},
			expectedUpdatedCachedEntry: nil, // no cache update need, request fulfilled using cache
		},
		{
			name: "Should not drop tiny extent that partially center-overlaps with tiny request",
			input: &tripperware.PrometheusRequest{
				Start: 106,
				End:   108,
				Step:  2,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(104, 110, 2),
				mkExtentWithStep(160, 166, 2),
			},
			expectedUpdatedCachedEntry: nil, // no cache update need, request fulfilled using cache
		},
		{
			name: "Should not drop tiny extent that partially left-overlaps with tiny request",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   106,
				Step:  2,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(104, 110, 2),
				mkExtentWithStep(160, 166, 2),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(100, 110, 2),
				mkExtentWithStep(160, 166, 2),
			},
		},
		{
			name: "Should not drop tiny extent that partially right-overlaps with tiny request",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   106,
				Step:  2,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(98, 102, 2),
				mkExtentWithStep(160, 166, 2),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(98, 106, 2),
				mkExtentWithStep(160, 166, 2),
			},
		},
		{
			name: "Should merge fragmented extents if request fills the hole",
			input: &tripperware.PrometheusRequest{
				Start: 40,
				End:   80,
				Step:  20,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 20, 20),
				mkExtentWithStep(80, 100, 20),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 100, 20),
			},
		},
		{
			name: "Should left-extend extent if request starts earlier than extent in cache",
			input: &tripperware.PrometheusRequest{
				Start: 40,
				End:   80,
				Step:  20,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 160, 20),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(40, 160, 20),
			},
		},
		{
			name: "Should right-extend extent if request ends later than extent in cache",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   180,
				Step:  20,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 160, 20),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 180, 20),
			},
		},
		{
			name: "Should not throw error if complete-overlapped smaller Extent is erroneous",
			input: &tripperware.PrometheusRequest{
				// This request is carefully created such that cachedEntry is not used to fulfill
				// the request.
				Start: 160,
				End:   180,
				Step:  20,
			},
			cachedEntry: []tripperware.Extent{
				{
					Start: 60,
					End:   80,

					// if the optimization of "sorting by End when Start of 2 Extents are equal" is not there, this nil
					// response would cause error during Extents merge phase. With the optimization
					// this bad Extent should be dropped. The good Extent below can be used instead.
					Response: nil,
				},
				mkExtentWithStep(60, 160, 20),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(60, 180, 20),
			},
		},
		{
			name: "Should replace tiny extents with old format that are cover by bigger request",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   200,
				Step:  5,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStepAndFormat(0, 50, 5, true),
				mkExtentWithStepAndFormat(60, 65, 5, true),
				mkExtentWithStepAndFormat(100, 105, 5, true),
				mkExtentWithStepAndFormat(110, 115, 5, true),
				mkExtentWithStepAndFormat(120, 125, 5, true),
				mkExtentWithStepAndFormat(220, 225, 5, true),
				mkExtentWithStepAndFormat(240, 250, 5, true),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 200, 5),
				mkExtentWithStep(220, 225, 5),
				mkExtentWithStep(240, 250, 5),
			},
		},
		{
			name: "Should replace tiny extents with mixed of new and old format that are cover by bigger request",
			input: &tripperware.PrometheusRequest{
				Start: 100,
				End:   200,
				Step:  5,
			},
			cachedEntry: []tripperware.Extent{
				mkExtentWithStepAndFormat(0, 50, 5, true),
				mkExtentWithStepAndFormat(60, 65, 5, false),
				mkExtentWithStepAndFormat(100, 105, 5, true),
				mkExtentWithStepAndFormat(110, 115, 5, false),
				mkExtentWithStepAndFormat(120, 125, 5, true),
				mkExtentWithStepAndFormat(220, 225, 5, false),
				mkExtentWithStepAndFormat(240, 250, 5, true),
			},
			expectedUpdatedCachedEntry: []tripperware.Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 200, 5),
				mkExtentWithStep(220, 225, 5),
				mkExtentWithStep(240, 250, 5),
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sut := resultsCache{
				extractor:      PrometheusResponseExtractor{},
				minCacheExtent: 10,
				limits:         mockLimits{},
				merger:         PrometheusCodec,
				next: tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
					return mkAPIResponse(req.GetStart(), req.GetEnd(), req.GetStep()), nil
				}),
			}

			ctx := user.InjectOrgID(context.Background(), "1")
			response, updatedExtents, err := sut.handleHit(ctx, tc.input, tc.cachedEntry, 0)
			require.NoError(t, err)

			expectedResponse := mkAPIResponse(tc.input.GetStart(), tc.input.GetEnd(), tc.input.GetStep())
			require.Equal(t, expectedResponse, response, "response does not match the expectation")
			require.Equal(t, tc.expectedUpdatedCachedEntry, updatedExtents, "updated cache entry does not match the expectation")
		})
	}
}

func TestResultsCache(t *testing.T) {
	t.Parallel()
	calls := 0
	cfg := ResultsCacheConfig{
		CacheConfig: cache.Config{
			Cache: cache.NewMockCache(),
		},
	}
	rcm, _, err := NewResultsCacheMiddleware(
		log.NewNopLogger(),
		cfg,
		constSplitter(day),
		mockLimits{},
		PrometheusCodec,
		PrometheusResponseExtractor{},
		nil,
		nil,
	)
	require.NoError(t, err)

	rc := rcm.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
		calls++
		return parsedResponse, nil
	}))
	ctx := user.InjectOrgID(context.Background(), "1")
	resp, err := rc.Do(ctx, parsedRequest)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, parsedResponse, resp)

	// Doing same request again shouldn't change anything.
	resp, err = rc.Do(ctx, parsedRequest)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, parsedResponse, resp)

	// Doing request with new end time should do one more query.
	req := parsedRequest.WithStartEnd(parsedRequest.GetStart(), parsedRequest.GetEnd()+100)
	_, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestResultsCacheRecent(t *testing.T) {
	t.Parallel()
	var cfg ResultsCacheConfig
	flagext.DefaultValues(&cfg)
	cfg.CacheConfig.Cache = cache.NewMockCache()
	cfg.CacheQueryableSamplesStats = true
	rcm, _, err := NewResultsCacheMiddleware(
		log.NewNopLogger(),
		cfg,
		constSplitter(day),
		mockLimits{maxCacheFreshness: 10 * time.Minute},
		PrometheusCodec,
		PrometheusResponseExtractor{},
		nil,
		nil,
	)
	require.NoError(t, err)

	req := parsedRequest.WithStartEnd(int64(model.Now())-(60*1e3), int64(model.Now()))

	calls := 0
	rc := rcm.Wrap(tripperware.HandlerFunc(func(_ context.Context, r tripperware.Request) (tripperware.Response, error) {
		calls++
		assert.Equal(t, r, req)
		return parsedResponse, nil
	}))
	ctx := user.InjectOrgID(context.Background(), "1")

	// Request should result in a query.
	resp, err := rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, parsedResponse, resp)

	// Doing same request again should result in another query.
	resp, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, parsedResponse, resp)
}

func TestResultsCacheMaxFreshness(t *testing.T) {
	t.Parallel()
	modelNow := model.Now()
	for i, tc := range []struct {
		fakeLimits       tripperware.Limits
		Handler          tripperware.HandlerFunc
		expectedResponse tripperware.Response
	}{
		{
			fakeLimits:       mockLimits{maxCacheFreshness: 5 * time.Second},
			Handler:          nil,
			expectedResponse: mkAPIResponse(int64(modelNow)-(50*1e3), int64(modelNow)-(10*1e3), 10),
		},
		{
			// should not lookup cache because per-tenant override will be applied
			fakeLimits: mockLimits{maxCacheFreshness: 10 * time.Minute},
			Handler: tripperware.HandlerFunc(func(_ context.Context, _ tripperware.Request) (tripperware.Response, error) {
				return parsedResponse, nil
			}),
			expectedResponse: parsedResponse,
		},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			var cfg ResultsCacheConfig
			flagext.DefaultValues(&cfg)
			cfg.CacheConfig.Cache = cache.NewMockCache()

			fakeLimits := tc.fakeLimits
			rcm, _, err := NewResultsCacheMiddleware(
				log.NewNopLogger(),
				cfg,
				constSplitter(day),
				fakeLimits,
				PrometheusCodec,
				PrometheusResponseExtractor{},
				nil,
				nil,
			)
			require.NoError(t, err)

			// create cache with handler
			rc := rcm.Wrap(tc.Handler)
			ctx := user.InjectOrgID(context.Background(), "1")

			// create request with start end within the key extents
			req := parsedRequest.WithStartEnd(int64(modelNow)-(50*1e3), int64(modelNow)-(10*1e3))

			// fill cache
			key := constSplitter(day).GenerateCacheKey("1", req)
			rc.(*resultsCache).put(ctx, key, []tripperware.Extent{mkExtent(int64(modelNow)-(600*1e3), int64(modelNow))})

			resp, err := rc.Do(ctx, req)
			require.NoError(t, err)
			require.Equal(t, tc.expectedResponse, resp)
		})
	}
}

func Test_resultsCache_MissingData(t *testing.T) {
	t.Parallel()
	cfg := ResultsCacheConfig{
		CacheConfig: cache.Config{
			Cache: cache.NewMockCache(),
		},
	}
	rm, _, err := NewResultsCacheMiddleware(
		log.NewNopLogger(),
		cfg,
		constSplitter(day),
		mockLimits{},
		PrometheusCodec,
		PrometheusResponseExtractor{},
		nil,
		nil,
	)
	require.NoError(t, err)
	rc := rm.Wrap(nil).(*resultsCache)
	ctx := context.Background()

	// fill up the cache
	rc.put(ctx, "empty", []tripperware.Extent{{
		Start:    100,
		End:      200,
		Response: nil,
	}})
	rc.put(ctx, "notempty", []tripperware.Extent{mkExtent(100, 120)})
	rc.put(ctx, "mixed", []tripperware.Extent{mkExtent(100, 120), {
		Start:    120,
		End:      200,
		Response: nil,
	}})

	extents, hit := rc.get(ctx, "empty")
	require.Empty(t, extents)
	require.False(t, hit)

	extents, hit = rc.get(ctx, "notempty")
	require.Equal(t, len(extents), 1)
	require.True(t, hit)

	extents, hit = rc.get(ctx, "mixed")
	require.Equal(t, len(extents), 0)
	require.False(t, hit)
}

func TestConstSplitter_generateCacheKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		r        tripperware.Request
		interval time.Duration
		want     string
	}{
		{"0", &tripperware.PrometheusRequest{Start: 0, Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:0"},
		{"<30m", &tripperware.PrometheusRequest{Start: toMs(10 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:0"},
		{"30m", &tripperware.PrometheusRequest{Start: toMs(30 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:1"},
		{"91m", &tripperware.PrometheusRequest{Start: toMs(91 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:3"},
		{"0", &tripperware.PrometheusRequest{Start: 0, Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:0"},
		{"<1d", &tripperware.PrometheusRequest{Start: toMs(22 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:0"},
		{"4d", &tripperware.PrometheusRequest{Start: toMs(4 * 24 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:4"},
		{"3d5h", &tripperware.PrometheusRequest{Start: toMs(77 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:3"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("%s - %s", tt.name, tt.interval), func(t *testing.T) {
			t.Parallel()
			if got := constSplitter(tt.interval).GenerateCacheKey("fake", tt.r); got != tt.want {
				t.Errorf("generateKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResultsCacheShouldCacheFunc(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name         string
		shouldCache  ShouldCacheFn
		requests     []tripperware.Request
		expectedCall int
	}{
		{
			name:         "normal",
			shouldCache:  nil,
			requests:     []tripperware.Request{parsedRequest, parsedRequest},
			expectedCall: 1,
		},
		{
			name: "always no cache",
			shouldCache: func(r tripperware.Request) bool {
				return false
			},
			requests:     []tripperware.Request{parsedRequest, parsedRequest},
			expectedCall: 2,
		},
		{
			name: "check cache based on request",
			shouldCache: func(r tripperware.Request) bool {
				if v, ok := r.(*tripperware.PrometheusRequest); ok {
					return !v.CachingOptions.Disabled
				}
				return false
			},
			requests:     []tripperware.Request{noCacheRequest, noCacheRequest},
			expectedCall: 2,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			var cfg ResultsCacheConfig
			flagext.DefaultValues(&cfg)
			cfg.CacheConfig.Cache = cache.NewMockCache()
			rcm, _, err := NewResultsCacheMiddleware(
				log.NewNopLogger(),
				cfg,
				constSplitter(day),
				mockLimits{maxCacheFreshness: 10 * time.Minute},
				PrometheusCodec,
				PrometheusResponseExtractor{},
				tc.shouldCache,
				nil,
			)
			require.NoError(t, err)
			rc := rcm.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
				calls++
				return parsedResponse, nil
			}))

			for _, req := range tc.requests {
				ctx := user.InjectOrgID(context.Background(), "1")
				_, err := rc.Do(ctx, req)
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedCall, calls)
		})
	}
}

func TestResultsCacheFillCompatibility(t *testing.T) {
	var cfg ResultsCacheConfig
	flagext.DefaultValues(&cfg)
	c := cache.NewMockCache()
	cfg.CacheConfig.Cache = c
	rcm, _, err := NewResultsCacheMiddleware(
		log.NewNopLogger(),
		cfg,
		constSplitter(day),
		mockLimits{maxCacheFreshness: 10 * time.Minute},
		PrometheusCodec,
		PrometheusResponseExtractor{},
		nil,
		nil,
	)
	require.NoError(t, err)
	rc := rcm.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
		return parsedResponse, nil
	}))
	ctx := user.InjectOrgID(context.Background(), "1")
	_, err = rc.Do(ctx, parsedRequest)
	require.NoError(t, err)

	// Check cache and make sure we write response in old format even though the response is new format.
	tenantIDs, err := tenant.TenantIDs(ctx)
	require.NoError(t, err)
	cacheKey := cache.HashKey(constSplitter(day).GenerateCacheKey(tenant.JoinTenantIDs(tenantIDs), parsedRequest))
	found, bufs, _ := c.Fetch(ctx, []string{cacheKey})
	require.Equal(t, []string{cacheKey}, found)
	require.Len(t, bufs, 1)

	var resp tripperware.CachedResponse
	require.NoError(t, proto.Unmarshal(bufs[0], &resp))
	require.Len(t, resp.Extents, 1)
	expectedResp := convertFromTripperwarePrometheusResponse(parsedResponse)
	any, err := types.MarshalAny(expectedResp)
	require.NoError(t, err)
	require.Equal(t, any, resp.Extents[0].Response)
}

func toMs(t time.Duration) int64 {
	return int64(t / time.Millisecond)
}
