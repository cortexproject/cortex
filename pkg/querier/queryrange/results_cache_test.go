package queryrange

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	query        = "/api/v1/query_range?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&step=120"
	responseBody = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
)

var (
	parsedRequest = &PrometheusRequest{
		Path:  "/api/v1/query_range",
		Start: 1536673680 * 1e3,
		End:   1536716898 * 1e3,
		Step:  120 * 1e3,
		Query: "sum(container_memory_rss) by (namespace)",
	}
	respHeaders = []*PrometheusResponseHeader{
		{
			Name:   "Content-Type",
			Values: []string{"application/json"},
		},
	}
	parsedResponse = &PrometheusResponse{
		Status: "success",
		Data: PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []SampleStream{
				{
					Labels: []client.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Samples: []client.Sample{
						{Value: 137, TimestampMs: 1536673680000},
						{Value: 137, TimestampMs: 1536673780000},
					},
				},
			},
		},
	}
)

func mkAPIResponse(start, end, step int64) *PrometheusResponse {
	var samples []client.Sample
	for i := start; i <= end; i += step {
		samples = append(samples, client.Sample{
			TimestampMs: int64(i),
			Value:       float64(i),
		})
	}

	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: matrix,
			Result: []SampleStream{
				{
					Labels: []client.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Samples: samples,
				},
			},
		},
	}
}

func mkExtent(start, end int64) Extent {
	res := mkAPIResponse(start, end, 10)
	any, err := types.MarshalAny(res)
	if err != nil {
		panic(err)
	}
	return Extent{
		Start:    start,
		End:      end,
		Response: any,
	}
}

func TestShouldCache(t *testing.T) {
	c := &resultsCache{logger: util.Logger, cacheGenNumberLoader: newMockCacheGenNumberLoader()}
	for _, tc := range []struct {
		name                   string
		input                  Response
		cacheGenNumberToInject string
		expected               bool
	}{
		// Tests only for cacheControlHeader
		{
			name: "does not contain the cacheControl header",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   "meaninglessheader",
						Values: []string{},
					},
				},
			}),
			expected: true,
		},
		{
			name: "does contain the cacheControl header which has the value",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   cachecontrolHeader,
						Values: []string{noCacheValue},
					},
				},
			}),
			expected: false,
		},
		{
			name: "cacheControl header contains extra values but still good",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   cachecontrolHeader,
						Values: []string{"foo", noCacheValue},
					},
				},
			}),
			expected: false,
		},
		{
			name:     "broken response",
			input:    Response(&PrometheusResponse{}),
			expected: true,
		},
		{
			name: "nil headers",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{nil},
			}),
			expected: true,
		},
		{
			name: "had cacheControl header but no values",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{{Name: cachecontrolHeader}},
			}),
			expected: true,
		},

		// Tests only for cacheGenNumber header
		{
			name: "cacheGenNumber not set in both header and store",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   "meaninglessheader",
						Values: []string{},
					},
				},
			}),
			expected: true,
		},
		{
			name: "cacheGenNumber set in store but not in header",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   "meaninglessheader",
						Values: []string{},
					},
				},
			}),
			cacheGenNumberToInject: "1",
			expected:               false,
		},
		{
			name: "cacheGenNumber set in header but not in store",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   ResultsCacheGenNumberHeaderName,
						Values: []string{"1"},
					},
				},
			}),
			expected: false,
		},
		{
			name: "cacheGenNumber in header and store are the same",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   ResultsCacheGenNumberHeaderName,
						Values: []string{"1", "1"},
					},
				},
			}),
			cacheGenNumberToInject: "1",
			expected:               true,
		},
		{
			name: "inconsistency between cacheGenNumber in header and store",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   ResultsCacheGenNumberHeaderName,
						Values: []string{"1", "2"},
					},
				},
			}),
			cacheGenNumberToInject: "1",
			expected:               false,
		},
		{
			name: "cacheControl header says not to catch and cacheGenNumbers in store and headers have consistency",
			input: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   cachecontrolHeader,
						Values: []string{noCacheValue},
					},
					{
						Name:   ResultsCacheGenNumberHeaderName,
						Values: []string{"1", "1"},
					},
				},
			}),
			cacheGenNumberToInject: "1",
			expected:               false,
		},
	} {
		{
			t.Run(tc.name, func(t *testing.T) {
				ctx := cache.InjectCacheGenNumber(context.Background(), tc.cacheGenNumberToInject)
				ret := c.shouldCacheResponse(ctx, tc.input)
				require.Equal(t, tc.expected, ret)
			})
		}
	}
}

func TestPartiton(t *testing.T) {
	for i, tc := range []struct {
		input                  Request
		prevCachedResponse     []Extent
		expectedRequests       []Request
		expectedCachedResponse []Response
	}{
		// 1. Test a complete hit.
		{
			input: &PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []Extent{
				mkExtent(0, 100),
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(0, 100, 10),
			},
		},

		// Test with a complete miss.
		{
			input: &PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []Extent{
				mkExtent(110, 210),
			},
			expectedRequests: []Request{
				&PrometheusRequest{
					Start: 0,
					End:   100,
				}},
			expectedCachedResponse: nil,
		},

		// Test a partial hit.
		{
			input: &PrometheusRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 100),
			},
			expectedRequests: []Request{
				&PrometheusRequest{
					Start: 0,
					End:   50,
				},
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(50, 100, 10),
			},
		},

		// Test multiple partial hits.
		{
			input: &PrometheusRequest{
				Start: 100,
				End:   200,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 120),
				mkExtent(160, 250),
			},
			expectedRequests: []Request{
				&PrometheusRequest{
					Start: 120,
					End:   160,
				},
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(100, 120, 10),
				mkAPIResponse(160, 200, 10),
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			reqs, resps, err := partition(tc.input, tc.prevCachedResponse, PrometheusResponseExtractor{})
			require.Nil(t, err)
			require.Equal(t, tc.expectedRequests, reqs)
			require.Equal(t, tc.expectedCachedResponse, resps)
		})
	}
}

type fakeLimits struct{}

func (fakeLimits) MaxQueryLength(string) time.Duration {
	return 0 // Disable.
}

func (fakeLimits) MaxQueryParallelism(string) int {
	return 14 // Flag default.
}

func (fakeLimits) MaxCacheFreshness(string) time.Duration {
	return time.Duration(0)
}

type fakeLimitsHighMaxCacheFreshness struct {
	fakeLimits
}

func (fakeLimitsHighMaxCacheFreshness) MaxCacheFreshness(string) time.Duration {
	return 10 * time.Minute
}

func TestResultsCache(t *testing.T) {
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
		fakeLimits{},
		PrometheusCodec,
		PrometheusResponseExtractor{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	rc := rcm.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
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
	var cfg ResultsCacheConfig
	flagext.DefaultValues(&cfg)
	cfg.CacheConfig.Cache = cache.NewMockCache()
	rcm, _, err := NewResultsCacheMiddleware(
		log.NewNopLogger(),
		cfg,
		constSplitter(day),
		fakeLimitsHighMaxCacheFreshness{},
		PrometheusCodec,
		PrometheusResponseExtractor{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	req := parsedRequest.WithStartEnd(int64(model.Now())-(60*1e3), int64(model.Now()))

	calls := 0
	rc := rcm.Wrap(HandlerFunc(func(_ context.Context, r Request) (Response, error) {
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
	modelNow := model.Now()
	for i, tc := range []struct {
		legacyMaxCacheFreshness time.Duration
		fakeLimits              Limits
		Handler                 HandlerFunc
		expectedResponse        *PrometheusResponse
	}{
		{
			// should lookup cache because legacy cache max freshness will be applied
			legacyMaxCacheFreshness: 5 * time.Second,
			fakeLimits:              fakeLimits{},
			Handler:                 nil,
			expectedResponse:        mkAPIResponse(int64(modelNow)-(50*1e3), int64(modelNow)-(10*1e3), 10),
		},
		{
			// should not lookup cache because per-tenant override will be applied
			legacyMaxCacheFreshness: time.Duration(0),
			fakeLimits:              fakeLimitsHighMaxCacheFreshness{},
			Handler: HandlerFunc(func(_ context.Context, _ Request) (Response, error) {
				return parsedResponse, nil
			}),
			expectedResponse: parsedResponse,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var cfg ResultsCacheConfig
			flagext.DefaultValues(&cfg)
			cfg.CacheConfig.Cache = cache.NewMockCache()

			cfg.LegacyMaxCacheFreshness = tc.legacyMaxCacheFreshness

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
			rc.(*resultsCache).put(ctx, key, []Extent{mkExtent(int64(modelNow)-(60*1e3), int64(modelNow))})

			resp, err := rc.Do(ctx, req)
			require.NoError(t, err)
			require.Equal(t, tc.expectedResponse, resp)
		})
	}
}

func Test_resultsCache_MissingData(t *testing.T) {
	cfg := ResultsCacheConfig{
		CacheConfig: cache.Config{
			Cache: cache.NewMockCache(),
		},
	}
	rm, _, err := NewResultsCacheMiddleware(
		log.NewNopLogger(),
		cfg,
		constSplitter(day),
		fakeLimits{},
		PrometheusCodec,
		PrometheusResponseExtractor{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	rc := rm.Wrap(nil).(*resultsCache)
	ctx := context.Background()

	// fill up the cache
	rc.put(ctx, "empty", []Extent{{
		Start:    100,
		End:      200,
		Response: nil,
	}})
	rc.put(ctx, "notempty", []Extent{mkExtent(100, 120)})
	rc.put(ctx, "mixed", []Extent{mkExtent(100, 120), {
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
		r        Request
		interval time.Duration
		want     string
	}{
		{"0", &PrometheusRequest{Start: 0, Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:0"},
		{"<30m", &PrometheusRequest{Start: toMs(10 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:0"},
		{"30m", &PrometheusRequest{Start: toMs(30 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:1"},
		{"91m", &PrometheusRequest{Start: toMs(91 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:3"},
		{"0", &PrometheusRequest{Start: 0, Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:0"},
		{"<1d", &PrometheusRequest{Start: toMs(22 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:0"},
		{"4d", &PrometheusRequest{Start: toMs(4 * 24 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:4"},
		{"3d5h", &PrometheusRequest{Start: toMs(77 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:3"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s - %s", tt.name, tt.interval), func(t *testing.T) {
			if got := constSplitter(tt.interval).GenerateCacheKey("fake", tt.r); got != tt.want {
				t.Errorf("generateKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func toMs(t time.Duration) int64 {
	return int64(t / time.Millisecond)
}

type mockCacheGenNumberLoader struct {
}

func newMockCacheGenNumberLoader() CacheGenNumberLoader {
	return mockCacheGenNumberLoader{}
}

func (mockCacheGenNumberLoader) GetResultsCacheGenNumber(userID string) string {
	return ""
}
