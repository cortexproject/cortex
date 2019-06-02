package queryrange

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	query        = "/api/v1/query_range?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&step=120"
	responseBody = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
)

var (
	parsedRequest = &Request{
		Path:  "/api/v1/query_range",
		Start: 1536673680 * 1e3,
		End:   1536716898 * 1e3,
		Step:  120 * 1e3,
		Query: "sum(container_memory_rss) by (namespace)",
	}
	parsedResponse = &APIResponse{
		Status: "success",
		Data: Response{
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

var dummyResponse = &APIResponse{
	Status: statusSuccess,
	Data: Response{
		ResultType: Matrix,
		Result: []SampleStream{
			{
				Labels: []client.LabelAdapter{
					{Name: "foo", Value: "bar"},
				},
				Samples: []client.Sample{
					{
						TimestampMs: 60,
						Value:       60,
					},
				},
			},
		},
	},
}

func mkAPIResponse(start, end, step int64) *APIResponse {
	var samples []client.Sample
	for i := start; i <= end; i += step {
		samples = append(samples, client.Sample{
			TimestampMs: int64(i),
			Value:       float64(i),
		})
	}

	return &APIResponse{
		Status: statusSuccess,
		Data: Response{
			ResultType: Matrix,
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
	return Extent{
		Start:    start,
		End:      end,
		Response: mkAPIResponse(start, end, 10),
	}
}

func TestPartiton(t *testing.T) {
	for i, tc := range []struct {
		input                  *Request
		prevCachedResponse     []Extent
		expectedRequests       []*Request
		expectedCachedResponse []*APIResponse
	}{
		// 1. Test a complete hit.
		{
			input: &Request{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []Extent{
				mkExtent(0, 100),
			},
			expectedCachedResponse: []*APIResponse{
				mkAPIResponse(0, 100, 10),
			},
		},

		// Test with a complete miss.
		{
			input: &Request{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []Extent{
				mkExtent(110, 210),
			},
			expectedRequests: []*Request{{
				Start: 0,
				End:   100,
			}},
			expectedCachedResponse: nil,
		},

		// Test a partial hit.
		{
			input: &Request{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 100),
			},
			expectedRequests: []*Request{
				{
					Start: 0,
					End:   50,
				},
			},
			expectedCachedResponse: []*APIResponse{
				mkAPIResponse(50, 100, 10),
			},
		},

		// Test multiple partial hits.
		{
			input: &Request{
				Start: 100,
				End:   200,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 120),
				mkExtent(160, 250),
			},
			expectedRequests: []*Request{
				{
					Start: 120,
					End:   160,
				},
			},
			expectedCachedResponse: []*APIResponse{
				mkAPIResponse(100, 120, 10),
				mkAPIResponse(160, 200, 10),
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			reqs, resps := partition(tc.input, tc.prevCachedResponse)
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

func TestResultsCache(t *testing.T) {
	calls := 0
	rcm, err := NewResultsCacheMiddlewareFromConfig(
		log.NewNopLogger(),
		ResultsCacheConfig{
			CacheConfig: cache.Config{
				Cache: cache.NewMockCache(),
			},
		},
		fakeLimits{},
	)
	require.NoError(t, err)

	rc := rcm.Wrap(HandlerFunc(func(_ context.Context, req *Request) (*APIResponse, error) {
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
	req := parsedRequest.Copy()
	req.End += 100
	resp, err = rc.Do(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestResultsCacheRecent(t *testing.T) {
	var cfg ResultsCacheConfig
	flagext.DefaultValues(&cfg)
	cfg.CacheConfig.Cache = cache.NewMockCache()
	rcm, err := NewResultsCacheMiddlewareFromConfig(log.NewNopLogger(), cfg, fakeLimits{})
	require.NoError(t, err)

	req := parsedRequest.Copy()
	req.End = int64(model.Now())
	req.Start = req.End - (60 * 1e3)

	calls := 0
	rc := rcm.Wrap(HandlerFunc(func(_ context.Context, r *Request) (*APIResponse, error) {
		calls++
		assert.Equal(t, r, &req)
		return parsedResponse, nil
	}))
	ctx := user.InjectOrgID(context.Background(), "1")

	// Request should result in a query.
	resp, err := rc.Do(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, parsedResponse, resp)

	// Doing same request again should result in another query.
	resp, err = rc.Do(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, parsedResponse, resp)
}
