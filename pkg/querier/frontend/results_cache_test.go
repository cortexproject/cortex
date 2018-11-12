package frontend

import (
	"context"
	"strconv"
	"testing"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/wire"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

var dummyResponse = &APIResponse{
	Status: statusSuccess,
	Data: QueryRangeResponse{
		ResultType: matrix,
		Result: []SampleStream{
			{
				Labels: []client.LabelPair{
					{wire.Bytes("foo"), wire.Bytes("bar")},
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
	samples := []client.Sample{}
	for i := start; i <= end; i += step {
		samples = append(samples, client.Sample{
			TimestampMs: int64(i),
			Value:       float64(i),
		})
	}

	return &APIResponse{
		Status: statusSuccess,
		Data: QueryRangeResponse{
			ResultType: matrix,
			Result: []SampleStream{
				{
					Labels: []client.LabelPair{
						{wire.Bytes("foo"), wire.Bytes("bar")},
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
		input                  *QueryRangeRequest
		prevCachedResponse     []Extent
		expectedRequests       []*QueryRangeRequest
		expectedCachedResponse []*APIResponse
	}{
		// 1. Test a complete hit.
		{
			input: &QueryRangeRequest{
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
			input: &QueryRangeRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []Extent{
				mkExtent(110, 210),
			},
			expectedRequests: []*QueryRangeRequest{{
				Start: 0,
				End:   100,
			}},
			expectedCachedResponse: nil,
		},

		// Test a partial hit.
		{
			input: &QueryRangeRequest{
				Start: 0,
				End:   100,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 100),
			},
			expectedRequests: []*QueryRangeRequest{
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
			input: &QueryRangeRequest{
				Start: 100,
				End:   200,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 120),
				mkExtent(160, 250),
			},
			expectedRequests: []*QueryRangeRequest{
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

func TestResultsCache(t *testing.T) {
	calls := 0
	rcm, err := newResultsCacheMiddleware(
		resultsCacheConfig{
			cacheConfig: cache.Config{
				Cache: cache.NewMockCache(),
			},
		})
	require.NoError(t, err)

	rc := rcm.Wrap(queryRangeHandlerFunc(func(_ context.Context, req *QueryRangeRequest) (*APIResponse, error) {
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
	req := parsedRequest.copy()
	req.End += 100
	resp, err = rc.Do(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestResultsCacheRecent(t *testing.T) {
	var cfg resultsCacheConfig
	util.DefaultValues(&cfg)
	cfg.cacheConfig.Cache = cache.NewMockCache()
	rcm, err := newResultsCacheMiddleware(cfg)
	require.NoError(t, err)

	req := parsedRequest.copy()
	req.End = int64(model.Now())
	req.Start = req.End - (60 * 1e3)

	calls := 0
	rc := rcm.Wrap(queryRangeHandlerFunc(func(_ context.Context, r *QueryRangeRequest) (*APIResponse, error) {
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
