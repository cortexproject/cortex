package frontend

import (
	"context"
	"strconv"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk/cache"
	"github.com/weaveworks/cortex/pkg/util"
)

var dummyResponse = &apiResponse{
	Status: statusSuccess,
	Data: queryRangeResponse{
		ResultType: model.ValMatrix,
		Result: model.Matrix{
			&model.SampleStream{
				Metric: model.Metric{
					"foo": "bar",
				},
				Values: []model.SamplePair{
					{
						Timestamp: 60,
						Value:     60,
					},
				},
			},
		},
	},
}

func mkAPIResponse(start, end, step int64) *apiResponse {
	samples := []model.SamplePair{}
	for i := start; i <= end; i += step {
		samples = append(samples, model.SamplePair{
			Timestamp: model.Time(i),
			Value:     model.SampleValue(float64(i)),
		})
	}

	return &apiResponse{
		Status: statusSuccess,
		Data: queryRangeResponse{
			ResultType: model.ValMatrix,
			Result: model.Matrix{
				&model.SampleStream{
					Metric: model.Metric{
						"foo": "bar",
					},
					Values: samples,
				},
			},
		},
	}
}

func mkExtent(start, end int64) extent {
	return extent{
		Start:    start,
		End:      end,
		Response: mkAPIResponse(start, end, 10),
	}
}

func TestPartiton(t *testing.T) {
	for i, tc := range []struct {
		input                  *queryRangeRequest
		prevCachedResponse     []extent
		expectedRequests       []*queryRangeRequest
		expectedCachedResponse []*apiResponse
	}{
		// 1. Test a complete hit.
		{
			input: &queryRangeRequest{
				start: 0,
				end:   100,
			},
			prevCachedResponse: []extent{
				mkExtent(0, 100),
			},
			expectedCachedResponse: []*apiResponse{
				mkAPIResponse(0, 100, 10),
			},
		},

		// Test with a complete miss.
		{
			input: &queryRangeRequest{
				start: 0,
				end:   100,
			},
			prevCachedResponse: []extent{
				mkExtent(110, 210),
			},
			expectedRequests: []*queryRangeRequest{{
				start: 0,
				end:   100,
			}},
			expectedCachedResponse: nil,
		},

		// Test a partial hit.
		{
			input: &queryRangeRequest{
				start: 0,
				end:   100,
			},
			prevCachedResponse: []extent{
				mkExtent(50, 100),
			},
			expectedRequests: []*queryRangeRequest{
				{
					start: 0,
					end:   50,
				},
			},
			expectedCachedResponse: []*apiResponse{
				mkAPIResponse(50, 100, 10),
			},
		},

		// Test multiple partial hits.
		{
			input: &queryRangeRequest{
				start: 100,
				end:   200,
			},
			prevCachedResponse: []extent{
				mkExtent(50, 120),
				mkExtent(160, 250),
			},
			expectedRequests: []*queryRangeRequest{
				{
					start: 120,
					end:   160,
				},
			},
			expectedCachedResponse: []*apiResponse{
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

	rc := rcm.Wrap(queryRangeHandlerFunc(func(_ context.Context, req *queryRangeRequest) (*apiResponse, error) {
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
	req.end += 100
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
	req.end = int64(model.Now())
	req.start = req.end - (60 * 1e3)

	calls := 0
	rc := rcm.Wrap(queryRangeHandlerFunc(func(_ context.Context, r *queryRangeRequest) (*apiResponse, error) {
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
