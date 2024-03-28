package tripperware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	queryRange                    = "/api/v1/query_range?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&stats=all&step=120"
	query                         = "/api/v1/query?time=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29"
	queryNonShardable             = "/api/v1/query?time=1536716898&query=container_memory_rss"
	queryExemplar                 = "/api/v1/query_exemplars?query=test_exemplar_metric_total&start=2020-09-14T15:22:25.479Z&end=2020-09-14T15:23:25.479Z'"
	querySubqueryStepSizeTooSmall = "/api/v1/query?query=up%5B30d%3A%5D"
	queryExceedsMaxQueryLength    = "/api/v1/query?query=up%5B90d%5D"
	seriesQuery                   = "/api/v1/series?match[]"

	responseBody        = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
	instantResponseBody = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
)

type mockRequest struct {
	Request
	resp string
}

type mockResponse struct {
	Response
	resp string
}

type mockCodec struct {
	Codec
}

func (c mockCodec) DecodeRequest(_ context.Context, r *http.Request, _ []string) (Request, error) {
	if strings.HasPrefix(r.URL.String(), "/api/v1/query_range") {
		return &mockRequest{resp: responseBody}, nil
	}
	if strings.HasPrefix(r.URL.String(), "/api/v1/query") {
		return &mockRequest{resp: instantResponseBody}, nil
	}
	return mockRequest{}, nil
}

func (c mockCodec) EncodeResponse(_ context.Context, resp Response) (*http.Response, error) {
	r := resp.(*mockResponse)
	return &http.Response{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:          io.NopCloser(bytes.NewBuffer([]byte(r.resp))),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len([]byte(r.resp))),
	}, nil
}

type mockMiddleware struct {
}

func (m mockMiddleware) Do(_ context.Context, req Request) (Response, error) {
	r := req.(*mockRequest)
	return &mockResponse{resp: r.resp}, nil
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("bar"))
			if err != nil {
				t.Fatal(err)
			}
		}),
	)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)

	downstream := singleHostRoundTripper{
		host: u.Host,
		next: http.DefaultTransport,
	}

	instantMiddlewares := []Middleware{
		MiddlewareFunc(func(next Handler) Handler {
			return mockMiddleware{}
		}),
	}
	rangeMiddlewares := []Middleware{
		MiddlewareFunc(func(next Handler) Handler {
			return mockMiddleware{}
		}),
	}

	limits := validation.Limits{
		MaxQueryLength: model.Duration(time.Hour * 24 * 60),
	}
	flagext.DefaultValues(&limits)
	defaultOverrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	limitsWithVerticalSharding := validation.Limits{QueryVerticalShardSize: 3}
	shardingOverrides, err := validation.NewOverrides(limitsWithVerticalSharding, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		path, expectedBody string
		expectedErr        error
		limits             Limits
		maxSubQuerySteps   int64
	}{
		{
			path:             "/foo",
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
		},
		{
			path:             queryExemplar,
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
		},
		{
			path:             seriesQuery,
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
		},
		{
			path:             queryRange,
			expectedBody:     responseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
		},
		{
			path:             query,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
		},
		{
			path:             queryNonShardable,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
		},
		{
			path:             query,
			expectedBody:     instantResponseBody,
			limits:           shardingOverrides,
			maxSubQuerySteps: 11000,
		},
		// Shouldn't hit subquery step limit because max steps is set to 0 so this check is disabled.
		{
			path:             querySubqueryStepSizeTooSmall,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 0,
		},
		// Shouldn't hit subquery step limit because max steps is higher, which is 100K.
		{
			path:             querySubqueryStepSizeTooSmall,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 100000,
		},
		{
			path:             querySubqueryStepSizeTooSmall,
			expectedErr:      httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, 11000),
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
		},
		{
			// The query should go to instant query middlewares rather than forwarding to next.
			path:             queryExceedsMaxQueryLength,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
		},
	} {
		t.Run(tc.path, func(t *testing.T) {
			//parallel testing causes data race
			req, err := http.NewRequest("GET", tc.path, http.NoBody)
			require.NoError(t, err)

			// query-frontend doesn't actually authenticate requests, we rely on
			// the queriers to do this.  Hence we ensure the request doesn't have a
			// org ID in the ctx, but does have the header.
			ctx := user.InjectOrgID(context.Background(), "1")
			req = req.WithContext(ctx)
			err = user.InjectOrgIDIntoHTTPRequest(ctx, req)
			require.NoError(t, err)

			tw := NewQueryTripperware(log.NewNopLogger(),
				nil,
				nil,
				rangeMiddlewares,
				instantMiddlewares,
				mockCodec{},
				mockCodec{},
				tc.limits,
				querysharding.NewQueryAnalyzer(),
				time.Minute,
				tc.maxSubQuerySteps,
				0,
			)
			resp, err := tw(downstream).RoundTrip(req)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, 200, resp.StatusCode)

				bs, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, tc.expectedBody, string(bs))
			} else {
				require.Equal(t, tc.expectedErr, err)
			}
		})
	}
}
