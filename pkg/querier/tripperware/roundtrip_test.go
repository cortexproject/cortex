package tripperware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
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
	remoteReadQuery               = "/api/v1/read"
	labelNamesQuery               = "/api/v1/labels"
	labelValuesQuery              = "/api/v1/label/label/values"
	metadataQuery                 = "/api/v1/metadata"

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

func (c mockCodec) EncodeResponse(_ context.Context, _ *http.Request, resp Response) (*http.Response, error) {
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

var (
	instantMiddlewares = []Middleware{
		MiddlewareFunc(func(next Handler) Handler {
			return mockMiddleware{}
		}),
	}
	rangeMiddlewares = []Middleware{
		MiddlewareFunc(func(next Handler) Handler {
			return mockMiddleware{}
		}),
	}
)

func TestRoundTrip(t *testing.T) {
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
		userAgent          string
		expectedMetric     string
	}{
		{
			path:             "/foo",
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.0",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query", source="api", user="1"} 1
`,
		},
		{
			path:             queryExemplar,
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.1",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query_exemplars", source="api", user="1"} 1
`,
		},
		{
			path:             seriesQuery,
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.2",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="series", source="api", user="1"} 1
`,
		},
		{
			path:             labelNamesQuery,
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.2",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="label_names", source="api", user="1"} 1
`,
		},
		{
			path:             labelValuesQuery,
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.2",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="label_values", source="api", user="1"} 1
`,
		},
		{
			path:             metadataQuery,
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.2",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="metadata", source="api", user="1"} 1
`,
		},
		{
			path:             remoteReadQuery,
			expectedBody:     "bar",
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.2",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="remote_read", source="api", user="1"} 1
`,
		},
		{
			path:             queryRange,
			expectedBody:     responseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.0",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query_range", source="api", user="1"} 1
`,
		},
		{
			path:             query,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.1",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query", source="api", user="1"} 1
`,
		},
		{
			path:             queryNonShardable,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.2",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query", source="api", user="1"} 1
`,
		},
		{
			path:             query,
			expectedBody:     instantResponseBody,
			limits:           shardingOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/1.0",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query", source="api", user="1"} 1
`,
		},
		// Shouldn't hit subquery step limit because max steps is set to 0 so this check is disabled.
		{
			path:             querySubqueryStepSizeTooSmall,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 0,
			userAgent:        "dummyUserAgent/1.0",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query", source="api", user="1"} 1
`,
		},
		// Shouldn't hit subquery step limit because max steps is higher, which is 100K.
		{
			path:             querySubqueryStepSizeTooSmall,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 100000,
			userAgent:        "dummyUserAgent/1.0",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query", source="api", user="1"} 1
`,
		},
		{
			path:             querySubqueryStepSizeTooSmall,
			expectedErr:      httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, 11000),
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        fmt.Sprintf("%s/%s", RulerUserAgent, version.Version),
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query", source="ruler", user="1"} 1
`,
		},
		{
			// The query should go to instant query middlewares rather than forwarding to next.
			path:             queryExceedsMaxQueryLength,
			expectedBody:     instantResponseBody,
			limits:           defaultOverrides,
			maxSubQuerySteps: 11000,
			userAgent:        "dummyUserAgent/2.0",
			expectedMetric: `
# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
# TYPE cortex_query_frontend_queries_total counter
cortex_query_frontend_queries_total{op="query", source="api", user="1"} 1
`,
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

			req.Header.Set("User-Agent", tc.userAgent)

			reg := prometheus.NewPedanticRegistry()
			tw := NewQueryTripperware(log.NewNopLogger(),
				reg,
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
				false,
			)
			resp, err := tw(downstream).RoundTrip(req)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, 200, resp.StatusCode)

				bs, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, tc.expectedBody, string(bs))
				require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(tc.expectedMetric), "cortex_query_frontend_queries_total"))
			} else {
				require.Equal(t, tc.expectedErr, err)
			}
		})
	}
}
