package tripperware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	queryRange        = "/api/v1/query_range?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&stats=all&step=120"
	query             = "/api/v1/query?time=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680"
	queryNonShardable = "/api/v1/query?time=1536716898&query=container_memory_rss&start=1536673680"
	queryExemplar     = "/api/v1/query_exemplars?query=test_exemplar_metric_total&start=2020-09-14T15:22:25.479Z&end=2020-09-14T15:23:25.479Z'"
	responseBody      = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
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
	if r.URL.String() == query || r.URL.String() == queryRange {
		return &mockRequest{resp: responseBody}, nil
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

	middlewares := []Middleware{
		MiddlewareFunc(func(next Handler) Handler {
			return mockMiddleware{}
		}),
	}

	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	defaultOverrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	limitsWithVerticalSharding := validation.Limits{QueryVerticalShardSize: 3}
	shardingOverrides, err := validation.NewOverrides(limitsWithVerticalSharding, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		path, expectedBody string
		limits             Limits
	}{
		{
			path:         "/foo",
			expectedBody: "bar",
			limits:       defaultOverrides,
		},
		{
			path:         queryExemplar,
			expectedBody: "bar",
			limits:       defaultOverrides,
		},
		{
			path:         queryRange,
			expectedBody: responseBody,
			limits:       defaultOverrides,
		},
		{
			path:         query,
			expectedBody: "bar",
			limits:       defaultOverrides,
		},
		{
			path:         queryNonShardable,
			expectedBody: "bar",
			limits:       defaultOverrides,
		},
		{
			path:         queryNonShardable,
			expectedBody: "bar",
			limits:       shardingOverrides,
		},
		{
			path:         query,
			expectedBody: responseBody,
			limits:       shardingOverrides,
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
				middlewares,
				middlewares,
				mockCodec{},
				mockCodec{},
				tc.limits,
				querysharding.NewQueryAnalyzer(),
			)
			resp, err := tw(downstream).RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			bs, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(bs))
		})
	}
}
