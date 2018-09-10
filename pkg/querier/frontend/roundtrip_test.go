package frontend

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
)

const (
	query        = "/api/v1/query_range?end=1536760200&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&step=120"
	responseBody = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1536763606.651,"137"],[1536763607.651,"137"]]}]}}`
)

var parsedResponse = &apiResponse{
	Status: "success",
	Data: queryRangeResponse{
		ResultType: model.ValMatrix,
		Result: model.Matrix{
			&model.SampleStream{
				Metric: model.Metric{},
				Values: []model.SamplePair{
					{1536763606651, 137},
					{1536763607651, 137},
				},
			},
		},
	},
}

func TestQueryRangeRequest(t *testing.T) {
	for i, tc := range []struct {
		url         string
		expected    *queryRangeRequest
		expectedErr error
	}{
		{
			url: query,
			expected: &queryRangeRequest{
				path:  "/api/v1/query_range",
				start: 1536673680 * 1e3,
				end:   1536760200 * 1e3,
				step:  120 * 1e3,
				query: "sum(container_memory_rss) by (namespace)",
			},
		},
		{
			url:         "api/v1/query_range?start=foo",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "cannot parse \"foo\" to a valid timestamp"),
		},
		{
			url:         "api/v1/query_range?start=123&end=bar",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "cannot parse \"bar\" to a valid timestamp"),
		},
		{
			url:         "api/v1/query_range?start=123&end=0",
			expectedErr: errEndBeforeStart,
		},
		{
			url:         "api/v1/query_range?start=123&end=456&step=baz",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "cannot parse \"baz\" to a valid duration"),
		},
		{
			url:         "api/v1/query_range?start=123&end=456&step=-1",
			expectedErr: errNegativeStep,
		},
		{
			url:         "api/v1/query_range?start=0&end=11001&step=1",
			expectedErr: errStepTooSmall,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r, err := http.NewRequest("GET", tc.url, nil)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "1")
			r = r.WithContext(ctx)

			req, err := parseQueryRangeRequest(r)
			if err != nil {
				require.EqualValues(t, tc.expectedErr, err)
				return
			}
			require.EqualValues(t, tc.expected, req)

			rdash, err := req.toHTTPRequest(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, tc.url, rdash.RequestURI)
		})
	}
}

func TestQueryRangeResponse(t *testing.T) {
	for i, tc := range []struct {
		body     string
		expected *apiResponse
	}{
		{
			body:     responseBody,
			expected: parsedResponse,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			response := &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       ioutil.NopCloser(bytes.NewBuffer([]byte(tc.body))),
			}
			resp, err := parseQueryRangeResponse(response)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, resp)

			// Reset response, as the above call will have consumed the body reader.
			response = &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       ioutil.NopCloser(bytes.NewBuffer([]byte(tc.body))),
			}
			resp2, err := resp.toHTTPResponse()
			require.NoError(t, err)
			assert.Equal(t, response, resp2)
		})
	}
}

func TestRoundTrip(t *testing.T) {
	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.RequestURI == query {
					w.Write([]byte(responseBody))
				} else {
					w.Write([]byte("bar"))
				}
			}),
		),
	)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)

	downstream := singleHostRoundTripper{
		host:       u.Host,
		downstream: http.DefaultTransport,
	}
	roundtripper := queryRangeRoundTripper{
		downstream: downstream,
		queryRangeMiddleware: queryRangeTerminator{
			downstream: downstream,
		},
	}

	for i, tc := range []struct {
		path, expectedBody string
	}{
		{"/foo", "bar"},
		{query, responseBody},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			req, err := http.NewRequest("GET", tc.path, http.NoBody)
			require.NoError(t, err)

			// query-frontend doesn't actually authenticate requests, we rely on
			// the queriers to do this.  Hence we ensure the request doesn't have a
			// org ID in the ctx, but does have the header.
			ctx := user.InjectOrgID(context.Background(), "1")
			req = req.WithContext(ctx)
			err = user.InjectOrgIDIntoHTTPRequest(ctx, req)
			require.NoError(t, err)

			resp, err := roundtripper.RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			bs, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(bs))
		})
	}
}

type singleHostRoundTripper struct {
	host       string
	downstream http.RoundTripper
}

func (s singleHostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = "http"
	r.URL.Host = s.host
	return s.downstream.RoundTrip(r)
}
