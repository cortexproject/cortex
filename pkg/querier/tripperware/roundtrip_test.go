package tripperware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

const (
	query        = "/api/v1/query_range?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&stats=all&step=120"
	responseBody = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
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
	if r.URL.String() == query {
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
	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var err error
			if r.RequestURI == query {
				_, err = w.Write([]byte(responseBody))
			} else {
				_, err = w.Write([]byte("bar"))
			}
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

	tw := NewQueryTripperware(log.NewNopLogger(),
		nil,
		nil,
		[]Middleware{
			MiddlewareFunc(func(next Handler) Handler {
				return mockMiddleware{}
			}),
		},
		mockCodec{},
	)

	if err != nil {
		t.Fatal(err)
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

			resp, err := tw(downstream).RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			bs, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(bs))
		})
	}
}

type singleHostRoundTripper struct {
	host string
	next http.RoundTripper
}

func (s singleHostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = "http"
	r.URL.Host = s.host
	return s.next.RoundTrip(r)
}
