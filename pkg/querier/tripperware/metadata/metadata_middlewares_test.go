package metadata

import (
	"context"
	io "io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
)

const (
	requestURI   = "/api/v1/series?end=1536716898&match%5B%5D=%7B__name__%3D~%22metric_0.%2A%22%7D&match%5B%5D=%7B__name__%3D~%22metric_1.%2A%22%7D&start=1536710400"
	requestURI2  = "/api/v1/series?match%5B%5D=%7B__name__%3D~%22metric_0.%2A%22%7D&match%5B%5D=%7B__name__%3D~%22metric_1.%2A%22%7D"
	responseBody = `{"status":"success","data":[{"__name__":"metric0","label1":"value"}]}`
)

func TestRoundTrip(t *testing.T) {
	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				if r.RequestURI == requestURI {
					_, err = w.Write([]byte(responseBody))
				} else if strings.Contains(r.RequestURI, "/api/v1/series") {
					// This is when we don't specify the start and end times.
					_, err = w.Write([]byte(responseBody))
				} else {
					_, err = w.Write([]byte("bar"))
				}
				if err != nil {
					t.Fatal(err)
				}
			}),
		),
	)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)

	downstream := singleHostRoundTripper{
		host: u.Host,
		next: http.DefaultTransport,
	}

	seriesmiddlewares, err := SeriesMiddlewares(
		queryrange.Config{
			SplitMetadataByInterval: 24 * time.Hour,
		},
		log.NewNopLogger(),
		mockLimits{},
		nil,
	)

	tw := tripperware.NewQueryTripperware(log.NewNopLogger(),
		nil,
		nil,
		nil,
		nil,
		seriesmiddlewares,
		nil,
		nil,
		NewSeriesCodec(queryrange.Config{}),
	)

	if err != nil {
		t.Fatal(err)
	}

	for i, tc := range []struct {
		path, expectedBody string
	}{
		{"/foo", "bar"},
		{requestURI, responseBody},
		{requestURI2, responseBody},
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

type mockLimits struct {
	maxQueryLookback  time.Duration
	maxQueryLength    time.Duration
	maxCacheFreshness time.Duration
	shardSize         int
}

func (m mockLimits) MaxQueryLookback(string) time.Duration {
	return m.maxQueryLookback
}

func (m mockLimits) MaxQueryLength(string) time.Duration {
	return m.maxQueryLength
}

func (mockLimits) MaxQueryParallelism(string) int {
	return 14 // Flag default.
}

func (m mockLimits) MaxCacheFreshness(string) time.Duration {
	return m.maxCacheFreshness
}

func (m mockLimits) QueryVerticalShardSize(userID string) int {
	return m.shardSize
}
