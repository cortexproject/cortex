package queryrange

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

var (
	PrometheusCodec        = NewPrometheusCodec(false)
	ShardedPrometheusCodec = NewPrometheusCodec(false)
)

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
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
		),
	)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)

	downstream := singleHostRoundTripper{
		host: u.Host,
		next: http.DefaultTransport,
	}

	qa := querysharding.NewQueryAnalyzer()
	queyrangemiddlewares, _, err := Middlewares(Config{},
		log.NewNopLogger(),
		mockLimits{},
		nil,
		nil,
		qa,
		PrometheusCodec,
		ShardedPrometheusCodec,
	)
	require.NoError(t, err)

	tw := tripperware.NewQueryTripperware(log.NewNopLogger(),
		nil,
		nil,
		queyrangemiddlewares,
		nil,
		PrometheusCodec,
		nil,
		nil,
		qa,
		time.Minute,
		0,
		0,
	)

	for i, tc := range []struct {
		path, expectedBody string
	}{
		{"/foo", "bar"},
		{query, responseBody},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
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
