package queryrange

import (
	"context"
	io "io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/version"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func TestRoundTrip(t *testing.T) {
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

	queyrangemiddlewares, _, err := Middlewares(Config{},
		log.NewNopLogger(),
		mockLimits{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	version.Version = "v1.14.0"
	type buildInfoResponse struct {
		Status string                `json:"status"`
		Data   *v1.PrometheusVersion `json:"data"`
	}
	resp := &buildInfoResponse{
		Status: "success",
		Data: &v1.PrometheusVersion{
			Version:   version.Version,
			GoVersion: runtime.Version(),
		},
	}
	expectedResp, err := json.Marshal(resp)
	require.NoError(t, err)

	buildInfoRoundTripper := tripperware.NewBuildInfoRoundTripper()

	tw := tripperware.NewQueryTripperware(log.NewNopLogger(),
		nil,
		nil,
		queyrangemiddlewares,
		nil,
		PrometheusCodec,
		nil,
		buildInfoRoundTripper,
	)
	require.NoError(t, err)

	for i, tc := range []struct {
		path, expectedBody string
	}{
		{"/foo", "bar"},
		{query, responseBody},
		{"/api/v1/status/buildinfo", string(expectedResp)},
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
