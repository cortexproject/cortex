package instantquery

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
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

var (
	query        = "/api/v1/query?time=1536716898&query=sum by (label) (up)&stats=all"
	responseBody = `{"status":"success","data":{"resultType":"vector","result":[]}}`
)

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	var try atomic.Int32
	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				if try.Inc() > 2 {
					_, err = w.Write([]byte(responseBody))
				} else {
					http.Error(w, `{"status":"error"}`, http.StatusInternalServerError)
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
	limits := tripperware.MockLimits{
		ShardSize: 2,
	}
	qa := querysharding.NewQueryAnalyzer()
	instantQueryMiddlewares, err := Middlewares(
		log.NewNopLogger(),
		limits,
		nil,
		3,
		qa)
	require.NoError(t, err)

	tw := tripperware.NewQueryTripperware(
		log.NewNopLogger(),
		nil,
		nil,
		nil,
		instantQueryMiddlewares,
		nil,
		InstantQueryCodec,
		limits,
		qa,
		time.Minute,
	)

	for i, tc := range []struct {
		path, expectedBody string
	}{
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
