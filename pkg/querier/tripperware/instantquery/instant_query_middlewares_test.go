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

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	distributedExecEnabled = false
	queryAll               = "/api/v1/instant_query?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536716898&stats=all"
	responseBody           = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
)

func TestRoundTrip(t *testing.T) {
	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				switch r.RequestURI {
				case queryAll:
					_, err = w.Write([]byte(responseBody))
				default:
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
	instantQueryMiddleware, err := Middlewares(
		log.NewNopLogger(),
		mockLimitsShard{maxQueryLookback: 2},
		nil,
		qa,
		5*time.Minute,
		false,
		distributedExecEnabled,
		true,
	)
	require.NoError(t, err)

	defaultLimits := validation.NewOverrides(validation.Limits{}, nil)

	tw := tripperware.NewQueryTripperware(log.NewNopLogger(),
		nil,
		nil,
		nil,
		instantQueryMiddleware,
		testInstantQueryCodec,
		nil,
		defaultLimits,
		qa,
		time.Minute,
		0,
		0,
		false,
	)

	for i, tc := range []struct {
		path, expectedBody string
	}{
		{"/foo", "bar"},
		{queryAll, responseBody},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			req, err := http.NewRequest("POST", tc.path, http.NoBody)
			require.NoError(t, err)

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
