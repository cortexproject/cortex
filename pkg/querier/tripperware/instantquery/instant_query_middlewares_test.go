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
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	queryAll     = "/api/v1/instant_query?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536716898&stats=all"
	responseBody = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
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
		time.Minute,
		false,
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

func TestRoundTripWithAndWithoutDistributedExec(t *testing.T) {
	// Common test server setup
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

	testCases := []struct {
		name               string
		distributedEnabled bool
		pReq               *tripperware.PrometheusRequest
		expectEmptyBody    bool
	}{
		{
			name:               "With distributed execution",
			distributedEnabled: true,
			pReq: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "node_cpu_seconds_total{mode!=\"idle\"}[5m]",
			},
			expectEmptyBody: false,
		},
		{
			name:               "Without distributed execution",
			distributedEnabled: false,
			pReq: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "node_cpu_seconds_total{mode!=\"idle\"}[5m]",
			},
			expectEmptyBody: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qa := querysharding.NewQueryAnalyzer()
			instantQueryMiddlewares, err := Middlewares(
				log.NewNopLogger(),
				mockLimitsShard{shardSize: 2},
				nil,
				qa,
				5*time.Minute,
				time.Minute,
				tc.distributedEnabled,
			)
			require.NoError(t, err)

			defaultLimits := validation.NewOverrides(validation.Limits{}, nil)

			tw := tripperware.NewQueryTripperware(log.NewNopLogger(),
				nil,
				nil,
				nil,
				instantQueryMiddlewares,
				testInstantQueryCodec,
				nil,
				defaultLimits,
				qa,
				time.Minute,
				0,
				0,
			)

			ctx := user.InjectOrgID(context.Background(), "1")

			// test middlewares
			for _, mw := range instantQueryMiddlewares {
				handler := mw.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
					return nil, nil
				}))
				_, err := handler.Do(ctx, tc.pReq)
				require.NoError(t, err)
			}

			// encode and prepare request
			req, err := tripperware.Codec.EncodeRequest(testInstantQueryCodec, context.Background(), tc.pReq)
			require.NoError(t, err)
			req = req.WithContext(ctx)
			err = user.InjectOrgIDIntoHTTPRequest(ctx, req)
			require.NoError(t, err)

			// check request body
			body := []byte(req.PostFormValue("plan"))
			if tc.expectEmptyBody {
				require.Empty(t, body)
			} else {
				require.NotEmpty(t, body)
				byteLP, err := logicalplan.Marshal(tc.pReq.LogicalPlan.Root())
				require.NoError(t, err)
				require.Equal(t, byteLP, body)
			}

			// test round trip
			resp, err := tw(downstream).RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			_, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
		})
	}
}

type mockLimitsShard struct {
	maxQueryLookback     time.Duration
	maxQueryLength       time.Duration
	maxCacheFreshness    time.Duration
	maxQueryResponseSize int64
	shardSize            int
	queryPriority        validation.QueryPriority
	queryRejection       validation.QueryRejection
}

func (m mockLimitsShard) MaxQueryLookback(string) time.Duration {
	return m.maxQueryLookback
}

func (m mockLimitsShard) MaxQueryLength(string) time.Duration {
	return m.maxQueryLength
}

func (mockLimitsShard) MaxQueryParallelism(string) int {
	return 14 // Flag default.
}

func (m mockLimitsShard) MaxCacheFreshness(string) time.Duration {
	return m.maxCacheFreshness
}

func (m mockLimitsShard) MaxQueryResponseSize(string) int64 {
	return m.maxQueryResponseSize
}

func (m mockLimitsShard) QueryVerticalShardSize(userID string) int {
	return m.shardSize
}

func (m mockLimitsShard) QueryPriority(userID string) validation.QueryPriority {
	return m.queryPriority
}

func (m mockLimitsShard) QueryRejection(userID string) validation.QueryRejection {
	return m.queryRejection
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
