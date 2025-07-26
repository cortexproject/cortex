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
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	PrometheusCodec        = NewPrometheusCodec(false, "", "protobuf")
	ShardedPrometheusCodec = NewPrometheusCodec(false, "", "protobuf")
)

func TestRoundTrip(t *testing.T) {
	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				switch r.RequestURI {
				case queryAll:
					_, err = w.Write([]byte(responseBody))
				case queryWithWarnings:
					_, err = w.Write([]byte(responseBodyWithWarnings))
				case queryWithInfos:
					_, err = w.Write([]byte(responseBodyWithInfos))
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
	queyrangemiddlewares, _, err := Middlewares(Config{},
		log.NewNopLogger(),
		mockLimits{},
		nil,
		nil,
		qa,
		PrometheusCodec,
		ShardedPrometheusCodec,
		5*time.Minute,
		time.Minute,
		false,
	)
	require.NoError(t, err)

	defaultLimits := validation.NewOverrides(validation.Limits{}, nil)

	tw := tripperware.NewQueryTripperware(log.NewNopLogger(),
		nil,
		nil,
		queyrangemiddlewares,
		nil,
		PrometheusCodec,
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
			//parallel testing causes data race
			req, err := http.NewRequest("POST", tc.path, http.NoBody)
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

func TestRoundTripWithAndWithoutDistributedExec(t *testing.T) {
	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				switch r.RequestURI {
				case queryAll:
					_, err = w.Write([]byte(responseBody))
				case queryWithWarnings:
					_, err = w.Write([]byte(responseBodyWithWarnings))
				case queryWithInfos:
					_, err = w.Write([]byte(responseBodyWithInfos))
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
				End:   200000,
				Step:  15000,
				Query: "node_cpu_seconds_total{mode!=\"idle\"}[5m]",
			},
			expectEmptyBody: false,
		},
		{
			name:               "Without distributed execution",
			distributedEnabled: false,
			pReq: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  15000,
				Query: "node_cpu_seconds_total{mode!=\"idle\"}[5m]",
			},
			expectEmptyBody: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qa := querysharding.NewQueryAnalyzer()
			queyrangemiddlewares, _, err := Middlewares(Config{},
				log.NewNopLogger(),
				mockLimits{},
				nil,
				nil,
				qa,
				PrometheusCodec,
				ShardedPrometheusCodec,
				5*time.Minute,
				time.Minute,
				tc.distributedEnabled,
			)
			require.NoError(t, err)

			defaultLimits := validation.NewOverrides(validation.Limits{}, nil)

			tw := tripperware.NewQueryTripperware(log.NewNopLogger(),
				nil,
				nil,
				queyrangemiddlewares,
				nil,
				PrometheusCodec,
				nil,
				defaultLimits,
				qa,
				time.Minute,
				0,
				0,
			)

			ctx := user.InjectOrgID(context.Background(), "1")

			// test middlewares
			for _, mw := range queyrangemiddlewares {
				handler := mw.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
					return nil, nil
				}))
				_, err := handler.Do(ctx, tc.pReq)
				require.NoError(t, err)
			}

			// encode and prepare request
			req, err := tripperware.Codec.EncodeRequest(PrometheusCodec, context.Background(), tc.pReq)
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

type singleHostRoundTripper struct {
	host string
	next http.RoundTripper
}

func (s singleHostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = "http"
	r.URL.Host = s.host
	return s.next.RoundTrip(r)
}
