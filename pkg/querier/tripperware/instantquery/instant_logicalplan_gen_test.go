package instantquery

import (
	"context"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// TestInstantLogicalPlan ensures that the instant logical plan generation middleware
// correctly produces a logical plan and encodes it into the Prometheus request body.

func TestInstantLogicalPlan(t *testing.T) {
	for i, tc := range []struct {
		name  string
		input *tripperware.PrometheusRequest
		err   error
	}{
		{
			name: "rate vector selector",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])",
			},
			err: nil,
		},
		{
			name: "memory usage expression",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))",
			},
			err: nil,
		},
		{
			name: "scalar only query",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "42",
			},
			err: nil,
		},
		{
			name: "vector arithmetic",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "node_load1 / ignoring(cpu) node_cpu_seconds_total",
			},
			err: nil,
		},
		{
			name: "avg_over_time with nested rate",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "avg_over_time(rate(http_requests_total[5m])[30m:5m])",
			},
			err: nil,
		},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			lpm := InstantLogicalPlanGenMiddleware(
				time.Duration(1000),
				false,
				true,
			)
			handler := lpm.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
				return nil, nil
			}))

			// Test Group 1: Execute middleware to populate the logical plan
			_, _ = handler.Do(context.Background(), tc.input)
			require.NotEmpty(t, tc.input.LogicalPlan, "prom request should not be empty")

			// Test Group 2: Ensure the logical plan can be deserialized back
			qOpts := query.Options{
				Start: time.Unix(tc.input.Start, 0),
				End:   time.Unix(tc.input.End, 0),
				Step:  time.Duration(1000),
			}
			planOpts := logicalplan.PlanOptions{DisableDuplicateLabelCheck: true}
			_, err := logicalplan.NewFromBytes(tc.input.LogicalPlan, &qOpts, planOpts)
			require.NoError(t, err)

			// Test 3: Encode the request and validate method and body
			httpReq, err := tripperware.Codec.EncodeRequest(testInstantQueryCodec, context.Background(), tc.input)
			require.NoError(t, err)
			require.Equal(t, httpReq.Method, http.MethodPost, "Method should be POST")

			bodyBytes, err := io.ReadAll(httpReq.Body)
			require.NoError(t, err)
			require.NotEmpty(t, bodyBytes, "HTTP body should not be empty")
			require.Equal(t, bodyBytes, tc.input.LogicalPlan, "logical plan in request body does not match expected bytes")
		})
	}
}

// Test 4: Integration test for query round-trip with distributed execution enabled (feature flag).
// Checks logical plan is generated, included in request body, and processed correctly.

func TestRoundTripWithDistributedExec(t *testing.T) {
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

	instantQueryMiddlewares, err := Middlewares(
		log.NewNopLogger(),
		mockLimitsShard{shardSize: 2},
		nil,
		qa,
		5*time.Minute,
		false,
		true,
		true,
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
		false,
	)

	for i, tc := range []struct {
		pReq *tripperware.PrometheusRequest
	}{
		{pReq: &tripperware.PrometheusRequest{
			Start: 100000,
			End:   100000,
			Query: "node_cpu_seconds_total{mode!=\"idle\"}[5m]",
		}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "1")

			for _, mw := range instantQueryMiddlewares {
				handler := mw.Wrap(tripperware.HandlerFunc(func(_ context.Context, pReq tripperware.Request) (tripperware.Response, error) {
					return nil, nil
				}))
				_, err := handler.Do(ctx, tc.pReq)
				require.NoError(t, err)
			}

			req, err := tripperware.Codec.EncodeRequest(testInstantQueryCodec, context.Background(), tc.pReq)
			require.NoError(t, err)
			req = req.WithContext(ctx)
			err = user.InjectOrgIDIntoHTTPRequest(ctx, req)
			require.NoError(t, err)
			body, err := io.ReadAll(req.Body)
			require.NotEmpty(t, body)
			require.Equal(t, body, tc.pReq.LogicalPlan)

			resp, err := tw(downstream).RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			_, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
		})
	}
}

func TestRoundTripWithoutDistributedExec(t *testing.T) {
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
	instantQueryMiddlewares, err := Middlewares(
		log.NewNopLogger(),
		mockLimitsShard{shardSize: 2},
		nil,
		qa,
		5*time.Minute,
		false,
		false,
		true,
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
		false,
	)

	for i, tc := range []struct {
		pReq *tripperware.PrometheusRequest
	}{
		{pReq: &tripperware.PrometheusRequest{
			Start: 100000,
			End:   100000,
			Query: "node_cpu_seconds_total{mode!=\"idle\"}[5m]",
		}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "1")

			for _, mw := range instantQueryMiddlewares {
				handler := mw.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
					return nil, nil
				}))
				_, err := handler.Do(ctx, tc.pReq)
				require.NoError(t, err)
			}

			req, err := tripperware.Codec.EncodeRequest(testInstantQueryCodec, context.Background(), tc.pReq)
			require.NoError(t, err)
			req = req.WithContext(ctx)
			err = user.InjectOrgIDIntoHTTPRequest(ctx, req)
			require.NoError(t, err)
			body, err := io.ReadAll(req.Body)
			require.Empty(t, body)

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
