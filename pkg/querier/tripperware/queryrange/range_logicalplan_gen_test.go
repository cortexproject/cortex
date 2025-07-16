package queryrange

import (
	"context"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/log"
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

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

// TestRangeLogicalPlan validates the range logical plan generation middleware.
func TestRangeLogicalPlan(t *testing.T) {
	testCases := []struct {
		name  string
		input *tripperware.PrometheusRequest
	}{
		{
			name: "rate vector over time",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  15000,
				Query: "rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])",
			},
		},
		{
			name: "memory usage ratio",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  30000,
				Query: "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))",
			},
		},
		{
			name: "avg_over_time function",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  60000,
				Query: "avg_over_time(http_requests_total[5m])",
			},
		},
		{
			name: "vector arithmetic with range",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  10000,
				Query: "rate(node_network_receive_bytes_total[1m]) / rate(node_network_transmit_bytes_total[1m])",
			},
		},
		{
			name: "simple scalar operation",
			input: &tripperware.PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  15000,
				Query: "2 + 2",
			},
		},
	}

	for i, tc := range testCases {
		tc := tc
		t.Run(strconv.Itoa(i)+"_"+tc.name, func(t *testing.T) {
			t.Parallel()

			middleware := RangeLogicalPlanGenMiddleware(
				5*time.Minute,
				true,
				true,
			)

			handler := middleware.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
				return nil, nil
			}))

			// Test Group 1: Execute middleware to populate the logical plan
			_, err := handler.Do(context.Background(), tc.input)
			require.NoError(t, err)
			require.NotEmpty(t, tc.input.LogicalPlan, "logical plan should be populated")

			// Test Group 2: Ensure the logical plan can be deserialized back
			start := time.Unix(0, tc.input.Start*int64(time.Millisecond))
			end := time.Unix(0, tc.input.End*int64(time.Millisecond))
			step := time.Duration(tc.input.Step) * time.Millisecond

			qOpts := query.Options{
				Start:              start,
				End:                end,
				Step:               step,
				StepsBatch:         10,
				LookbackDelta:      5 * time.Minute,
				EnablePerStepStats: true,
			}
			planOpts := logicalplan.PlanOptions{
				DisableDuplicateLabelCheck: true,
			}
			_, err = logicalplan.NewFromBytes(tc.input.LogicalPlan, &qOpts, planOpts)
			require.NoError(t, err, "logical plan should be valid and de-serializable")

			// Test 3: Encode the request and validate method and body
			httpReq, err := tripperware.Codec.EncodeRequest(PrometheusCodec, context.Background(), tc.input)
			require.NoError(t, err)
			require.Equal(t, http.MethodPost, httpReq.Method)

			body, err := io.ReadAll(httpReq.Body)
			require.NoError(t, err)
			require.NotEmpty(t, body, "HTTP body should not be empty")
			require.Equal(t, body, tc.input.LogicalPlan, "logical plan in request body does not match expected bytes")
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
		false,
		true,
		true,
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
		false,
	)

	for i, tc := range []struct {
		pReq *tripperware.PrometheusRequest
	}{
		{pReq: &tripperware.PrometheusRequest{
			Start: 100000,
			End:   200000,
			Step:  15000,
			Query: "node_cpu_seconds_total{mode!=\"idle\"}[5m]",
		}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "1")

			for _, mw := range queyrangemiddlewares {
				handler := mw.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
					return nil, nil
				}))
				_, err := handler.Do(ctx, tc.pReq)
				require.NoError(t, err)
			}

			req, err := tripperware.Codec.EncodeRequest(PrometheusCodec, context.Background(), tc.pReq)
			require.NoError(t, err)
			req = req.WithContext(ctx)
			err = user.InjectOrgIDIntoHTTPRequest(ctx, req)
			require.NoError(t, err)
			body, err := io.ReadAll(req.Body)
			require.NotEmpty(t, body)
			require.Equal(t, tc.pReq.LogicalPlan, body)

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
		false,
		false,
		true,
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
		false,
	)

	for i, tc := range []struct {
		pReq *tripperware.PrometheusRequest
	}{
		{pReq: &tripperware.PrometheusRequest{
			Start: 100000,
			End:   200000,
			Step:  15000,
			Query: "node_cpu_seconds_total{mode!=\"idle\"}[5m]",
		}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "1")

			for _, mw := range queyrangemiddlewares {
				handler := mw.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
					return nil, nil
				}))
				_, err := handler.Do(ctx, tc.pReq)
				require.NoError(t, err)
			}

			req, err := tripperware.Codec.EncodeRequest(PrometheusCodec, context.Background(), tc.pReq)
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
