package queryrange

import (
	"context"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"
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

			middleware := tripperware.LogicalPlanGenMiddleware()

			handler := middleware.Wrap(tripperware.HandlerFunc(func(_ context.Context, req tripperware.Request) (tripperware.Response, error) {
				return nil, nil
			}))

			// Test Group 1: Execute middleware to populate the logical plan
			_, err := handler.Do(context.Background(), tc.input)
			require.NoError(t, err)
			require.NotEmpty(t, tc.input.LogicalPlan, "logical plan should be populated")

			// Test 2: Encode the request and validate method and body
			httpReq, err := tripperware.Codec.EncodeRequest(PrometheusCodec, context.Background(), tc.input)
			require.NoError(t, err)
			require.Equal(t, http.MethodPost, httpReq.Method)

			body, err := io.ReadAll(httpReq.Body)
			require.NoError(t, err)
			require.NotEmpty(t, body, "HTTP body should not be empty")

			// Test Group 3: Ensure the logical plan can be deserialized back
			start := time.Unix(0, tc.input.Start*int64(time.Millisecond))
			end := time.Unix(0, tc.input.End*int64(time.Millisecond))
			step := time.Duration(tc.input.Step) * time.Millisecond

			qOpts := query.Options{
				Start:              start,
				End:                end,
				Step:               step,
				StepsBatch:         10,
				LookbackDelta:      5 * time.Minute,
				EnablePerStepStats: false,
			}
			planOpts := logicalplan.PlanOptions{
				DisableDuplicateLabelCheck: false,
			}
			_, err = logicalplan.NewFromBytes(body, &qOpts, planOpts)
			require.NoError(t, err, "logical plan should be valid and de-serializable")
		})
	}
}
