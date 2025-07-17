package tripperware

import (
	"context"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

// TestInstantLogicalPlan ensures that the instant logical plan generation middleware
// correctly produces a logical plan and insert it into the Prometheus request body.

func TestInstantLogicalPlan(t *testing.T) {
	for i, tc := range []struct {
		name  string
		input *PrometheusRequest
		err   error
	}{
		{
			name: "rate vector selector",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])",
			},
			err: nil,
		},
		{
			name: "memory usage expression",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))",
			},
			err: nil,
		},
		{
			name: "scalar only query",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "42",
			},
			err: nil,
		},
		{
			name: "vector arithmetic",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "node_load1 / ignoring(cpu) node_cpu_seconds_total",
			},
			err: nil,
		},
		{
			name: "avg_over_time with nested rate",
			input: &PrometheusRequest{
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

			lpm := LogicalPlanGenMiddleware()
			handler := lpm.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, nil
			}))

			// Test: Execute middleware to populate the logical plan
			_, err := handler.Do(context.Background(), tc.input)
			require.NoError(t, err)
			require.NotEmpty(t, tc.input.LogicalPlan, "prom request should not be empty")
		})
	}
}

// TestRangeLogicalPlan validates the range logical plan generation middleware.
func TestRangeLogicalPlan(t *testing.T) {
	testCases := []struct {
		name  string
		input *PrometheusRequest
	}{
		{
			name: "rate vector over time",
			input: &PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  15000,
				Query: "rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])",
			},
		},
		{
			name: "memory usage ratio",
			input: &PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  30000,
				Query: "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))",
			},
		},
		{
			name: "avg_over_time function",
			input: &PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  60000,
				Query: "avg_over_time(http_requests_total[5m])",
			},
		},
		{
			name: "vector arithmetic with range",
			input: &PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  10000,
				Query: "rate(node_network_receive_bytes_total[1m]) / rate(node_network_transmit_bytes_total[1m])",
			},
		},
		{
			name: "simple scalar operation",
			input: &PrometheusRequest{
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

			middleware := LogicalPlanGenMiddleware()

			handler := middleware.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, nil
			}))

			// Test: Execute middleware to populate the logical plan
			_, err := handler.Do(context.Background(), tc.input)
			require.NoError(t, err)
			require.NotEmpty(t, tc.input.LogicalPlan, "logical plan should be populated")

		})
	}
}
