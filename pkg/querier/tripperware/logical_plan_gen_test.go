package tripperware

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalPlanGeneration(t *testing.T) {
	testCases := []struct {
		name      string
		queryType string // "instant" or "range"
		input     *PrometheusRequest
		err       error
	}{
		// instant query test cases
		{
			name:      "instant - rate vector selector",
			queryType: "instant",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])",
			},
		},
		{
			name:      "instant - memory usage expression",
			queryType: "instant",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))",
			},
		},
		{
			name:      "instant - scalar only query",
			queryType: "instant",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "42",
			},
		},
		{
			name:      "instant - vector arithmetic",
			queryType: "instant",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "node_load1 / ignoring(cpu) node_cpu_seconds_total",
			},
		},
		{
			name:      "instant - avg_over_time with nested rate",
			queryType: "instant",
			input: &PrometheusRequest{
				Start: 100000,
				End:   100000,
				Query: "avg_over_time(rate(http_requests_total[5m])[30m:5m])",
			},
		},

		// query range test cases
		{
			name:      "range - rate vector over time",
			queryType: "range",
			input: &PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  15000,
				Query: "rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])",
			},
		},
		{
			name:      "range - memory usage ratio",
			queryType: "range",
			input: &PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  30000,
				Query: "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))",
			},
		},
		{
			name:      "range - avg_over_time function",
			queryType: "range",
			input: &PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  60000,
				Query: "avg_over_time(http_requests_total[5m])",
			},
		},
		{
			name:      "range - vector arithmetic with range",
			queryType: "range",
			input: &PrometheusRequest{
				Start: 100000,
				End:   200000,
				Step:  10000,
				Query: "rate(node_network_receive_bytes_total[1m]) / rate(node_network_transmit_bytes_total[1m])",
			},
		},
		{
			name:      "range - simple scalar operation",
			queryType: "range",
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

			// additional validation on the test cases based on query type
			if tc.queryType == "range" {
				require.NotZero(t, tc.input.Step, "range query should have non-zero step")
				require.NotEqual(t, tc.input.Start, tc.input.End, "range query should have different start and end times")
			} else {
				require.Equal(t, tc.input.Start, tc.input.End, "instant query should have equal start and end times")
				require.Zero(t, tc.input.Step, "instant query should have zero step")
			}

			// test: execute middleware to populate the logical plan
			_, err := handler.Do(context.Background(), tc.input)
			require.NoError(t, err)
			require.NotEmpty(t, tc.input.LogicalPlan, "logical plan should be populated")

		})
	}
}
