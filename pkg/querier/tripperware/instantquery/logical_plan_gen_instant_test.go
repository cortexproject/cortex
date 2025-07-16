package instantquery

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

			lpm := tripperware.LogicalPlanGenMiddleware(
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
