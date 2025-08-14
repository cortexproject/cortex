package distributed_execution

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

func TestDistributedOptimizer(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name            string
		query           string
		start           time.Time
		end             time.Time
		step            time.Duration
		remoteExecCount int
	}{
		{
			name:            "binary operation with aggregations",
			query:           "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))",
			start:           now,
			end:             now,
			step:            time.Minute,
			remoteExecCount: 2,
		},
		{
			name:            "multiple binary operations with aggregations",
			query:           "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) - sum(rate(http_requests_total{job=\"cache\"}[5m]))",
			start:           now,
			end:             now,
			step:            time.Minute,
			remoteExecCount: 4,
		},
		{
			name:            "subquery with aggregation",
			query:           "sum(rate(container_network_transmit_bytes_total[5m:1m]))",
			start:           now,
			end:             now,
			step:            time.Minute,
			remoteExecCount: 0,
		},
		{
			name:            "function applied on binary operation",
			query:           "rate(http_requests_total[5m]) + rate(http_errors_total[5m]) > bool 0",
			start:           now,
			end:             now,
			step:            time.Minute,
			remoteExecCount: 0,
		},
		{
			name:            "numerical binary query",
			query:           "(1 + 1) + (1 + 1)",
			start:           now,
			end:             now,
			step:            time.Minute,
			remoteExecCount: 0,
		},
		{
			name:            "binary non-aggregation query",
			query:           "up + up",
			start:           now,
			end:             now,
			step:            time.Minute,
			remoteExecCount: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lp, _, err := CreateTestLogicalPlan(tc.query, tc.start, tc.end, tc.step)
			require.NoError(t, err)

			d := DistributedOptimizer{}
			newRoot, _, err := d.Optimize((*lp).Root())
			require.NoError(t, err)

			remoteNodeCount := 0
			logicalplan.TraverseBottomUp(nil, &newRoot, func(parent, current *logicalplan.Node) bool {
				if RemoteNode == (*current).Type() {
					remoteNodeCount++
				}
				return false
			})
			require.Equal(t, tc.remoteExecCount, remoteNodeCount)
		})
	}
}

func getStartAndEnd(start time.Time, end time.Time, step time.Duration) (time.Time, time.Time) {
	if step == 0 {
		return start, start
	}
	return start, end
}

func CreateTestLogicalPlan(qs string, start time.Time, end time.Time, step time.Duration) (*logicalplan.Plan, query.Options, error) {

	start, end = getStartAndEnd(start, end, step)

	qOpts := query.Options{
		Start:      start,
		End:        end,
		Step:       step,
		StepsBatch: 10,
		NoStepSubqueryIntervalFn: func(duration time.Duration) time.Duration {
			return 0
		},
		LookbackDelta:      0,
		EnablePerStepStats: false,
	}

	expr, err := parser.NewParser(qs, parser.WithFunctions(parser.Functions)).ParseExpr()
	if err != nil {
		return nil, qOpts, err
	}

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	logicalPlan, err := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	if err != nil {
		return nil, qOpts, err
	}
	optimizedPlan, _ := logicalPlan.Optimize(logicalplan.DefaultOptimizers)

	return &optimizedPlan, qOpts, nil
}
