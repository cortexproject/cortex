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
		remoteExecCount int
		expectedResult  string
	}{
		{
			name:            "binary operation with aggregations",
			query:           "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))",
			remoteExecCount: 2,
			expectedResult:  "remote(sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m]))) + remote(sum(rate(node_memory_Active_bytes[5m])))",
		},
		{
			name:            "binary operation with aggregations 2",
			query:           "count(node_cpu_seconds_total{mode!=\"idle\"}) + count(node_memory_Active_bytes)",
			remoteExecCount: 2,
			expectedResult:  "remote(count(node_cpu_seconds_total{mode!=\"idle\"})) + remote(count(node_memory_Active_bytes))",
		},
		{
			name:            "multiple binary operations with aggregations",
			query:           "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) - sum(rate(http_requests_total{job=\"cache\"}[5m]))",
			remoteExecCount: 4,
			expectedResult:  "remote(remote(sum(rate(http_requests_total{job=\"api\"}[5m]))) + remote(sum(rate(http_requests_total{job=\"web\"}[5m])))) - remote(sum(rate(http_requests_total{job=\"cache\"}[5m])))",
		},
		{
			name:            "subquery with aggregation",
			query:           "sum(rate(container_network_transmit_bytes_total[5m:1m]))",
			remoteExecCount: 0,
			expectedResult:  "sum(rate(container_network_transmit_bytes_total[5m:1m]))",
		},
		{
			name:            "numerical binary query",
			query:           "(1 + 1) + (1 + 1)",
			remoteExecCount: 0,
			expectedResult:  "4",
		},
		{
			name:            "binary non-aggregation query",
			query:           "up + up",
			remoteExecCount: 0,
			expectedResult:  "up + up",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lp, _, err := CreateTestLogicalPlan(tc.query, now, now, time.Minute)
			require.NoError(t, err)

			node := (*lp).Root()

			remoteNodeCount := 0
			logicalplan.TraverseBottomUp(nil, &node, func(parent, current *logicalplan.Node) bool {
				if RemoteNode == (*current).Type() {
					remoteNodeCount++
				}
				return false
			})
			require.Equal(t, tc.remoteExecCount, remoteNodeCount)
			require.Equal(t, (*lp).Root().String(), tc.expectedResult)
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

	distributedOptimizer := DistributedOptimizer{}
	dOptimizedNode, _ := distributedOptimizer.Optimize(optimizedPlan.Root(), &qOpts)

	plan := logicalplan.New(dOptimizedNode, &qOpts, planOpts)

	return &plan, qOpts, nil
}
