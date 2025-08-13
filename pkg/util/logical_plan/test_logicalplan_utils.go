package logical_plan

import (
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

func getStartAndEnd(start time.Time, end time.Time, step time.Duration) (time.Time, time.Time) {
	if step == 0 {
		return start, start
	}
	return start, end
}

func CreateTestLogicalPlan(qs string, start time.Time, end time.Time, step time.Duration) (*logicalplan.Plan, error) {

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
		return nil, err
	}

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	logicalPlan, err := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	if err != nil {
		return nil, err
	}
	optimizedPlan, _ := logicalPlan.Optimize(logicalplan.DefaultOptimizers)

	return &optimizedPlan, nil
}
