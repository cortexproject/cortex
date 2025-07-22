package tripperware

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/weaveworks/common/httpgrpc"
)

const (
	stepBatch = 10
)

func DistributedQueryMiddleware(enablePerStepStats bool, lookBackDelta time.Duration) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return distributedQueryMiddleware{
			next:               next,
			enablePerStepStats: enablePerStepStats,
			lookBackDelta:      lookBackDelta,
		}
	})
}

func getStartAndEnd(start time.Time, end time.Time, step time.Duration) (time.Time, time.Time) {
	if step == 0 {
		return start, start
	}
	return start, end
}

type distributedQueryMiddleware struct {
	next               Handler
	enablePerStepStats bool
	lookBackDelta      time.Duration
}

func (d distributedQueryMiddleware) newLogicalPlan(qs string, start time.Time, end time.Time, step time.Duration) (*logicalplan.Plan, error) {

	start, end = getStartAndEnd(start, end, step)

	qOpts := query.Options{
		Start:              start,
		End:                end,
		Step:               step,
		StepsBatch:         stepBatch,
		LookbackDelta:      d.lookBackDelta,
		EnablePerStepStats: d.enablePerStepStats,
	}

	expr, err := parser.NewParser(qs, parser.WithFunctions(parser.Functions)).ParseExpr()
	if err != nil {
		return nil, err
	}

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	logicalPlan := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	optimizedPlan, _ := logicalPlan.Optimize(logicalplan.DefaultOptimizers)

	//TODO: Add distributed query optimizer

	return &optimizedPlan, nil
}

func (d distributedQueryMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	promReq, ok := r.(*PrometheusRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}

	startTime := time.Unix(0, promReq.Start*int64(time.Millisecond))
	endTime := time.Unix(0, promReq.End*int64(time.Millisecond))
	step := time.Duration(promReq.Step) * time.Millisecond

	var err error

	newLogicalPlan, err := d.newLogicalPlan(promReq.Query, startTime, endTime, step)
	if err != nil {
		return nil, err
	}

	promReq.LogicalPlan = *newLogicalPlan

	return d.next.Do(ctx, r)
}
