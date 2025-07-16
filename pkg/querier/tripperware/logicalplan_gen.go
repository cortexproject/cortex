package tripperware

import (
	"context"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/weaveworks/common/httpgrpc"
	"net/http"
	"time"
)

const (
	stepBatch = 10
)

func LogicalPlanGenMiddleware(lookbackDelta time.Duration, enablePerStepStats bool, disableDuplicateLabelChecks bool) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return logicalPlanGen{
			lookbackDelta:               lookbackDelta,
			enabledPerStepStats:         enablePerStepStats,
			next:                        next,
			disableDuplicateLabelChecks: disableDuplicateLabelChecks,
		}
	})
}

type logicalPlanGen struct {
	lookbackDelta               time.Duration
	enabledPerStepStats         bool
	next                        Handler
	disableDuplicateLabelChecks bool
}

func (l logicalPlanGen) NewInstantLogicalPlan(qs string, ts time.Time) ([]byte, error) {

	qOpts := query.Options{
		Start:              ts,
		End:                ts,
		Step:               0,
		StepsBatch:         stepBatch,
		LookbackDelta:      l.lookbackDelta,
		EnablePerStepStats: l.disableDuplicateLabelChecks,
	}

	expr, err := parser.NewParser(qs, parser.WithFunctions(parser.Functions)).ParseExpr()
	if err != nil {
		return nil, err
	}

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: l.disableDuplicateLabelChecks,
	}

	logicalPlan := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	optimizedPlan, _ := logicalPlan.Optimize(logicalplan.DefaultOptimizers)

	// TODO: Add distributed optimizer for remote node insertion

	byteLP, err := logicalplan.Marshal(optimizedPlan.Root())
	if err != nil {
		return nil, err
	}

	return byteLP, nil
}

func (l logicalPlanGen) NewRangeLogicalPlan(qs string, start, end time.Time, interval time.Duration) ([]byte, error) {

	qOpts := query.Options{
		Start:              start,
		End:                end,
		Step:               interval,
		StepsBatch:         stepBatch,
		LookbackDelta:      l.lookbackDelta,
		EnablePerStepStats: l.enabledPerStepStats,
	}

	expr, err := parser.NewParser(qs, parser.WithFunctions(parser.Functions)).ParseExpr()
	if err != nil {
		return nil, err
	}

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: l.disableDuplicateLabelChecks,
	}

	lPlan := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	optimizedPlan, _ := lPlan.Optimize(logicalplan.DefaultOptimizers)
	byteLP, err := logicalplan.Marshal(optimizedPlan.Root())
	if err != nil {
		return nil, err
	}

	// TODO: Add distributed optimizer for remote node insertion

	return byteLP, nil
}

func (l logicalPlanGen) Do(ctx context.Context, r Request) (Response, error) {
	promReq, ok := r.(*PrometheusRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}

	startTime := time.Unix(0, promReq.Start*int64(time.Millisecond))
	endTime := time.Unix(0, promReq.End*int64(time.Millisecond))
	duration := time.Duration(promReq.Step) * time.Millisecond

	var byteLP []byte
	var err error
	if promReq.Step != 0 {
		byteLP, err = l.NewRangeLogicalPlan(promReq.Query, startTime, endTime, duration)
	} else {
		byteLP, err = l.NewInstantLogicalPlan(promReq.Query, startTime)
	}

	if err != nil {
		return nil, err
	}
	promReq.LogicalPlan = byteLP

	return l.next.Do(ctx, r)
}
