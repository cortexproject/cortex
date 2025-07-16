package queryrange

import (
	"context"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
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

func RangeLogicalPlanGenMiddleware(lookbackDelta time.Duration, enablePerStepStats bool, disableDuplicateLabelChecks bool) tripperware.Middleware {
	return tripperware.MiddlewareFunc(func(next tripperware.Handler) tripperware.Handler {
		return rangeLogicalPlanGen{
			lookbackDelta:               lookbackDelta,
			enabledPerStepStats:         enablePerStepStats,
			next:                        next,
			disableDuplicateLabelChecks: disableDuplicateLabelChecks,
		}
	})
}

type rangeLogicalPlanGen struct {
	lookbackDelta               time.Duration
	enabledPerStepStats         bool
	next                        tripperware.Handler
	disableDuplicateLabelChecks bool
}

// NewRangeLogicalPlan generates an optimized and serialized logical query plan
func (l rangeLogicalPlanGen) NewRangeLogicalPlan(qs string, start, end time.Time, interval time.Duration) ([]byte, error) {

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

func (l rangeLogicalPlanGen) Do(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
	promReq, ok := r.(*tripperware.PrometheusRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}

	startTime := time.Unix(0, promReq.Start*int64(time.Millisecond))
	endTime := time.Unix(0, promReq.End*int64(time.Millisecond))
	duration := time.Duration(promReq.Step) * time.Millisecond

	byteLP, err := l.NewRangeLogicalPlan(promReq.Query, startTime, endTime, duration)
	if err != nil {
		return nil, err
	}
	promReq.LogicalPlan = byteLP

	return l.next.Do(ctx, r)
}
