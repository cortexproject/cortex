package instantquery

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

func InstantLogicalPlanGenMiddleware(lookbackDelta time.Duration, enablePerStepStats bool, disableDuplicateLabelChecks bool) tripperware.Middleware {
	return tripperware.MiddlewareFunc(func(next tripperware.Handler) tripperware.Handler {
		return instantLogicalPlanGen{
			lookbackDelta:               lookbackDelta,
			enabledPerStepStats:         enablePerStepStats,
			disableDuplicateLabelChecks: disableDuplicateLabelChecks,
			next:                        next,
		}
	})
}

type instantLogicalPlanGen struct {
	next                        tripperware.Handler
	lookbackDelta               time.Duration
	enabledPerStepStats         bool
	disableDuplicateLabelChecks bool
}

func (l instantLogicalPlanGen) NewInstantLogicalPlan(qs string, ts time.Time) ([]byte, error) {

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

func (l instantLogicalPlanGen) Do(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
	promReq, ok := r.(*tripperware.PrometheusRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}

	instantTime := time.Unix(0, promReq.Start*int64(time.Millisecond))

	byteLP, err := l.NewInstantLogicalPlan(promReq.Query, instantTime)
	if err != nil {
		return nil, err
	}
	promReq.LogicalPlan = byteLP

	return l.next.Do(ctx, r)
}
