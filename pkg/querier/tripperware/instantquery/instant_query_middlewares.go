package instantquery

import (
	"time"

	"github.com/go-kit/log"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/querysharding"

	"github.com/cortexproject/cortex/pkg/distributed_execution"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func Middlewares(
	log log.Logger,
	limits tripperware.Limits,
	merger tripperware.Merger,
	queryAnalyzer querysharding.Analyzer,
	lookbackDelta time.Duration,
	defaultEvaluationInterval time.Duration,
	distributedExecEnabled bool,
	localOptimizers []logicalplan.Optimizer,
) ([]tripperware.Middleware, error) {
	m := []tripperware.Middleware{
		NewLimitsMiddleware(limits, lookbackDelta),
		tripperware.ShardByMiddleware(log, limits, merger, queryAnalyzer),
	}

	if distributedExecEnabled {
		m = append(m,
			tripperware.DistributedQueryMiddleware(defaultEvaluationInterval, lookbackDelta,
				append(localOptimizers, &distributed_execution.DistributedOptimizer{})))
	}

	return m, nil
}
