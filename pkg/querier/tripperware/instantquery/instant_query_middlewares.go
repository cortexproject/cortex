package instantquery

import (
	"time"

	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/querysharding"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func Middlewares(
	log log.Logger,
	limits tripperware.Limits,
	merger tripperware.Merger,
	queryAnalyzer querysharding.Analyzer,
	lookbackDelta time.Duration,
) ([]tripperware.Middleware, error) {
	m := []tripperware.Middleware{
		NewLimitsMiddleware(limits, lookbackDelta),
		tripperware.ShardByMiddleware(log, limits, merger, queryAnalyzer),
	}
	return m, nil
}
