package instantquery

import (
	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/querysharding"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
)

func Middlewares(
	log log.Logger,
	limits tripperware.Limits,
	retryMiddlewareMetrics *queryrange.RetryMiddlewareMetrics,
	maxRetries int,
	queryAnalyzer querysharding.Analyzer,
) ([]tripperware.Middleware, error) {
	var m []tripperware.Middleware

	if maxRetries > 0 {
		m = append(m, queryrange.NewRetryMiddleware(log, maxRetries, retryMiddlewareMetrics))
	}
	m = append(m, tripperware.ShardByMiddleware(log, limits, InstantQueryCodec, queryAnalyzer))
	return m, nil
}
