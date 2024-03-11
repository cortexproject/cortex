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
	queryAnalyzer querysharding.Analyzer,
	lookbackDelta time.Duration,
) ([]tripperware.Middleware, error) {
	m := []tripperware.Middleware{
		NewLimitsMiddleware(limits, lookbackDelta),
		tripperware.ShardByMiddleware(log, limits, InstantQueryCodec, queryAnalyzer),
	}
	return m, nil
}
