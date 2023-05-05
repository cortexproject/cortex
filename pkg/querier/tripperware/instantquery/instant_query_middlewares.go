package instantquery

import (
	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/querysharding"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func Middlewares(
	log log.Logger,
	limits tripperware.Limits,
	queryAnalyzer querysharding.Analyzer,
	codec tripperware.Codec,
) ([]tripperware.Middleware, error) {
	var m []tripperware.Middleware

	m = append(m, tripperware.ShardByMiddleware(log, limits, codec, queryAnalyzer))
	return m, nil
}
