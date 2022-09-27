package instantquery

import (
	"github.com/go-kit/log"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func Middlewares(
	log log.Logger,
	limits tripperware.Limits,
) ([]tripperware.Middleware, error) {
	var m []tripperware.Middleware

	m = append(m, tripperware.ShardByMiddleware(log, limits, InstantQueryCodec))
	return m, nil
}
