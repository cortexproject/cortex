package metadata

import (
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
)

func SeriesMiddlewares(
	cfg queryrange.Config,
	log log.Logger,
	limits tripperware.Limits,
	registerer prometheus.Registerer,
) ([]tripperware.Middleware, error) {
	var m []tripperware.Middleware

	if cfg.SplitMetadataByInterval != 0 {
		staticIntervalFn := func(_ tripperware.Request) time.Duration { return cfg.SplitMetadataByInterval }
		m = append(m, queryrange.SplitByIntervalMiddleware(staticIntervalFn, limits, NewSeriesCodec(cfg), registerer))
	}

	return m, nil
}
