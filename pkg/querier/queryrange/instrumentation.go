package queryrange

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/instrument"
)

var (
	queryRangeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "frontend_query_range_duration_seconds",
		Help:      "Total time spent in seconds doing query range requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "status_code"})

	mappedASTCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "frontend_mapped_asts_total",
		Help:      "Total number of queries that have undergone AST mapping",
	})

	splitByCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "frontend_split_queries_total",
		Help:      "Total number of underlying query requests after the split by interval is applied",
	})
)

// InstrumentMiddleware can be inserted into the middleware chain to expose timing information.
func InstrumentMiddleware(name string) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, req Request) (Response, error) {
			var resp Response
			err := instrument.CollectedRequest(ctx, name, instrument.NewHistogramCollector(queryRangeDuration), instrument.ErrorCode, func(ctx context.Context) error {
				var err error
				resp, err = next.Do(ctx, req)
				return err
			})
			return resp, err
		})
	})
}
