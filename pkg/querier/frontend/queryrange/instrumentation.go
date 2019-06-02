package frontend

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	instr "github.com/weaveworks/common/instrument"
)

var queryRangeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "frontend_query_range_duration_seconds",
	Help:      "Total time spent in seconds doing query range requests.",
	Buckets:   prometheus.DefBuckets,
}, []string{"method", "status_code"})

func instrument(name string) queryRangeMiddleware {
	return queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
		return queryRangeHandlerFunc(func(ctx context.Context, req *QueryRangeRequest) (*APIResponse, error) {
			var resp *APIResponse
			err := instr.TimeRequestHistogram(ctx, name, queryRangeDuration, func(ctx context.Context) error {
				var err error
				resp, err = next.Do(ctx, req)
				return err
			})
			return resp, err
		})
	})
}
