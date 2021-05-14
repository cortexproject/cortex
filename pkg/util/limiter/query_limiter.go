package limiter

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type queryLimiterCtxKey struct{}

var (
	qlCtxKey        = &queryLimiterCtxKey{}
	errMaxSeriesHit = "The query hit the max number of series limit while fetching chunks (limit: %d)"
)

type QueryLimiter struct {
	uniqueSeriesMx sync.RWMutex
	uniqueSeries   map[model.Fingerprint]struct{}

	maxSeriesPerQuery int
}

// NewQueryLimiter makes a new per-query limiter. Each query limiter
// is configured using the `maxSeriesPerQuery` limit.
func NewQueryLimiter(maxSeriesPerQuery int) *QueryLimiter {
	return &QueryLimiter{
		uniqueSeriesMx: sync.RWMutex{},
		uniqueSeries:   map[model.Fingerprint]struct{}{},

		maxSeriesPerQuery: maxSeriesPerQuery,
	}
}

func AddQueryLimiterToContext(ctx context.Context, limiter *QueryLimiter) context.Context {
	return context.WithValue(ctx, qlCtxKey, limiter)
}

// QueryLimiterFromContextWithFallback returns a QueryLimiter from the current context.
// If there is not a QueryLimiter on the context it will return a new no-op limiter.
func QueryLimiterFromContextWithFallback(ctx context.Context) *QueryLimiter {
	ql, ok := ctx.Value(qlCtxKey).(*QueryLimiter)
	if !ok {
		// If there's no limiter return a new unlimited limiter as a fallback
		ql = NewQueryLimiter(0)
	}
	return ql
}

// AddSeries adds the input series and returns an error if the limit is reached.
func (ql *QueryLimiter) AddSeries(seriesLabels []cortexpb.LabelAdapter) error {
	// If the max series is unlimited just return without managing map
	if ql.maxSeriesPerQuery == 0 {
		return nil
	}

	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()

	ql.uniqueSeries[client.FastFingerprint(seriesLabels)] = struct{}{}
	if len(ql.uniqueSeries) > ql.maxSeriesPerQuery {
		// Format error with query and max limit
		return validation.LimitError(fmt.Sprintf(errMaxSeriesHit, ql.maxSeriesPerQuery))
	}
	return nil
}

// UniqueSeries returns the count of unique series seen by this query limiter.
func (ql *QueryLimiter) uniqueSeriesCount() int {
	ql.uniqueSeriesMx.RLock()
	defer ql.uniqueSeriesMx.RUnlock()
	return len(ql.uniqueSeries)
}
