package limiter

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type QueryLimiter struct {
	uniqueSeriesMx sync.RWMutex
	uniqueSeries   map[model.Fingerprint]struct{}

	chunkBytesCount   *atomic.Int32
	maxSeriesPerQuery int
}
type queryLimiterCtxKey struct{}

var (
	qlCtxKey        = &queryLimiterCtxKey{}
	errMaxSeriesHit = "The query hit the max number of series limit while fetching chunks %s (limit: %d)"
)

// NewQueryLimiter makes a new per-query rate limiter. Each per-query limiter
// is configured using the `maxSeriesPerQuery` and `maxChunkBytesPerQuery` limits.
func NewQueryLimiter(maxSeriesPerQuery int) *QueryLimiter {
	return &QueryLimiter{
		uniqueSeriesMx: sync.RWMutex{},
		uniqueSeries:   map[model.Fingerprint]struct{}{},

		chunkBytesCount: atomic.NewInt32(0),

		maxSeriesPerQuery: maxSeriesPerQuery,
	}
}

func NewQueryLimiterOnContext(ctx context.Context, maxSeriesPerQuery int) context.Context {
	return context.WithValue(ctx, qlCtxKey, NewQueryLimiter(maxSeriesPerQuery))
}

func AddQueryLimiterToContext(ctx context.Context, limiter *QueryLimiter) context.Context {
	return context.WithValue(ctx, qlCtxKey, limiter)
}

// QueryLimiterFromContextWithFallback returns a Query Limiter from the current context.
// IF there is Per Query Limiter on the context we will return a new no-op limiter
func QueryLimiterFromContextWithFallback(ctx context.Context) *QueryLimiter {
	ql, ok := ctx.Value(qlCtxKey).(*QueryLimiter)
	if !ok {
		// If there's no limiter return a new unlimited limiter as a fallback
		ql = NewQueryLimiter(0)
	}
	return ql
}

// AddSeries Add labels for series to the count of unique series. If the
// added series label causes us to go over the limit of maxSeriesPerQuery we will
// return a validation error
func (ql *QueryLimiter) AddSeries(labelAdapter []cortexpb.LabelAdapter, matchers []*labels.Matcher) error {
	// If the max series is unlimited just return without managing map
	if ql.maxSeriesPerQuery == 0 {
		return nil
	}

	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()

	ql.uniqueSeries[client.FastFingerprint(labelAdapter)] = struct{}{}
	if len(ql.uniqueSeries) > ql.maxSeriesPerQuery {
		// Format error with query and max limit
		return validation.LimitError(fmt.Sprintf(errMaxSeriesHit, util.LabelMatchersToString(matchers), ql.maxSeriesPerQuery))
	}
	return nil
}

// UniqueSeries returns the count of unique series seen by this query limiter.
func (ql *QueryLimiter) UniqueSeries() int {
	ql.uniqueSeriesMx.RLock()
	defer ql.uniqueSeriesMx.RUnlock()
	return len(ql.uniqueSeries)
}
