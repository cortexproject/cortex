package limiter

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/atomic"
	"sync"
)

type PerQueryLimiter struct {
	uniqueSeriesMx sync.RWMutex
	uniqueSeries   map[model.Fingerprint]struct{}

	chunkBytesCount       *atomic.Int32
	maxSeriesPerQuery     int
	maxChunkBytesPerQuery int
}
type perQueryLimiterCtxMarker struct{}

var (
	pqlCtxKey           = &perQueryLimiterCtxMarker{}
	errMaxSeriesHit     = "The query hit the max number of series limit while fetching chunks %s (limit: %d)"
	errMaxChunkBytesHit = "The query hit the max number of chunk bytes limit while fetching chunks %s (limit: %d)"
)

// NewPerQueryLimiter makes a new per-query rate limiter. Each per-query limiter
// is configured using the `maxSeriesPerQuery` and `maxChunkBytesPerQuery` limits.
func NewPerQueryLimiter(maxSeriesPerQuery int, maxChunkBytesPerQuery int) *PerQueryLimiter {
	return &PerQueryLimiter{
		uniqueSeriesMx: sync.RWMutex{},
		uniqueSeries:   map[model.Fingerprint]struct{}{},

		chunkBytesCount: atomic.NewInt32(0),

		maxChunkBytesPerQuery: maxChunkBytesPerQuery,
		maxSeriesPerQuery:     maxSeriesPerQuery,
	}
}

func NewPerQueryLimiterOnContext(ctx context.Context, maxSeriesPerQuery int, maxChunkBytesPerQuery int) context.Context {
	return context.WithValue(ctx, pqlCtxKey, NewPerQueryLimiter(maxSeriesPerQuery, maxChunkBytesPerQuery))
}

func AddPerQueryLimiterToContext(ctx context.Context, limiter *PerQueryLimiter) context.Context {
	return context.WithValue(ctx, pqlCtxKey, limiter)
}

// PerQueryLimiterFromContext returns a Per Query Limiter from the current context.
// IF there is Per Query Limiter on the context we will return ???
func PerQueryLimiterFromContext(ctx context.Context) *PerQueryLimiter {
	//Create fallback limiter of a new limiter??
	return FromContextWithFallback(ctx)
}

// FromContextWithFallback returns a Per Query Limiter from the current context.
// IF there is Per Query Limiter on the context we will return ???
func FromContextWithFallback(ctx context.Context) *PerQueryLimiter {
	pql, ok := ctx.Value(pqlCtxKey).(*PerQueryLimiter)
	if !ok {
		//If there's no limiter return a new unlimited limiter as a fallback
		pql = NewPerQueryLimiter(0, 0)
	}
	return pql
}

// AddFingerPrint Add a label adapter fast fingerprint to the map of unique fingerprints. If the
// added series label causes us to go over the limit of maxSeriesPerQuery we will
// return a validation error
func (pql *PerQueryLimiter) AddFingerPrint(labelAdapter []cortexpb.LabelAdapter, matchers []*labels.Matcher) error {
	//If the max series is unlimited just return without managing map
	if pql.maxSeriesPerQuery == 0 {
		return nil
	}

	pql.uniqueSeriesMx.Lock()
	//Unlock after return
	defer pql.uniqueSeriesMx.Unlock()

	pql.uniqueSeries[client.FastFingerprint(labelAdapter)] = struct{}{}
	if len(pql.uniqueSeries) > pql.maxSeriesPerQuery {
		//Format error with query and max limit
		return validation.LimitError(fmt.Sprintf(errMaxSeriesHit, util.LabelMatchersToString(matchers), pql.maxSeriesPerQuery))
	}
	return nil
}

// UniqueFingerPrints returns the count of unique series fingerprints seen by this per query limiter
func (pql *PerQueryLimiter) UniqueFingerPrints() int {
	pql.uniqueSeriesMx.RLock()
	defer pql.uniqueSeriesMx.RUnlock()
	mapL := len(pql.uniqueSeries)
	return mapL
}

// AddChunkBytes add number of chunk bytes to running query total of bytes
func (pql *PerQueryLimiter) AddChunkBytes(bytesCount int32) error {
	if pql.maxChunkBytesPerQuery == 0 {
		return nil
	}

	totalChunkBytes := pql.chunkBytesCount.Add(bytesCount)
	if totalChunkBytes > int32(pql.maxChunkBytesPerQuery) {
		//TODO format real error message here
		return validation.LimitError("Too many samples")
	} else {
		return nil
	}
}

func (pql *PerQueryLimiter) ChunkBytesCount() int32 {
	//Is there a better way to get a value here?
	return pql.chunkBytesCount.Add(0)
}
