package limiter

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/common/model"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
)

type queryLimiterCtxKey struct{}

var (
	ctxKey                    = &queryLimiterCtxKey{}
	ErrMaxSeriesHit           = "the query hit the max number of series limit (limit: %d series)"
	ErrMaxChunkBytesHit       = "the query hit the aggregated chunks size limit (limit: %d bytes)"
	ErrMaxDataBytesHit        = "the query hit the aggregated data size limit (limit: %d bytes)"
	ErrMaxChunksPerQueryLimit = "the query hit the max number of chunks limit (limit: %d chunks)"
)

type QueryLimiter struct {
	uniqueSeriesMx sync.Mutex
	uniqueSeries   map[model.Fingerprint]struct{}

	chunkBytesCount atomic.Int64
	dataBytesCount  atomic.Int64
	chunkCount      atomic.Int64

	maxSeriesPerQuery     int
	maxChunkBytesPerQuery int
	maxDataBytesPerQuery  int
	maxChunksPerQuery     int
}

// NewQueryLimiter makes a new per-query limiter. Each query limiter
// is configured using the `maxSeriesPerQuery` limit.
func NewQueryLimiter(maxSeriesPerQuery, maxChunkBytesPerQuery, maxChunksPerQuery, maxDataBytesPerQuery int) *QueryLimiter {
	return &QueryLimiter{
		uniqueSeriesMx: sync.Mutex{},
		uniqueSeries:   map[model.Fingerprint]struct{}{},

		maxSeriesPerQuery:     maxSeriesPerQuery,
		maxChunkBytesPerQuery: maxChunkBytesPerQuery,
		maxChunksPerQuery:     maxChunksPerQuery,
		maxDataBytesPerQuery:  maxDataBytesPerQuery,
	}
}

func AddQueryLimiterToContext(ctx context.Context, limiter *QueryLimiter) context.Context {
	return context.WithValue(ctx, ctxKey, limiter)
}

// QueryLimiterFromContextWithFallback returns a QueryLimiter from the current context.
// If there is not a QueryLimiter on the context it will return a new no-op limiter.
func QueryLimiterFromContextWithFallback(ctx context.Context) *QueryLimiter {
	ql, ok := ctx.Value(ctxKey).(*QueryLimiter)
	if !ok {
		// If there's no limiter return a new unlimited limiter as a fallback
		ql = NewQueryLimiter(0, 0, 0, 0)
	}
	return ql
}

// AddSeries adds the batch of input series and returns an error if the limit is reached.
func (ql *QueryLimiter) AddSeries(series ...[]cortexpb.LabelAdapter) error {
	// If the max series is unlimited just return without managing map
	if ql.maxSeriesPerQuery == 0 {
		return nil
	}
	fps := make([]model.Fingerprint, 0, len(series))
	for _, s := range series {
		fps = append(fps, client.FastFingerprint(s))
	}

	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()
	for _, fp := range fps {
		ql.uniqueSeries[fp] = struct{}{}
	}

	if len(ql.uniqueSeries) > ql.maxSeriesPerQuery {
		// Format error with max limit
		return fmt.Errorf(ErrMaxSeriesHit, ql.maxSeriesPerQuery)
	}
	return nil
}

// uniqueSeriesCount returns the count of unique series seen by this query limiter.
func (ql *QueryLimiter) uniqueSeriesCount() int {
	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()
	return len(ql.uniqueSeries)
}

// AddChunkBytes adds the input chunk size in bytes and returns an error if the limit is reached.
func (ql *QueryLimiter) AddChunkBytes(chunkSizeInBytes int) error {
	if ql.maxChunkBytesPerQuery == 0 {
		return nil
	}
	if ql.chunkBytesCount.Add(int64(chunkSizeInBytes)) > int64(ql.maxChunkBytesPerQuery) {
		return fmt.Errorf(ErrMaxChunkBytesHit, ql.maxChunkBytesPerQuery)
	}
	return nil
}

// AddDataBytes adds the queried data bytes and returns an error if the limit is reached.
func (ql *QueryLimiter) AddDataBytes(dataSizeInBytes int) error {
	if ql.maxDataBytesPerQuery == 0 {
		return nil
	}
	if ql.dataBytesCount.Add(int64(dataSizeInBytes)) > int64(ql.maxDataBytesPerQuery) {
		return fmt.Errorf(ErrMaxDataBytesHit, ql.maxDataBytesPerQuery)
	}
	return nil
}

func (ql *QueryLimiter) AddChunks(count int) error {
	if ql.maxChunksPerQuery == 0 {
		return nil
	}

	if ql.chunkCount.Add(int64(count)) > int64(ql.maxChunksPerQuery) {
		return fmt.Errorf(ErrMaxChunksPerQueryLimit, ql.maxChunksPerQuery)
	}
	return nil
}
