package stats

import (
	"context"
	"sync/atomic"
	"time"
)

type contextKey int

var ctxKey = contextKey(0)

// AddToContext adds a new Stats to the context. Returns the original context
// without overwriting the stats if the stats have already been initialised.
func AddToContext(ctx context.Context) (*Stats, context.Context) {
	if stats := FromContext(ctx); stats != nil {
		return stats, ctx
	}

	stats := &Stats{}
	ctx = context.WithValue(ctx, ctxKey, stats)
	return stats, ctx
}

// FromContext gets the Stats out of the Context. Returns nil if stats have not
// been initialised in the context.
func FromContext(ctx context.Context) *Stats {
	o := ctx.Value(ctxKey)
	if o == nil {
		return nil
	}
	return o.(*Stats)
}

// AddSeries adds some series to the counter.
func (s *Stats) AddSeries(series int) {
	if s == nil {
		return
	}

	atomic.AddInt32(&s.Series, int32(series))
}

// AddSamples adds some series to the counter.
func (s *Stats) AddSamples(samples int64) {
	if s == nil {
		return
	}

	atomic.AddInt64(&s.Samples, samples)
}

// AddWallTime adds some time to the counter.
func (s *Stats) AddWallTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.WallTime), int64(t))
}

// Merge the provide Stats into this one.
func (s *Stats) Merge(other *Stats) {
	if s == nil || other == nil {
		return
	}

	s.WallTime += other.WallTime
	s.Series += other.Series
	s.Samples += other.Samples
}
