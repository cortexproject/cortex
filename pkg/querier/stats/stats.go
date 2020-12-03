package stats

import (
	"context"
	"sync/atomic"
	"time"
)

type contextKey int

var ctxKey = contextKey(0)

// ContextWithEmptyStats returns a context with empty stats.
func ContextWithEmptyStats(ctx context.Context) (*Stats, context.Context) {
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

	// TODO when we read, we need to use atomic too.
	s.AddWallTime(other.WallTime)
	s.AddSamples(other.Samples)
}
