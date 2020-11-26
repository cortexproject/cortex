package stats

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/user"
)

type contextKey int

var (
	ctxKey       = contextKey(0)
	querySeconds = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_seconds_total",
		Help: "Total amount of wall clock time spend processing queries.",
	}, []string{"userid"})
	querySamples = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_samples_total",
		Help: "Total number of samples queried.",
	}, []string{"userid"})
	querySeries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_series_total",
		Help: "Total number of series queried.",
	}, []string{"userid"})
)

// AddToContext adds a new Stats to the context.
func AddToContext(ctx context.Context) (*Stats, context.Context) {
	stats := &Stats{}
	ctx = context.WithValue(ctx, ctxKey, stats)
	return stats, ctx
}

// FromContext gets the Stats out of the Context.
func FromContext(ctx context.Context) *Stats {
	o := ctx.Value(ctxKey)
	if o == nil {
		// To make user of this function's life easier, return an empty stats
		// if none is found in the context.
		return &Stats{}
	}
	return o.(*Stats)
}

// AddSeries adds some series to the counter.
func (s *Stats) AddSeries(series int) {
	atomic.AddInt32(&s.Series, int32(series))
}

// AddSamples adds some series to the counter.
func (s *Stats) AddSamples(samples int64) {
	atomic.AddInt64(&s.Samples, samples)
}

// AddWallTime adds some time to the counter.
func (s *Stats) AddWallTime(t time.Duration) {
	atomic.AddInt64((*int64)(&s.WallTime), int64(t))
}

// Merge the provide Stats into this one.
func (s *Stats) Merge(other *Stats) {
	s.WallTime += other.WallTime
	s.Series += other.Series
	s.Samples += other.Samples
}

// Record the stats to metrics.
func (s *Stats) Record(ctx context.Context) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return
	}

	querySeconds.WithLabelValues(userID).Add(float64(s.WallTime))
	querySamples.WithLabelValues(userID).Add(float64(s.Samples))
	querySeries.WithLabelValues(userID).Add(float64(s.Series))
}

// Middleware initialises the stats in the request context, records wall clock time
// and logs the results.
type Middleware struct {
	logger log.Logger
}

// NewMiddleware makes a new Middleware.
func NewMiddleware(logger log.Logger) Middleware {
	return Middleware{
		logger: logger,
	}
}

// Wrap implements middleware.Interface.
func (m Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, err := user.ExtractOrgID(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		start := time.Now()
		stats := FromContext(r.Context())

		defer func() {
			stats.AddWallTime(time.Since(start))
			level.Info(m.logger).Log(
				"user", userID,
				"wallTime", stats.WallTime,
				"series", stats.Series,
				"samples", stats.Samples,
			)
		}()

		next.ServeHTTP(w, r)
	})
}
