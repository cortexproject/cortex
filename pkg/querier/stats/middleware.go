package stats

import (
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/tenant"
)

// Middleware initialises the stats in the request context, records wall clock time
// and logs the results.
type Middleware struct {
	logger log.Logger

	querySeconds *prometheus.CounterVec
	querySeries  *prometheus.CounterVec
	querySamples *prometheus.CounterVec
}

// NewMiddleware makes a new Middleware.
func NewMiddleware(logger log.Logger, reg prometheus.Registerer) Middleware {
	return Middleware{
		logger: logger,
		querySeconds: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_seconds_total",
			Help: "Total amount of wall clock time spend processing queries.",
		}, []string{"user"}),
		querySeries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_series_total",
			Help: "Total number of series queried.",
		}, []string{"user"}),
		querySamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_samples_total",
			Help: "Total number of samples queried.",
		}, []string{"user"}),
	}
}

// Wrap implements middleware.Interface.
func (m Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, err := tenant.TenantID(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Initialise the stats in the context and make sure it's propagated
		// down the request chain.
		stats, ctx := AddToContext(r.Context())
		r = r.WithContext(ctx)

		start := time.Now()
		next.ServeHTTP(w, r)

		// Track statistics.
		stats.AddWallTime(time.Since(start))
		m.querySeconds.WithLabelValues(userID).Add(float64(stats.WallTime))
		m.querySamples.WithLabelValues(userID).Add(float64(stats.Samples))
		m.querySeries.WithLabelValues(userID).Add(float64(stats.Series))

		level.Info(m.logger).Log(
			"user", userID,
			"wallTime", stats.WallTime,
			"series", stats.Series,
			"samples", stats.Samples,
		)
	})
}
