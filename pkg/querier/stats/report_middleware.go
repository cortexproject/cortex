package stats

import (
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/tenant"
)

// ReportMiddleware logs and track metrics with the query statistics.
type ReportMiddleware struct {
	logger log.Logger

	querySeconds *prometheus.CounterVec
	querySamples *prometheus.CounterVec
}

// NewReportMiddleware makes a new ReportMiddleware.
func NewReportMiddleware(logger log.Logger, reg prometheus.Registerer) ReportMiddleware {
	return ReportMiddleware{
		logger: logger,
		querySeconds: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_seconds_total",
			Help: "Total amount of wall clock time spend processing queries.",
		}, []string{"user"}),
		querySamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_samples_total",
			Help: "Total number of samples queried.",
		}, []string{"user"}),
	}
}

// Wrap implements middleware.Interface.
func (m ReportMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, err := tenant.TenantID(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Initialise the stats in the context and make sure it's propagated
		// down the request chain.
		stats, ctx := ContextWithEmptyStats(r.Context())
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)

		// Track statistics.
		// TODO we should use atomic to read
		m.querySeconds.WithLabelValues(userID).Add(float64(stats.WallTime))
		m.querySamples.WithLabelValues(userID).Add(float64(stats.Samples))

		level.Info(m.logger).Log(
			"msg", "query stats",
			"user", userID,
			"method", r.Method,
			"path", r.URL.Path,
			"wallTime", stats.WallTime,
			"samples", stats.Samples,
		)
	})
}
