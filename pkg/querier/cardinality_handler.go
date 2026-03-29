package querier

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	cardinalityDefaultLimit = 10
	cardinalityMaxLimit     = 512
)

type cardinalityResponse struct {
	Status string          `json:"status"`
	Data   cardinalityData `json:"data"`
}

type cardinalityData struct {
	NumSeries                   uint64                `json:"numSeries"`
	Approximated                bool                  `json:"approximated"`
	SeriesCountByMetricName     []cardinalityStatItem `json:"seriesCountByMetricName"`
	LabelValueCountByLabelName  []cardinalityStatItem `json:"labelValueCountByLabelName"`
	SeriesCountByLabelValuePair []cardinalityStatItem `json:"seriesCountByLabelValuePair"`
}

type cardinalityStatItem struct {
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

type cardinalityErrorResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

// cardinalityMetrics holds Prometheus metrics for the cardinality endpoint.
type cardinalityMetrics struct {
	requestDuration  *prometheus.HistogramVec
	requestsTotal    *prometheus.CounterVec
	inflightRequests *prometheus.GaugeVec
}

func newCardinalityMetrics(reg prometheus.Registerer) *cardinalityMetrics {
	return &cardinalityMetrics{
		requestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "cardinality_request_duration_seconds",
			Help:      "Time (in seconds) spent serving cardinality requests.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"source", "status_code"}),
		requestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "cardinality_requests_total",
			Help:      "Total number of cardinality requests.",
		}, []string{"source", "status_code"}),
		inflightRequests: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "cardinality_inflight_requests",
			Help:      "Current number of in-flight cardinality requests.",
		}, []string{"source"}),
	}
}

// cardinalityConcurrencyLimiter tracks per-tenant concurrency for cardinality requests.
type cardinalityConcurrencyLimiter struct {
	mu      sync.Mutex
	tenants map[string]int
	limits  *validation.Overrides
}

func newCardinalityConcurrencyLimiter(limits *validation.Overrides) *cardinalityConcurrencyLimiter {
	return &cardinalityConcurrencyLimiter{
		tenants: make(map[string]int),
		limits:  limits,
	}
}

func (l *cardinalityConcurrencyLimiter) tryAcquire(tenantID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	maxConcurrent := l.limits.CardinalityMaxConcurrentRequests(tenantID)
	if l.tenants[tenantID] >= maxConcurrent {
		return false
	}
	l.tenants[tenantID]++
	return true
}

func (l *cardinalityConcurrencyLimiter) release(tenantID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tenants[tenantID]--
	if l.tenants[tenantID] <= 0 {
		delete(l.tenants, tenantID)
	}
}

// CardinalityHandler returns an HTTP handler for cardinality statistics.
// The Distributor interface (which includes the Cardinality method) is used
// for the head path. The limits parameter provides per-tenant configuration.
func CardinalityHandler(d Distributor, limits *validation.Overrides, reg prometheus.Registerer) http.Handler {
	metrics := newCardinalityMetrics(reg)
	limiter := newCardinalityConcurrencyLimiter(limits)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		// Extract tenant ID.
		tenantID, err := user.ExtractOrgID(r.Context())
		if err != nil {
			writeCardinalityError(w, http.StatusBadRequest, "bad_data", err.Error())
			return
		}

		// Check if cardinality API is enabled for this tenant.
		if !limits.CardinalityAPIEnabled(tenantID) {
			writeCardinalityError(w, http.StatusForbidden, "bad_data", "cardinality API is not enabled for this tenant")
			return
		}

		// Parse source parameter.
		source := r.FormValue("source")
		if source == "" {
			source = "head"
		}
		if source != "head" && source != "blocks" {
			writeCardinalityError(w, http.StatusBadRequest, "bad_data", `invalid source: must be "head" or "blocks"`)
			return
		}

		// Parse and validate limit parameter.
		limit := int32(cardinalityDefaultLimit)
		if s := r.FormValue("limit"); s != "" {
			v, err := strconv.Atoi(s)
			if err != nil || v < 1 || v > cardinalityMaxLimit {
				writeCardinalityError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid limit: must be an integer between 1 and %d", cardinalityMaxLimit))
				return
			}
			limit = int32(v)
		}

		// Validate source-specific parameters.
		if source == "head" {
			if r.FormValue("start") != "" || r.FormValue("end") != "" {
				writeCardinalityError(w, http.StatusBadRequest, "bad_data", "start and end parameters are not supported for source=head")
				return
			}
		}

		if source == "blocks" {
			startParam := r.FormValue("start")
			endParam := r.FormValue("end")
			if startParam == "" || endParam == "" {
				writeCardinalityError(w, http.StatusBadRequest, "bad_data", "start and end are required for source=blocks")
				return
			}

			startTs, err := parseTimestamp(startParam)
			if err != nil {
				writeCardinalityError(w, http.StatusBadRequest, "bad_data", "invalid start/end: must be RFC3339 or Unix timestamp")
				return
			}

			endTs, err := parseTimestamp(endParam)
			if err != nil {
				writeCardinalityError(w, http.StatusBadRequest, "bad_data", "invalid start/end: must be RFC3339 or Unix timestamp")
				return
			}

			if !startTs.Before(endTs) {
				writeCardinalityError(w, http.StatusBadRequest, "bad_data", "invalid time range: start must be before end")
				return
			}

			maxRange := limits.CardinalityMaxQueryRange(tenantID)
			if maxRange > 0 && endTs.Sub(startTs) > maxRange {
				writeCardinalityError(w, http.StatusBadRequest, "bad_data",
					fmt.Sprintf("the query time range exceeds the limit (query length: %s, limit: %s)", endTs.Sub(startTs), maxRange))
				return
			}

			// TODO: Implement blocks path in Phase 2
			writeCardinalityError(w, http.StatusNotImplemented, "bad_data", "source=blocks is not yet implemented")
			return
		}

		// Check concurrency limit.
		if !limiter.tryAcquire(tenantID) {
			statusCode := http.StatusTooManyRequests
			metrics.requestsTotal.WithLabelValues(source, strconv.Itoa(statusCode)).Inc()
			writeCardinalityError(w, statusCode, "bad_data", "too many concurrent cardinality requests for this tenant")
			return
		}
		defer limiter.release(tenantID)

		metrics.inflightRequests.WithLabelValues(source).Inc()
		defer metrics.inflightRequests.WithLabelValues(source).Dec()

		// Apply per-tenant query timeout.
		timeout := limits.CardinalityQueryTimeout(tenantID)
		if timeout > 0 {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			r = r.WithContext(ctx)
		}

		// Execute the cardinality query via the distributor (head path).
		req := &client.CardinalityRequest{Limit: limit}
		result, err := d.Cardinality(r.Context(), req)

		statusCode := http.StatusOK
		if err != nil {
			statusCode = http.StatusInternalServerError
			duration := time.Since(startTime).Seconds()
			metrics.requestDuration.WithLabelValues(source, strconv.Itoa(statusCode)).Observe(duration)
			metrics.requestsTotal.WithLabelValues(source, strconv.Itoa(statusCode)).Inc()
			writeCardinalityError(w, statusCode, "bad_data", err.Error())
			return
		}

		duration := time.Since(startTime).Seconds()
		metrics.requestDuration.WithLabelValues(source, strconv.Itoa(statusCode)).Observe(duration)
		metrics.requestsTotal.WithLabelValues(source, strconv.Itoa(statusCode)).Inc()

		util.WriteJSONResponse(w, cardinalityResponse{
			Status: statusSuccess,
			Data: cardinalityData{
				NumSeries:                   result.NumSeries,
				Approximated:                false, // Head path is never approximated when all ingesters respond.
				SeriesCountByMetricName:     convertStatItems(result.SeriesCountByMetricName),
				LabelValueCountByLabelName:  convertStatItems(result.LabelValueCountByLabelName),
				SeriesCountByLabelValuePair: convertStatItems(result.SeriesCountByLabelValuePair),
			},
		})
	})
}

func convertStatItems(items []*cortexpb.CardinalityStatItem) []cardinalityStatItem {
	if items == nil {
		return []cardinalityStatItem{}
	}
	result := make([]cardinalityStatItem, len(items))
	for i, item := range items {
		result[i] = cardinalityStatItem{
			Name:  item.Name,
			Value: item.Value,
		}
	}
	return result
}

func writeCardinalityError(w http.ResponseWriter, statusCode int, errorType, message string) {
	w.WriteHeader(statusCode)
	util.WriteJSONResponse(w, cardinalityErrorResponse{
		Status:    statusError,
		ErrorType: errorType,
		Error:     message,
	})
}

// parseTimestamp parses a time value from either RFC3339 or Unix timestamp format.
func parseTimestamp(s string) (time.Time, error) {
	// Try RFC3339 first.
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Try Unix timestamp (seconds, possibly with decimal).
	if strings.Contains(s, ".") {
		parts := strings.SplitN(s, ".", 2)
		sec, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", s)
		}
		fracStr := parts[1]
		for len(fracStr) < 9 {
			fracStr += "0"
		}
		nsec, err := strconv.ParseInt(fracStr[:9], 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", s)
		}
		return time.Unix(sec, nsec), nil
	}

	sec, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", s)
	}
	return time.Unix(sec, 0), nil
}
