package querier

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	cardinalityDefaultLimit = 10
	cardinalityMaxLimit     = 512

	cardinalitySourceHead   = "head"
	cardinalitySourceBlocks = "blocks"

	cardinalityErrorTypeBadData  = "bad_data"
	cardinalityErrorTypeInternal = "internal"
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

// BlocksCardinalityQuerier is the interface for querying cardinality from blocks storage.
type BlocksCardinalityQuerier interface {
	BlocksCardinality(ctx context.Context, userID string, minT, maxT int64, limit int32) (*client.CardinalityResponse, bool, error)
}

// CardinalityHandler returns an HTTP handler for cardinality statistics.
// The Distributor interface (which includes the Cardinality method) is used
// for the head path. The BlocksCardinalityQuerier is used for the blocks path.
// The limits parameter provides per-tenant configuration.
func CardinalityHandler(d Distributor, blocksQuerier BlocksCardinalityQuerier, limits *validation.Overrides, reg prometheus.Registerer) http.Handler {
	metrics := newCardinalityMetrics(reg)
	limiter := newCardinalityConcurrencyLimiter(limits)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		// Extract tenant ID.
		tenantID, err := users.TenantID(r.Context())
		if err != nil {
			writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData, err.Error())
			return
		}

		// Check if cardinality API is enabled for this tenant.
		if !limits.CardinalityAPIEnabled(tenantID) {
			writeCardinalityError(w, http.StatusForbidden, cardinalityErrorTypeBadData, "cardinality API is not enabled for this tenant")
			return
		}

		// Parse source parameter.
		source := r.FormValue("source")
		if source == "" {
			source = cardinalitySourceHead
		}
		if source != cardinalitySourceHead && source != cardinalitySourceBlocks {
			writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData, `invalid source: must be "head" or "blocks"`)
			return
		}

		// Parse and validate limit parameter.
		limit := int32(cardinalityDefaultLimit)
		if s := r.FormValue("limit"); s != "" {
			v, err := strconv.Atoi(s)
			if err != nil || v < 1 || v > cardinalityMaxLimit {
				writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData, fmt.Sprintf("invalid limit: must be an integer between 1 and %d", cardinalityMaxLimit))
				return
			}
			limit = int32(v)
		}

		// Validate source-specific parameters and parse time range for blocks.
		var minT, maxT int64
		if source == cardinalitySourceHead {
			if r.FormValue("start") != "" || r.FormValue("end") != "" {
				writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData, "start and end parameters are not supported for source=head")
				return
			}
		}

		if source == cardinalitySourceBlocks {
			startParam := r.FormValue("start")
			endParam := r.FormValue("end")
			if startParam == "" || endParam == "" {
				writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData, "start and end are required for source=blocks")
				return
			}

			minT, err = util.ParseTime(startParam)
			if err != nil {
				writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData, "invalid start/end: must be RFC3339 or Unix timestamp")
				return
			}

			maxT, err = util.ParseTime(endParam)
			if err != nil {
				writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData, "invalid start/end: must be RFC3339 or Unix timestamp")
				return
			}

			startTs := util.TimeFromMillis(minT)
			endTs := util.TimeFromMillis(maxT)

			if !startTs.Before(endTs) {
				writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData, "invalid time range: start must be before end")
				return
			}

			maxRange := limits.CardinalityMaxQueryRange(tenantID)
			if maxRange > 0 && endTs.Sub(startTs) > maxRange {
				writeCardinalityError(w, http.StatusBadRequest, cardinalityErrorTypeBadData,
					fmt.Sprintf("the query time range exceeds the limit (query length: %s, limit: %s)", endTs.Sub(startTs), maxRange))
				return
			}

			if blocksQuerier == nil {
				writeCardinalityError(w, http.StatusNotImplemented, cardinalityErrorTypeBadData, "source=blocks is not available")
				return
			}
		}

		// Check concurrency limit.
		if !limiter.tryAcquire(tenantID) {
			statusCode := http.StatusTooManyRequests
			metrics.requestsTotal.WithLabelValues(source, strconv.Itoa(statusCode)).Inc()
			writeCardinalityError(w, statusCode, cardinalityErrorTypeBadData, "too many concurrent cardinality requests for this tenant")
			return
		}
		defer limiter.release(tenantID)

		metrics.inflightRequests.WithLabelValues(source).Inc()
		defer metrics.inflightRequests.WithLabelValues(source).Dec()

		// Apply per-tenant query timeout.
		ctx := r.Context()
		timeout := limits.CardinalityQueryTimeout(tenantID)
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		// Execute the cardinality query.
		var result *client.CardinalityResponse
		var approximated bool

		switch source {
		case cardinalitySourceHead:
			req := &client.CardinalityRequest{Limit: limit}
			result, err = d.Cardinality(ctx, req)
		case cardinalitySourceBlocks:
			result, approximated, err = blocksQuerier.BlocksCardinality(ctx, tenantID, minT, maxT, limit)
		}

		statusCode := http.StatusOK
		if err != nil {
			statusCode = http.StatusInternalServerError
			duration := time.Since(startTime).Seconds()
			metrics.requestDuration.WithLabelValues(source, strconv.Itoa(statusCode)).Observe(duration)
			metrics.requestsTotal.WithLabelValues(source, strconv.Itoa(statusCode)).Inc()
			writeCardinalityError(w, statusCode, cardinalityErrorTypeInternal, err.Error())
			return
		}

		duration := time.Since(startTime).Seconds()
		metrics.requestDuration.WithLabelValues(source, strconv.Itoa(statusCode)).Observe(duration)
		metrics.requestsTotal.WithLabelValues(source, strconv.Itoa(statusCode)).Inc()

		util.WriteJSONResponse(w, cardinalityResponse{
			Status: statusSuccess,
			Data: cardinalityData{
				NumSeries:                   result.NumSeries,
				Approximated:                approximated,
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
