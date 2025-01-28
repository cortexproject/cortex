package transport

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/status"

	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/querier/tenantfederation"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	util_api "github.com/cortexproject/cortex/pkg/util/api"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

const (
	// StatusClientClosedRequest is the status code for when a client request cancellation of a http request
	StatusClientClosedRequest = 499
	ServiceTimingHeaderName   = "Server-Timing"

	errTooManyTenants = "too many tenants, max: %d, actual: %d"
)

var (
	errCanceled              = httpgrpc.Errorf(StatusClientClosedRequest, context.Canceled.Error())
	errDeadlineExceeded      = httpgrpc.Errorf(http.StatusGatewayTimeout, context.DeadlineExceeded.Error())
	errRequestEntityTooLarge = httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "http: request body too large")
)

const (
	reasonRequestBodySizeExceeded  = "request_body_size_exceeded"
	reasonResponseBodySizeExceeded = "response_body_size_exceeded"
	reasonTooManyRequests          = "too_many_requests"
	reasonTimeRangeExceeded        = "time_range_exceeded"
	reasonTooManySamples           = "too_many_samples"
	reasonSeriesFetched            = "series_fetched"
	reasonChunksFetched            = "chunks_fetched"
	reasonChunkBytesFetched        = "chunk_bytes_fetched"
	reasonDataBytesFetched         = "data_bytes_fetched"
	reasonSeriesLimitStoreGateway  = "store_gateway_series_limit"
	reasonChunksLimitStoreGateway  = "store_gateway_chunks_limit"
	reasonBytesLimitStoreGateway   = "store_gateway_bytes_limit"

	limitTooManySamples    = `query processing would load too many samples into memory`
	limitTimeRangeExceeded = `the query time range exceeds the limit`
	limitSeriesFetched     = `the query hit the max number of series limit`
	limitChunksFetched     = `the query hit the max number of chunks limit`
	limitChunkBytesFetched = `the query hit the aggregated chunks size limit`
	limitDataBytesFetched  = `the query hit the aggregated data size limit`

	// Store gateway limits.
	limitSeriesStoreGateway = `exceeded series limit`
	limitChunksStoreGateway = `exceeded chunks limit`
	limitBytesStoreGateway  = `exceeded bytes limit`
)

// Config for a Handler.
type HandlerConfig struct {
	LogQueriesLongerThan time.Duration `yaml:"log_queries_longer_than"`
	MaxBodySize          int64         `yaml:"max_body_size"`
	QueryStatsEnabled    bool          `yaml:"query_stats_enabled"`
}

func (cfg *HandlerConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.LogQueriesLongerThan, "frontend.log-queries-longer-than", 0, "Log queries that are slower than the specified duration. Set to 0 to disable. Set to < 0 to enable on all queries.")
	f.Int64Var(&cfg.MaxBodySize, "frontend.max-body-size", 10*1024*1024, "Max body size for downstream prometheus.")
	f.BoolVar(&cfg.QueryStatsEnabled, "frontend.query-stats-enabled", false, "True to enable query statistics tracking. When enabled, a message with some statistics is logged for every query.")
}

// Handler accepts queries and forwards them to RoundTripper. It can log slow queries,
// but all other logic is inside the RoundTripper.
type Handler struct {
	cfg                 HandlerConfig
	tenantFederationCfg tenantfederation.Config
	log                 log.Logger
	roundTripper        http.RoundTripper

	// Metrics.
	querySeconds        *prometheus.CounterVec
	queryFetchedSeries  *prometheus.CounterVec
	queryFetchedSamples *prometheus.CounterVec
	queryScannedSamples *prometheus.CounterVec
	queryPeakSamples    *prometheus.HistogramVec
	queryChunkBytes     *prometheus.CounterVec
	queryDataBytes      *prometheus.CounterVec
	rejectedQueries     *prometheus.CounterVec
	activeUsers         *util.ActiveUsersCleanupService
}

// NewHandler creates a new frontend handler.
func NewHandler(cfg HandlerConfig, tenantFederationCfg tenantfederation.Config, roundTripper http.RoundTripper, log log.Logger, reg prometheus.Registerer) *Handler {
	h := &Handler{
		cfg:                 cfg,
		tenantFederationCfg: tenantFederationCfg,
		log:                 log,
		roundTripper:        roundTripper,
	}

	if cfg.QueryStatsEnabled {
		h.querySeconds = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_seconds_total",
			Help: "Total amount of wall clock time spend processing queries.",
		}, []string{"source", "user"})

		h.queryFetchedSeries = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_series_total",
			Help: "Number of series fetched to execute a query.",
		}, []string{"source", "user"})

		h.queryFetchedSamples = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_samples_total",
			Help: "Number of samples fetched to execute a query.",
		}, []string{"source", "user"})

		// It tracks TotalSamples in https://github.com/prometheus/prometheus/blob/main/util/stats/query_stats.go#L237 for each user.
		h.queryScannedSamples = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_samples_scanned_total",
			Help: "Number of samples scanned to execute a query.",
		}, []string{"source", "user"})

		h.queryPeakSamples = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_query_peak_samples",
			Help:                            "Highest count of samples considered to execute a query.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}, []string{"source", "user"})

		h.queryChunkBytes = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_chunks_bytes_total",
			Help: "Size of all chunks fetched to execute a query in bytes.",
		}, []string{"source", "user"})

		h.queryDataBytes = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_data_bytes_total",
			Help: "Size of all data fetched to execute a query in bytes.",
		}, []string{"source", "user"})

		h.rejectedQueries = promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "cortex_rejected_queries_total",
				Help: "The total number of queries that were rejected.",
			},
			[]string{"reason", "source", "user"},
		)

		h.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
			h.querySeconds.DeleteLabelValues(user)
			h.queryFetchedSeries.DeleteLabelValues(user)
			h.queryFetchedSamples.DeleteLabelValues(user)
			h.queryScannedSamples.DeleteLabelValues(user)
			h.queryPeakSamples.DeleteLabelValues(user)
			h.queryChunkBytes.DeleteLabelValues(user)
			h.queryDataBytes.DeleteLabelValues(user)
			if err := util.DeleteMatchingLabels(h.rejectedQueries, map[string]string{"user": user}); err != nil {
				level.Warn(log).Log("msg", "failed to remove cortex_rejected_queries_total metric for user", "user", user, "err", err)
			}
		})
		// If cleaner stops or fail, we will simply not clean the metrics for inactive users.
		_ = h.activeUsers.StartAsync(context.Background())
	}

	return h
}

func (f *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		stats       *querier_stats.QueryStats
		queryString url.Values
	)

	tenantIDs, err := tenant.TenantIDs(r.Context())
	if err != nil {
		return
	}

	if f.tenantFederationCfg.Enabled {
		maxTenant := f.tenantFederationCfg.MaxTenant
		if maxTenant > 0 && len(tenantIDs) > maxTenant {
			http.Error(w, fmt.Errorf(errTooManyTenants, maxTenant, len(tenantIDs)).Error(), http.StatusBadRequest)
			return
		}
	}

	userID := tenant.JoinTenantIDs(tenantIDs)

	// Initialise the stats in the context and make sure it's propagated
	// down the request chain.
	if f.cfg.QueryStatsEnabled {
		// Check if querier stats is enabled in the context.
		stats = querier_stats.FromContext(r.Context())
		if stats == nil {
			var ctx context.Context
			stats, ctx = querier_stats.ContextWithEmptyStats(r.Context())
			r = r.WithContext(ctx)
		}
	}

	defer func() {
		_ = r.Body.Close()
	}()

	// Buffer the body for later use to track slow queries.
	var buf bytes.Buffer
	r.Body = http.MaxBytesReader(w, r.Body, f.cfg.MaxBodySize)
	r.Body = io.NopCloser(io.TeeReader(r.Body, &buf))
	// We parse form here so that we can use buf as body, in order to
	// prevent https://github.com/cortexproject/cortex/issues/5201.
	// Exclude remote read here as we don't have to buffer its body.
	if !strings.Contains(r.URL.Path, "api/v1/read") {
		if err := r.ParseForm(); err != nil {
			statusCode := http.StatusBadRequest
			if util.IsRequestBodyTooLarge(err) {
				statusCode = http.StatusRequestEntityTooLarge
			}
			http.Error(w, err.Error(), statusCode)
			if f.cfg.QueryStatsEnabled && util.IsRequestBodyTooLarge(err) {
				source := tripperware.GetSource(r.Header.Get("User-Agent"))
				f.rejectedQueries.WithLabelValues(reasonRequestBodySizeExceeded, source, userID).Inc()
			}
			return
		}
		r.Body = io.NopCloser(&buf)
	}

	// Log request
	if f.cfg.QueryStatsEnabled {
		queryString = f.parseRequestQueryString(r, buf)
		f.logQueryRequest(r, queryString)
	}

	startTime := time.Now()
	resp, err := f.roundTripper.RoundTrip(r)
	queryResponseTime := time.Since(startTime)

	// Check if we need to parse the query string to avoid parsing twice.
	shouldReportSlowQuery := f.cfg.LogQueriesLongerThan != 0 && queryResponseTime > f.cfg.LogQueriesLongerThan
	if shouldReportSlowQuery && !f.cfg.QueryStatsEnabled {
		queryString = f.parseRequestQueryString(r, buf)
	}
	if shouldReportSlowQuery {
		f.reportSlowQuery(r, queryString, queryResponseTime)
	}

	if f.cfg.QueryStatsEnabled {
		// Try to parse error and get status code.
		var statusCode int
		if err != nil {
			statusCode = getStatusCodeFromError(err)
		} else if resp != nil {
			statusCode = resp.StatusCode
			// If the response status code is not 2xx, try to get the
			// error message from response body.
			if resp.StatusCode/100 != 2 {
				body, err2 := tripperware.BodyBuffer(resp, f.log)
				if err2 == nil {
					err = httpgrpc.Errorf(resp.StatusCode, string(body))
				}
			}
		}

		source := tripperware.GetSource(r.Header.Get("User-Agent"))
		f.reportQueryStats(r, source, userID, queryString, queryResponseTime, stats, err, statusCode, resp)
	}

	hs := w.Header()
	if f.cfg.QueryStatsEnabled {
		writeServiceTimingHeader(queryResponseTime, hs, stats)
	}

	logger := util_log.WithContext(r.Context(), f.log)
	if err != nil {
		writeError(logger, w, err, hs)
		return
	}

	for h, vs := range resp.Header {
		hs[h] = vs
	}

	w.WriteHeader(resp.StatusCode)
	// log copy response body error so that we will know even though success response code returned
	bytesCopied, err := io.Copy(w, resp.Body)
	if err != nil && !errors.Is(err, syscall.EPIPE) {
		level.Error(logger).Log("msg", "write response body error", "bytesCopied", bytesCopied, "err", err)
	}
}

func formatGrafanaStatsFields(r *http.Request) []interface{} {
	// NOTE(GiedriusS): see https://github.com/grafana/grafana/pull/60301 for more info.

	fields := make([]interface{}, 0, 4)
	if dashboardUID := r.Header.Get("X-Dashboard-Uid"); dashboardUID != "" {
		fields = append(fields, "X-Dashboard-Uid", dashboardUID)
	}
	if panelID := r.Header.Get("X-Panel-Id"); panelID != "" {
		fields = append(fields, "X-Panel-Id", panelID)
	}
	return fields
}

// logQueryRequest logs query request before query execution.
func (f *Handler) logQueryRequest(r *http.Request, queryString url.Values) {
	logMessage := []interface{}{
		"msg", "query request",
		"component", "query-frontend",
		"method", r.Method,
		"path", r.URL.Path,
	}
	grafanaFields := formatGrafanaStatsFields(r)
	if len(grafanaFields) > 0 {
		logMessage = append(logMessage, grafanaFields...)
	}
	logMessage = append(logMessage, formatQueryString(queryString)...)

	level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
}

// reportSlowQuery reports slow queries.
func (f *Handler) reportSlowQuery(r *http.Request, queryString url.Values, queryResponseTime time.Duration) {
	logMessage := []interface{}{
		"msg", "slow query detected",
		"method", r.Method,
		"host", r.Host,
		"path", r.URL.Path,
		"time_taken", queryResponseTime.String(),
	}
	grafanaFields := formatGrafanaStatsFields(r)
	if len(grafanaFields) > 0 {
		logMessage = append(logMessage, grafanaFields...)
	}
	logMessage = append(logMessage, formatQueryString(queryString)...)

	level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
}

func (f *Handler) reportQueryStats(r *http.Request, source, userID string, queryString url.Values, queryResponseTime time.Duration, stats *querier_stats.QueryStats, error error, statusCode int, resp *http.Response) {
	wallTime := stats.LoadWallTime()
	queryStorageWallTime := stats.LoadQueryStorageWallTime()
	numResponseSeries := stats.LoadResponseSeries()
	numFetchedSeries := stats.LoadFetchedSeries()
	numFetchedChunks := stats.LoadFetchedChunks()
	numFetchedSamples := stats.LoadFetchedSamples()
	numScannedSamples := stats.LoadScannedSamples()
	numPeakSamples := stats.LoadPeakSamples()
	numChunkBytes := stats.LoadFetchedChunkBytes()
	numDataBytes := stats.LoadFetchedDataBytes()
	numStoreGatewayTouchedPostings := stats.LoadStoreGatewayTouchedPostings()
	numStoreGatewayTouchedPostingBytes := stats.LoadStoreGatewayTouchedPostingBytes()
	splitQueries := stats.LoadSplitQueries()
	dataSelectMaxTime := stats.LoadDataSelectMaxTime()
	dataSelectMinTime := stats.LoadDataSelectMinTime()

	// Track stats.
	f.querySeconds.WithLabelValues(source, userID).Add(wallTime.Seconds())
	f.queryFetchedSeries.WithLabelValues(source, userID).Add(float64(numFetchedSeries))
	f.queryFetchedSamples.WithLabelValues(source, userID).Add(float64(numFetchedSamples))
	f.queryScannedSamples.WithLabelValues(source, userID).Add(float64(numScannedSamples))
	f.queryPeakSamples.WithLabelValues(source, userID).Observe(float64(numPeakSamples))
	f.queryChunkBytes.WithLabelValues(source, userID).Add(float64(numChunkBytes))
	f.queryDataBytes.WithLabelValues(source, userID).Add(float64(numDataBytes))
	f.activeUsers.UpdateUserTimestamp(userID, time.Now())

	var (
		contentLength int64
		encoding      string
	)
	if resp != nil {
		contentLength = resp.ContentLength
		encoding = resp.Header.Get("Content-Encoding")
	}

	// Log stats.
	logMessage := append([]interface{}{
		"msg", "query stats",
		"component", "query-frontend",
		"method", r.Method,
		"path", r.URL.Path,
		"response_time", queryResponseTime,
		"query_wall_time_seconds", wallTime.Seconds(),
		"response_series_count", numResponseSeries,
		"fetched_series_count", numFetchedSeries,
		"fetched_chunks_count", numFetchedChunks,
		"fetched_samples_count", numFetchedSamples,
		"fetched_chunks_bytes", numChunkBytes,
		"fetched_data_bytes", numDataBytes,
		"split_queries", splitQueries,
		"status_code", statusCode,
		"response_size", contentLength,
	}, stats.LoadExtraFields()...)

	if numStoreGatewayTouchedPostings > 0 {
		logMessage = append(logMessage, "store_gateway_touched_postings_count", numStoreGatewayTouchedPostings)
		logMessage = append(logMessage, "store_gateway_touched_posting_bytes", numStoreGatewayTouchedPostingBytes)
	}

	grafanaFields := formatGrafanaStatsFields(r)
	if len(grafanaFields) > 0 {
		logMessage = append(logMessage, grafanaFields...)
	}

	if len(encoding) > 0 {
		logMessage = append(logMessage, "content_encoding", encoding)
	}

	if dataSelectMaxTime > 0 {
		logMessage = append(logMessage, "data_select_max_time", util.FormatMillisToSeconds(dataSelectMaxTime))
	}
	if dataSelectMinTime > 0 {
		logMessage = append(logMessage, "data_select_min_time", util.FormatMillisToSeconds(dataSelectMinTime))
	}
	if query := queryString.Get("query"); len(query) > 0 {
		logMessage = append(logMessage, "query_length", len(query))
	}
	if ua := r.Header.Get("User-Agent"); len(ua) > 0 {
		logMessage = append(logMessage, "user_agent", ua)
	}
	if priority, ok := stats.LoadPriority(); ok {
		logMessage = append(logMessage, "priority", priority)
	}
	if sws := queryStorageWallTime.Seconds(); sws > 0 {
		// Only include query storage wall time field if set. This value can be 0
		// for query APIs that don't call `Querier` interface.
		logMessage = append(logMessage, "query_storage_wall_time_seconds", sws)
	}

	if error != nil {
		s, ok := status.FromError(error)
		if !ok {
			logMessage = append(logMessage, "error", error)
		} else {
			logMessage = append(logMessage, "error", s.Message())
		}
	}
	logMessage = append(logMessage, formatQueryString(queryString)...)
	if error != nil {
		level.Error(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
	} else {
		level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
	}

	var reason string
	if statusCode == http.StatusTooManyRequests {
		reason = reasonTooManyRequests
	} else if statusCode == http.StatusRequestEntityTooLarge {
		reason = reasonResponseBodySizeExceeded
	} else if statusCode == http.StatusUnprocessableEntity {
		errMsg := error.Error()
		if strings.Contains(errMsg, limitTooManySamples) {
			reason = reasonTooManySamples
		} else if strings.Contains(errMsg, limitTimeRangeExceeded) {
			reason = reasonTimeRangeExceeded
		} else if strings.Contains(errMsg, limitSeriesFetched) {
			reason = reasonSeriesFetched
		} else if strings.Contains(errMsg, limitChunksFetched) {
			reason = reasonChunksFetched
		} else if strings.Contains(errMsg, limitChunkBytesFetched) {
			reason = reasonChunkBytesFetched
		} else if strings.Contains(errMsg, limitDataBytesFetched) {
			reason = reasonDataBytesFetched
		} else if strings.Contains(errMsg, limitSeriesStoreGateway) {
			reason = reasonSeriesLimitStoreGateway
		} else if strings.Contains(errMsg, limitChunksStoreGateway) {
			reason = reasonChunksLimitStoreGateway
		} else if strings.Contains(errMsg, limitBytesStoreGateway) {
			reason = reasonBytesLimitStoreGateway
		}
	}
	if len(reason) > 0 {
		f.rejectedQueries.WithLabelValues(reason, source, userID).Inc()
		stats.LimitHit = reason
	}
}

func (f *Handler) parseRequestQueryString(r *http.Request, bodyBuf bytes.Buffer) url.Values {
	// Use previously buffered body.
	r.Body = io.NopCloser(&bodyBuf)

	// Ensure the form has been parsed so all the parameters are present
	err := r.ParseForm()
	if err != nil {
		level.Warn(util_log.WithContext(r.Context(), f.log)).Log("msg", "unable to parse request form", "err", err)
		return nil
	}

	return r.Form
}

func formatQueryString(queryString url.Values) (fields []interface{}) {
	var queryFields []interface{}
	for k, v := range queryString {
		// If `query` or `match[]` field exists, we always put it as the last field.
		if k == "query" || k == "match[]" {
			queryFields = []interface{}{fmt.Sprintf("param_%s", k), strings.Join(v, ",")}
			continue
		}
		fields = append(fields, fmt.Sprintf("param_%s", k), strings.Join(v, ","))
	}
	if len(queryFields) > 0 {
		fields = append(fields, queryFields...)
	}
	return fields
}

func writeError(logger log.Logger, w http.ResponseWriter, err error, additionalHeaders http.Header) {
	switch err {
	case context.Canceled:
		err = errCanceled
	case context.DeadlineExceeded:
		err = errDeadlineExceeded
	default:
		if util.IsRequestBodyTooLarge(err) {
			err = errRequestEntityTooLarge
		}
	}

	headers := w.Header()
	for k, values := range additionalHeaders {
		for _, value := range values {
			headers.Set(k, value)
		}
	}
	util_api.RespondFromGRPCError(logger, w, err)
}

func writeServiceTimingHeader(queryResponseTime time.Duration, headers http.Header, stats *querier_stats.QueryStats) {
	if stats != nil {
		parts := make([]string, 0)
		parts = append(parts, statsValue("querier_wall_time", stats.LoadWallTime()))
		parts = append(parts, statsValue("response_time", queryResponseTime))
		headers.Set(ServiceTimingHeaderName, strings.Join(parts, ", "))
	}
}

func statsValue(name string, d time.Duration) string {
	durationInMs := strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', -1, 64)
	return name + ";dur=" + durationInMs
}

// getStatusCodeFromError extracts http status code based on error, similar to how writeError was implemented.
func getStatusCodeFromError(err error) int {
	switch err {
	case context.Canceled:
		return util_api.StatusClientClosedRequest
	case context.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		if util.IsRequestBodyTooLarge(err) {
			return http.StatusRequestEntityTooLarge
		}
	}
	resp, ok := httpgrpc.HTTPResponseFromError(err)
	if ok {
		return int(resp.Code)
	}
	return http.StatusInternalServerError
}
