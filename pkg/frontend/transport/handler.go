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
	"github.com/weaveworks/common/httpgrpc/server"
	"google.golang.org/grpc/status"

	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

const (
	// StatusClientClosedRequest is the status code for when a client request cancellation of an http request
	StatusClientClosedRequest = 499
	ServiceTimingHeaderName   = "Server-Timing"
)

var (
	errCanceled              = httpgrpc.Errorf(StatusClientClosedRequest, context.Canceled.Error())
	errDeadlineExceeded      = httpgrpc.Errorf(http.StatusGatewayTimeout, context.DeadlineExceeded.Error())
	errRequestEntityTooLarge = httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "http: request body too large")
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
	cfg          HandlerConfig
	log          log.Logger
	roundTripper http.RoundTripper

	// Metrics.
	querySeconds    *prometheus.CounterVec
	querySeries     *prometheus.CounterVec
	queryChunkBytes *prometheus.CounterVec
	queryDataBytes  *prometheus.CounterVec
	activeUsers     *util.ActiveUsersCleanupService
}

// NewHandler creates a new frontend handler.
func NewHandler(cfg HandlerConfig, roundTripper http.RoundTripper, log log.Logger, reg prometheus.Registerer) http.Handler {
	h := &Handler{
		cfg:          cfg,
		log:          log,
		roundTripper: roundTripper,
	}

	if cfg.QueryStatsEnabled {
		h.querySeconds = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_seconds_total",
			Help: "Total amount of wall clock time spend processing queries.",
		}, []string{"user"})

		h.querySeries = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_series_total",
			Help: "Number of series fetched to execute a query.",
		}, []string{"user"})

		h.queryChunkBytes = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_chunks_bytes_total",
			Help: "Size of all chunks fetched to execute a query in bytes.",
		}, []string{"user"})

		h.queryDataBytes = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_fetched_data_bytes_total",
			Help: "Size of all data fetched to execute a query in bytes.",
		}, []string{"user"})

		h.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
			h.querySeconds.DeleteLabelValues(user)
			h.querySeries.DeleteLabelValues(user)
			h.queryChunkBytes.DeleteLabelValues(user)
			h.queryDataBytes.DeleteLabelValues(user)
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
			writeError(w, err)
			return
		}
		r.Body = io.NopCloser(&buf)
	}

	startTime := time.Now()
	resp, err := f.roundTripper.RoundTrip(r)
	queryResponseTime := time.Since(startTime)

	// Check whether we should parse the query string.
	shouldReportSlowQuery := f.cfg.LogQueriesLongerThan != 0 && queryResponseTime > f.cfg.LogQueriesLongerThan
	if shouldReportSlowQuery || f.cfg.QueryStatsEnabled {
		queryString = f.parseRequestQueryString(r, buf)
	}

	if shouldReportSlowQuery {
		f.reportSlowQuery(r, queryString, queryResponseTime)
	}
	if f.cfg.QueryStatsEnabled {
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

		f.reportQueryStats(r, queryString, queryResponseTime, stats, err, statusCode)
	}

	if err != nil {
		writeError(w, err)
		return
	}

	hs := w.Header()
	for h, vs := range resp.Header {
		hs[h] = vs
	}

	if f.cfg.QueryStatsEnabled {
		writeServiceTimingHeader(queryResponseTime, hs, stats)
	}

	w.WriteHeader(resp.StatusCode)
	// log copy response body error so that we will know even though success response code returned
	bytesCopied, err := io.Copy(w, resp.Body)
	if err != nil && !errors.Is(err, syscall.EPIPE) {
		level.Error(util_log.WithContext(r.Context(), f.log)).Log("msg", "write response body error", "bytesCopied", bytesCopied, "err", err)
	}
}

func formatGrafanaStatsFields(r *http.Request) []interface{} {
	fields := make([]interface{}, 0, 2)
	if dashboardUID := r.Header.Get("X-Dashboard-Uid"); dashboardUID != "" {
		fields = append(fields, dashboardUID)
	}
	if panelID := r.Header.Get("X-Panel-Id"); panelID != "" {
		fields = append(fields, panelID)
	}
	return fields
}

// reportSlowQuery reports slow queries.
func (f *Handler) reportSlowQuery(r *http.Request, queryString url.Values, queryResponseTime time.Duration) {
	// NOTE(GiedriusS): see https://github.com/grafana/grafana/pull/60301 for more info.

	logMessage := append([]interface{}{
		"msg", "slow query detected",
		"method", r.Method,
		"host", r.Host,
		"path", r.URL.Path,
		"time_taken", queryResponseTime.String(),
	}, formatQueryString(queryString)...)
	grafanaFields := formatGrafanaStatsFields(r)
	if len(grafanaFields) > 0 {
		logMessage = append(logMessage, grafanaFields...)
	}

	level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
}

func (f *Handler) reportQueryStats(r *http.Request, queryString url.Values, queryResponseTime time.Duration, stats *querier_stats.QueryStats, error error, statusCode int) {
	tenantIDs, err := tenant.TenantIDs(r.Context())
	if err != nil {
		return
	}
	userID := tenant.JoinTenantIDs(tenantIDs)
	wallTime := stats.LoadWallTime()
	numSeries := stats.LoadFetchedSeries()
	numChunks := stats.LoadFetchedChunks()
	numSamples := stats.LoadFetchedSamples()
	numChunkBytes := stats.LoadFetchedChunkBytes()
	numDataBytes := stats.LoadFetchedDataBytes()

	// Track stats.
	f.querySeconds.WithLabelValues(userID).Add(wallTime.Seconds())
	f.querySeries.WithLabelValues(userID).Add(float64(numSeries))
	f.queryChunkBytes.WithLabelValues(userID).Add(float64(numChunkBytes))
	f.queryDataBytes.WithLabelValues(userID).Add(float64(numDataBytes))
	f.activeUsers.UpdateUserTimestamp(userID, time.Now())

	// Log stats.
	logMessage := append([]interface{}{
		"msg", "query stats",
		"component", "query-frontend",
		"method", r.Method,
		"path", r.URL.Path,
		"response_time", queryResponseTime,
		"query_wall_time_seconds", wallTime.Seconds(),
		"fetched_series_count", numSeries,
		"fetched_chunks_count", numChunks,
		"fetched_samples_count", numSamples,
		"fetched_chunks_bytes", numChunkBytes,
		"fetched_data_bytes", numDataBytes,
		"status_code", statusCode,
	}, stats.LoadExtraFields()...)

	logMessage = append(logMessage, formatQueryString(queryString)...)
	grafanaFields := formatGrafanaStatsFields(r)
	if len(grafanaFields) > 0 {
		logMessage = append(logMessage, grafanaFields...)
	}

	if error != nil {
		s, ok := status.FromError(error)
		if !ok {
			logMessage = append(logMessage, "error", error)
		} else {
			logMessage = append(logMessage, "error", s.Message())
		}
		level.Error(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
	} else {
		level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
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
	for k, v := range queryString {
		fields = append(fields, fmt.Sprintf("param_%s", k), strings.Join(v, ","))
	}
	return fields
}

func writeError(w http.ResponseWriter, err error) {
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
	server.WriteError(w, err)
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
		return StatusClientClosedRequest
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
