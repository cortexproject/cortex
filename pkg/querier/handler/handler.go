package handler

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/instantquery"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/regexp"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	github_com_cortexproject_cortex_pkg_cortexpb "github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/httputil"
	"github.com/prometheus/prometheus/util/stats"
)

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"

	// Non-standard status code (originally introduced by nginx) for the case when a client closes
	// the connection while the server is still processing the request.
	statusClientClosedConnection = 499
)

type errorType string

const (
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
	errorNotFound    errorType = "not_found"
)

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type StatsRenderer func(context.Context, *stats.Statistics, string) stats.QueryStats

func defaultStatsRenderer(_ context.Context, s *stats.Statistics, param string) stats.QueryStats {
	if param != "" {
		return stats.NewQueryStats(s)
	}
	return nil
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

type apiFuncResult struct {
	data      interface{}
	err       *apiError
	warnings  storage.Warnings
	finalizer func()
}

type apiFunc func(r *http.Request) apiFuncResult

// QueryEngine defines the interface for the *promql.Engine, so it can be replaced, wrapped or mocked.
type QueryEngine interface {
	SetQueryLogger(l promql.QueryLogger)
	NewInstantQuery(ctx context.Context, q storage.Queryable, opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error)
	NewRangeQuery(ctx context.Context, q storage.Queryable, opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error)
}

// API can register a set of endpoints in a router and handle
// them using the provided storage and query engine.
type API struct {
	Queryable     storage.SampleAndChunkQueryable
	QueryEngine   QueryEngine
	now           func() time.Time
	ready         func(http.HandlerFunc) http.HandlerFunc
	logger        log.Logger
	CORSOrigin    *regexp.Regexp
	isAgent       bool
	statsRenderer StatsRenderer
}

// NewAPI returns an initialized API type.
func NewAPI(
	qe QueryEngine,
	q storage.SampleAndChunkQueryable,
	readyFunc func(http.HandlerFunc) http.HandlerFunc,
	logger log.Logger,
	isAgent bool,
	corsOrigin *regexp.Regexp,
	statsRenderer StatsRenderer,
) *API {
	a := &API{
		QueryEngine:   qe,
		Queryable:     q,
		now:           time.Now,
		ready:         readyFunc,
		logger:        logger,
		CORSOrigin:    corsOrigin,
		isAgent:       isAgent,
		statsRenderer: defaultStatsRenderer,
	}

	if statsRenderer != nil {
		a.statsRenderer = statsRenderer
	}

	return a
}

func setUnavailStatusOnTSDBNotReady(r apiFuncResult) apiFuncResult {
	if r.err != nil && errors.Cause(r.err.err) == tsdb.ErrNotReady {
		r.err.typ = errorUnavailable
	}
	return r
}

// Register the API's endpoints in the given router.
func (api *API) Register(r *route.Router) {
	wrap := func(f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			httputil.SetCORS(w, api.CORSOrigin, r)
			result := setUnavailStatusOnTSDBNotReady(f(r))
			if result.finalizer != nil {
				defer result.finalizer()
			}
			if result.err != nil {
				api.respondError(w, result.err, result.data)
				return
			}

			if result.data != nil {
				api.respond(w, result.data, result.warnings)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		return api.ready(CompressionHandler{
			Handler: hf,
		}.ServeHTTP)
	}

	wrapAgent := func(f apiFunc) http.HandlerFunc {
		return wrap(func(r *http.Request) apiFuncResult {
			if api.isAgent {
				return apiFuncResult{nil, &apiError{errorExec, errors.New("unavailable with Prometheus Agent")}, nil, nil}
			}
			return f(r)
		})
	}

	r.Get("/query", wrapAgent(api.query))
	r.Post("/query", wrapAgent(api.query))
	r.Get("/query_range", wrapAgent(api.queryRange))
	r.Post("/query_range", wrapAgent(api.queryRange))
}

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
	Stats      stats.QueryStats `json:"stats,omitempty"`
}

func invalidParamError(err error, parameter string) apiFuncResult {
	return apiFuncResult{nil, &apiError{
		errorBadData, errors.Wrapf(err, "invalid parameter %q", parameter),
	}, nil, nil}
}

func (api *API) query(r *http.Request) (result apiFuncResult) {
	ts, err := parseTimeParam(r, "time", api.now())
	if err != nil {
		return invalidParamError(err, "time")
	}
	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	opts, err := extractQueryOpts(r)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	qry, err := api.QueryEngine.NewInstantQuery(ctx, api.Queryable, opts, r.FormValue("query"), ts)
	if err != nil {
		return invalidParamError(err, "query")
	}

	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	sr := api.statsRenderer
	if sr == nil {
		sr = defaultStatsRenderer
	}
	qs := sr(ctx, qry.Stats(), r.FormValue("stats"))

	return apiFuncResult{createPrometheusInstantQueryResponse(&queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}), nil, res.Warnings, qry.Close}
}

func extractQueryOpts(r *http.Request) (*promql.QueryOpts, error) {
	opts := &promql.QueryOpts{
		EnablePerStepStats: r.FormValue("stats") == "all",
	}
	if strDuration := r.FormValue("lookback_delta"); strDuration != "" {
		duration, err := parseDuration(strDuration)
		if err != nil {
			return nil, fmt.Errorf("error parsing lookback delta duration: %w", err)
		}
		opts.LookbackDelta = duration
	}
	return opts, nil
}

func (api *API) queryRange(r *http.Request) (result apiFuncResult) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return invalidParamError(err, "end")
	}
	if end.Before(start) {
		return invalidParamError(errors.New("end timestamp must not be before start time"), "end")
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return invalidParamError(err, "step")
	}

	if step <= 0 {
		return invalidParamError(errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer"), "step")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	opts, err := extractQueryOpts(r)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	qry, err := api.QueryEngine.NewRangeQuery(ctx, api.Queryable, opts, r.FormValue("query"), start, end, step)
	if err != nil {
		return invalidParamError(err, "query")
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	sr := api.statsRenderer
	if sr == nil {
		sr = defaultStatsRenderer
	}
	qs := sr(ctx, qry.Stats(), r.FormValue("stats"))

	return apiFuncResult{createPrometheusResponse(&queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}), nil, res.Warnings, qry.Close}
}

func returnAPIError(err error) *apiError {
	if err == nil {
		return nil
	}

	cause := errors.Unwrap(err)
	if cause == nil {
		cause = err
	}

	switch cause.(type) {
	case promql.ErrQueryCanceled:
		return &apiError{errorCanceled, err}
	case promql.ErrQueryTimeout:
		return &apiError{errorTimeout, err}
	case promql.ErrStorage:
		return &apiError{errorInternal, err}
	}

	if errors.Is(err, context.Canceled) {
		return &apiError{errorCanceled, err}
	}

	return &apiError{errorExec, err}
}

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

func (api *API) respond(w http.ResponseWriter, data interface{}, warnings storage.Warnings) {
	statusMessage := statusSuccess
	var warningStrings []string
	for _, warning := range warnings {
		warningStrings = append(warningStrings, warning.Error())
	}
	var b []byte
	var err error
	switch data.(type) {
	case queryrange.PrometheusResponse:
		prometheusResponse, _ := data.(queryrange.PrometheusResponse)
		prometheusResponse.Status = string(statusMessage)
		b, err = proto.Marshal(&prometheusResponse)
	case instantquery.PrometheusInstantQueryResponse:
		prometheusInstantQueryResponse, _ := data.(instantquery.PrometheusInstantQueryResponse)
		prometheusInstantQueryResponse.Status = string(statusMessage)
		b, err = proto.Marshal(&prometheusInstantQueryResponse)
	default:
		level.Error(api.logger).Log("msg", "error asserting response type")
		http.Error(w, "error asserting response type", http.StatusInternalServerError)
		return
	}
	if err != nil {
		level.Error(api.logger).Log("msg", "error marshaling protobuf response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/protobuf")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(api.logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func (api *API) respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		level.Error(api.logger).Log("msg", "error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = http.StatusUnprocessableEntity
	case errorCanceled:
		code = statusClientClosedConnection
	case errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	case errorNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		level.Error(api.logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
}

func createPrometheusResponse(data *queryData) queryrange.PrometheusResponse {
	if data == nil {
		return queryrange.PrometheusResponse{
			Status:    "",
			Data:      queryrange.PrometheusData{},
			ErrorType: "",
			Error:     "",
			Headers:   []*tripperware.PrometheusResponseHeader{},
		}
	}

	sampleStreams := getSampleStreams(data)

	var stats *tripperware.PrometheusResponseStats
	if data.Stats != nil {
		builtin := data.Stats.Builtin()
		stats = &tripperware.PrometheusResponseStats{Samples: getStats(&builtin)}
	}

	return queryrange.PrometheusResponse{
		Status: "",
		Data: queryrange.PrometheusData{
			ResultType: string(data.ResultType),
			Result:     *sampleStreams,
			Stats:      stats,
		},
		ErrorType: "",
		Error:     "",
		Headers:   []*tripperware.PrometheusResponseHeader{},
	}
}

func createPrometheusInstantQueryResponse(data *queryData) instantquery.PrometheusInstantQueryResponse {
	if data == nil {
		return instantquery.PrometheusInstantQueryResponse{
			Status:    "",
			Data:      instantquery.PrometheusInstantQueryData{},
			ErrorType: "",
			Error:     "",
			Headers:   []*tripperware.PrometheusResponseHeader{},
		}
	}

	var instantQueryResult instantquery.PrometheusInstantQueryResult
	switch string(data.ResultType) {
	case "matrix":
		instantQueryResult.Result = &instantquery.PrometheusInstantQueryResult_Matrix{
			Matrix: &instantquery.Matrix{
				SampleStreams: *getSampleStreams(data),
			},
		}
	case "vector":
		instantQueryResult.Result = &instantquery.PrometheusInstantQueryResult_Vector{
			Vector: &instantquery.Vector{
				Samples: *getSamples(data),
			},
		}
	default:
		// TODO: add scalar and string instant query responses
	}

	var stats *tripperware.PrometheusResponseStats
	if data.Stats != nil {
		builtin := data.Stats.Builtin()
		stats = &tripperware.PrometheusResponseStats{Samples: getStats(&builtin)}
	}

	return instantquery.PrometheusInstantQueryResponse{
		Status: "",
		Data: instantquery.PrometheusInstantQueryData{
			ResultType: string(data.ResultType),
			Result:     instantQueryResult,
			Stats:      stats,
		},
		ErrorType: "",
		Error:     "",
		Headers:   []*tripperware.PrometheusResponseHeader{},
	}
}

func getStats(builtin *stats.BuiltinStats) *tripperware.PrometheusResponseSamplesStats {
	queryableSamplesStatsPerStepLen := len(builtin.Samples.TotalQueryableSamplesPerStep)
	queryableSamplesStatsPerStep := make([]*tripperware.PrometheusResponseQueryableSamplesStatsPerStep, queryableSamplesStatsPerStepLen)
	for i := 0; i < queryableSamplesStatsPerStepLen; i++ {
		queryableSamplesStatsPerStep[i] = &tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
			Value:       builtin.Samples.TotalQueryableSamplesPerStep[i].V,
			TimestampMs: builtin.Samples.TotalQueryableSamplesPerStep[i].T,
		}
	}

	statSamples := tripperware.PrometheusResponseSamplesStats{
		TotalQueryableSamples:        builtin.Samples.TotalQueryableSamples,
		TotalQueryableSamplesPerStep: queryableSamplesStatsPerStep,
	}

	return &statSamples
}

func getSampleStreams(data *queryData) *[]tripperware.SampleStream {
	sampleStreamsLen := len(data.Result.(promql.Matrix))
	sampleStreams := make([]tripperware.SampleStream, sampleStreamsLen)

	for i := 0; i < sampleStreamsLen; i++ {
		labelsLen := len(data.Result.(promql.Matrix)[i].Metric)
		labels := make([]github_com_cortexproject_cortex_pkg_cortexpb.LabelAdapter, labelsLen)
		for j := 0; j < labelsLen; j++ {
			labels[j] = github_com_cortexproject_cortex_pkg_cortexpb.LabelAdapter{
				Name:  data.Result.(promql.Matrix)[i].Metric[j].Name,
				Value: data.Result.(promql.Matrix)[i].Metric[j].Value,
			}
		}

		samplesLen := len(data.Result.(promql.Matrix)[i].Floats)
		samples := make([]cortexpb.Sample, samplesLen)
		for j := 0; j < samplesLen; j++ {
			samples[j] = cortexpb.Sample{
				Value:       data.Result.(promql.Matrix)[i].Floats[j].F,
				TimestampMs: data.Result.(promql.Matrix)[i].Floats[j].T,
			}
		}
		sampleStreams[i] = tripperware.SampleStream{Labels: labels, Samples: samples}
	}
	return &sampleStreams
}

func getSamples(data *queryData) *[]*instantquery.Sample {
	vectorSamplesLen := len(data.Result.(promql.Vector))
	vectorSamples := make([]*instantquery.Sample, vectorSamplesLen)

	for i := 0; i < vectorSamplesLen; i++ {
		labelsLen := len(data.Result.(promql.Vector)[i].Metric)
		labels := make([]github_com_cortexproject_cortex_pkg_cortexpb.LabelAdapter, labelsLen)
		for j := 0; j < labelsLen; j++ {
			labels[j] = github_com_cortexproject_cortex_pkg_cortexpb.LabelAdapter{
				Name:  data.Result.(promql.Vector)[i].Metric[j].Name,
				Value: data.Result.(promql.Vector)[i].Metric[j].Value,
			}
		}

		vectorSamples[i] = &instantquery.Sample{Labels: labels,
			Sample: cortexpb.Sample{
				TimestampMs: data.Result.(promql.Vector)[i].T,
				Value:       data.Result.(promql.Vector)[i].F,
			},
		}
	}
	return &vectorSamples
}
