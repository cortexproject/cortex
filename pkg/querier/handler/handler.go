package handler

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/instantquery"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/regexp"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"math"
	"net/http"
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

type ApiFuncResult struct {
	Data      interface{}
	Err       *apiError
	Warnings  storage.Warnings
	Finalizer func()
}

type ApiFunc func(r *http.Request) ApiFuncResult

type API struct {
	Queryable     storage.SampleAndChunkQueryable
	QueryEngine   v1.QueryEngine
	Now           func() time.Time
	Ready         func(http.HandlerFunc) http.HandlerFunc
	Logger        log.Logger
	CORSOrigin    *regexp.Regexp
	IsAgent       bool
	StatsRenderer v1.StatsRenderer
}

// NewAPI returns an initialized API type.
func NewAPI(
	qe v1.QueryEngine,
	q storage.SampleAndChunkQueryable,
	readyFunc func(http.HandlerFunc) http.HandlerFunc,
	logger log.Logger,
	isAgent bool,
	corsOrigin *regexp.Regexp,
	statsRenderer v1.StatsRenderer,
) *API {
	a := &API{
		QueryEngine:   qe,
		Queryable:     q,
		Now:           time.Now,
		Ready:         readyFunc,
		Logger:        logger,
		CORSOrigin:    corsOrigin,
		IsAgent:       isAgent,
		StatsRenderer: defaultStatsRenderer,
	}

	if statsRenderer != nil {
		a.StatsRenderer = statsRenderer
	}

	return a
}

func SetUnavailStatusOnTSDBNotReady(r ApiFuncResult) ApiFuncResult {
	if r.Err != nil && errors.Cause(r.Err.err) == tsdb.ErrNotReady {
		r.Err.typ = errorUnavailable
	}
	return r
}

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
	Stats      stats.QueryStats `json:"stats,omitempty"`
}

func invalidParamError(err error, parameter string) ApiFuncResult {
	return ApiFuncResult{nil, &apiError{
		errorBadData, errors.Wrapf(err, "invalid parameter %q", parameter),
	}, nil, nil}
}

func (api *API) Query(r *http.Request) (result ApiFuncResult) {
	tms, err := instantquery.ParseTimeParam(r, "time", api.Now().Unix())
	ts := time.Unix(tms/1000, (tms%1000)*10e6)
	if err != nil {
		return invalidParamError(err, "time")
	}
	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := time.ParseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	opts, err := extractQueryOpts(r)
	if err != nil {
		return ApiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	qry, err := api.QueryEngine.NewInstantQuery(ctx, api.Queryable, opts, r.FormValue("query"), ts)
	if err != nil {
		return invalidParamError(err, "query")
	}

	// From now on, we must only return with a Finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.Finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return ApiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	sr := api.StatsRenderer
	if sr == nil {
		sr = defaultStatsRenderer
	}
	qs := sr(ctx, qry.Stats(), r.FormValue("stats"))

	return ApiFuncResult{createPrometheusInstantQueryResponse(&queryData{
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
		duration, err := time.ParseDuration(strDuration)
		if err != nil {
			return nil, fmt.Errorf("error parsing lookback delta duration: %w", err)
		}
		opts.LookbackDelta = duration
	}
	return opts, nil
}

func (api *API) QueryRange(r *http.Request) (result ApiFuncResult) {
	startMs, err := util.ParseTime(r.FormValue("start"))
	start := time.Unix(startMs/1000, (startMs%1000)*10e6)
	if err != nil {
		return invalidParamError(err, "start")
	}
	endMs, err := util.ParseTime(r.FormValue("end"))
	end := time.Unix(endMs/1000, (endMs%1000)*10e6)
	if err != nil {
		return invalidParamError(err, "end")
	}
	if end.Before(start) {
		return invalidParamError(errors.New("end timestamp must not be before start time"), "end")
	}

	step, err := time.ParseDuration(r.FormValue("step"))
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
		return ApiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := time.ParseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	opts, err := extractQueryOpts(r)
	if err != nil {
		return ApiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	qry, err := api.QueryEngine.NewRangeQuery(ctx, api.Queryable, opts, r.FormValue("query"), start, end, step)
	if err != nil {
		return invalidParamError(err, "query")
	}
	// From now on, we must only return with a Finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.Finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return ApiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	sr := api.StatsRenderer
	if sr == nil {
		sr = defaultStatsRenderer
	}
	qs := sr(ctx, qry.Stats(), r.FormValue("stats"))

	return ApiFuncResult{createPrometheusResponse(&queryData{
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

func (api *API) Respond(w http.ResponseWriter, data interface{}, warnings storage.Warnings) {
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
		level.Error(api.Logger).Log("msg", "error asserting response type")
		http.Error(w, "error asserting response type", http.StatusInternalServerError)
		return
	}
	if err != nil {
		level.Error(api.Logger).Log("msg", "error marshaling protobuf response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/protobuf")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(api.Logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func (api *API) RespondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		level.Error(api.Logger).Log("msg", "error marshaling json response", "err", err)
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
		level.Error(api.Logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
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
		rawBytes, err := jsoniter.Marshal(data)
		if err != nil {
			// TODO: handler error
		}
		instantQueryResult.Result = &instantquery.PrometheusInstantQueryResult_RawBytes{RawBytes: rawBytes}
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
