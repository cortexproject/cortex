package queryapi

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"net/http"
	"time"

	"github.com/cortexproject/cortex/pkg/util/analysis"
	thanosengine "github.com/thanos-io/promql-engine/engine"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	"github.com/munnerz/goautoneg"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/httputil"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/engine"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/api"
)

type QueryData struct {
	ResultType parser.ValueType         `json:"resultType"`
	Result     parser.Value             `json:"result"`
	Stats      stats.QueryStats         `json:"stats,omitempty"`
	Analysis   *analysis.QueryTelemetry `json:"analysis,omitempty"`
}

type QueryAPI struct {
	queryable     storage.SampleAndChunkQueryable
	queryEngine   promql.QueryEngine
	now           func() time.Time
	statsRenderer v1.StatsRenderer
	logger        log.Logger
	codecs        []v1.Codec
	CORSOrigin    *regexp.Regexp
}

func NewQueryAPI(
	qe promql.QueryEngine,
	q storage.SampleAndChunkQueryable,
	statsRenderer v1.StatsRenderer,
	logger log.Logger,
	codecs []v1.Codec,
	CORSOrigin *regexp.Regexp,
) *QueryAPI {
	return &QueryAPI{
		queryEngine:   qe,
		queryable:     q,
		statsRenderer: statsRenderer,
		logger:        logger,
		codecs:        codecs,
		CORSOrigin:    CORSOrigin,
		now:           time.Now,
	}
}

func (q *QueryAPI) RangeQueryHandler(r *http.Request) (result apiFuncResult) {
	// TODO(Sungjin1212): Change to emit basic error (not gRPC)
	start, err := util.ParseTime(r.FormValue("start"))
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := util.ParseTime(r.FormValue("end"))
	if err != nil {
		return invalidParamError(err, "end")
	}
	if end < start {
		return invalidParamError(ErrEndBeforeStart, "end")
	}

	step, err := util.ParseDurationMs(r.FormValue("step"))
	if err != nil {
		return invalidParamError(err, "step")
	}

	if step <= 0 {
		return invalidParamError(ErrNegativeStep, "step")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (end-start)/step > 11000 {
		return apiFuncResult{nil, &apiError{errorBadData, ErrStepTooSmall}, nil, nil}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := util.ParseDurationMs(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, convertMsToDuration(timeout))
		defer cancel()
	}

	opts, err := extractQueryOpts(r)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx = engine.AddEngineTypeToContext(ctx, r)
	qry, err := q.queryEngine.NewRangeQuery(ctx, q.queryable, opts, r.FormValue("query"), convertMsToTime(start), convertMsToTime(end), convertMsToDuration(step))
	if err != nil {
		return invalidParamError(httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error()), "query")
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
	var queryAnalysis analysis.QueryTelemetry
	if q.parseQueryAnalyzeParam(r) {
		engineType := engine.GetEngineType(ctx)
		queryAnalysis, err = analyzeQueryOutput(qry, engineType)
	}

	warnings := res.Warnings
	qs := q.statsRenderer(ctx, qry.Stats(), r.FormValue("stats"))

	return apiFuncResult{&QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
		Analysis:   &queryAnalysis,
	}, nil, warnings, qry.Close}
}

func (q *QueryAPI) InstantQueryHandler(r *http.Request) (result apiFuncResult) {
	// TODO(Sungjin1212): Change to emit basic error (not gRPC)
	ts, err := util.ParseTimeParam(r, "time", q.now().Unix())
	if err != nil {
		return invalidParamError(err, "time")
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := util.ParseDurationMs(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithDeadline(ctx, q.now().Add(convertMsToDuration(timeout)))
		defer cancel()
	}

	opts, err := extractQueryOpts(r)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx = engine.AddEngineTypeToContext(ctx, r)
	qry, err := q.queryEngine.NewInstantQuery(ctx, q.queryable, opts, r.FormValue("query"), convertMsToTime(ts))
	if err != nil {
		return invalidParamError(httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error()), "query")
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
	var queryAnalysis analysis.QueryTelemetry
	if q.parseQueryAnalyzeParam(r) {
		engineType := engine.GetEngineType(ctx)
		queryAnalysis, err = analyzeQueryOutput(qry, engineType)
	}

	warnings := res.Warnings
	qs := q.statsRenderer(ctx, qry.Stats(), r.FormValue("stats"))

	return apiFuncResult{&QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
		Analysis:   &queryAnalysis,
	}, nil, warnings, qry.Close}
}

func (q *QueryAPI) Wrap(f apiFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httputil.SetCORS(w, q.CORSOrigin, r)

		result := f(r)
		if result.finalizer != nil {
			defer result.finalizer()
		}

		if result.err != nil {
			api.RespondFromGRPCError(q.logger, w, result.err.err)
			return
		}

		if result.data != nil {
			q.respond(w, r, result.data, result.warnings, r.FormValue("query"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (q *QueryAPI) respond(w http.ResponseWriter, req *http.Request, data interface{}, warnings annotations.Annotations, query string) {
	warn, info := warnings.AsStrings(query, 10, 10)

	resp := &v1.Response{
		Status:   statusSuccess,
		Data:     data,
		Warnings: warn,
		Infos:    info,
	}

	codec, err := q.negotiateCodec(req, resp)
	if err != nil {
		api.RespondFromGRPCError(q.logger, w, httpgrpc.Errorf(http.StatusNotAcceptable, "%s", &apiError{errorNotAcceptable, err}))
		return
	}

	b, err := codec.Encode(resp)
	if err != nil {
		level.Error(q.logger).Log("error marshaling response", "url", req.URL, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", codec.ContentType().String())
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(q.logger).Log("error writing response", "url", req.URL, "bytesWritten", n, "err", err)
	}
}

func (q *QueryAPI) negotiateCodec(req *http.Request, resp *v1.Response) (v1.Codec, error) {
	for _, clause := range goautoneg.ParseAccept(req.Header.Get("Accept")) {
		for _, codec := range q.codecs {
			if codec.ContentType().Satisfies(clause) && codec.CanEncode(resp) {
				return codec, nil
			}
		}
	}

	defaultCodec := q.codecs[0]
	if !defaultCodec.CanEncode(resp) {
		return nil, fmt.Errorf("cannot encode response as %s", defaultCodec.ContentType())
	}

	return defaultCodec, nil
}

func (q *QueryAPI) parseQueryAnalyzeParam(r *http.Request) bool {
	return r.FormValue("analyze") == "true"
}

func analyzeQueryOutput(query promql.Query, engineType engine.Type) (analysis.QueryTelemetry, error) {
	if eq, ok := query.(thanosengine.ExplainableQuery); ok {
		if analyze := eq.Analyze(); analyze != nil {
			return processAnalysis(analyze), nil
		} else {
			return analysis.QueryTelemetry{}, errors.Errorf("Query: %v not analyzable", query)
		}
	}

	var warning error
	if engineType == engine.Thanos {
		warning = errors.New("Query fallback to prometheus engine; not analyzable.")
	} else {
		warning = errors.New("Query not analyzable; change engine to 'thanos'.")
	}

	return analysis.QueryTelemetry{}, warning
}

func processAnalysis(a *thanosengine.AnalyzeOutputNode) analysis.QueryTelemetry {
	var analysis analysis.QueryTelemetry
	analysis.OperatorName = a.OperatorTelemetry.String()
	analysis.Execution = a.OperatorTelemetry.ExecutionTimeTaken().String()
	analysis.PeakSamples = a.PeakSamples()
	analysis.TotalSamples = a.TotalSamples()
	for _, c := range a.Children {
		analysis.Children = append(analysis.Children, processAnalysis(c))
	}
	return analysis
}
