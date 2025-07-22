package queryapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	"github.com/munnerz/goautoneg"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/httputil"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/engine"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/api"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type QueryAPI struct {
	queryable     storage.SampleAndChunkQueryable
	queryEngine   engine.Engine
	now           func() time.Time
	statsRenderer v1.StatsRenderer
	logger        log.Logger
	codecs        []v1.Codec
	CORSOrigin    *regexp.Regexp
}

func NewQueryAPI(
	qe engine.Engine,
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
	ctx = querier.AddBlockStoreTypeToContext(ctx, r.Header.Get(querier.BlockStoreTypeHeader))

	var qry promql.Query
	byteLP, err := io.ReadAll(r.Body)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	if len(byteLP) != 0 {
		logicalPlan, err := logicalplan.Unmarshal(byteLP)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
		qry, err = q.queryEngine.MakeRangeQueryFromPlan(ctx, q.queryable, opts, logicalPlan, convertMsToTime(start), convertMsToTime(end), convertMsToDuration(step), r.FormValue("query"))
		if err != nil {
			return invalidParamError(httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error()), "query")
		}
	} else {
		qry, err = q.queryEngine.NewRangeQuery(ctx, q.queryable, opts, r.FormValue("query"), convertMsToTime(start), convertMsToTime(end), convertMsToDuration(step))
		if err != nil {
			return invalidParamError(httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error()), "query")
		}
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

	warnings := res.Warnings
	qs := q.statsRenderer(ctx, qry.Stats(), r.FormValue("stats"))

	return apiFuncResult{&v1.QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
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
	ctx = querier.AddBlockStoreTypeToContext(ctx, r.Header.Get(querier.BlockStoreTypeHeader))

	var qry promql.Query
	byteLP, err := io.ReadAll(r.Body)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	if len(byteLP) != 0 {
		logicalPlan, err := logicalplan.Unmarshal(byteLP)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
		qry, err = q.queryEngine.MakeInstantQueryFromPlan(ctx, q.queryable, opts, logicalPlan, convertMsToTime(ts), r.FormValue("query"))
		if err != nil {
			return invalidParamError(httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error()), "query")
		}
	} else {
		qry, err = q.queryEngine.NewInstantQuery(ctx, q.queryable, opts, r.FormValue("query"), convertMsToTime(ts))
		if err != nil {
			return invalidParamError(httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error()), "query")
		}
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

	warnings := res.Warnings
	qs := q.statsRenderer(ctx, qry.Stats(), r.FormValue("stats"))

	return apiFuncResult{&v1.QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}, nil, warnings, qry.Close}
}

func (q *QueryAPI) Wrap(f apiFunc) http.HandlerFunc {
	hf := func(w http.ResponseWriter, r *http.Request) {
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

	return httputil.CompressionHandler{
		Handler: http.HandlerFunc(hf),
	}.ServeHTTP
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
