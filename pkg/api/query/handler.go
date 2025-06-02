package query

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	jsoniter "github.com/json-iterator/go"
	"github.com/munnerz/goautoneg"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/httputil"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/cortexproject/cortex/pkg/engine"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util/api"
)

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
	start, err := api.ParseTime(r.FormValue("start"))
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := api.ParseTime(r.FormValue("end"))
	if err != nil {
		return invalidParamError(err, "end")
	}

	if end.Before(start) {
		return invalidParamError(errors.New("end timestamp must not be before start time"), "end")
	}

	step, err := api.ParseDuration(r.FormValue("step"))
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
		timeout, err := api.ParseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	opts, err := api.ExtractQueryOpts(r)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx = engine.AddEngineTypeToContext(ctx, r)
	ctx = querier.AddBlockStoreTypeToContext(ctx, r.Header.Get(querier.BlockStoreTypeHeader))
	qry, err := q.queryEngine.NewRangeQuery(ctx, q.queryable, opts, r.FormValue("query"), start, end, step)
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

	warnings := res.Warnings
	qs := q.statsRenderer(ctx, qry.Stats(), r.FormValue("stats"))

	return apiFuncResult{&v1.QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}, nil, warnings, qry.Close}
}

func (q *QueryAPI) InstantQueryHandler(r *http.Request) (result apiFuncResult) {
	ts, err := api.ParseTimeParam(r, "time", q.now())
	if err != nil {
		return invalidParamError(err, "time")
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := api.ParseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithDeadline(ctx, q.now().Add(timeout))
		defer cancel()
	}

	opts, err := api.ExtractQueryOpts(r)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx = engine.AddEngineTypeToContext(ctx, r)
	ctx = querier.AddBlockStoreTypeToContext(ctx, r.Header.Get(querier.BlockStoreTypeHeader))
	qry, err := q.queryEngine.NewInstantQuery(ctx, q.queryable, opts, r.FormValue("query"), ts)
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
			q.respondError(w, result.err, result.data)
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

func (q *QueryAPI) respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&Response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		level.Error(q.logger).Log("error marshaling json response", "err", err)
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
	case errorNotAcceptable:
		code = http.StatusNotAcceptable
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		level.Error(q.logger).Log("error writing response", "bytesWritten", n, "err", err)
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
		q.respondError(w, &apiError{errorNotAcceptable, err}, nil)
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
