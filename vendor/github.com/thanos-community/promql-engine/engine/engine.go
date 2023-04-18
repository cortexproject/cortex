// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"context"

	promparser "github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"io"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/thanos-community/promql-engine/api"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-community/promql-engine/execution"
	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
	"github.com/thanos-community/promql-engine/logicalplan"
)

type QueryType int

type engineMetrics struct {
	currentQueries prometheus.Gauge
	queries        *prometheus.CounterVec
}

const (
	namespace    string    = "thanos"
	subsystem    string    = "engine"
	InstantQuery QueryType = 1
	RangeQuery   QueryType = 2
)

type Opts struct {
	promql.EngineOpts

	// LogicalOptimizers are optimizers that are run if the value is not nil. If it is nil then the default optimizers are run. Default optimizer list is available in the logicalplan package.
	LogicalOptimizers []logicalplan.Optimizer

	// DisableFallback enables mode where engine returns error if some expression of feature is not yet implemented
	// in the new engine, instead of falling back to prometheus engine.
	DisableFallback bool

	// DebugWriter specifies output for debug (multi-line) information meant for humans debugging the engine.
	// If nil, nothing will be printed.
	// NOTE: Users will not check the errors, debug writing is best effort.
	DebugWriter io.Writer

	// ExtLookbackDelta specifies what time range to use to determine valid previous sample for extended range functions.
	// Defaults to 1 hour if not specified.
	ExtLookbackDelta time.Duration

	// EnableXFunctions enables custom xRate, xIncrease and xDelta functions.
	// This will default to false.
	EnableXFunctions bool

	// FallbackEngine
	Engine v1.QueryEngine
}

func (o Opts) getLogicalOptimizers() []logicalplan.Optimizer {
	var optimizers []logicalplan.Optimizer
	if o.LogicalOptimizers == nil {
		optimizers = logicalplan.DefaultOptimizers
	} else {
		optimizers = o.LogicalOptimizers
	}
	return append(optimizers, logicalplan.TrimSortFunctions{})
}

type remoteEngine struct {
	q         storage.Queryable
	engine    *compatibilityEngine
	labelSets []labels.Labels
	maxt      int64
	mint      int64
}

func NewRemoteEngine(opts Opts, q storage.Queryable, mint, maxt int64, labelSets []labels.Labels) *remoteEngine {
	return &remoteEngine{
		q:         q,
		labelSets: labelSets,
		maxt:      maxt,
		mint:      mint,
		engine:    New(opts),
	}
}

func (l remoteEngine) MaxT() int64 {
	return l.maxt
}

func (l remoteEngine) MinT() int64 {
	return l.mint
}

func (l remoteEngine) LabelSets() []labels.Labels {
	return l.labelSets
}

func (l remoteEngine) NewRangeQuery(opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return l.engine.NewRangeQuery(l.q, opts, qs, start, end, interval)
}

type distributedEngine struct {
	endpoints    api.RemoteEndpoints
	remoteEngine *compatibilityEngine
}

func NewDistributedEngine(opts Opts, endpoints api.RemoteEndpoints) v1.QueryEngine {
	opts.LogicalOptimizers = []logicalplan.Optimizer{
		logicalplan.DistributedExecutionOptimizer{Endpoints: endpoints},
	}

	return &distributedEngine{
		endpoints:    endpoints,
		remoteEngine: New(opts),
	}
}

func (l distributedEngine) SetQueryLogger(log promql.QueryLogger) {}

func (l distributedEngine) NewInstantQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	ts = ts.Truncate(time.Second)

	return l.remoteEngine.NewInstantQuery(q, opts, qs, ts)
}

func (l distributedEngine) NewRangeQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	start = start.Truncate(time.Second)
	end = end.Truncate(time.Second)
	interval = interval.Truncate(time.Second)

	return l.remoteEngine.NewRangeQuery(q, opts, qs, start, end, interval)
}

func New(opts Opts) *compatibilityEngine {
	if opts.Logger == nil {
		opts.Logger = log.NewNopLogger()
	}
	if opts.LookbackDelta == 0 {
		opts.LookbackDelta = 5 * time.Minute
		level.Debug(opts.Logger).Log("msg", "lookback delta is zero, setting to default value", "value", 5*time.Minute)
	}
	if opts.ExtLookbackDelta == 0 {
		opts.ExtLookbackDelta = 1 * time.Hour
		level.Debug(opts.Logger).Log("msg", "externallookback delta is zero, setting to default value", "value", 1*24*time.Hour)
	}

	if opts.EnableXFunctions {
		parser.Functions["xdelta"] = parse.Functions["xdelta"]
		parser.Functions["xincrease"] = parse.Functions["xincrease"]
		parser.Functions["xrate"] = parse.Functions["xrate"]
	}

	metrics := &engineMetrics{
		currentQueries: promauto.With(opts.Reg).NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "queries",
				Help:      "The current number of queries being executed or waiting.",
			},
		),
		queries: promauto.With(opts.Reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "queries_total",
				Help:      "Number of PromQL queries.",
			}, []string{"fallback"},
		),
	}

	var engine v1.QueryEngine
	if opts.Engine == nil {
		engine = promql.NewEngine(opts.EngineOpts)
	} else {
		engine = opts.Engine
	}

	return &compatibilityEngine{
		prom: engine,

		debugWriter:       opts.DebugWriter,
		disableFallback:   opts.DisableFallback,
		logger:            opts.Logger,
		lookbackDelta:     opts.LookbackDelta,
		logicalOptimizers: opts.getLogicalOptimizers(),
		timeout:           opts.Timeout,
		metrics:           metrics,
		extLookbackDelta:  opts.ExtLookbackDelta,
	}
}

type compatibilityEngine struct {
	prom v1.QueryEngine

	debugWriter io.Writer

	disableFallback   bool
	logger            log.Logger
	lookbackDelta     time.Duration
	logicalOptimizers []logicalplan.Optimizer
	timeout           time.Duration
	metrics           *engineMetrics

	extLookbackDelta time.Duration
}

func (e *compatibilityEngine) SetQueryLogger(l promql.QueryLogger) {
	e.prom.SetQueryLogger(l)
}

func (e *compatibilityEngine) NewInstantQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = &promql.QueryOpts{}
	}

	if opts.LookbackDelta <= 0 {
		opts.LookbackDelta = e.lookbackDelta
	}

	// determine sorting order before optimizers run, we do this by looking for "sort"
	// and "sort_desc" and optimize them away afterwards since they are only needed at
	// the presentation layer and not when computing the results.
	resultSort := newResultSort(expr)

	lplan := logicalplan.New(expr, &logicalplan.Opts{
		Start:         ts,
		End:           ts,
		Step:          1,
		LookbackDelta: opts.LookbackDelta,
	})
	lplan = lplan.Optimize(e.logicalOptimizers)

	exec, err := execution.New(lplan.Expr(), q, ts, ts, 0, opts.LookbackDelta, e.extLookbackDelta)
	if e.triggerFallback(err) {
		e.metrics.queries.WithLabelValues("true").Inc()
		return e.prom.NewInstantQuery(q, opts, qs, ts)
	}
	e.metrics.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}

	if e.debugWriter != nil {
		explain(e.debugWriter, exec, "", "")
	}

	return &compatibilityQuery{
		Query:      &Query{exec: exec, opts: opts},
		engine:     e,
		expr:       expr,
		ts:         ts,
		t:          InstantQuery,
		resultSort: resultSort,
	}, nil
}

func (e *compatibilityEngine) NewRangeQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	// Use same check as Prometheus for range queries.
	if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
		return nil, errors.Newf("invalid expression type %q for range query, must be Scalar or instant Vector", parser.DocumentedType(expr.Type()))
	}

	if opts == nil {
		opts = &promql.QueryOpts{}
	}

	if opts.LookbackDelta <= 0 {
		opts.LookbackDelta = e.lookbackDelta
	}

	lplan := logicalplan.New(expr, &logicalplan.Opts{
		Start:         start,
		End:           end,
		Step:          step,
		LookbackDelta: opts.LookbackDelta,
	})
	lplan = lplan.Optimize(e.logicalOptimizers)

	exec, err := execution.New(lplan.Expr(), q, start, end, step, opts.LookbackDelta, e.extLookbackDelta)
	if e.triggerFallback(err) {
		e.metrics.queries.WithLabelValues("true").Inc()
		return e.prom.NewRangeQuery(q, opts, qs, start, end, step)
	}
	e.metrics.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}

	if e.debugWriter != nil {
		explain(e.debugWriter, exec, "", "")
	}

	return &compatibilityQuery{
		Query:  &Query{exec: exec, opts: opts},
		engine: e,
		expr:   expr,
		t:      RangeQuery,
	}, nil
}

type Query struct {
	exec model.VectorOperator
	opts *promql.QueryOpts
}

// Explain returns human-readable explanation of the created executor.
func (q *Query) Explain() string {
	// TODO(bwplotka): Explain plan and steps.
	return "not implemented"
}

func (q *Query) Profile() {
	// TODO(bwplotka): Return profile.
}

type sortOrder bool

const (
	sortOrderAsc  sortOrder = false
	sortOrderDesc sortOrder = true
)

type resultSorter interface {
	comparer(samples *promql.Vector) func(i, j int) bool
}

type sortFuncResultSort struct {
	sortOrder sortOrder
}

type aggregateResultSort struct {
	sortingLabels []string
	groupBy       bool

	sortOrder sortOrder
}

type noSortResultSort struct {
}

func newResultSort(expr parser.Expr) resultSorter {
	switch texpr := expr.(type) {
	case *parser.Call:
		switch texpr.Func.Name {
		case "sort":
			return sortFuncResultSort{sortOrder: sortOrderAsc}
		case "sort_desc":
			return sortFuncResultSort{sortOrder: sortOrderDesc}
		}
	case *parser.AggregateExpr:
		switch texpr.Op {
		case parser.TOPK:
			return aggregateResultSort{
				sortingLabels: texpr.Grouping,
				sortOrder:     sortOrderDesc,
				groupBy:       !texpr.Without,
			}
		case parser.BOTTOMK:
			return aggregateResultSort{
				sortingLabels: texpr.Grouping,
				sortOrder:     sortOrderAsc,
				groupBy:       !texpr.Without,
			}
		}
	}
	return noSortResultSort{}
}
func (s noSortResultSort) comparer(samples *promql.Vector) func(i, j int) bool {
	return func(i, j int) bool { return i < j }
}

func valueCompare(order sortOrder, l, r float64) bool {
	if math.IsNaN(r) {
		return true
	}
	if order == sortOrderAsc {
		return l < r
	}
	return l > r
}

func (s sortFuncResultSort) comparer(samples *promql.Vector) func(i, j int) bool {
	return func(i, j int) bool {
		return valueCompare(s.sortOrder, (*samples)[i].F, (*samples)[j].F)
	}
}

func (s aggregateResultSort) comparer(samples *promql.Vector) func(i, j int) bool {
	return func(i int, j int) bool {
		var iLbls labels.Labels
		var jLbls labels.Labels
		iLb := labels.NewBuilder((*samples)[i].Metric)
		jLb := labels.NewBuilder((*samples)[j].Metric)
		if s.groupBy {
			iLbls = iLb.Keep(s.sortingLabels...).Labels()
			jLbls = jLb.Keep(s.sortingLabels...).Labels()
		} else {
			iLbls = iLb.Del(s.sortingLabels...).Labels()
			jLbls = jLb.Del(s.sortingLabels...).Labels()
		}

		lblsCmp := labels.Compare(iLbls, jLbls)
		if lblsCmp != 0 {
			return lblsCmp < 0
		}
		return valueCompare(s.sortOrder, (*samples)[i].F, (*samples)[j].F)
	}
}

type compatibilityQuery struct {
	*Query
	engine     *compatibilityEngine
	expr       parser.Expr
	ts         time.Time // Empty for range queries.
	t          QueryType
	resultSort resultSorter

	cancel context.CancelFunc
}

func (q *compatibilityQuery) Exec(ctx context.Context) (ret *promql.Result) {
	// Handle case with strings early on as this does not need us to process samples.
	// TODO(saswatamcode): Modify models.StepVector to support all types and check during executor creation.
	ret = &promql.Result{
		Value: promql.Vector{},
	}
	defer recoverEngine(q.engine.logger, q.expr, &ret.Err)

	q.engine.metrics.currentQueries.Inc()
	defer q.engine.metrics.currentQueries.Dec()

	ctx, cancel := context.WithTimeout(ctx, q.engine.timeout)
	defer cancel()
	q.cancel = cancel

	resultSeries, err := q.Query.exec.Series(ctx)
	if err != nil {
		return newErrResult(ret, err)
	}
	if containsDuplicateLabelSet(resultSeries) {
		return newErrResult(ret, errors.New("vector cannot contain metrics with the same labelset"))
	}

	series := make([]promql.Series, len(resultSeries))
	for i := 0; i < len(resultSeries); i++ {
		series[i].Metric = resultSeries[i]
	}
loop:
	for {
		select {
		case <-ctx.Done():
			return newErrResult(ret, ctx.Err())
		default:
			r, err := q.Query.exec.Next(ctx)
			if err != nil {
				return newErrResult(ret, err)
			}
			if r == nil {
				break loop
			}

			// Case where Series call might return nil, but samples are present.
			// For example scalar(http_request_total) where http_request_total has multiple values.
			if len(series) == 0 && len(r) != 0 {
				series = make([]promql.Series, len(r[0].Samples))
			}

			for _, vector := range r {
				for i, s := range vector.SampleIDs {
					if len(series[s].Floats) == 0 {
						series[s].Floats = make([]promql.FPoint, 0, 121) // Typically 1h of data.
					}
					series[s].Floats = append(series[s].Floats, promql.FPoint{
						T: vector.T,
						F: vector.Samples[i],
					})
				}
				for i, s := range vector.HistogramIDs {
					if len(series[s].Histograms) == 0 {
						series[s].Histograms = make([]promql.HPoint, 0, 121) // Typically 1h of data.
					}
					series[s].Histograms = append(series[s].Histograms, promql.HPoint{
						T: vector.T,
						H: vector.Histograms[i],
					})
				}
				q.Query.exec.GetPool().PutStepVector(vector)
			}
			q.Query.exec.GetPool().PutVectors(r)
		}
	}

	// For range Query we expect always a Matrix value type.
	if q.t == RangeQuery {
		resultMatrix := make(promql.Matrix, 0, len(series))
		for _, s := range series {
			if len(s.Floats)+len(s.Histograms) == 0 {
				continue
			}
			resultMatrix = append(resultMatrix, s)
		}
		sort.Sort(resultMatrix)
		ret.Value = resultMatrix
		return ret
	}

	var result promparser.Value
	switch q.expr.Type() {
	case parser.ValueTypeMatrix:
		result = promql.Matrix(series)
	case parser.ValueTypeVector:
		// Convert matrix with one value per series into vector.
		vector := make(promql.Vector, 0, len(resultSeries))
		for i := range series {
			if len(series[i].Floats)+len(series[i].Histograms) == 0 {
				continue
			}
			// Point might have a different timestamp, force it to the evaluation
			// timestamp as that is when we ran the evaluation.
			if len(series[i].Floats) > 0 {
				vector = append(vector, promql.Sample{
					Metric: series[i].Metric,
					F:      series[i].Floats[0].F,
					T:      q.ts.UnixMilli(),
				})
			} else {
				vector = append(vector, promql.Sample{
					Metric: series[i].Metric,
					H:      series[i].Histograms[0].H,
					T:      q.ts.UnixMilli(),
				})
			}
		}
		sort.Slice(vector, q.resultSort.comparer(&vector))
		result = vector
	case parser.ValueTypeScalar:
		v := math.NaN()
		if len(series) != 0 {
			v = series[0].Floats[0].F
		}
		result = promql.Scalar{V: v, T: q.ts.UnixMilli()}
	default:
		panic(errors.Newf("new.Engine.exec: unexpected expression type %q", q.expr.Type()))
	}

	ret.Value = result
	return ret
}

func newErrResult(r *promql.Result, err error) *promql.Result {
	if r == nil {
		r = &promql.Result{}
	}
	if r.Err == nil && err != nil {
		r.Err = err
	}
	return r
}

func containsDuplicateLabelSet(series []labels.Labels) bool {
	if len(series) <= 1 {
		return false
	}
	var h uint64
	buf := make([]byte, 0)
	seen := make(map[uint64]struct{}, len(series))
	for i := range series {
		buf = buf[:0]
		h = xxhash.Sum64(series[i].Bytes(buf))
		if _, ok := seen[h]; ok {
			return true
		}
		seen[h] = struct{}{}
	}
	return false
}

func (q *compatibilityQuery) Statement() promparser.Statement { return nil }

// Stats always returns empty query stats for now to avoid panic.
func (q *compatibilityQuery) Stats() *stats.Statistics {
	var enablePerStepStats bool
	if q.opts != nil {
		enablePerStepStats = q.opts.EnablePerStepStats
	}
	return &stats.Statistics{Timers: stats.NewQueryTimers(), Samples: stats.NewQuerySamples(enablePerStepStats)}
}

func (q *compatibilityQuery) Close() { q.Cancel() }

func (q *compatibilityQuery) String() string { return q.expr.String() }

func (q *compatibilityQuery) Cancel() {
	if q.cancel != nil {
		q.cancel()
		q.cancel = nil
	}
}

func (e *compatibilityEngine) triggerFallback(err error) bool {
	if e.disableFallback {
		return false
	}

	return errors.Is(err, parse.ErrNotSupportedExpr) || errors.Is(err, parse.ErrNotImplemented)
}

func recoverEngine(logger log.Logger, expr parser.Expr, errp *error) {
	e := recover()
	if e == nil {
		return
	}

	switch err := e.(type) {
	case runtime.Error:
		// Print the stack trace but do not inhibit the running application.
		buf := make([]byte, 64<<10)
		buf = buf[:runtime.Stack(buf, false)]

		level.Error(logger).Log("msg", "runtime panic in engine", "expr", expr.String(), "err", e, "stacktrace", string(buf))
		*errp = errors.Wrap(err, "unexpected error")
	}
}

func explain(w io.Writer, o model.VectorOperator, indent, indentNext string) {
	me, next := o.Explain()
	_, _ = w.Write([]byte(indent))
	_, _ = w.Write([]byte(me))
	if len(next) == 0 {
		_, _ = w.Write([]byte("\n"))
		return
	}

	if me == "[*CancellableOperator]" {
		_, _ = w.Write([]byte(": "))
		explain(w, next[0], "", indentNext)
		return
	}
	_, _ = w.Write([]byte(":\n"))

	for i, n := range next {
		if i == len(next)-1 {
			explain(w, n, indentNext+"└──", indentNext+"   ")
		} else {
			explain(w, n, indentNext+"├──", indentNext+"│  ")
		}
	}
}
