// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"context"

	"io"
	"math"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/execution"
	"github.com/thanos-io/promql-engine/execution/function"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/execution/warnings"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
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
	stepsBatch             = 10
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

	// EnableSubqueries enables the engine to handle subqueries without falling back to prometheus.
	// This will default to false.
	EnableSubqueries bool

	// FallbackEngine
	Engine v1.QueryEngine

	// EnableAnalysis enables query analysis.
	EnableAnalysis bool

	// SelectorBatchSize specifies the maximum number of samples to be returned by selectors in a single batch.
	SelectorBatchSize int64
}

func (o Opts) getLogicalOptimizers() []logicalplan.Optimizer {
	var optimizers []logicalplan.Optimizer
	if o.LogicalOptimizers == nil {
		optimizers = make([]logicalplan.Optimizer, len(logicalplan.DefaultOptimizers))
		copy(optimizers, logicalplan.DefaultOptimizers)
	} else {
		optimizers = make([]logicalplan.Optimizer, len(o.LogicalOptimizers))
		copy(optimizers, o.LogicalOptimizers)
	}
	return optimizers
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

func (l remoteEngine) NewRangeQuery(ctx context.Context, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return l.engine.NewRangeQuery(ctx, l.q, opts, qs, start, end, interval)
}

type distributedEngine struct {
	endpoints    api.RemoteEndpoints
	remoteEngine *compatibilityEngine
}

func NewDistributedEngine(opts Opts, endpoints api.RemoteEndpoints) v1.QueryEngine {
	opts.LogicalOptimizers = []logicalplan.Optimizer{
		logicalplan.PassthroughOptimizer{Endpoints: endpoints},
		logicalplan.DistributeAvgOptimizer{},
		logicalplan.DistributedExecutionOptimizer{Endpoints: endpoints},
	}

	return &distributedEngine{
		endpoints:    endpoints,
		remoteEngine: New(opts),
	}
}

func (l distributedEngine) SetQueryLogger(log promql.QueryLogger) {}

func (l distributedEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	ts = ts.Truncate(time.Second)

	return l.remoteEngine.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (l distributedEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	start = start.Truncate(time.Second)
	end = end.Truncate(time.Second)
	interval = interval.Truncate(time.Second)

	return l.remoteEngine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
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
	if opts.SelectorBatchSize != 0 {
		opts.LogicalOptimizers = append(
			[]logicalplan.Optimizer{logicalplan.SelectorBatchSize{Size: opts.SelectorBatchSize}},
			opts.LogicalOptimizers...,
		)
	}

	functions := make(map[string]*parser.Function, len(parser.Functions))
	for k, v := range parser.Functions {
		functions[k] = v
	}
	if opts.EnableXFunctions {
		functions["xdelta"] = function.XFunctions["xdelta"]
		functions["xincrease"] = function.XFunctions["xincrease"]
		functions["xrate"] = function.XFunctions["xrate"]
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
		prom:      engine,
		functions: functions,

		debugWriter:       opts.DebugWriter,
		disableFallback:   opts.DisableFallback,
		logger:            opts.Logger,
		lookbackDelta:     opts.LookbackDelta,
		logicalOptimizers: opts.getLogicalOptimizers(),
		timeout:           opts.Timeout,
		metrics:           metrics,
		extLookbackDelta:  opts.ExtLookbackDelta,
		enableAnalysis:    opts.EnableAnalysis,
		enableSubqueries:  opts.EnableSubqueries,
		noStepSubqueryIntervalFn: func(d time.Duration) time.Duration {
			return time.Duration(opts.NoStepSubqueryIntervalFn(d.Milliseconds()) * 1000000)
		},
	}
}

type compatibilityEngine struct {
	prom      v1.QueryEngine
	functions map[string]*parser.Function

	debugWriter io.Writer

	disableFallback   bool
	logger            log.Logger
	lookbackDelta     time.Duration
	logicalOptimizers []logicalplan.Optimizer
	timeout           time.Duration
	metrics           *engineMetrics

	extLookbackDelta         time.Duration
	enableAnalysis           bool
	enableSubqueries         bool
	noStepSubqueryIntervalFn func(time.Duration) time.Duration
}

func (e *compatibilityEngine) SetQueryLogger(l promql.QueryLogger) {
	e.prom.SetQueryLogger(l)
}

func (e *compatibilityEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	expr, err := parser.NewParser(qs, parser.WithFunctions(e.functions)).ParseExpr()
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, e.lookbackDelta)
	}
	if opts.LookbackDelta() <= 0 {
		opts = promql.NewPrometheusQueryOpts(opts.EnablePerStepStats(), e.lookbackDelta)
	}

	// determine sorting order before optimizers run, we do this by looking for "sort"
	// and "sort_desc" and optimize them away afterwards since they are only needed at
	// the presentation layer and not when computing the results.
	resultSort := newResultSort(expr)

	qOpts := &query.Options{
		Context:                  ctx,
		Start:                    ts,
		End:                      ts,
		Step:                     0,
		StepsBatch:               stepsBatch,
		LookbackDelta:            opts.LookbackDelta(),
		ExtLookbackDelta:         e.extLookbackDelta,
		EnableAnalysis:           e.enableAnalysis,
		EnableSubqueries:         e.enableSubqueries,
		NoStepSubqueryIntervalFn: e.noStepSubqueryIntervalFn,
	}

	lplan := logicalplan.New(expr, qOpts).Optimize(e.logicalOptimizers)
	exec, err := execution.New(lplan.Expr(), q, qOpts)
	if e.triggerFallback(err) {
		e.metrics.queries.WithLabelValues("true").Inc()
		return e.prom.NewInstantQuery(ctx, q, opts, qs, ts)
	}
	e.metrics.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}

	if e.debugWriter != nil {
		explain(e.debugWriter, exec, "", "")
	}

	return &compatibilityQuery{
		Query:       &Query{exec: exec, opts: opts},
		engine:      e,
		expr:        expr,
		ts:          ts,
		t:           InstantQuery,
		resultSort:  resultSort,
		debugWriter: e.debugWriter,
	}, nil
}

func (e *compatibilityEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	expr, err := parser.NewParser(qs, parser.WithFunctions(e.functions)).ParseExpr()
	if err != nil {
		return nil, err
	}

	// Use same check as Prometheus for range queries.
	if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
		return nil, errors.Newf("invalid expression type %q for range query, must be Scalar or instant Vector", parser.DocumentedType(expr.Type()))
	}

	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, e.lookbackDelta)
	}
	if opts.LookbackDelta() <= 0 {
		opts = promql.NewPrometheusQueryOpts(opts.EnablePerStepStats(), e.lookbackDelta)
	}

	qOpts := &query.Options{
		Context:                  ctx,
		Start:                    start,
		End:                      end,
		Step:                     step,
		StepsBatch:               stepsBatch,
		LookbackDelta:            opts.LookbackDelta(),
		ExtLookbackDelta:         e.extLookbackDelta,
		EnableAnalysis:           e.enableAnalysis,
		EnableSubqueries:         false, // not yet implemented for range queries.
		NoStepSubqueryIntervalFn: e.noStepSubqueryIntervalFn,
	}

	lplan := logicalplan.New(expr, qOpts).Optimize(e.logicalOptimizers)
	exec, err := execution.New(lplan.Expr(), q, qOpts)
	if e.triggerFallback(err) {
		e.metrics.queries.WithLabelValues("true").Inc()
		return e.prom.NewRangeQuery(ctx, q, opts, qs, start, end, step)
	}
	e.metrics.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}

	if e.debugWriter != nil {
		explain(e.debugWriter, exec, "", "")
	}

	return &compatibilityQuery{
		Query:       &Query{exec: exec, opts: opts},
		engine:      e,
		expr:        expr,
		t:           RangeQuery,
		debugWriter: e.debugWriter,
	}, nil
}

type ExplainableQuery interface {
	promql.Query

	Explain() *ExplainOutputNode
	Analyze() *AnalyzeOutputNode
}

type AnalyzeOutputNode struct {
	OperatorTelemetry model.OperatorTelemetry `json:"telemetry,omitempty"`
	Children          []AnalyzeOutputNode     `json:"children,omitempty"`
}

type ExplainOutputNode struct {
	OperatorName string              `json:"name,omitempty"`
	Children     []ExplainOutputNode `json:"children,omitempty"`
}

var _ ExplainableQuery = &compatibilityQuery{}

type Query struct {
	exec model.VectorOperator
	opts promql.QueryOpts
}

// Explain returns human-readable explanation of the created executor.
func (q *Query) Explain() *ExplainOutputNode {
	// TODO(bwplotka): Explain plan and steps.
	return explainVector(q.exec)
}

func (q *Query) Analyze() *AnalyzeOutputNode {
	if observableRoot, ok := q.exec.(model.ObservableVectorOperator); ok {
		return analyzeVector(observableRoot)
	}
	return nil
}

func analyzeVector(obsv model.ObservableVectorOperator) *AnalyzeOutputNode {
	telemetry, obsVectors := obsv.Analyze()

	var children []AnalyzeOutputNode
	for _, vector := range obsVectors {
		children = append(children, *analyzeVector(vector))
	}

	return &AnalyzeOutputNode{
		OperatorTelemetry: telemetry,
		Children:          children,
	}
}

func explainVector(v model.VectorOperator) *ExplainOutputNode {
	name, vectors := v.Explain()

	var children []ExplainOutputNode
	for _, vector := range vectors {
		children = append(children, *explainVector(vector))
	}

	return &ExplainOutputNode{
		OperatorName: name,
		Children:     children,
	}
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

	debugWriter io.Writer
}

func (q *compatibilityQuery) Exec(ctx context.Context) (ret *promql.Result) {
	ctx = warnings.NewContext(ctx)
	defer func() {
		if warns := warnings.FromContext(ctx); len(warns) > 0 {
			ret.Warnings = warns
		}
	}()

	// Handle case with strings early on as this does not need us to process samples.
	switch e := q.expr.(type) {
	case *parser.StringLiteral:
		if q.debugWriter != nil {
			analyze(q.debugWriter, q.exec.(model.ObservableVectorOperator), " ", "")
		}
		return &promql.Result{Value: promql.String{V: e.Val, T: q.ts.UnixMilli()}}
	}
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
		if resultMatrix.ContainsSameLabelset() {
			return newErrResult(ret, extlabels.ErrDuplicateLabelSet)
		}
		ret.Value = resultMatrix
		if q.debugWriter != nil {
			analyze(q.debugWriter, q.exec.(model.ObservableVectorOperator), "", "")
		}
		return ret
	}

	var result parser.Value
	switch q.expr.Type() {
	case parser.ValueTypeMatrix:
		matrix := promql.Matrix(series)
		if matrix.ContainsSameLabelset() {
			return newErrResult(ret, extlabels.ErrDuplicateLabelSet)
		}
		result = matrix
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
		if vector.ContainsSameLabelset() {
			return newErrResult(ret, extlabels.ErrDuplicateLabelSet)
		}
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

func (q *compatibilityQuery) Statement() parser.Statement { return nil }

// Stats always returns empty query stats for now to avoid panic.
func (q *compatibilityQuery) Stats() *stats.Statistics {
	var enablePerStepStats bool
	if q.opts != nil {
		enablePerStepStats = q.opts.EnablePerStepStats()
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

func analyze(w io.Writer, o model.ObservableVectorOperator, indent, indentNext string) {
	telemetry, next := o.Analyze()
	_, _ = w.Write([]byte(indent))
	_, _ = w.Write([]byte("Operator Time :"))
	_, _ = w.Write([]byte(strconv.FormatInt(int64(telemetry.ExecutionTimeTaken()), 10)))
	if len(next) == 0 {
		_, _ = w.Write([]byte("\n"))
		return
	}
	_, _ = w.Write([]byte(":\n"))

	for i, n := range next {
		if i == len(next)-1 {
			analyze(w, n, indentNext+"└──", indentNext+"   ")
		} else {
			analyze(w, n, indentNext+"└──", indentNext+"   ")
		}
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
