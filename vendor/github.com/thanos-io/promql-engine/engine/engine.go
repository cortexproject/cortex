// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"context"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/thanos-io/promql-engine/execution/telemetry"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/stats"

	"github.com/thanos-io/promql-engine/execution"
	"github.com/thanos-io/promql-engine/execution/function"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/execution/warnings"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	engstorage "github.com/thanos-io/promql-engine/storage"
	promstorage "github.com/thanos-io/promql-engine/storage/prometheus"
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

	// ExtLookbackDelta specifies what time range to use to determine valid previous sample for extended range functions.
	// Defaults to 1 hour if not specified.
	ExtLookbackDelta time.Duration

	// DecodingConcurrency is the maximum number of goroutines that can be used to decode samples. Defaults to GOMAXPROCS / 2.
	DecodingConcurrency int

	// EnableXFunctions enables custom xRate, xIncrease and xDelta functions.
	// This will default to false.
	EnableXFunctions bool

	// FallbackEngine
	Engine promql.QueryEngine

	// EnableAnalysis enables query analysis.
	EnableAnalysis bool

	// EnablePartialResponses enables partial responses in distributed mode.
	EnablePartialResponses bool

	// SelectorBatchSize specifies the maximum number of samples to be returned by selectors in a single batch.
	SelectorBatchSize int64

	// The Prometheus engine has internal check for duplicate labels produced by functions, aggregations or binary operators.
	// This check can produce false positives when querying time-series data which does not conform to the Prometheus data model,
	// and can be disabled if it leads to false positives.
	DisableDuplicateLabelChecks bool
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

// QueryOpts implements promql.QueryOpts but allows to override more engine default options.
type QueryOpts struct {
	// These values are used to implement promql.QueryOpts, they have weird "Param" suffix because
	// they are accessed by methods of the same name.
	LookbackDeltaParam      time.Duration
	EnablePerStepStatsParam bool

	// DecodingConcurrency can be used to override the DecodingConcurrency engine setting.
	DecodingConcurrency int

	// EnablePartialResponses can be used to override the EnablePartialResponses engine setting.
	EnablePartialResponses bool
}

func (opts QueryOpts) LookbackDelta() time.Duration { return opts.LookbackDeltaParam }
func (opts QueryOpts) EnablePerStepStats() bool     { return opts.EnablePerStepStatsParam }

func fromPromQLOpts(opts promql.QueryOpts) *QueryOpts {
	if opts == nil {
		return &QueryOpts{}
	}
	return &QueryOpts{
		LookbackDeltaParam:      opts.LookbackDelta(),
		EnablePerStepStatsParam: opts.EnablePerStepStats(),
	}
}

// New creates a new query engine with the given options. The query engine will
// use the storage passed in NewInstantQuery and NewRangeQuery for retrieving
// data when executing queries.
func New(opts Opts) *Engine {
	return NewWithScanners(opts, nil)
}

// NewWithScanners creates a new query engine with the given options and storage.Scanners.
// When executing queries, the engine will create scanner operators using the storage.Scanners and will ignore the
// Prometheus storage passed in NewInstantQuery and NewRangeQuery.
// This method is useful when the data being queried does not easily fit into the Prometheus storage model.
func NewWithScanners(opts Opts, scanners engstorage.Scanners) *Engine {
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

	var engine promql.QueryEngine
	if opts.Engine == nil {
		engine = promql.NewEngine(opts.EngineOpts)
	} else {
		engine = opts.Engine
	}

	decodingConcurrency := opts.DecodingConcurrency
	if opts.DecodingConcurrency < 1 {
		decodingConcurrency = runtime.GOMAXPROCS(0) / 2
		if decodingConcurrency < 1 {
			decodingConcurrency = 1
		}
	}

	var queryTracker promql.QueryTracker = nopQueryTracker{}
	if opts.ActiveQueryTracker != nil {
		queryTracker = opts.ActiveQueryTracker
	}

	return &Engine{
		prom:               engine,
		functions:          functions,
		scanners:           scanners,
		activeQueryTracker: queryTracker,

		disableDuplicateLabelChecks: opts.DisableDuplicateLabelChecks,
		disableFallback:             opts.DisableFallback,

		logger:                 opts.Logger,
		lookbackDelta:          opts.LookbackDelta,
		enablePerStepStats:     opts.EnablePerStepStats,
		logicalOptimizers:      opts.getLogicalOptimizers(),
		timeout:                opts.Timeout,
		metrics:                metrics,
		extLookbackDelta:       opts.ExtLookbackDelta,
		enableAnalysis:         opts.EnableAnalysis,
		enablePartialResponses: opts.EnablePartialResponses,
		noStepSubqueryIntervalFn: func(d time.Duration) time.Duration {
			return time.Duration(opts.NoStepSubqueryIntervalFn(d.Milliseconds()) * 1000000)
		},
		decodingConcurrency: decodingConcurrency,
	}
}

var (
	// Duplicate label checking logic uses a bitmap with 64 bits currently.
	// As long as we use this method we need to have batches that are smaller
	// then 64 steps.
	ErrStepsBatchTooLarge = errors.New("'StepsBatch' must be less than 64")
)

type Engine struct {
	prom               promql.QueryEngine
	functions          map[string]*parser.Function
	scanners           engstorage.Scanners
	activeQueryTracker promql.QueryTracker

	disableDuplicateLabelChecks bool
	disableFallback             bool

	logger             log.Logger
	lookbackDelta      time.Duration
	enablePerStepStats bool
	logicalOptimizers  []logicalplan.Optimizer
	timeout            time.Duration
	metrics            *engineMetrics

	extLookbackDelta         time.Duration
	decodingConcurrency      int
	enableAnalysis           bool
	enablePartialResponses   bool
	noStepSubqueryIntervalFn func(time.Duration) time.Duration
}

func (e *Engine) MakeInstantQuery(ctx context.Context, q storage.Queryable, opts *QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	expr, err := parser.NewParser(qs, parser.WithFunctions(e.functions)).ParseExpr()
	if err != nil {
		return nil, err
	}
	// determine sorting order before optimizers run, we do this by looking for "sort"
	// and "sort_desc" and optimize them away afterwards since they are only needed at
	// the presentation layer and not when computing the results.
	resultSort := newResultSort(expr)

	qOpts := e.makeQueryOpts(ts, ts, 0, opts)
	if qOpts.StepsBatch > 64 {
		return nil, ErrStepsBatchTooLarge
	}

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: e.disableDuplicateLabelChecks,
	}
	lplan, warns := logicalplan.NewFromAST(expr, qOpts, planOpts).Optimize(e.logicalOptimizers)

	scanners, err := e.storageScanners(q, qOpts, lplan)
	if err != nil {
		return nil, errors.Wrap(err, "creating storage scanners")
	}

	ctx = warnings.NewContext(ctx)
	defer func() { warns.Merge(warnings.FromContext(ctx)) }()
	exec, err := execution.New(ctx, lplan.Root(), scanners, qOpts)
	if e.triggerFallback(err) {
		e.metrics.queries.WithLabelValues("true").Inc()
		return e.prom.NewInstantQuery(ctx, q, opts, qs, ts)
	}
	e.metrics.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}
	return &compatibilityQuery{
		Query:      &Query{exec: exec, opts: opts},
		engine:     e,
		plan:       lplan,
		ts:         ts,
		warns:      warns,
		t:          InstantQuery,
		resultSort: resultSort,
		scanners:   scanners,
		start:      ts,
		end:        ts,
		step:       0,
	}, nil
}

func (e *Engine) MakeInstantQueryFromPlan(ctx context.Context, q storage.Queryable, opts *QueryOpts, root logicalplan.Node, ts time.Time) (promql.Query, error) {
	idx, err := e.activeQueryTracker.Insert(ctx, root.String())
	if err != nil {
		return nil, err
	}
	defer e.activeQueryTracker.Delete(idx)

	qOpts := e.makeQueryOpts(ts, ts, 0, opts)
	if qOpts.StepsBatch > 64 {
		return nil, ErrStepsBatchTooLarge
	}
	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: e.disableDuplicateLabelChecks,
	}
	lplan, warns := logicalplan.New(root, qOpts, planOpts).Optimize(e.logicalOptimizers)

	ctx = warnings.NewContext(ctx)
	defer func() { warns.Merge(warnings.FromContext(ctx)) }()

	scnrs, err := e.storageScanners(q, qOpts, lplan)
	if err != nil {
		return nil, errors.Wrap(err, "creating storage scanners")
	}

	exec, err := execution.New(ctx, lplan.Root(), scnrs, qOpts)
	if e.triggerFallback(err) {
		e.metrics.queries.WithLabelValues("true").Inc()
		return e.prom.NewInstantQuery(ctx, q, opts, root.String(), ts)
	}
	e.metrics.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}

	return &compatibilityQuery{
		Query:  &Query{exec: exec, opts: opts},
		engine: e,
		plan:   lplan,
		ts:     ts,
		warns:  warns,
		t:      InstantQuery,
		// TODO(fpetkovski): Infer the sort order from the plan, ideally without copying the newResultSort function.
		resultSort: noSortResultSort{},
		scanners:   scnrs,
		start:      ts,
		end:        ts,
		step:       0,
	}, nil
}

func (e *Engine) MakeRangeQuery(ctx context.Context, q storage.Queryable, opts *QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	idx, err := e.activeQueryTracker.Insert(ctx, qs)
	if err != nil {
		return nil, err
	}
	defer e.activeQueryTracker.Delete(idx)

	expr, err := parser.NewParser(qs, parser.WithFunctions(e.functions)).ParseExpr()
	if err != nil {
		return nil, err
	}

	// Use same check as Prometheus for range queries.
	if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
		return nil, errors.Newf("invalid expression type %q for range query, must be Scalar or instant Vector", parser.DocumentedType(expr.Type()))
	}
	qOpts := e.makeQueryOpts(start, end, step, opts)
	if qOpts.StepsBatch > 64 {
		return nil, ErrStepsBatchTooLarge
	}
	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: e.disableDuplicateLabelChecks,
	}
	lplan, warns := logicalplan.NewFromAST(expr, qOpts, planOpts).Optimize(e.logicalOptimizers)

	ctx = warnings.NewContext(ctx)
	defer func() { warns.Merge(warnings.FromContext(ctx)) }()
	scnrs, err := e.storageScanners(q, qOpts, lplan)
	if err != nil {
		return nil, errors.Wrap(err, "creating storage scanners")
	}

	exec, err := execution.New(ctx, lplan.Root(), scnrs, qOpts)
	if e.triggerFallback(err) {
		e.metrics.queries.WithLabelValues("true").Inc()
		return e.prom.NewRangeQuery(ctx, q, opts, qs, start, end, step)
	}
	e.metrics.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}

	return &compatibilityQuery{
		Query:    &Query{exec: exec, opts: opts},
		engine:   e,
		plan:     lplan,
		warns:    warns,
		t:        RangeQuery,
		scanners: scnrs,
		start:    start,
		end:      end,
		step:     step,
	}, nil
}

func (e *Engine) MakeRangeQueryFromPlan(ctx context.Context, q storage.Queryable, opts *QueryOpts, root logicalplan.Node, start, end time.Time, step time.Duration) (promql.Query, error) {
	idx, err := e.activeQueryTracker.Insert(ctx, root.String())
	if err != nil {
		return nil, err
	}
	defer e.activeQueryTracker.Delete(idx)

	qOpts := e.makeQueryOpts(start, end, step, opts)
	if qOpts.StepsBatch > 64 {
		return nil, ErrStepsBatchTooLarge
	}
	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: e.disableDuplicateLabelChecks,
	}
	lplan, warns := logicalplan.New(root, qOpts, planOpts).Optimize(e.logicalOptimizers)

	scnrs, err := e.storageScanners(q, qOpts, lplan)
	if err != nil {
		return nil, errors.Wrap(err, "creating storage scanners")
	}

	ctx = warnings.NewContext(ctx)
	defer func() { warns.Merge(warnings.FromContext(ctx)) }()
	exec, err := execution.New(ctx, lplan.Root(), scnrs, qOpts)
	if e.triggerFallback(err) {
		e.metrics.queries.WithLabelValues("true").Inc()
		return e.prom.NewRangeQuery(ctx, q, opts, lplan.Root().String(), start, end, step)
	}
	e.metrics.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}
	return &compatibilityQuery{
		Query:    &Query{exec: exec, opts: opts},
		engine:   e,
		plan:     lplan,
		warns:    warns,
		t:        RangeQuery,
		scanners: scnrs,
		start:    start,
		end:      end,
		step:     step,
	}, nil
}

// PromQL compatibility constructors

// NewInstantQuery implements the promql.Engine interface.
func (e *Engine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return e.MakeInstantQuery(ctx, q, fromPromQLOpts(opts), qs, ts)
}

// NewRangeQuery implements the promql.Engine interface.
func (e *Engine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	return e.MakeRangeQuery(ctx, q, fromPromQLOpts(opts), qs, start, end, step)
}

func (e *Engine) makeQueryOpts(start time.Time, end time.Time, step time.Duration, opts *QueryOpts) *query.Options {
	res := &query.Options{
		Start:                    start,
		End:                      end,
		Step:                     step,
		StepsBatch:               stepsBatch,
		LookbackDelta:            e.lookbackDelta,
		EnablePerStepStats:       e.enablePerStepStats,
		ExtLookbackDelta:         e.extLookbackDelta,
		EnableAnalysis:           e.enableAnalysis,
		EnablePartialResponses:   e.enablePartialResponses,
		NoStepSubqueryIntervalFn: e.noStepSubqueryIntervalFn,
		DecodingConcurrency:      e.decodingConcurrency,
	}
	if opts == nil {
		return res
	}

	if opts.LookbackDelta() > 0 {
		res.LookbackDelta = opts.LookbackDelta()
	}
	if opts.EnablePerStepStats() {
		res.EnablePerStepStats = opts.EnablePerStepStats()
	}

	if opts.DecodingConcurrency != 0 {
		res.DecodingConcurrency = opts.DecodingConcurrency
	}
	if opts.EnablePartialResponses {
		res.EnablePartialResponses = opts.EnablePartialResponses
	}
	return res
}

func (e *Engine) storageScanners(queryable storage.Queryable, qOpts *query.Options, lplan logicalplan.Plan) (engstorage.Scanners, error) {
	if e.scanners == nil {
		return promstorage.NewPrometheusScanners(queryable, qOpts, lplan)
	}
	return e.scanners, nil
}

func (e *Engine) triggerFallback(err error) bool {
	if e.disableFallback {
		return false
	}

	return errors.Is(err, parse.ErrNotSupportedExpr) || errors.Is(err, parse.ErrNotImplemented)
}

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
	if observableRoot, ok := q.exec.(telemetry.ObservableVectorOperator); ok {
		return analyzeQuery(observableRoot)
	}
	return nil
}

type compatibilityQuery struct {
	*Query
	engine *Engine
	plan   logicalplan.Plan
	ts     time.Time // Empty for range queries.
	warns  annotations.Annotations
	start  time.Time
	end    time.Time
	step   time.Duration

	t          QueryType
	resultSort resultSorter
	cancel     context.CancelFunc

	scanners engstorage.Scanners
}

func (q *compatibilityQuery) Exec(ctx context.Context) (ret *promql.Result) {
	idx, err := q.engine.activeQueryTracker.Insert(ctx, q.String())
	if err != nil {
		return &promql.Result{Err: err}
	}
	defer q.engine.activeQueryTracker.Delete(idx)

	ctx = warnings.NewContext(ctx)
	defer func() {
		ret.Warnings = ret.Warnings.Merge(warnings.FromContext(ctx))
	}()

	// Handle case with strings early on as this does not need us to process samples.
	switch e := q.plan.Root().(type) {
	case *logicalplan.StringLiteral:
		return &promql.Result{Value: promql.String{V: e.Val, T: q.ts.UnixMilli()}}
	}
	ret = &promql.Result{
		Value:    promql.Vector{},
		Warnings: q.warns,
	}
	defer recoverEngine(q.engine.logger, q.plan, &ret.Err)

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
	for i, s := range resultSeries {
		series[i].Metric = s
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
		matrix := make(promql.Matrix, 0, len(series))
		for _, s := range series {
			if len(s.Floats)+len(s.Histograms) == 0 {
				continue
			}
			matrix = append(matrix, s)
		}
		sort.Sort(matrix)
		ret.Value = matrix
		if matrix.ContainsSameLabelset() {
			return newErrResult(ret, extlabels.ErrDuplicateLabelSet)
		}
		return ret
	}

	var result parser.Value
	switch q.plan.Root().ReturnType() {
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
		panic(errors.Newf("new.Engine.exec: unexpected expression type %q", q.plan.Root().ReturnType()))
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

	analysis := q.Analyze()
	samples := stats.NewQuerySamples(enablePerStepStats)
	if enablePerStepStats {
		samples.InitStepTracking(q.start.UnixMilli(), q.end.UnixMilli(), telemetry.StepTrackingInterval(q.step))
	}

	if analysis != nil {
		samples.PeakSamples = int(analysis.PeakSamples())
		samples.TotalSamples = analysis.TotalSamples()
		samples.TotalSamplesPerStep = analysis.TotalSamplesPerStep()
	}

	return &stats.Statistics{Timers: stats.NewQueryTimers(), Samples: samples}
}

func (q *compatibilityQuery) Close() {
	if err := q.scanners.Close(); err != nil {
		level.Warn(q.engine.logger).Log("msg", "error closing storage scanners, some memory might have leaked", "err", err)
	}
}

func (q *compatibilityQuery) String() string { return q.plan.Root().String() }

func (q *compatibilityQuery) Cancel() {
	if q.cancel != nil {
		q.cancel()
		q.cancel = nil
	}
}

type nopQueryTracker struct{}

func (n nopQueryTracker) GetMaxConcurrent() int                                 { return -1 }
func (n nopQueryTracker) Insert(ctx context.Context, query string) (int, error) { return 0, nil }
func (n nopQueryTracker) Delete(insertIndex int)                                {}
func (n nopQueryTracker) Close() error                                          { return nil }

func recoverEngine(logger log.Logger, plan logicalplan.Plan, errp *error) {
	e := recover()
	if e == nil {
		return
	}

	switch err := e.(type) {
	case runtime.Error:
		// Print the stack trace but do not inhibit the running application.
		buf := make([]byte, 64<<10)
		buf = buf[:runtime.Stack(buf, false)]

		level.Error(logger).Log("msg", "runtime panic in engine", "expr", plan.Root().String(), "err", e, "stacktrace", string(buf))
		*errp = errors.Wrap(err, "unexpected error")
	}
}
