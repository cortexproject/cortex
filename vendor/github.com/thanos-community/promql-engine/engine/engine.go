// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"context"

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
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"

	"github.com/thanos-community/promql-engine/execution"
	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
	"github.com/thanos-community/promql-engine/logicalplan"
)

type QueryType int

const (
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
}

func (o Opts) getLogicalOptimizers() []logicalplan.Optimizer {
	if o.LogicalOptimizers == nil {
		return logicalplan.DefaultOptimizers
	}

	return o.LogicalOptimizers
}

type remoteEngine struct {
	q         storage.Queryable
	engine    *compatibilityEngine
	labelSets []labels.Labels
	maxt      int64
}

func NewRemoteEngine(opts Opts, q storage.Queryable, maxt int64, labelSets []labels.Labels) *remoteEngine {
	return &remoteEngine{
		q:         q,
		labelSets: labelSets,
		maxt:      maxt,
		engine:    New(opts),
	}
}

func (l remoteEngine) MaxT() int64 {
	return l.maxt
}

func (l remoteEngine) LabelSets() []labels.Labels {
	return l.labelSets
}

func (l remoteEngine) NewInstantQuery(opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return l.engine.NewInstantQuery(l.q, opts, qs, ts)
}

func (l remoteEngine) NewRangeQuery(opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return l.engine.NewRangeQuery(l.q, opts, qs, start, end, interval)
}

type distributedEngine struct {
	endpoints    api.RemoteEndpoints
	remoteEngine *compatibilityEngine
}

func NewDistributedEngine(opts Opts, endpoints api.RemoteEndpoints) v1.QueryEngine {
	opts.LogicalOptimizers = append(
		opts.LogicalOptimizers,
		logicalplan.DistributedExecutionOptimizer{Endpoints: endpoints},
	)
	return &distributedEngine{
		endpoints:    endpoints,
		remoteEngine: New(opts),
	}
}

func (l distributedEngine) SetQueryLogger(log promql.QueryLogger) {}

func (l distributedEngine) NewInstantQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return l.remoteEngine.NewInstantQuery(q, opts, qs, ts)
}

func (l distributedEngine) NewRangeQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
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

	return &compatibilityEngine{
		prom: promql.NewEngine(opts.EngineOpts),
		queries: promauto.With(opts.Reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "promql_engine_queries_total",
				Help: "Number of PromQL queries.",
			}, []string{"fallback"},
		),
		debugWriter:       opts.DebugWriter,
		disableFallback:   opts.DisableFallback,
		logger:            opts.Logger,
		lookbackDelta:     opts.LookbackDelta,
		logicalOptimizers: opts.getLogicalOptimizers(),
	}
}

type compatibilityEngine struct {
	prom    *promql.Engine
	queries *prometheus.CounterVec

	debugWriter io.Writer

	disableFallback   bool
	logger            log.Logger
	lookbackDelta     time.Duration
	logicalOptimizers []logicalplan.Optimizer
}

func (e *compatibilityEngine) SetQueryLogger(l promql.QueryLogger) {
	e.prom.SetQueryLogger(l)
}

func (e *compatibilityEngine) NewInstantQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	lplan := logicalplan.New(expr, ts, ts)
	lplan = lplan.Optimize(e.logicalOptimizers)

	exec, err := execution.New(lplan.Expr(), q, ts, ts, 0, e.lookbackDelta)
	if e.triggerFallback(err) {
		e.queries.WithLabelValues("true").Inc()
		return e.prom.NewInstantQuery(q, opts, qs, ts)
	}
	e.queries.WithLabelValues("false").Inc()
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
		resultSort: newResultSort(expr),
	}, nil
}

func (e *compatibilityEngine) NewRangeQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	// Use same check as Prometheus for range queries.
	if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
		return nil, errors.Newf("invalid expression type %q for range Query, must be Scalar or instant Vector", parser.DocumentedType(expr.Type()))
	}

	lplan := logicalplan.New(expr, start, end)
	lplan = lplan.Optimize(e.logicalOptimizers)

	exec, err := execution.New(lplan.Expr(), q, start, end, step, e.lookbackDelta)
	if e.triggerFallback(err) {
		e.queries.WithLabelValues("true").Inc()
		return e.prom.NewRangeQuery(q, opts, qs, start, end, step)
	}
	e.queries.WithLabelValues("false").Inc()
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

type resultSort struct {
	sortByValues  bool
	sortOrder     sortOrder
	sortingLabels []string
	groupBy       bool
}

func newResultSort(expr parser.Expr) resultSort {
	aggr, ok := expr.(*parser.AggregateExpr)
	if !ok {
		return resultSort{}
	}

	switch aggr.Op {
	case parser.TOPK:
		return resultSort{
			sortByValues:  true,
			sortingLabels: aggr.Grouping,
			sortOrder:     sortOrderDesc,
			groupBy:       !aggr.Without,
		}
	case parser.BOTTOMK:
		return resultSort{
			sortByValues:  true,
			sortingLabels: aggr.Grouping,
			sortOrder:     sortOrderAsc,
			groupBy:       !aggr.Without,
		}
	default:
		return resultSort{}
	}
}

func (s resultSort) comparer(samples *promql.Vector) func(i int, j int) bool {
	return func(i int, j int) bool {
		if !s.sortByValues {
			return i < j
		}

		var iLbls labels.Labels
		var jLbls labels.Labels
		iLb := labels.NewBuilder((*samples)[i].Metric)
		jLb := labels.NewBuilder((*samples)[j].Metric)
		if s.groupBy {
			iLbls = iLb.Keep(s.sortingLabels...).Labels(nil)
			jLbls = jLb.Keep(s.sortingLabels...).Labels(nil)
		} else {
			iLbls = iLb.Del(s.sortingLabels...).Labels(nil)
			jLbls = jLb.Del(s.sortingLabels...).Labels(nil)
		}

		lblsCmp := labels.Compare(iLbls, jLbls)
		if lblsCmp != 0 {
			return lblsCmp < 0
		}

		if s.sortOrder == sortOrderAsc {
			return (*samples)[i].V < (*samples)[j].V
		}
		return (*samples)[i].V > (*samples)[j].V
	}
}

type compatibilityQuery struct {
	*Query
	engine     *compatibilityEngine
	expr       parser.Expr
	ts         time.Time // Empty for range queries.
	t          QueryType
	resultSort resultSort

	cancel context.CancelFunc
}

func (q *compatibilityQuery) Exec(ctx context.Context) (ret *promql.Result) {
	// Handle case with strings early on as this does not need us to process samples.
	// TODO(saswatamcode): Modify models.StepVector to support all types and check during executor creation.
	ret = &promql.Result{
		Value: promql.Vector{},
	}
	defer recoverEngine(q.engine.logger, q.expr, &ret.Err)

	ctx, cancel := context.WithCancel(ctx)
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
			if len(resultSeries) == 0 && len(r) != 0 {
				numSeries := 0
				for i := range r {
					numSeries += len(r[i].Samples)
				}

				series = make([]promql.Series, numSeries)

				for _, vector := range r {
					for i := range vector.Samples {
						series[i].Points = append(series[i].Points, promql.Point{
							T: vector.T,
							V: vector.Samples[i],
						})
					}
					q.Query.exec.GetPool().PutStepVector(vector)
				}
				q.Query.exec.GetPool().PutVectors(r)
				continue
			}

			for _, vector := range r {
				for i, s := range vector.SampleIDs {
					if len(series[s].Points) == 0 {
						series[s].Points = make([]promql.Point, 0, 121) // Typically 1h of data.
					}
					series[s].Points = append(series[s].Points, promql.Point{
						T: vector.T,
						V: vector.Samples[i],
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
			if len(s.Points) == 0 {
				continue
			}
			resultMatrix = append(resultMatrix, s)
		}
		sort.Sort(resultMatrix)
		ret.Value = resultMatrix
		return ret
	}

	var result parser.Value
	switch q.expr.Type() {
	case parser.ValueTypeMatrix:
		result = promql.Matrix(series)
	case parser.ValueTypeVector:
		// Convert matrix with one value per series into vector.
		vector := make(promql.Vector, 0, len(resultSeries))
		for i := range series {
			if len(series[i].Points) == 0 {
				continue
			}
			// Point might have a different timestamp, force it to the evaluation
			// timestamp as that is when we ran the evaluation.
			vector = append(vector, promql.Sample{
				Metric: series[i].Metric,
				Point: promql.Point{
					V: series[i].Points[0].V,
					T: q.ts.UnixMilli(),
				},
			})
		}
		sort.Slice(vector, q.resultSort.comparer(&vector))
		result = vector
	case parser.ValueTypeScalar:
		v := math.NaN()
		if len(series) != 0 {
			v = series[0].Points[0].V
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
