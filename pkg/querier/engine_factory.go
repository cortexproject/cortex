package querier

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type EngineFactory struct {
	prometheusEngine *promql.Engine
	thanosEngine     *engine.Engine

	fallbackQueriesTotal prometheus.Counter
}

func NewEngineFactory(opts promql.EngineOpts, enableThanosEngine bool, reg prometheus.Registerer) *EngineFactory {
	prometheusEngine := promql.NewEngine(opts)

	var thanosEngine *engine.Engine
	if enableThanosEngine {
		thanosEngine = engine.New(engine.Opts{
			EngineOpts:        opts,
			LogicalOptimizers: logicalplan.AllOptimizers,
			EnableAnalysis:    true,
		})
	}

	return &EngineFactory{
		prometheusEngine: prometheusEngine,
		thanosEngine:     thanosEngine,
		fallbackQueriesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_thanos_engine_fallback_queries_total",
			Help: "Total number of fallback queries due to not implementation in thanos engine",
		}),
	}
}

func (qf *EngineFactory) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	if qf.thanosEngine != nil {
		res, err := qf.thanosEngine.MakeInstantQuery(ctx, q, fromPromQLOpts(opts), qs, ts)
		if err != nil {
			if engine.IsUnimplemented(err) {
				// fallback to use prometheus engine
				qf.fallbackQueriesTotal.Inc()
				goto fallback
			}
			return nil, err
		}
		return res, nil
	}

fallback:
	return qf.prometheusEngine.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (qf *EngineFactory) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	if qf.thanosEngine != nil {
		res, err := qf.thanosEngine.MakeRangeQuery(ctx, q, fromPromQLOpts(opts), qs, start, end, interval)
		if err != nil {
			if engine.IsUnimplemented(err) {
				// fallback to use prometheus engine
				qf.fallbackQueriesTotal.Inc()
				goto fallback
			}
			return nil, err
		}
		return res, nil
	}

fallback:
	return qf.prometheusEngine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
}

func fromPromQLOpts(opts promql.QueryOpts) *engine.QueryOpts {
	if opts == nil {
		return &engine.QueryOpts{}
	}
	return &engine.QueryOpts{
		LookbackDeltaParam:      opts.LookbackDelta(),
		EnablePerStepStatsParam: opts.EnablePerStepStats(),
	}
}
