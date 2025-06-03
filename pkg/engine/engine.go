package engine

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	thanosengine "github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type engineKeyType struct{}

var engineKey = engineKeyType{}

const TypeHeader = "X-PromQL-EngineType"

type Type string

const (
	Prometheus Type = "prometheus"
	Thanos     Type = "thanos"
	None       Type = "none"
)

func AddEngineTypeToContext(ctx context.Context, r *http.Request) context.Context {
	ng := Type(r.Header.Get(TypeHeader))
	switch ng {
	case Prometheus, Thanos:
		return context.WithValue(ctx, engineKey, ng)
	default:
		return context.WithValue(ctx, engineKey, None)
	}
}

func GetEngineType(ctx context.Context) Type {
	if ng, ok := ctx.Value(engineKey).(Type); ok {
		return ng
	}
	return None
}

type Engine struct {
	prometheusEngine *promql.Engine
	thanosEngine     *thanosengine.Engine

	fallbackQueriesTotal     prometheus.Counter
	engineSwitchQueriesTotal *prometheus.CounterVec
}

func New(opts promql.EngineOpts, enableThanosEngine bool, reg prometheus.Registerer) *Engine {
	prometheusEngine := promql.NewEngine(opts)

	var thanosEngine *thanosengine.Engine
	if enableThanosEngine {
		thanosEngine = thanosengine.New(thanosengine.Opts{
			EngineOpts:        opts,
			LogicalOptimizers: logicalplan.DefaultOptimizers,
			EnableAnalysis:    true,
		})
	}

	return &Engine{
		prometheusEngine: prometheusEngine,
		thanosEngine:     thanosEngine,
		fallbackQueriesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_thanos_engine_fallback_queries_total",
			Help: "Total number of fallback queries due to not implementation in thanos engine",
		}),
		engineSwitchQueriesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_engine_switch_queries_total",
			Help: "Total number of queries where engine_type is set explicitly",
		}, []string{"engine_type"}),
	}
}

func (qf *Engine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	if engineType := GetEngineType(ctx); engineType == Prometheus {
		qf.engineSwitchQueriesTotal.WithLabelValues(string(Prometheus)).Inc()
		goto prom
	} else if engineType == Thanos {
		qf.engineSwitchQueriesTotal.WithLabelValues(string(Thanos)).Inc()
	}

	if qf.thanosEngine != nil {
		res, err := qf.thanosEngine.MakeInstantQuery(ctx, q, fromPromQLOpts(opts), qs, ts)
		if err != nil {
			if thanosengine.IsUnimplemented(err) {
				// fallback to use prometheus engine
				qf.fallbackQueriesTotal.Inc()
				goto prom
			}
			return nil, err
		}
		return res, nil
	}

prom:
	return qf.prometheusEngine.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (qf *Engine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	if engineType := GetEngineType(ctx); engineType == Prometheus {
		qf.engineSwitchQueriesTotal.WithLabelValues(string(Prometheus)).Inc()
		goto prom
	} else if engineType == Thanos {
		qf.engineSwitchQueriesTotal.WithLabelValues(string(Thanos)).Inc()
	}
	if qf.thanosEngine != nil {
		res, err := qf.thanosEngine.MakeRangeQuery(ctx, q, fromPromQLOpts(opts), qs, start, end, interval)
		if err != nil {
			if thanosengine.IsUnimplemented(err) {
				// fallback to use prometheus engine
				qf.fallbackQueriesTotal.Inc()
				goto prom
			}
			return nil, err
		}
		return res, nil
	}

prom:
	return qf.prometheusEngine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
}

func fromPromQLOpts(opts promql.QueryOpts) *thanosengine.QueryOpts {
	if opts == nil {
		return &thanosengine.QueryOpts{}
	}
	return &thanosengine.QueryOpts{
		LookbackDeltaParam:      opts.LookbackDelta(),
		EnablePerStepStatsParam: opts.EnablePerStepStats(),
	}
}
