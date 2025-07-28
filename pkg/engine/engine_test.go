package engine

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	utillog "github.com/cortexproject/cortex/pkg/util/log"
)

func TestEngine_Fallback(t *testing.T) {
	// add unimplemented function
	parser.Functions["unimplemented"] = &parser.Function{
		Name:       "unimplemented",
		ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
		ReturnType: parser.ValueTypeVector,
	}

	ctx := context.Background()
	reg := prometheus.NewRegistry()

	now := time.Now()
	start := time.Now().Add(-time.Minute * 5)
	step := time.Minute
	queryable := promqltest.LoadedStorage(t, "")
	opts := promql.EngineOpts{
		Logger: utillog.GoKitLogToSlog(log.NewNopLogger()),
		Reg:    reg,
	}
	queryEngine := New(opts, true, reg)

	// instant query, should go to fallback
	_, _ = queryEngine.NewInstantQuery(ctx, queryable, nil, "unimplemented(foo)", now)
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_thanos_engine_fallback_queries_total Total number of fallback queries due to not implementation in thanos engine
		# TYPE cortex_thanos_engine_fallback_queries_total counter
		cortex_thanos_engine_fallback_queries_total 1
	`), "cortex_thanos_engine_fallback_queries_total"))

	// range query, should go to fallback
	_, _ = queryEngine.NewRangeQuery(ctx, queryable, nil, "unimplemented(foo)", start, now, step)
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_thanos_engine_fallback_queries_total Total number of fallback queries due to not implementation in thanos engine
		# TYPE cortex_thanos_engine_fallback_queries_total counter
		cortex_thanos_engine_fallback_queries_total 2
	`), "cortex_thanos_engine_fallback_queries_total"))
}

func TestEngine_Switch(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()

	now := time.Now()
	start := time.Now().Add(-time.Minute * 5)
	step := time.Minute
	queryable := promqltest.LoadedStorage(t, "")
	opts := promql.EngineOpts{
		Logger: utillog.GoKitLogToSlog(log.NewNopLogger()),
		Reg:    reg,
	}
	queryEngine := New(opts, true, reg)

	// Query Prometheus engine
	r := &http.Request{Header: http.Header{}}
	r.Header.Set(TypeHeader, string(Prometheus))
	ctx = AddEngineTypeToContext(ctx, r)
	_, _ = queryEngine.NewInstantQuery(ctx, queryable, nil, "foo", now)
	_, _ = queryEngine.NewRangeQuery(ctx, queryable, nil, "foo", start, now, step)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_engine_switch_queries_total Total number of queries where engine_type is set explicitly
		# TYPE cortex_engine_switch_queries_total counter
		cortex_engine_switch_queries_total{engine_type="prometheus"} 2
	`), "cortex_engine_switch_queries_total"))

	// Query Thanos engine
	r.Header.Set(TypeHeader, string(Thanos))
	ctx = AddEngineTypeToContext(ctx, r)
	_, _ = queryEngine.NewInstantQuery(ctx, queryable, nil, "foo", now)
	_, _ = queryEngine.NewRangeQuery(ctx, queryable, nil, "foo", start, now, step)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_engine_switch_queries_total Total number of queries where engine_type is set explicitly
		# TYPE cortex_engine_switch_queries_total counter
		cortex_engine_switch_queries_total{engine_type="prometheus"} 2
		cortex_engine_switch_queries_total{engine_type="thanos"} 2
	`), "cortex_engine_switch_queries_total"))
}

func TestEngine_With_Logical_Plan(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()

	now := time.Now()
	start := time.Now().Add(-time.Minute * 5)
	step := time.Minute
	queryable := promqltest.LoadedStorage(t, "")
	opts := promql.EngineOpts{
		Logger: utillog.GoKitLogToSlog(log.NewNopLogger()),
		Reg:    reg,
	}
	queryEngine := New(opts, true, reg)

	range_lp := createTestLogicalPlan(t, start, now, step, "up")
	instant_lp := createTestLogicalPlan(t, now, now, 0, "up")

	r := &http.Request{Header: http.Header{}}
	r.Header.Set(TypeHeader, string(Thanos))
	ctx = AddEngineTypeToContext(ctx, r)

	// Case 1: Executing logical plan with thanos engine
	_, _ = queryEngine.MakeInstantQueryFromPlan(ctx, queryable, nil, instant_lp.Root(), now, "up")
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_thanos_engine_fallback_queries_total Total number of fallback queries due to not implementation in thanos engine
		# TYPE cortex_thanos_engine_fallback_queries_total counter
		cortex_thanos_engine_fallback_queries_total 0
	`), "cortex_thanos_engine_fallback_queries_total"))

	_, _ = queryEngine.MakeRangeQueryFromPlan(ctx, queryable, nil, range_lp.Root(), start, now, step, "up")
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_thanos_engine_fallback_queries_total Total number of fallback queries due to not implementation in thanos engine
		# TYPE cortex_thanos_engine_fallback_queries_total counter
		cortex_thanos_engine_fallback_queries_total 0
	`), "cortex_thanos_engine_fallback_queries_total"))

	// Case 2: Logical plan that thanos engine cannot execute (so it will fall back to prometheus engine)
	err_range_lp := createTestLogicalPlan(t, start, now, step, "up[10]")
	_, _ = queryEngine.MakeRangeQueryFromPlan(ctx, queryable, nil, err_range_lp.Root(), start, now, step, "up")
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_thanos_engine_fallback_queries_total Total number of fallback queries due to not implementation in thanos engine
		# TYPE cortex_thanos_engine_fallback_queries_total counter
		cortex_thanos_engine_fallback_queries_total 1
	`), "cortex_thanos_engine_fallback_queries_total"))

	// Case 3: executing with prometheus engine
	r.Header.Set(TypeHeader, string(Prometheus))
	ctx = AddEngineTypeToContext(ctx, r)

	_, _ = queryEngine.MakeInstantQueryFromPlan(ctx, queryable, nil, instant_lp.Root(), now, "up")
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_engine_switch_queries_total Total number of queries where engine_type is set explicitly
		# TYPE cortex_engine_switch_queries_total counter
		cortex_engine_switch_queries_total{engine_type="prometheus"} 1
		cortex_engine_switch_queries_total{engine_type="thanos"} 3
	`), "cortex_engine_switch_queries_total"))

	_, _ = queryEngine.MakeRangeQueryFromPlan(ctx, queryable, nil, range_lp.Root(), start, now, step, "up")
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_engine_switch_queries_total Total number of queries where engine_type is set explicitly
		# TYPE cortex_engine_switch_queries_total counter
		cortex_engine_switch_queries_total{engine_type="prometheus"} 2
		cortex_engine_switch_queries_total{engine_type="thanos"} 3
	`), "cortex_engine_switch_queries_total"))
}

func createTestLogicalPlan(t *testing.T, startTime time.Time, endTime time.Time, step time.Duration, q string) logicalplan.Plan {

	qOpts := query.Options{
		Start:              startTime,
		End:                startTime,
		Step:               0,
		StepsBatch:         10,
		LookbackDelta:      0,
		EnablePerStepStats: false,
	}

	if step != 0 {
		qOpts.End = endTime
		qOpts.Step = step
	}

	expr, err := parser.NewParser(q, parser.WithFunctions(parser.Functions)).ParseExpr()
	require.NoError(t, err)

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	logicalPlan := logicalplan.NewFromAST(expr, &qOpts, planOpts)

	return logicalPlan
}
