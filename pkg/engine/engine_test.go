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
