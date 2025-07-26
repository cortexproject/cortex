package engine

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/thanos-io/promql-engine/execution/parse"

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
	queryEngine := New(opts, ThanosEngineConfig{Enabled: true}, reg)

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
	queryEngine := New(opts, ThanosEngineConfig{Enabled: true}, reg)

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

func TestEngine_XFunctions(t *testing.T) {
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
	queryEngine := New(opts, ThanosEngineConfig{Enabled: true, EnableXFunctions: true}, reg)

	for name := range parse.XFunctions {
		t.Run(name, func(t *testing.T) {
			_, err := queryEngine.NewInstantQuery(ctx, queryable, nil, fmt.Sprintf("%s(foo[1m])", name), now)
			require.NoError(t, err)

			_, err = queryEngine.NewRangeQuery(ctx, queryable, nil, fmt.Sprintf("%s(foo[1m])", name), start, now, step)
			require.NoError(t, err)
		})
	}
}
