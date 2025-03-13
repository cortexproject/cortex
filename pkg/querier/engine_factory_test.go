package querier

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestEngineFactory_Fallback(t *testing.T) {
	// add unimplemented function
	parser.Functions["unimplemented"] = &parser.Function{
		Name:       "unimplemented",
		ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
		ReturnType: parser.ValueTypeVector,
	}

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.ThanosEngine = true
	ctx := context.Background()
	reg := prometheus.NewRegistry()

	chunkStore := &emptyChunkStore{}
	distributor := &errDistributor{}

	overrides, err := validation.NewOverrides(DefaultLimitsConfig(), nil)
	require.NoError(t, err)

	now := time.Now()
	start := time.Now().Add(-time.Minute * 5)
	step := time.Minute
	queryable, _, queryEngine := New(cfg, overrides, distributor, []QueryableWithFilter{UseAlwaysQueryable(NewMockStoreQueryable(chunkStore))}, reg, log.NewNopLogger(), nil)

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
