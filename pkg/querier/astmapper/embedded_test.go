package astmapper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestShallowEmbedSelectors(t *testing.T) {
	var testExpr = []struct {
		input    string
		expected string
	}{
		// already holds embedded queries, so noop (don't double encode)
		{
			`sum by(foo) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"prod\\\"}\"]}"})`,
			`sum by(foo) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"prod\\\"}\"]}"})`,
		},
		{
			`http_requests_total{cluster="prod"}`,
			`__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"prod\\\"}\"]}"}`,
		},
		{
			`rate(http_requests_total{cluster="eu-west2"}[5m]) or rate(http_requests_total{cluster="us-central1"}[5m])`,
			`rate(__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"eu-west2\\\"}[5m]\"]}"}[1m]) or rate(__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"us-central1\\\"}[5m]\"]}"}[1m])`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			mapper := ShallowEmbedSelectors
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := mapper.Map(expr)
			require.Nil(t, err)

			require.Equal(t, c.expected, res.String())
		})
	}
}

// placeholder to prevent compiler from optimizing away bench results
var result *promql.Result

func BenchmarkASTMapping(b *testing.B) {

	for _, tc := range []struct {
		shards int
		series int
		squash squasher
		desc   string
	}{
		{
			shards: 16,
			series: 1000,
			squash: OrSquasher,
			desc:   "with Or Nodes",
		},
		{
			shards: 16,
			series: 1000,
			squash: VectorSquasher,
			desc:   "with Concatenation",
		},
		{
			shards: 16,
			series: 10000,
			squash: OrSquasher,
			desc:   "with Or Nodes",
		},
		{
			shards: 16,
			series: 10000,
			squash: VectorSquasher,
			desc:   "with Concatenation",
		},
		{
			shards: 16,
			series: 100000,
			squash: OrSquasher,
			desc:   "with Or Nodes",
		},
		{
			shards: 16,
			series: 100000,
			squash: VectorSquasher,
			desc:   "with Concatenation",
		},
	} {
		q := NewMockShardedQueryable(1, []string{"a"}, tc.series)
		summer, err := NewShardSummer(tc.shards, tc.squash)
		if err != nil {
			b.Fatal(err)
		}

		expr, err := promql.ParseExpr(`sum(rate(some_metric[5m]))`)
		if err != nil {
			b.Fatal(err)
		}
		mapped, err := summer.Map(expr)
		engine := promql.NewEngine(promql.EngineOpts{
			Logger:        util.Logger,
			MaxConcurrent: 100,
			MaxSamples:    1e6,
			Timeout:       1 * time.Minute,
		})
		query, err := engine.NewRangeQuery(q, mapped.String(), time.Now().Add(-time.Minute), time.Now(), 1*time.Minute)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(
			fmt.Sprintf("desc:[%s]/shards:[%d]/series:[%d]", tc.desc, tc.shards, tc.series),
			func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					result = query.Exec(context.Background())
				}
			},
		)
	}
}
