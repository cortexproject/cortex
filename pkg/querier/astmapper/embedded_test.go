package astmapper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
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
		q := newMockShardedQueryable(tc.series)
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

func newMockShardedQueryable(n int) *mockShardedQueryable {
	s := make([]storage.Series, 0, n)
	sample := model.SamplePair{}
	for i := 0; i < n; i++ {
		s = append(
			s,
			series.NewConcreteSeries(
				labels.Labels{
					labels.Label{
						Name:  "some-label",
						Value: fmt.Sprintf("%d", i),
					},
				},
				[]model.SamplePair{sample},
			),
		)
	}
	return &mockShardedQueryable{s}
}

type mockShardedQueryable struct {
	series []storage.Series
}

func (q *mockShardedQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return q, nil
}

func (q *mockShardedQueryable) Select(
	_ *storage.SelectParams,
	matchers ...*labels.Matcher,
) (storage.SeriesSet, storage.Warnings, error) {
	shard, _, err := ShardFromMatchers(matchers)
	if err != nil {
		return nil, nil, err
	}
	if shard == nil {
		return nil, nil, errors.Errorf("did not find shard label")
	}

	// return the series range associated with this shard
	seriesPerShard := len(q.series) / shard.Of
	start := shard.Shard * seriesPerShard
	end := start + seriesPerShard
	if end > len(q.series) {
		end = len(q.series)
	}

	return series.NewConcreteSeriesSet(q.series[start:end]), nil, nil

}

func (q *mockShardedQueryable) LabelValues(name string) ([]string, storage.Warnings, error) {
	return nil, nil, errors.Errorf("unimplemented")
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q *mockShardedQueryable) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, errors.Errorf("unimplemented")
}

// Close releases the resources of the Querier.
func (q *mockShardedQueryable) Close() error {
	return nil
}
