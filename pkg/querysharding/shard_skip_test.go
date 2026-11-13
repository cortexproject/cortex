package querysharding

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"go.uber.org/atomic"

	cortexparser "github.com/cortexproject/cortex/pkg/parser"
)

const testTotalShards = 5

// shardedMatchers returns the matchers of a selector with the sharding info injected, the way the
// query frontend injects it.
func shardedMatchers(t *testing.T, selector string, info *storepb.ShardInfo) []*labels.Matcher {
	t.Helper()
	query, err := InjectShardingInfo(selector, info)
	require.NoError(t, err)
	expr, err := cortexparser.ParseExpr(query)
	require.NoError(t, err)
	vs, ok := expr.(*parser.VectorSelector)
	require.True(t, ok)
	return vs.LabelMatchers
}

func TestShardCannotMatchSelector(t *testing.T) {
	tests := []struct {
		name           string
		selector       string
		by             bool
		shardingLabels []string
		// pinned is true when exactly one shard can match and every other shard is skipped.
		pinned bool
	}{
		{name: "pinned sharding label", selector: `up{cluster="c1"}`, by: true, shardingLabels: []string{"cluster"}, pinned: true},
		{name: "pinned label with extra regex and negative matchers", selector: `up{cluster="c1", cluster=~"c.*", job!="a"}`, by: true, shardingLabels: []string{"cluster"}, pinned: true},
		{name: "all of multiple sharding labels pinned", selector: `up{cluster="c1", region="us"}`, by: true, shardingLabels: []string{"cluster", "region"}, pinned: true},
		{name: "duplicated sharding label", selector: `up{cluster="c1"}`, by: true, shardingLabels: []string{"cluster", "cluster"}, pinned: true},
		{name: "metric name as sharding label", selector: `up`, by: true, shardingLabels: []string{labels.MetricName}, pinned: true},
		{name: "same value pinned twice", selector: `up{cluster="c1", cluster="c1"}`, by: true, shardingLabels: []string{"cluster"}, pinned: true},
		{name: "missing matcher", selector: `up`, by: true, shardingLabels: []string{"cluster"}},
		{name: "regex matcher", selector: `up{cluster=~"c1"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "negative matcher", selector: `up{cluster!="c1"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "empty value also selects series without the label", selector: `up{cluster=""}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "conflicting values", selector: `up{cluster="c1", cluster="c2"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "one of multiple sharding labels not pinned", selector: `up{cluster="c1"}`, by: true, shardingLabels: []string{"cluster", "region"}},
		{name: "without sharding", selector: `up{cluster="c1"}`, by: false, shardingLabels: []string{"cluster"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matchingShards := 0
			for shard := range int64(testTotalShards) {
				matchers := shardedMatchers(t, tc.selector, &storepb.ShardInfo{
					TotalShards: testTotalShards,
					ShardIndex:  shard,
					By:          tc.by,
					Labels:      tc.shardingLabels,
				})
				if !ShardCannotMatchSelector(matchers) {
					matchingShards++
				}
			}
			if tc.pinned {
				require.Equal(t, 1, matchingShards)
			} else {
				require.Equal(t, testTotalShards, matchingShards)
			}
		})
	}
}

func TestShardCannotMatchSelector_NotSharded(t *testing.T) {
	require.False(t, ShardCannotMatchSelector([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "cluster", "c1")}))
	require.False(t, ShardCannotMatchSelector(shardedMatchers(t, `up{cluster="c1"}`, &storepb.ShardInfo{TotalShards: 0, By: true, Labels: []string{"cluster"}})))
	require.False(t, ShardCannotMatchSelector([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, CortexShardByLabel, "not-base64!")}))
}

// TestShardCannotMatchSelector_AgreesWithShardMatcher checks that the shard the function keeps is the
// shard storepb.ShardMatcher assigns to the selected series, whatever their other labels are.
func TestShardCannotMatchSelector_AgreesWithShardMatcher(t *testing.T) {
	shardingLabels := []string{"region", "cluster"}
	for totalShards := int64(1); totalShards <= 8; totalShards++ {
		for i := range 50 {
			cluster, region := fmt.Sprintf("cluster-%d", i), fmt.Sprintf("region-%d", i%3)
			selector := fmt.Sprintf(`up{cluster=%q, region=%q}`, cluster, region)
			series := labels.FromStrings(labels.MetricName, "up", "cluster", cluster, "instance", fmt.Sprintf("i-%d", i), "region", region)

			for shard := range totalShards {
				info := &storepb.ShardInfo{TotalShards: totalShards, ShardIndex: shard, By: true, Labels: shardingLabels}
				matcher := info.Matcher(&Buffers)
				seriesInShard := matcher.MatchesLabels(series)
				matcher.Close()

				require.Equal(t, seriesInShard, !ShardCannotMatchSelector(shardedMatchers(t, selector, info)))
			}
		}
	}
}

// TestShardCannotMatchSelector_Correctness evaluates queries with the PromQL engine unsharded, and
// vertically sharded with the shard matcher applied at Select the way ingesters and store-gateways
// apply it. The sharded run returns an empty result for every Select that ShardCannotMatchSelector
// skips. The merged sharded result must equal the unsharded result.
func TestShardCannotMatchSelector_Correctness(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
load 1m
	http_requests_total{cluster="c1", region="us", instance="a", job="api"} 0+1x20
	http_requests_total{cluster="c1", region="us", instance="b", job="api"} 0+2x20
	http_requests_total{cluster="c1", region="eu", instance="c", job="web"} 0+3x20
	http_requests_total{cluster="c2", region="us", instance="d", job="api"} 0+4x20
	http_requests_total{cluster="c2", region="eu", instance="e", job="web"} 0+5x20
	http_requests_total{cluster="c3", region="us", instance="f", job="api"} 0+6x20
	http_requests_total{region="us", instance="g", job="api"}               0+7x20
	up{cluster="c1", instance="a"} 1x20
	up{cluster="c1", instance="b"} 1x20
	up{cluster="c2", instance="d"} 1x20
	up{cluster="c3", instance="f"} 1x20
	up{cluster="c4", instance="h"} 1x20
	up{cluster="c5", instance="i"} 1x20
	env_info{cluster="c1"} 1x20
	env_info{cluster="c2"} 1x20
`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	engine := promql.NewEngine(promql.EngineOpts{MaxSamples: 1e6, Timeout: time.Minute})
	analyzer := querysharding.NewQueryAnalyzer()
	evalTime := time.Unix(0, 0).Add(15 * time.Minute)

	tests := []struct {
		query string
		// skip is true when at least one Select is skipped.
		skip bool
	}{
		{query: `up and on(cluster) up{cluster="c1"}`, skip: true},
		{query: `up and on(cluster) env_info{cluster="c1"}`, skip: true},
		{query: `up unless on(cluster) env_info{cluster="c2"}`, skip: true},
		{query: `up or on(cluster) env_info{cluster="c2"}`, skip: true},
		{query: `up * on(cluster) group_left env_info{cluster="c1"}`, skip: true},
		{query: `sum by (cluster) (rate(http_requests_total[5m])) / on(cluster) sum by (cluster) (rate(http_requests_total{cluster="c1"}[5m]))`, skip: true},
		{query: `sum by (cluster) (rate(http_requests_total{cluster="c1"}[5m]))`, skip: true},
		{query: `sum by (cluster, region) (rate(http_requests_total{cluster="c1", region="us"}[5m]))`, skip: true},
		{query: `count by (cluster) (http_requests_total{cluster="c1", job!="web"})`, skip: true},
		{query: `max_over_time(sum by (cluster) (rate(http_requests_total{cluster="c1"}[5m]))[10m:1m])`, skip: true},
		{query: `up and on(cluster) env_info`},
		{query: `up and on(cluster) env_info{cluster=~"c1"}`},
		{query: `sum by (cluster) (rate(http_requests_total{cluster=~"c1|c2"}[5m]))`},
		{query: `sum by (cluster) (rate(http_requests_total{cluster!="c1"}[5m]))`},
		{query: `sum by (cluster) (rate(http_requests_total{cluster=""}[5m]))`},
		{query: `sum without (instance) (rate(http_requests_total{cluster="c1"}[5m]))`},
	}

	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			analysis, err := analyzer.Analyze(tc.query)
			require.NoError(t, err)
			require.True(t, analysis.IsShardable())

			unsharded := evalInstant(t, engine, storage, tc.query, evalTime)
			require.NotEmpty(t, unsharded)

			queryable := &shardFilteringQueryable{Queryable: storage}
			var merged promql.Vector
			for shard := range int64(testTotalShards) {
				shardedQuery, err := InjectShardingInfo(tc.query, &storepb.ShardInfo{
					TotalShards: testTotalShards,
					ShardIndex:  shard,
					By:          analysis.ShardBy(),
					Labels:      analysis.ShardingLabels(),
				})
				require.NoError(t, err)
				merged = append(merged, evalInstant(t, engine, queryable, shardedQuery, evalTime)...)
			}
			sortVector(merged)

			require.Equal(t, unsharded, merged)
			if tc.skip {
				require.Positive(t, queryable.skipped.Load())
			} else {
				require.Zero(t, queryable.skipped.Load())
			}
		})
	}
}

func evalInstant(t *testing.T, engine *promql.Engine, queryable storage.Queryable, query string, ts time.Time) promql.Vector {
	t.Helper()
	q, err := engine.NewInstantQuery(context.Background(), queryable, nil, query, ts)
	require.NoError(t, err)
	defer q.Close()
	res := q.Exec(context.Background())
	require.NoError(t, res.Err)
	vector, err := res.Vector()
	require.NoError(t, err)
	sortVector(vector)
	return vector
}

func sortVector(v promql.Vector) {
	sort.Slice(v, func(i, j int) bool { return labels.Compare(v[i].Metric, v[j].Metric) < 0 })
}

// shardFilteringQueryable skips a Select that ShardCannotMatchSelector rejects, as the querier does,
// and otherwise applies the __CORTEX_SHARD_BY__ matcher the way ingesters and store-gateways do.
type shardFilteringQueryable struct {
	storage.Queryable
	skipped atomic.Int64
}

func (q *shardFilteringQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &shardFilteringQuerier{Querier: querier, parent: q}, nil
}

type shardFilteringQuerier struct {
	storage.Querier
	parent *shardFilteringQueryable
}

func (q *shardFilteringQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if ShardCannotMatchSelector(matchers) {
		q.parent.skipped.Add(1)
		return storage.EmptySeriesSet()
	}

	matchers, shardMatcher, err := ExtractShardingMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer shardMatcher.Close()

	set := q.Querier.Select(ctx, sortSeries, hints, matchers...)
	var series []storage.Series
	for set.Next() {
		if shardMatcher.MatchesLabels(set.At().Labels()) {
			series = append(series, set.At())
		}
	}
	if err := set.Err(); err != nil {
		return storage.ErrSeriesSet(err)
	}
	return &sliceSeriesSet{series: series, warnings: set.Warnings()}
}

type sliceSeriesSet struct {
	series   []storage.Series
	cur      int
	warnings annotations.Annotations
}

func (s *sliceSeriesSet) Next() bool {
	s.cur++
	return s.cur <= len(s.series)
}

func (s *sliceSeriesSet) At() storage.Series                { return s.series[s.cur-1] }
func (s *sliceSeriesSet) Err() error                        { return nil }
func (s *sliceSeriesSet) Warnings() annotations.Annotations { return s.warnings }
