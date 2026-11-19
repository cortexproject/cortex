package querysharding

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	type testCase struct {
		name           string
		selector       string
		by             bool
		shardingLabels []string
		// values lists the sharding label sets the selected series can carry. The shards that keep the
		// selector must be exactly the shards of these label sets. Nil means every shard keeps it.
		values []labels.Labels
	}
	c := func(cluster string) labels.Labels { return labels.FromStrings("cluster", cluster) }
	cr := func(cluster, region string) labels.Labels {
		return labels.FromStrings("cluster", cluster, "region", region)
	}

	tests := []testCase{
		// Skip cases.
		{name: "pinned sharding label", selector: `up{cluster="c1"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1")}},
		{name: "pinned label with extra regex and negative matchers", selector: `up{cluster="c1", cluster=~"c.*", job!="a"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1")}},
		{name: "all of multiple sharding labels pinned", selector: `up{cluster="c1", region="us"}`, by: true, shardingLabels: []string{"cluster", "region"}, values: []labels.Labels{cr("c1", "us")}},
		{name: "duplicated sharding label", selector: `up{cluster="c1"}`, by: true, shardingLabels: []string{"cluster", "cluster"}, values: []labels.Labels{c("c1")}},
		{name: "metric name as sharding label", selector: `up`, by: true, shardingLabels: []string{labels.MetricName}, values: []labels.Labels{labels.FromStrings(labels.MetricName, "up")}},
		{name: "same value pinned twice", selector: `up{cluster="c1", cluster="c1"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1")}},
		{name: "regex literal", selector: `up{cluster=~"c1"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1")}},
		{name: "regex alternation", selector: `up{cluster=~"c1|c2"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1"), c("c2")}},
		{name: "regex alternation in a group", selector: `up{cluster=~"(c1|c2)"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1"), c("c2")}},
		{name: "regex character class", selector: `up{cluster=~"c[12]"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1"), c("c2")}},
		{name: "regex alternation narrowed by equality", selector: `up{cluster=~"c1|c2", cluster="c1"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1")}},
		{name: "regex alternation narrowed by negative matcher", selector: `up{cluster=~"c1|c2", cluster!="c2"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1")}},
		{name: "regex alternation narrowed by negative regex", selector: `up{cluster=~"c1|c2|c3", cluster!~"c2|c3"}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1")}},
		{name: "regex alternation with the empty value excluded", selector: `up{cluster=~"c1|", cluster!=""}`, by: true, shardingLabels: []string{"cluster"}, values: []labels.Labels{c("c1")}},
		{name: "regex alternations on multiple sharding labels", selector: `up{cluster=~"c1|c2", region=~"us|eu"}`, by: true, shardingLabels: []string{"cluster", "region"}, values: []labels.Labels{cr("c1", "eu"), cr("c1", "us"), cr("c2", "eu"), cr("c2", "us")}},
		// No-skip cases.
		{name: "missing matcher", selector: `up`, by: true, shardingLabels: []string{"cluster"}},
		{name: "regex that is not a set of literals", selector: `up{cluster=~"c.*"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "case-insensitive regex", selector: `up{cluster=~"(?i)c1"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "regex alternation that includes the empty value", selector: `up{cluster=~"c1|"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "negative matcher", selector: `up{cluster!="c1"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "negative regex matcher", selector: `up{cluster!~"c1|c2"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "empty value also selects series without the label", selector: `up{cluster=""}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "conflicting values select nothing", selector: `up{cluster="c1", cluster="c2"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "regex and equality that do not overlap", selector: `up{cluster=~"c1|c2", cluster="c3"}`, by: true, shardingLabels: []string{"cluster"}},
		{name: "one of multiple sharding labels not restricted", selector: `up{cluster="c1"}`, by: true, shardingLabels: []string{"cluster", "region"}},
		{name: "without sharding", selector: `up{cluster="c1"}`, by: false, shardingLabels: []string{"cluster"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expected := map[int64]struct{}{}
			for shard := range int64(testTotalShards) {
				info := &storepb.ShardInfo{TotalShards: testTotalShards, ShardIndex: shard, By: tc.by, Labels: tc.shardingLabels}
				if tc.values == nil {
					expected[shard] = struct{}{}
					continue
				}
				matcher := info.Matcher(&Buffers)
				for _, v := range tc.values {
					if matcher.MatchesLabels(v) {
						expected[shard] = struct{}{}
					}
				}
				matcher.Close()
			}

			kept := map[int64]struct{}{}
			for shard := range int64(testTotalShards) {
				matchers := shardedMatchers(t, tc.selector, &storepb.ShardInfo{
					TotalShards: testTotalShards,
					ShardIndex:  shard,
					By:          tc.by,
					Labels:      tc.shardingLabels,
				})
				if !ShardCannotMatchSelector(matchers) {
					kept[shard] = struct{}{}
				}
			}
			require.Equal(t, expected, kept)
			if tc.values != nil {
				require.Less(t, len(kept), testTotalShards, "a skip case must skip at least one shard")
			}
		})
	}
}

func TestShardCannotMatchSelector_TooManyCombinations(t *testing.T) {
	alternation := func(prefix string, n int) string {
		values := make([]string, 0, n)
		for i := range n {
			values = append(values, fmt.Sprintf("%s%d", prefix, i))
		}
		return strings.Join(values, "|")
	}
	shardingLabels := []string{"cluster", "region"}

	// 16 x 16 = 256 combinations are checked. 17 x 16 = 272 are not.
	checked := fmt.Sprintf(`up{cluster=~%q, region=~%q}`, alternation("c", 16), alternation("r", 16))
	notChecked := fmt.Sprintf(`up{cluster=~%q, region=~%q}`, alternation("c", 17), alternation("r", 16))

	// With 1000 shards, 256 combinations cannot cover every shard, so some shards are skipped.
	const totalShards = 1000
	skippedChecked, skippedNotChecked := 0, 0
	for shard := range int64(totalShards) {
		info := &storepb.ShardInfo{TotalShards: totalShards, ShardIndex: shard, By: true, Labels: shardingLabels}
		if ShardCannotMatchSelector(shardedMatchers(t, checked, info)) {
			skippedChecked++
		}
		if ShardCannotMatchSelector(shardedMatchers(t, notChecked, info)) {
			skippedNotChecked++
		}
	}
	require.Positive(t, skippedChecked)
	require.Zero(t, skippedNotChecked)
}

func TestShardCannotMatchSelector_NotSharded(t *testing.T) {
	require.False(t, ShardCannotMatchSelector([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "cluster", "c1")}))
	require.False(t, ShardCannotMatchSelector(shardedMatchers(t, `up{cluster="c1"}`, &storepb.ShardInfo{TotalShards: 0, By: true, Labels: []string{"cluster"}})))
	require.False(t, ShardCannotMatchSelector(shardedMatchers(t, `up{cluster="c1"}`, &storepb.ShardInfo{TotalShards: 1, ShardIndex: 0, By: true, Labels: []string{"cluster"}})))
	require.False(t, ShardCannotMatchSelector([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, CortexShardByLabel, "not-base64!")}))
}

// TestShardCannotMatchSelector_AgreesWithShardMatcher checks that the shard the function keeps is the
// shard storepb.ShardMatcher assigns to the selected series, whatever their other labels are.
func TestShardCannotMatchSelector_AgreesWithShardMatcher(t *testing.T) {
	shardingLabels := []string{"region", "cluster"}
	for totalShards := int64(1); totalShards <= 8; totalShards++ {
		for i := range 50 {
			cluster, region := fmt.Sprintf("cluster-%d", i), fmt.Sprintf("region-%d", i%3)
			otherCluster := fmt.Sprintf("cluster-%d", i+1000)
			selector := fmt.Sprintf(`up{cluster=%q, region=%q}`, cluster, region)
			regexSelector := fmt.Sprintf(`up{cluster=~"%s|%s", region=%q}`, cluster, otherCluster, region)
			series := labels.FromStrings(labels.MetricName, "up", "cluster", cluster, "instance", fmt.Sprintf("i-%d", i), "region", region)
			otherSeries := labels.FromStrings(labels.MetricName, "up", "cluster", otherCluster, "instance", fmt.Sprintf("i-%d", i), "region", region)

			for shard := range totalShards {
				info := &storepb.ShardInfo{TotalShards: totalShards, ShardIndex: shard, By: true, Labels: shardingLabels}
				matcher := info.Matcher(&Buffers)
				seriesInShard := matcher.MatchesLabels(series)
				otherSeriesInShard := matcher.MatchesLabels(otherSeries)
				matcher.Close()

				require.Equal(t, seriesInShard, !ShardCannotMatchSelector(shardedMatchers(t, selector, info)))
				require.Equal(t, seriesInShard || otherSeriesInShard, !ShardCannotMatchSelector(shardedMatchers(t, regexSelector, info)))
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
		{query: `up and on(cluster) env_info{cluster=~"c1"}`, skip: true},
		{query: `up and on(cluster) env_info{cluster=~"c1|c2"}`, skip: true},
		{query: `up unless on(cluster) env_info{cluster="c2"}`, skip: true},
		{query: `up or on(cluster) env_info{cluster="c2"}`, skip: true},
		{query: `up * on(cluster) group_left env_info{cluster="c1"}`, skip: true},
		{query: `sum by (cluster) (rate(http_requests_total[5m])) / on(cluster) sum by (cluster) (rate(http_requests_total{cluster="c1"}[5m]))`, skip: true},
		{query: `sum by (cluster) (rate(http_requests_total{cluster="c1"}[5m]))`, skip: true},
		{query: `sum by (cluster) (rate(http_requests_total{cluster=~"c1|c2"}[5m]))`, skip: true},
		{query: `sum by (cluster) (rate(http_requests_total{cluster=~"c[12]", cluster!="c2"}[5m]))`, skip: true},
		{query: `sum by (cluster, region) (rate(http_requests_total{cluster="c1", region="us"}[5m]))`, skip: true},
		{query: `sum by (cluster, region) (rate(http_requests_total{cluster=~"c1|c2", region=~"us|eu"}[5m]))`, skip: true},
		{query: `count by (cluster) (http_requests_total{cluster="c1", job!="web"})`, skip: true},
		{query: `max_over_time(sum by (cluster) (rate(http_requests_total{cluster="c1"}[5m]))[10m:1m])`, skip: true},
		{query: `up and on(cluster) env_info`},
		{query: `up and on(cluster) env_info{cluster=~"c.*"}`},
		{query: `sum by (cluster) (rate(http_requests_total{cluster=~"(?i)C1"}[5m]))`},
		{query: `sum by (cluster) (rate(http_requests_total{cluster=~"c1|"}[5m]))`},
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
