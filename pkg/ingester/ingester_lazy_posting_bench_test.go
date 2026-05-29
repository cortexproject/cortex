package ingester

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// BenchmarkIngester_LazyPosting exercises the lazy-matcher-max-cardinality
// optimization on the head block cache miss path.
//
// The scenarios are based on the benchmark configurations below:
//   - "small_select_huge_regex_label": __name__ selects 1% of series, regex
//     targets a 100K-cardinality label. This is the primary case the lazy
//     matcher optimization is designed for; it should be a clear win.
//   - "balanced_select_huge_regex_label": __name__ selects 50% of series.
//     Lazy LabelValueFor calls are now done on a large set; could go either way.
//   - "small_select_low_card_regex": regex label has only 100 distinct values.
//     Below the cardinality threshold; optimization should not engage and
//     overhead must be near zero.
//   - "small_select_complex_regex": __name__ selects 1%, but regex is complex
//     (.*lit.*lit.*) — exercises both the cardinality saving and the cost of
//     running complex regex per-series during lazy filter.
//
// Each scenario runs:
//   - cache_disabled    : baseline; no cache, eager regex
//   - cache_enabled_eager  : cache enabled, lazy disabled (current default)
//   - cache_enabled_lazy   : cache enabled + lazy-matcher threshold
//
// The benchmark forces a cache miss by clearing the seed before each iteration.
func BenchmarkIngester_LazyPosting(b *testing.B) {
	scenarios := []struct {
		name string
		// Series cardinality knobs
		nameValues          int // number of distinct __name__ values
		seriesPerName       int // series per metric name (drives __name__= selectivity)
		podCardinality      int // distinct pod values across all series
		podSharedAcrossName bool
		// Query
		matchers      []*labels.Matcher
		expectedHits  int // sanity check; >0 means some series matched
		expectMatches bool
	}{
		{
			name:                "small_select_huge_regex_label",
			nameValues:          50,   // 50 distinct metric names
			seriesPerName:       2000, // each metric has 2000 series → __name__=cpu selects 2K of 100K
			podCardinality:      100000,
			podSharedAcrossName: false,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric_0"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*pod-1.*"),
			},
			expectMatches: true,
		},
		{
			name:                "balanced_select_huge_regex_label",
			nameValues:          2,     // only 2 metric names
			seriesPerName:       50000, // each has 50K series → __name__= selects 50K of 100K
			podCardinality:      100000,
			podSharedAcrossName: false,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric_0"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*pod-1.*"),
			},
			expectMatches: true,
		},
		{
			name:                "small_select_low_card_regex",
			nameValues:          50,
			seriesPerName:       2000,
			podCardinality:      100, // below default threshold
			podSharedAcrossName: true,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric_0"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*pod-1.*"),
			},
			expectMatches: true,
		},
		{
			name:                "small_select_complex_regex",
			nameValues:          50,
			seriesPerName:       2000,
			podCardinality:      100000,
			podSharedAcrossName: false,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric_0"),
				// .*foo.*bar.* — multi-substring contains; complex per-call
				labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*pod.*1.*"),
			},
			expectMatches: true,
		},
		{
			name:                "small_select_capture_regex",
			nameValues:          50,
			seriesPerName:       2000,
			podCardinality:      100000,
			podSharedAcrossName: false,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric_0"),
				// (foo|bar).* — capture group alternation, common in envoy_authority
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "(pod-1|pod-2).*"),
			},
			expectMatches: true,
		},
	}

	configs := []struct {
		name            string
		cacheEnabled    bool
		lazyCardinality int
	}{
		{name: "no_cache", cacheEnabled: false, lazyCardinality: 0},
		{name: "cache_eager", cacheEnabled: true, lazyCardinality: 0},
		{name: "cache_lazy_10k", cacheEnabled: true, lazyCardinality: 10000},
		{name: "cache_lazy_1k", cacheEnabled: true, lazyCardinality: 1000},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			for _, cfg := range configs {
				b.Run(cfg.name, func(b *testing.B) {
					runLazyPostingBenchmark(b, sc.nameValues, sc.seriesPerName, sc.podCardinality,
						sc.podSharedAcrossName, cfg.cacheEnabled, cfg.lazyCardinality, sc.matchers,
						sc.expectMatches)
				})
			}
		})
	}
}

// BenchmarkIngester_LazyPosting_CacheHit measures cache-hit overhead.
// The lazy optimization must not slow down the hit path.
func BenchmarkIngester_LazyPosting_CacheHit(b *testing.B) {
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric_0"),
		labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*pod-1.*"),
	}

	for _, lazy := range []int{0, 10000} {
		name := "eager"
		if lazy > 0 {
			name = "lazy"
		}
		b.Run(name, func(b *testing.B) {
			runCacheHitBenchmark(b, 50, 2000, 100000, matchers, lazy)
		})
	}
}

func runLazyPostingBenchmark(
	b *testing.B,
	nameValues, seriesPerName, podCardinality int,
	podSharedAcrossName bool,
	cacheEnabled bool,
	lazyCardinality int,
	matchers []*labels.Matcher,
	expectMatches bool,
) {
	b.Helper()
	const userID = "test"
	cfg := defaultIngesterTestConfig(b)
	if cacheEnabled {
		cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.Enabled = true
		cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.MaxBytes = 100 * 1024 * 1024
		cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.Ttl = time.Hour
		cfg.BlocksStorageConfig.TSDB.PostingsCache.LazyMatcherMaxCardinality = lazyCardinality
	}

	i, err := prepareIngesterWithBlocksStorage(b, cfg, prometheus.NewRegistry())
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	defer func() { _ = services.StopAndAwaitTerminated(context.Background(), i) }()

	test.Poll(b, time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

	ctx := user.InjectOrgID(context.Background(), userID)
	pushSeries(b, i, ctx, nameValues, seriesPerName, podCardinality, podSharedAcrossName)

	db, err := i.getTSDB(userID)
	require.NoError(b, err)
	require.NotNil(b, db)

	mockStream := &mockQueryStreamServer{ctx: ctx}
	sm := (&storepb.ShardInfo{TotalShards: 0}).Matcher(nil)

	// Warm up once and validate
	numSeries, _, _, _, err := i.queryStreamChunks(ctx, userID, db, 0, 5000, matchers, sm, mockStream)
	require.NoError(b, err)
	if expectMatches {
		require.Greater(b, numSeries, 0, "scenario must produce matches")
	}
	mockStream.series = mockStream.series[:0]

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// Force cache miss by mutating the seed for this metric name.
		// The seed-by-hash map is keyed by (userID, metricName); we bump it
		// to invalidate any cached promise for this query.
		if cacheEnabled && db.postingCache != nil {
			db.postingCache.ExpireSeries(labels.FromStrings(model.MetricNameLabel, "metric_0"))
		}
		_, _, _, _, err := i.queryStreamChunks(ctx, userID, db, 0, 5000, matchers, sm, mockStream)
		require.NoError(b, err)
		mockStream.series = mockStream.series[:0]
	}
}

func runCacheHitBenchmark(b *testing.B, nameValues, seriesPerName, podCardinality int, matchers []*labels.Matcher, lazyCardinality int) {
	const userID = "test"
	cfg := defaultIngesterTestConfig(b)
	cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.Enabled = true
	cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.MaxBytes = 100 * 1024 * 1024
	cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.Ttl = time.Hour
	cfg.BlocksStorageConfig.TSDB.PostingsCache.LazyMatcherMaxCardinality = lazyCardinality

	i, err := prepareIngesterWithBlocksStorage(b, cfg, prometheus.NewRegistry())
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	defer func() { _ = services.StopAndAwaitTerminated(context.Background(), i) }()
	test.Poll(b, time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

	ctx := user.InjectOrgID(context.Background(), userID)
	pushSeries(b, i, ctx, nameValues, seriesPerName, podCardinality, false)

	db, err := i.getTSDB(userID)
	require.NoError(b, err)

	mockStream := &mockQueryStreamServer{ctx: ctx}
	sm := (&storepb.ShardInfo{TotalShards: 0}).Matcher(nil)

	// Prime the cache
	_, _, _, _, err = i.queryStreamChunks(ctx, userID, db, 0, 5000, matchers, sm, mockStream)
	require.NoError(b, err)
	mockStream.series = mockStream.series[:0]

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _, _, _, err := i.queryStreamChunks(ctx, userID, db, 0, 5000, matchers, sm, mockStream)
		require.NoError(b, err)
		mockStream.series = mockStream.series[:0]
	}
}

// pushSeries creates `nameValues` distinct __name__ values, each with `seriesPerName`
// series. Each series gets a unique pod label drawn from `podCardinality` distinct values.
// When podSharedAcrossName is true, the same pod values are reused across name values
// (otherwise pods are distinct per name to inflate label cardinality).
//
// Pushes one series at a time using writeRequestSingleSeries, which is the
// proven-working pattern in the existing benchmarks. Slow but reliable.
func pushSeries(b *testing.B, i *Ingester, ctx context.Context, nameValues, seriesPerName, podCardinality int, podSharedAcrossName bool) {
	b.Helper()
	sample := []cortexpb.Sample{{Value: 1, TimestampMs: 1}}
	for n := range nameValues {
		metric := fmt.Sprintf("metric_%d", n)
		for s := range seriesPerName {
			var podIdx int
			if podSharedAcrossName {
				podIdx = s % podCardinality
			} else {
				podIdx = (n*seriesPerName + s) % podCardinality
			}
			lbls := labels.FromStrings(
				model.MetricNameLabel, metric,
				"pod", "pod-"+strconv.Itoa(podIdx),
				"region", "region-"+strconv.Itoa(s%10),
				"job", "job-"+strconv.Itoa(s%20),
			)
			_, err := i.Push(ctx, writeRequestSingleSeries(lbls, sample))
			require.NoError(b, err)
		}
	}
}
