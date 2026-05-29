package tsdb

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
)

func TestSplitMatchersForHead(t *testing.T) {
	ctx := context.Background()

	ir := &mockIndexReader{
		labelValues: map[string][]string{
			"__name__":  {"cpu", "memory", "disk"},
			"pod":       generateValues("pod-", 50000),
			"namespace": {"prod", "staging", "dev"},
			"service":   {"api", "worker", "gateway", "frontend", "backend"},
			"job":       {"api", "worker", "gateway"},
		},
		postingsCounts: map[string]int{
			"__name__\xffcpu":      1000,
			"__name__\xffmemory":   800,
			"service\xffapi":       200,
			"service\xffworker":    300,
			"namespace\xffprod":    500,
			"namespace\xffstaging": 300,
			"namespace\xffdev":     200,
		},
	}

	tests := []struct {
		name           string
		matchers       []*labels.Matcher
		maxCardinality int
		wantSelect     int
		wantLazy       int
		wantLazyLabels []string
	}{
		{
			name: "regex on high-cardinality label with selective equality matcher - deferred",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
				labels.MustNewMatcher(labels.MatchEqual, "service", "api"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*alan.*"),
			},
			maxCardinality: 10000,
			wantSelect:     2, // __name__ + service
			wantLazy:       1,
			wantLazyLabels: []string{"pod"},
		},
		{
			name: "regex on high-cardinality label with only __name__ equality - deferred (name is selective)",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*alan.*"),
			},
			maxCardinality: 10000,
			wantSelect:     1,
			wantLazy:       1,
			wantLazyLabels: []string{"pod"},
		},
		{
			name: "regex on low-cardinality label - NOT deferred regardless",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
				labels.MustNewMatcher(labels.MatchEqual, "service", "api"),
				labels.MustNewMatcher(labels.MatchRegexp, "namespace", "prod|staging"),
			},
			maxCardinality: 10000,
			wantSelect:     3, // namespace only has 3 values, below threshold
			wantLazy:       0,
		},
		{
			name: "no __name__ matcher - nothing deferred",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*alan.*"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "prod"),
			},
			maxCardinality: 10000,
			wantSelect:     2,
			wantLazy:       0,
		},
		{
			name: "disabled when maxCardinality is 0",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
				labels.MustNewMatcher(labels.MatchEqual, "service", "api"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*alan.*"),
			},
			maxCardinality: 0,
			wantSelect:     3,
			wantLazy:       0,
		},
		{
			name: "negative regex on high-cardinality with selective matcher - deferred",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "prod"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "pod", ".*test.*"),
			},
			maxCardinality: 10000,
			wantSelect:     2,
			wantLazy:       1,
			wantLazyLabels: []string{"pod"},
		},
		{
			name: "__name__ regex is never deferred",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
				labels.MustNewMatcher(labels.MatchEqual, "service", "api"),
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "cpu|memory"),
			},
			maxCardinality: 1,
			wantSelect:     3,
			wantLazy:       0,
		},
		{
			name: "single matcher - nothing deferred",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
			},
			maxCardinality: 1,
			wantSelect:     1,
			wantLazy:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectMs, lazyMs := splitMatchersForHeadWithConfig(ctx, ir, tt.matchers, lazyMatcherConfig{
				MaxCardinality: tt.maxCardinality,
				// Use ratio=1 so this test continues to assert on the old
				// (cardinality > minSelectPostings) gate semantics. Cost-ratio
				// behavior is covered separately in TestSplitMatchersForHead_CostRatio.
				SimpleRatio:  1,
				ComplexRatio: 1,
			}, newLabelCardinalityCache())
			assert.Len(t, selectMs, tt.wantSelect, "select matchers count")
			assert.Len(t, lazyMs, tt.wantLazy, "lazy matchers count")

			for i, name := range tt.wantLazyLabels {
				assert.Equal(t, name, lazyMs[i].Name)
			}
		})
	}
}

func TestFetchWithLazyMatchers(t *testing.T) {
	ctx := context.Background()

	// Build an in-memory head with known series
	ir := &mockIndexReaderWithSeries{
		mockIndexReader: mockIndexReader{
			labelValues: map[string][]string{
				"__name__": {"cpu"},
				"pod":      {"web-1", "web-2", "worker-1", "worker-2", "api-1"},
				"service":  {"frontend", "backend"},
			},
		},
		series: map[storage.SeriesRef]labels.Labels{
			1: labels.FromStrings("__name__", "cpu", "pod", "web-1", "service", "frontend"),
			2: labels.FromStrings("__name__", "cpu", "pod", "web-2", "service", "frontend"),
			3: labels.FromStrings("__name__", "cpu", "pod", "worker-1", "service", "backend"),
			4: labels.FromStrings("__name__", "cpu", "pod", "worker-2", "service", "backend"),
			5: labels.FromStrings("__name__", "cpu", "pod", "api-1", "service", "backend"),
		},
	}

	cache := &blocksPostingsForMatchersCache{
		postingsForMatchersFunc: func(_ context.Context, ix prom_tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
			// Simulate: selectMs = [__name__="cpu", service="frontend"] -> returns refs 1, 2
			return index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5}[:2]), nil
		},
	}

	selectMs := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
		labels.MustNewMatcher(labels.MatchEqual, "service", "frontend"),
	}
	lazyMs := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "pod", "web.*"),
	}

	refs, size, err := cache.fetchWithLazyMatchers(ctx, ir, selectMs, lazyMs)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(refs)*8), size)
	// Both series 1 and 2 have pod=web-*, so both should match
	assert.Equal(t, []storage.SeriesRef{1, 2}, refs)
}

func TestFetchWithLazyMatchers_FiltersCorrectly(t *testing.T) {
	ctx := context.Background()

	ir := &mockIndexReaderWithSeries{
		mockIndexReader: mockIndexReader{
			labelValues: map[string][]string{
				"pod": {"web-1", "worker-1", "web-2"},
			},
		},
		series: map[storage.SeriesRef]labels.Labels{
			1: labels.FromStrings("pod", "web-1"),
			2: labels.FromStrings("pod", "worker-1"),
			3: labels.FromStrings("pod", "web-2"),
		},
	}

	cache := &blocksPostingsForMatchersCache{
		postingsForMatchersFunc: func(_ context.Context, _ prom_tsdb.IndexReader, _ ...*labels.Matcher) (index.Postings, error) {
			return index.NewListPostings([]storage.SeriesRef{1, 2, 3}), nil
		},
	}

	selectMs := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
	}
	lazyMs := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "pod", "web.*"),
	}

	refs, _, err := cache.fetchWithLazyMatchers(ctx, ir, selectMs, lazyMs)
	assert.NoError(t, err)
	assert.Equal(t, []storage.SeriesRef{1, 3}, refs)
}

func TestFetchWithLazyMatchers_PropagatesLabelValueForError(t *testing.T) {
	ctx := context.Background()
	injectedErr := errors.New("injected disk error")

	ir := &mockIndexReaderWithError{
		mockIndexReaderWithSeries: mockIndexReaderWithSeries{
			mockIndexReader: mockIndexReader{
				labelValues:    map[string][]string{"pod": {"web-0", "web-1", "db-0"}},
				postingsCounts: map[string]int{"__name__\xffcpu": 3},
			},
			series: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("__name__", "cpu", "pod", "web-0"),
				2: labels.FromStrings("__name__", "cpu", "pod", "db-0"),
				3: labels.FromStrings("__name__", "cpu", "pod", "web-1"),
			},
		},
		errOnRef: 2,
		err:      injectedErr,
	}

	cache := &blocksPostingsForMatchersCache{
		postingsForMatchersFunc: func(_ context.Context, _ prom_tsdb.IndexReader, _ ...*labels.Matcher) (index.Postings, error) {
			return index.NewListPostings([]storage.SeriesRef{1, 2, 3}), nil
		},
	}
	selectMs := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu"),
	}
	lazyMs := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "pod", "web.*"),
	}

	_, _, err := cache.fetchWithLazyMatchers(ctx, ir, selectMs, lazyMs)
	assert.ErrorIs(t, err, injectedErr)
}

// mockIndexReaderWithError returns an error for a specific series ref.
type mockIndexReaderWithError struct {
	mockIndexReaderWithSeries
	errOnRef storage.SeriesRef
	err      error
}

func (m *mockIndexReaderWithError) LabelValueFor(ctx context.Context, id storage.SeriesRef, label string) (string, error) {
	if id == m.errOnRef {
		return "", m.err
	}
	return m.mockIndexReaderWithSeries.LabelValueFor(ctx, id, label)
}

// --- Mocks ---

type mockIndexReader struct {
	prom_tsdb.IndexReader
	labelValues    map[string][]string
	postingsCounts map[string]int // "name\xffvalue" -> count
}

func (m *mockIndexReader) LabelValues(_ context.Context, name string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, error) {
	return m.labelValues[name], nil
}

func (m *mockIndexReader) Close() error              { return nil }
func (m *mockIndexReader) Symbols() index.StringIter { return nil }
func (m *mockIndexReader) LabelNames(_ context.Context, _ ...*labels.Matcher) ([]string, error) {
	return nil, nil
}
func (m *mockIndexReader) SortedLabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, error) {
	return nil, nil
}
func (m *mockIndexReader) Postings(_ context.Context, name string, values ...string) (index.Postings, error) {
	if m.postingsCounts != nil && len(values) == 1 {
		key := name + "\xff" + values[0]
		if n, ok := m.postingsCounts[key]; ok {
			refs := make([]storage.SeriesRef, n)
			for i := range refs {
				refs[i] = storage.SeriesRef(i + 1)
			}
			return index.NewListPostings(refs), nil
		}
	}
	return index.EmptyPostings(), nil
}
func (m *mockIndexReader) PostingsForLabelMatching(_ context.Context, _ string, _ func(string) bool) index.Postings {
	return index.EmptyPostings()
}
func (m *mockIndexReader) PostingsForAllLabelValues(_ context.Context, _ string) index.Postings {
	return index.EmptyPostings()
}
func (m *mockIndexReader) SortedPostings(p index.Postings) index.Postings               { return p }
func (m *mockIndexReader) ShardedPostings(p index.Postings, _, _ uint64) index.Postings { return p }
func (m *mockIndexReader) Series(_ storage.SeriesRef, _ *labels.ScratchBuilder, _ *[]chunks.Meta) error {
	return nil
}
func (m *mockIndexReader) LabelValueFor(_ context.Context, _ storage.SeriesRef, _ string) (string, error) {
	return "", storage.ErrNotFound
}
func (m *mockIndexReader) LabelNamesFor(_ context.Context, _ index.Postings) ([]string, error) {
	return nil, nil
}

// mockIndexReaderWithSeries extends mockIndexReader with series label data
type mockIndexReaderWithSeries struct {
	mockIndexReader
	series map[storage.SeriesRef]labels.Labels
}

func (m *mockIndexReaderWithSeries) LabelValueFor(_ context.Context, id storage.SeriesRef, label string) (string, error) {
	lbls, ok := m.series[id]
	if !ok {
		return "", storage.ErrNotFound
	}
	v := lbls.Get(label)
	if v == "" {
		return "", storage.ErrNotFound
	}
	return v, nil
}

func generateValues(prefix string, count int) []string {
	vals := make([]string, count)
	for i := range vals {
		vals[i] = prefix + string(rune('0'+i%10)) + string(rune('0'+i/10%10))
	}
	return vals
}

// TestRegexCostClass verifies the complexity classifier we use to choose
// the cardinality:postings ratio gate. The classifier MUST agree with the
// fast-path semantics in postingsForMatcher: regexes that prometheus would
// short-circuit via setMatches or prefix-only matching are "simple" (cheap
// per-call); everything else (multi-substring contains, captures, character
// classes) is "complex" (expensive per-call, lazy iteration wins at lower
// cardinality:postings ratio).
func TestRegexCostClass(t *testing.T) {
	cases := []struct {
		name      string
		matcher   *labels.Matcher
		wantClass regexCost
	}{
		{
			"prefix only - cheap containsInOrder fast-reject",
			labels.MustNewMatcher(labels.MatchRegexp, "x", "foo.*"),
			regexCostSimple,
		},
		{
			"single contains - moderate",
			labels.MustNewMatcher(labels.MatchRegexp, "x", ".*foo.*"),
			regexCostSimple,
		},
		{
			"multi-substring contains - complex",
			labels.MustNewMatcher(labels.MatchRegexp, "x", ".*foo.*bar.*"),
			regexCostComplex,
		},
		{
			"capture group + literal - complex",
			labels.MustNewMatcher(labels.MatchRegexp, "x", "(.+)-(.+)-(.+)"),
			regexCostComplex,
		},
		{
			"alternation of contains - complex",
			labels.MustNewMatcher(labels.MatchRegexp, "x", ".*a.*|.*b.*|.*c.*"),
			regexCostComplex,
		},
		{
			"plain anchored regex with character class - complex",
			labels.MustNewMatcher(labels.MatchRegexp, "x", "^foo[0-9]+$"),
			regexCostComplex,
		},
		{
			"NotRegexp single contains - moderate",
			labels.MustNewMatcher(labels.MatchNotRegexp, "x", ".*foo.*"),
			regexCostSimple,
		},
		{
			"NotRegexp multi-substring - complex",
			labels.MustNewMatcher(labels.MatchNotRegexp, "x", ".*a.*b.*"),
			regexCostComplex,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := regexCostClass(tc.matcher)
			assert.Equal(t, tc.wantClass, got, "%s: got %v want %v", tc.matcher.String(), got, tc.wantClass)
		})
	}
}

// TestSplitMatchersForHead_ZeroRatioUsesDefaults verifies that programmatic
// callers who construct lazyMatcherConfig without going through flag
// registration get the calibrated defaults (6 and 2), NOT a clamped 1 (which
// would silently re-introduce the original broken gate). See the regexCost
// docstring for the cost model.
func TestSplitMatchersForHead_ZeroRatioUsesDefaults(t *testing.T) {
	ctx := context.Background()
	ir := &mockIndexReader{
		labelValues: map[string][]string{
			"__name__": {"metric_a"},
			"pod":      generateValues("pod-", 30000),
		},
		postingsCounts: map[string]int{
			"__name__\xffmetric_a": 10000,
		},
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric_a"),
		// simple regex: prefix-only.
		labels.MustNewMatcher(labels.MatchRegexp, "pod", "foo.*"),
	}
	// Cardinality:postings ratio = 30000/10000 = 3.
	// With ratio=1 (clamped), 3 > 1 → would defer.
	// With default ratio=6, 3 > 6 is false → must NOT defer.
	cfg := lazyMatcherConfig{
		MaxCardinality: 1000,
		// SimpleRatio and ComplexRatio left zero — must use defaults.
	}
	_, lazyMs := splitMatchersForHeadWithConfig(ctx, ir, matchers, cfg, newLabelCardinalityCache())
	assert.Len(t, lazyMs, 0, "zero SimpleRatio must default to %d, not be clamped to 1", defaultSimpleCostRatio)
}

// The original gate was `cardinality > minSelectPostings`, which incorrectly
// deferred regex evaluation when LabelValueFor (per-series) cost would exceed
// PostingsForLabelMatching (per-value) cost. The fixed gate is
// `cardinality > minSelectPostings * ratio` where ratio depends on regex cost
// class:
//   - simple regex (prefix-only / single contains): ratio=6 (LabelValueFor is
//     ~5x more expensive than a fast-path regex evaluation, +1 margin)
//   - complex regex (multi-substring, capture, char class): ratio=2 (per-call
//     regex cost is high enough that lazy wins at lower ratio)
func TestSplitMatchersForHead_CostRatio(t *testing.T) {
	const (
		simpleRatio  = 6
		complexRatio = 2
	)
	// Build an index where __name__=metric_a has 10000 series and pod has
	// varying cardinalities to test the gate.
	build := func(podCard int) *mockIndexReader {
		return &mockIndexReader{
			labelValues: map[string][]string{
				"__name__": {"metric_a"},
				"pod":      generateValues("pod-", podCard),
			},
			postingsCounts: map[string]int{
				"__name__\xffmetric_a": 10000,
			},
		}
	}

	cases := []struct {
		name          string
		podCard       int
		regex         string
		simpleRatio   int
		complexRatio  int
		wantLazyCount int
	}{
		{
			// 20K cardinality, 10K postings → ratio = 2 → for SIMPLE regex this
			// is below the 6x threshold; should NOT defer (this is the
			// `balanced_select` failure mode the original code triggered).
			name:          "simple regex 2x ratio - NOT deferred (cost gate)",
			podCard:       20000,
			regex:         "foo.*",
			simpleRatio:   simpleRatio,
			complexRatio:  complexRatio,
			wantLazyCount: 0,
		},
		{
			// 100K cardinality, 10K postings → ratio = 10 → above 6x; defer.
			name:          "simple regex 10x ratio - deferred",
			podCard:       100000,
			regex:         "foo.*",
			simpleRatio:   simpleRatio,
			complexRatio:  complexRatio,
			wantLazyCount: 1,
		},
		{
			// 25K cardinality, 10K postings → ratio = 2.5 → above complex
			// threshold of 2; defer.
			name:          "complex regex 2.5x ratio - deferred",
			podCard:       25000,
			regex:         ".*foo.*bar.*",
			simpleRatio:   simpleRatio,
			complexRatio:  complexRatio,
			wantLazyCount: 1,
		},
		{
			// 15K cardinality, 10K postings → ratio = 1.5 → below complex
			// threshold of 2; do NOT defer.
			name:          "complex regex 1.5x ratio - NOT deferred",
			podCard:       15000,
			regex:         ".*foo.*bar.*",
			simpleRatio:   simpleRatio,
			complexRatio:  complexRatio,
			wantLazyCount: 0,
		},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ir := build(tc.podCard)
			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric_a"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", tc.regex),
			}
			cfg := lazyMatcherConfig{
				MaxCardinality: 1000, // engage threshold (< all podCard above)
				SimpleRatio:    tc.simpleRatio,
				ComplexRatio:   tc.complexRatio,
			}
			_, lazyMs := splitMatchersForHeadWithConfig(ctx, ir, matchers, cfg, newLabelCardinalityCache())
			assert.Len(t, lazyMs, tc.wantLazyCount,
				"podCard=%d regex=%q: lazyCount mismatch", tc.podCard, tc.regex)
		})
	}
}
