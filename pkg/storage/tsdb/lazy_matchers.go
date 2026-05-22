package tsdb

import (
	"context"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

// regexCost classifies how expensive per-call evaluation of a regex matcher
// is, relative to a single LabelValueFor call. We use this to choose the
// cardinality:postings ratio gate that decides whether to defer a regex
// matcher to lazy iteration on the head block.
//
// Calibration is empirical, from BenchmarkIngester_LazyPosting:
//   - LabelValueFor on the head ≈ 1µs per call (lock + map lookup + label parse)
//   - simple regex eval (prefix-only / single contains via FastRegexMatcher
//     fast paths) ≈ 200ns per call
//   - complex regex eval (multi-substring containsInOrder, capture groups,
//     character classes - falls through to RE2 or multi-Index) ≈ 1µs+ per call
//
// Cost model:
//
//	eager_cost  ≈ cardinality        * regex_per_call_cost
//	lazy_cost   ≈ selective_postings * (LabelValueFor_cost + regex_per_call_cost)
//
// Lazy is a win when:
//
//	selective_postings * (LV + regex) < cardinality * regex
//	⇒ cardinality / selective_postings > (LV + regex) / regex
//
// For simple regex (regex ≈ 200ns, LV ≈ 1µs): ratio > 6 (with margin → 6).
// For complex regex (regex ≈ 1µs, LV ≈ 1µs):  ratio > 2.
type regexCost int

const (
	// regexCostUnknown is a defensive sentinel returned by regexCostClass for
	// non-regex matchers. Production code never reaches this path (callers
	// type-check before invoking).
	regexCostUnknown regexCost = iota
	// regexCostSimple covers regexes that the FastRegexMatcher fast-paths via
	// setMatches, prefix anchoring, suffix anchoring, or single-contains. Per-call
	// cost is dominated by the underlying string op, not RE2 evaluation.
	regexCostSimple
	// regexCostComplex covers everything else: multi-substring contains
	// (.*a.*b.*), alternation of contains, capture groups with siblings,
	// character classes, lookaheads, etc. Per-call cost includes the full RE2
	// fallback or multi-step containsInOrder.
	regexCostComplex
)

// Calibrated default cost ratios. Used both as struct field defaults and by
// the flag registration in TSDBPostingsCacheConfig.RegisterFlagsWithPrefix.
// See regexCost docstring for derivation.
const (
	defaultSimpleCostRatio  = 6
	defaultComplexCostRatio = 2
)

// regexCostClass returns the per-call cost class of a regex matcher.
//
// Notes:
//   - Matchers with non-empty SetMatches() are short-circuited by the
//     postingsForMatcher fast-path and never reach our lazy code, but we
//     classify them as simple for safety.
//   - Matchers with a non-empty Prefix() are also fast-pathed differently
//     in postingsForLabelMatching (containsInOrder fast-reject), so they're
//     classified as simple.
//   - Negative regex (MatchNotRegexp) is classified the same as MatchRegexp
//     since the per-call evaluation cost is identical.
func regexCostClass(m *labels.Matcher) regexCost {
	if m.Type != labels.MatchRegexp && m.Type != labels.MatchNotRegexp {
		return regexCostUnknown
	}

	v := m.GetRegexString()

	// Prefix-only regex (e.g. `foo.*`): the FastRegexMatcher uses HasPrefix
	// per call. But a regex like `^foo[0-9]+$` ALSO has Prefix()=="foo"
	// while requiring full RE2 evaluation on positive matches. We can
	// distinguish by checking the regex string: the prefix is "simple" only
	// when the remainder of the regex is trivially-matching (.* or empty).
	if p := m.Prefix(); p != "" {
		if isPureLiteralPrefix(v, p) {
			return regexCostSimple
		}
		// Prefix exists but the remainder is non-trivial — RE2 still runs
		// on positive matches.
		return regexCostComplex
	}

	// At this point the regex has no setMatches and no prefix. Use the regex
	// string to detect the remaining "simple" shapes the FastRegexMatcher
	// optimizes specially.
	switch {
	case isSingleContainsRegex(v):
		// .*foo.* — vanilla extracts m.contains=["foo"], runs containsInOrder
		// once per value (single strings.Index call). Per-call ≈ regex cost.
		return regexCostSimple
	case isPureSuffixRegex(v):
		// .*foo — extracted as m.suffix; HasSuffix per call.
		return regexCostSimple
	}

	return regexCostComplex
}

// isPureLiteralPrefix returns true when the regex string is just <prefix>
// optionally followed by `.*` or `.*$` (trivial tail). This is the
// pattern shape the FastRegexMatcher fully fast-paths via HasPrefix
// without falling through to RE2.
func isPureLiteralPrefix(regex, prefix string) bool {
	// Strip optional ^ anchor.
	r := strings.TrimPrefix(regex, "^")
	// The regex must start with the literal prefix.
	if !strings.HasPrefix(r, prefix) {
		return false
	}
	rest := r[len(prefix):]
	// Strip optional $ anchor.
	rest = strings.TrimSuffix(rest, "$")
	// Trailing must be empty (anchored exact prefix), `.*` (any tail), or
	// `.+` (any non-empty tail). Anything else (character class, alternation,
	// nested groups, additional literals) requires the full regex engine.
	return rest == "" || rest == ".*" || rest == ".+"
}

// isSingleContainsRegex returns true for `.*<literal>.*` patterns where
// <literal> contains no regex metacharacters.
func isSingleContainsRegex(s string) bool {
	if !strings.HasPrefix(s, ".*") || !strings.HasSuffix(s, ".*") || len(s) <= 4 {
		return false
	}
	inner := s[2 : len(s)-2]
	return inner != "" && !containsRegexMeta(inner)
}

// isPureSuffixRegex returns true for `.*<literal>` patterns where <literal>
// contains no regex metacharacters and the pattern has no trailing .*
// (otherwise it's single-contains).
func isPureSuffixRegex(s string) bool {
	if !strings.HasPrefix(s, ".*") || strings.HasSuffix(s, ".*") {
		return false
	}
	return !containsRegexMeta(s[2:])
}

// containsRegexMeta reports whether s contains any regex metacharacter.
func containsRegexMeta(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.', '+', '*', '?', '|', '(', ')', '[', ']', '{', '}', '\\', '^', '$':
			return true
		}
	}
	return false
}

// lazyMatcherConfig configures the cost-ratio gates used by
// splitMatchersForHeadWithConfig. Zero-valued SimpleRatio/ComplexRatio
// fields are treated as "use the calibrated default" (defaultSimpleCostRatio
// and defaultComplexCostRatio respectively), NOT as "no margin" — guarding
// callers who construct the config struct programmatically without going
// through flag registration.
type lazyMatcherConfig struct {
	// MaxCardinality is the floor cardinality below which a label is never
	// considered for lazy evaluation, regardless of selectivity.
	MaxCardinality int
	// SimpleRatio is the cardinality:postings ratio above which simple regex
	// matchers are deferred. Tuned empirically; see regexCostClass docs.
	// 0 means "use defaultSimpleCostRatio".
	SimpleRatio int
	// ComplexRatio is the cardinality:postings ratio above which complex regex
	// matchers are deferred. 0 means "use defaultComplexCostRatio".
	ComplexRatio int
}

// splitMatchersForHeadWithConfig separates matchers into those used for postings
// lookup and those applied lazily during iteration, using the configured cost
// ratios. See lazyMatcherConfig.
//
// A matcher is deferred only when ALL of:
//   - The query already contains a __name__ equality matcher (anchors selectivity)
//   - The matcher is a regex or negative regex on a non-__name__ label
//   - The label's cardinality exceeds MaxCardinality
//   - cardinality > minSelectPostings * ratio, where ratio depends on the
//     regex's per-call cost class (see regexCostClass)
func splitMatchersForHeadWithConfig(ctx context.Context, ix prom_tsdb.IndexReader, ms []*labels.Matcher, cfg lazyMatcherConfig, cardinalityCache *expirable.LRU[string, int]) (selectMatchers, lazyMatchers []*labels.Matcher) {
	if cfg.MaxCardinality <= 0 || len(ms) < 2 {
		return ms, nil
	}
	// Treat zero-valued ratios as "use the calibrated default", not as
	// "ratio of 1" (which would silently fall back to the original broken
	// gate). This protects programmatic callers who construct the config
	// without flag-registration defaults.
	if cfg.SimpleRatio < 1 {
		cfg.SimpleRatio = defaultSimpleCostRatio
	}
	if cfg.ComplexRatio < 1 {
		cfg.ComplexRatio = defaultComplexCostRatio
	}

	hasMetricNameMatcher := false
	for _, m := range ms {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			hasMetricNameMatcher = true
			break
		}
	}
	if !hasMetricNameMatcher {
		return ms, nil
	}

	// First pass: identify regex matchers that are candidates for deferral and
	// estimate the number of series the selective (equality) matchers would return.
	type regexCandidate struct {
		matcher     *labels.Matcher
		cardinality int
		cost        regexCost
	}

	var candidates []regexCandidate
	selectMatchers = make([]*labels.Matcher, 0, len(ms))
	minSelectPostings := 0

	for _, m := range ms {
		if m.Type == labels.MatchRegexp || m.Type == labels.MatchNotRegexp {
			// Never defer __name__ regex matchers.
			if m.Name == labels.MetricName {
				selectMatchers = append(selectMatchers, m)
				continue
			}

			// Matchers with SetMatches (e.g. "foo|bar|baz") are resolved via
			// direct posting lookups in postingsForMatcher — already fast.
			// Never defer these.
			if len(m.SetMatches()) > 0 {
				selectMatchers = append(selectMatchers, m)
				continue
			}

			// Check if the label has high cardinality.
			cardinality := labelCardinality(ctx, ix, m.Name, cardinalityCache)
			if cardinality <= cfg.MaxCardinality {
				selectMatchers = append(selectMatchers, m)
				continue
			}

			candidates = append(candidates, regexCandidate{
				matcher:     m,
				cardinality: cardinality,
				cost:        regexCostClass(m),
			})
			continue
		}

		selectMatchers = append(selectMatchers, m)
		if m.Type == labels.MatchEqual {
			if n := postingsLen(ctx, ix, m.Name, m.Value); n > 0 {
				if minSelectPostings == 0 || n < minSelectPostings {
					minSelectPostings = n
				}
			}
		}
	}

	if len(candidates) == 0 || minSelectPostings == 0 {
		return ms, nil
	}

	for _, c := range candidates {
		ratio := cfg.SimpleRatio
		if c.cost == regexCostComplex {
			ratio = cfg.ComplexRatio
		}
		// Defer only when lazy iteration is cheaper than the eager scan.
		// Cost model: cardinality * regex_per_call > selective_postings * (LV + regex).
		if c.cardinality > minSelectPostings*ratio {
			lazyMatchers = append(lazyMatchers, c.matcher)
		} else {
			selectMatchers = append(selectMatchers, c.matcher)
		}
	}

	if len(lazyMatchers) == 0 {
		return ms, nil
	}

	return selectMatchers, lazyMatchers
}

// postingsLen returns the number of series matching a single label pair.
// For the head block, Postings() for a single value returns a *ListPostings
// directly, so Len() is O(1) — just a slice length read.
func postingsLen(ctx context.Context, ix prom_tsdb.IndexReader, name, value string) int {
	p, err := ix.Postings(ctx, name, value)
	if err != nil {
		return 0
	}
	if lp, ok := p.(*index.ListPostings); ok {
		return lp.Len()
	}
	return 0
}

const (
	labelCardinalityTTL       = 60 * time.Second
	labelCardinalityCacheSize = 10000 // max label names cached per tenant
)

// newLabelCardinalityCache creates a bounded, TTL-expiring cache for label cardinality.
func newLabelCardinalityCache() *expirable.LRU[string, int] {
	return expirable.NewLRU[string, int](labelCardinalityCacheSize, nil, labelCardinalityTTL)
}

// labelCardinality returns the number of unique values for a label, using a
// cache to avoid repeated LabelValues calls on the head block.
func labelCardinality(ctx context.Context, ix prom_tsdb.IndexReader, name string, cache *expirable.LRU[string, int]) int {
	if v, ok := cache.Get(name); ok {
		return v
	}
	vals, err := ix.LabelValues(ctx, name, (*storage.LabelHints)(nil))
	if err != nil {
		return 0
	}
	cache.Add(name, len(vals))
	return len(vals)
}
