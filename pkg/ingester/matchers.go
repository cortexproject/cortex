package ingester

import "slices"

import "github.com/prometheus/prometheus/model/labels"

// optimizeMatchers categorizes input matchers to matchers used in select and matchers applied lazily
// when scanning series.
func optimizeMatchers(matchers []*labels.Matcher) ([]*labels.Matcher, []*labels.Matcher) {
	// If there is only 1 matcher, use it for select.
	// If there is no matcher to optimize, also return early.
	if len(matchers) < 2 || !canOptimizeMatchers(matchers) {
		return matchers, nil
	}
	selectMatchers := make([]*labels.Matcher, 0, len(matchers))
	lazyMatchers := make([]*labels.Matcher, 0)
	for _, m := range matchers {
		// =~.* is a noop as it matches everything.
		if m.Type == labels.MatchRegexp && m.Value == ".*" {
			continue
		}
		if lazyMatcher(m) {
			lazyMatchers = append(lazyMatchers, m)
			continue
		}
		selectMatchers = append(selectMatchers, m)
	}

	// We need at least 1 select matcher.
	if len(selectMatchers) == 0 {
		selectMatchers = lazyMatchers[:1]
		lazyMatchers = lazyMatchers[1:]
	}

	return selectMatchers, lazyMatchers
}

func canOptimizeMatchers(matchers []*labels.Matcher) bool {
	return slices.ContainsFunc(matchers, lazyMatcher)
}

func labelsMatches(lbls labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}

// lazyMatcher checks if the label matcher should be applied lazily when scanning series instead of fetching postings
// for matcher. The matchers to apply lazily are matchers that are known to have low selectivity.
func lazyMatcher(matcher *labels.Matcher) bool {
	if matcher.Value == ".+" && matcher.Type == labels.MatchRegexp {
		return true
	}
	if matcher.Value == "" && (matcher.Type == labels.MatchNotEqual || matcher.Type == labels.MatchNotRegexp) {
		return true
	}
	return false
}
