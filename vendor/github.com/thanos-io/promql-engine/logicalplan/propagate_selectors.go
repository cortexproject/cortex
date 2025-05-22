// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"slices"
	"sort"
	"strings"

	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

// PropagateMatchersOptimizer implements matcher propagation between
// two vector selectors in a binary expression.
type PropagateMatchersOptimizer struct{}

func (m PropagateMatchersOptimizer) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	Traverse(&plan, func(expr *Node) {
		binOp, ok := (*expr).(*Binary)
		if !ok {
			return
		}

		// The optimizer cannot be applied to comparison operations or 'atan2'.
		if binOp.Op.IsComparisonOperator() || binOp.Op.String() == "atan2" {
			return
		}
		// Skip OR and UNLESS as their JOIN logic is different from AND and
		// other binary arithmetic operations.
		if binOp.Op == parser.LOR || binOp.Op == parser.LUNLESS {
			return
		}

		vm := binOp.VectorMatching
		if vm == nil {
			propagateMatchers(binOp)
			return
		}

		// Skip matching on metric name for now.
		if vm.On && slices.Contains(vm.MatchingLabels, labels.MetricName) {
			return
		}

		propagateMatchers(binOp)
	})

	return plan, nil
}

func propagateMatchers(binOp *Binary) {
	lhSelector, ok := binOp.LHS.(*VectorSelector)
	if !ok {
		return
	}
	rhSelector, ok := binOp.RHS.(*VectorSelector)
	if !ok {
		return
	}
	// Only handle vector selectors with equal metric name matcher now.
	lhMetricNameMatcher := extractMetricNameMatcher(lhSelector.LabelMatchers)
	if lhMetricNameMatcher == nil || lhMetricNameMatcher.Type != labels.MatchEqual {
		return
	}
	rhMetricNameMatcher := extractMetricNameMatcher(rhSelector.LabelMatchers)
	if rhMetricNameMatcher == nil || rhMetricNameMatcher.Type != labels.MatchEqual {
		return
	}

	// There are cases where VectorSelector.Name is empty when the metric name is
	// specified using {__name__="http_requests_total"} instead of http_requests_total{}.
	if lhSelector.Name == "" {
		lhSelector.Name = lhMetricNameMatcher.Value
	}
	if rhSelector.Name == "" {
		rhSelector.Name = rhMetricNameMatcher.Value
	}
	// This case is handled by MergeSelectsOptimizer.
	if lhSelector.Name == rhSelector.Name {
		return
	}

	vm := binOp.VectorMatching
	labelRequired := func(label string) bool {
		if label == labels.MetricName {
			return false
		}
		if vm == nil {
			return true
		}
		if !vm.On && len(vm.MatchingLabels) == 0 {
			return true
		}

		if vm.On && slices.Contains(vm.MatchingLabels, label) {
			return true
		}
		if !vm.On && !slices.Contains(vm.MatchingLabels, label) {
			return true
		}
		return false
	}

	lhMatchers := toMatcherMap(lhSelector.LabelMatchers)
	rhMatchers := toMatcherMap(rhSelector.LabelMatchers)
	union, stop := makeUnion(lhMatchers, rhMatchers, labelRequired)
	if stop || len(union) == 0 {
		return
	}

	// Matchers to add or replace.
	matchersToChange := toSlice(union)
	updateSelectorMatchers(lhSelector, matchersToChange)
	updateSelectorMatchers(rhSelector, matchersToChange)
}

func toSlice(union map[string]*labels.Matcher) []*labels.Matcher {
	finalMatchers := make([]*labels.Matcher, 0, len(union))
	for _, m := range union {
		finalMatchers = append(finalMatchers, m)
	}

	sort.Slice(finalMatchers, func(i, j int) bool { return finalMatchers[i].Name < finalMatchers[j].Name })
	return finalMatchers
}

func makeUnion(lhMatchers map[string]*labels.Matcher, rhMatchers map[string]*labels.Matcher, labelRequired func(label string) bool) (map[string]*labels.Matcher, bool) {
	union := make(map[string]*labels.Matcher)
	for _, m := range lhMatchers {
		if !labelRequired(m.Name) {
			continue
		}
		// Add every label required from left side to the union.
		union[m.Name] = m
	}

	for _, m := range rhMatchers {
		if !labelRequired(m.Name) {
			continue
		}
		existing, ok := union[m.Name]
		if !ok {
			union[m.Name] = m
			continue
		}
		newMatcher, stop := mergeMatcher(existing, m)
		// Matchers unable to be merged, stop early.
		if stop {
			return nil, true
		}
		union[m.Name] = newMatcher
	}
	return union, false
}

func toMatcherMap(matchers []*labels.Matcher) map[string]*labels.Matcher {
	lhMatchers := make(map[string]*labels.Matcher)
	for _, m := range matchers {
		lhMatchers[m.Name] = m
	}
	return lhMatchers
}

func extractMetricNameMatcher(matchers []*labels.Matcher) *labels.Matcher {
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName {
			return matcher
		}
	}
	return nil
}

func updateSelectorMatchers(selector *VectorSelector, matchers []*labels.Matcher) {
	selectorMatchersMap := toMatcherMap(selector.LabelMatchers)
	for _, m := range matchers {
		selectorMatchersMap[m.Name] = m
	}
	selectorMatchers := make([]*labels.Matcher, 0, len(selectorMatchersMap))
	for _, m := range selectorMatchersMap {
		selectorMatchers = append(selectorMatchers, m)
	}
	selector.LabelMatchers = selectorMatchers
	sort.Slice(selector.LabelMatchers, func(i, j int) bool {
		return selector.LabelMatchers[i].Name < selector.LabelMatchers[j].Name
	})
}

func matcherEqual(a, b *labels.Matcher) bool {
	return a.Name == b.Name && a.Type == b.Type && a.Value == b.Value
}

func mergeMatcher(existingMatcher *labels.Matcher, newMatcher *labels.Matcher) (*labels.Matcher, bool) {
	// Same matcher.
	if matcherEqual(existingMatcher, newMatcher) {
		return existingMatcher, false
	}
	// Different matchers.

	// As long as one matcher matches empty value or nothing, it overrides any other matcher
	// that matches value.
	if matchesEmptyValue(existingMatcher) || matchesNothing(existingMatcher) {
		return existingMatcher, false
	}
	if matchesEmptyValue(newMatcher) || matchesNothing(newMatcher) {
		return newMatcher, false
	}
	if matchesAllValues(existingMatcher) || matchesEverything(existingMatcher) {
		return newMatcher, false
	}
	if matchesAllValues(newMatcher) || matchesEverything(newMatcher) {
		return existingMatcher, false
	}

	if existingMatcher.Type == labels.MatchNotEqual && newMatcher.Type == labels.MatchNotEqual {
		return labels.MustNewMatcher(labels.MatchNotRegexp, existingMatcher.Name, strings.Join([]string{existingMatcher.Value, newMatcher.Value}, "|")), false
	}
	// One equal matcher with another matcher type. Always use equal matcher to scope down.
	if existingMatcher.Type == labels.MatchEqual || newMatcher.Type == labels.MatchEqual {
		var equalMatcher *labels.Matcher
		var otherMatcher *labels.Matcher
		if existingMatcher.Type == labels.MatchEqual {
			equalMatcher = existingMatcher
			otherMatcher = newMatcher
		} else {
			equalMatcher = newMatcher
			otherMatcher = existingMatcher
		}
		if otherMatcher.Matches(equalMatcher.Value) {
			return equalMatcher, false
		}
	}

	return nil, true
}

func matchesEmptyValue(matcher *labels.Matcher) bool {
	if matcher.Value == "" && (matcher.Type == labels.MatchEqual || matcher.Type == labels.MatchRegexp) {
		return true
	}
	if matcher.Value == ".+" && matcher.Type == labels.MatchNotRegexp {
		return true
	}
	return false
}

func matchesAllValues(matcher *labels.Matcher) bool {
	if matcher.Value == ".+" && matcher.Type == labels.MatchRegexp {
		return true
	}
	if matcher.Value == "" && (matcher.Type == labels.MatchNotEqual || matcher.Type == labels.MatchNotRegexp) {
		return true
	}
	return false
}

func matchesEverything(matcher *labels.Matcher) bool {
	return matcher.Value == ".*" && matcher.Type == labels.MatchRegexp
}

func matchesNothing(matcher *labels.Matcher) bool {
	return matcher.Value == ".*" && matcher.Type == labels.MatchNotRegexp
}
