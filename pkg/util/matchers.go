package util

import (
	"github.com/prometheus/prometheus/pkg/labels"
)

// SplitFiltersAndMatchers splits empty matchers off, which are treated as filters, see #220
func SplitFiltersAndMatchers(allMatchers []*labels.Matcher) (filters, matchers []*labels.Matcher) {
	for _, matcher := range allMatchers {
		// If a matcher matches "", we need to fetch possible chunks where
		// there is no value and will therefore not be in our label index.
		// e.g. {foo=""} and {foo!="bar"} both match "", so we need to return
		// chunks which do not have a foo label set. When looking entries in
		// the index, we should ignore this matcher to fetch all possible chunks
		// and then filter on the matcher after the chunks have been fetched.
		if matcher.Matches("") {
			filters = append(filters, matcher)
		} else {
			matchers = append(matchers, matcher)
		}
	}
	return
}

// CompareMatchersWithLabels compares matchers with labels and returns true if they match otherwise it returns false
func CompareMatchersWithLabels(allMatchers []*labels.Matcher, labels labels.Labels) bool {
	filters, matchers := SplitFiltersAndMatchers(allMatchers)
	labelsMap := make(map[string]string, len(labels))
	for i := range labels {
		labelsMap[string(labels[i].Name)] = string(labels[i].Value)
	}

	for i := range matchers {
		if !matchers[i].Matches(labelsMap[matchers[i].Name]) {
			return false
		}
	}

	for i := range filters {
		if _, isOK := labelsMap[filters[i].Name]; isOK {
			return false
		}
	}

	return true
}
