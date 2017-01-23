package util

import (
	"github.com/prometheus/prometheus/storage/metric"
)

// SplitFiltersAndMatchers splits empty matchers off, which are treated as filters, see #220
func SplitFiltersAndMatchers(allMatchers []*metric.LabelMatcher) (filters, matchers []*metric.LabelMatcher) {
	for _, matcher := range allMatchers {
		if len(matcher.Value) == 0 && (matcher.Type == metric.Equal || matcher.Type == metric.RegexMatch) {
			filters = append(filters, matcher)
		} else {
			matchers = append(matchers, matcher)
		}
	}
	return
}
