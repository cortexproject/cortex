package util

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
)

// SplitFiltersAndMatchers splits empty matchers off, which are treated as filters, see #220
func SplitFiltersAndMatchers(allMatchers []*metric.LabelMatcher) (filters, matchers []*metric.LabelMatcher) {
	for _, matcher := range allMatchers {
		if matcher.Match("") {
			filters = append(filters, matcher)
		} else {
			matchers = append(matchers, matcher)
		}
	}
	return
}

// ExtractMetricNameFromMetric extract the metric name from a model.Metric
func ExtractMetricNameFromMetric(m model.Metric) (model.LabelValue, error) {
	for name, value := range m {
		if name == model.MetricNameLabel {
			return value, nil
		}
	}
	return "", fmt.Errorf("no MetricNameLabel for chunk")
}

// ExtractMetricNameFromMatchers extracts the metric name from a set of matchers
func ExtractMetricNameFromMatchers(matchers []*metric.LabelMatcher) (model.LabelValue, []*metric.LabelMatcher, error) {
	for i, matcher := range matchers {
		if matcher.Name != model.MetricNameLabel {
			continue
		}
		if matcher.Type != metric.Equal {
			return "", nil, fmt.Errorf("must have equality matcher for MetricNameLabel")
		}
		metricName := matcher.Value
		matchers = matchers[:i+copy(matchers[i:], matchers[i+1:])]
		return metricName, matchers, nil
	}
	return "", nil, fmt.Errorf("no matcher for MetricNameLabel")
}
