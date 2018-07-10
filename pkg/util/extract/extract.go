package extract

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/cortex/pkg/ingester/client"
)

var labelNameBytes = []byte(model.MetricNameLabel)

// MetricNameFromLabelPairs extracts the metric name from a list of LabelPairs.
func MetricNameFromLabelPairs(labels []client.LabelPair) ([]byte, error) {
	for _, label := range labels {
		if label.Name.Equal(labelNameBytes) {
			return label.Value, nil
		}
	}
	return nil, fmt.Errorf("No metric name label")
}

// MetricNameFromMetric extract the metric name from a model.Metric
func MetricNameFromMetric(m model.Metric) (model.LabelValue, error) {
	for name, value := range m {
		if name == model.MetricNameLabel {
			return value, nil
		}
	}
	return "", fmt.Errorf("no MetricNameLabel for chunk")
}

// MetricNameMatcherFromMatchers extracts the metric name from a set of matchers
func MetricNameMatcherFromMatchers(matchers []*labels.Matcher) (*labels.Matcher, []*labels.Matcher, bool) {
	// Handle the case where there is no metric name and all matchers have been
	// filtered out e.g. {foo=""}.
	if len(matchers) == 0 {
		return nil, matchers, false
	}

	outMatchers := make([]*labels.Matcher, len(matchers)-1)
	for i, matcher := range matchers {
		if matcher.Name != model.MetricNameLabel {
			continue
		}

		// Copy other matchers, excluding the found metric name matcher
		copy(outMatchers, matchers[:i])
		copy(outMatchers[i:], matchers[i+1:])
		return matcher, outMatchers, true
	}
	// Return all matchers if none are metric name matchers
	return nil, matchers, false
}
