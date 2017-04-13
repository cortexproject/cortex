package util

import (
	"reflect"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/stretchr/testify/assert"
)

func TestExtractMetricNameFromMatchers(t *testing.T) {
	metricMatcher, err := metric.NewLabelMatcher(metric.Equal, model.MetricNameLabel, "testmetric")
	if err != nil {
		t.Fatal(err)
	}
	labelMatcher1, err := metric.NewLabelMatcher(metric.Equal, "label", "value1")
	if err != nil {
		t.Fatal(err)
	}
	labelMatcher2, err := metric.NewLabelMatcher(metric.Equal, "label", "value2")
	if err != nil {
		t.Fatal(err)
	}

	tests := [][]*metric.LabelMatcher{
		{metricMatcher, labelMatcher1, labelMatcher2},
		{labelMatcher1, metricMatcher, labelMatcher2},
		{labelMatcher1, labelMatcher2, metricMatcher},
	}

	for i, matchers := range tests {
		matchersCopy := make([]*metric.LabelMatcher, len(matchers))
		copy(matchersCopy, matchers)

		name, outMatchers, ok, err := ExtractMetricNameFromMatchers(matchers)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(matchers, matchersCopy) {
			t.Fatalf("%d. Matchers got mutated; want %v, got %v", i, matchersCopy, matchers)
		}

		if name != "testmetric" {
			t.Fatalf("%d. Wrong metric name; want 'testmetric', got %q", i, name)
		}

		expOutMatchers := []*metric.LabelMatcher{labelMatcher1, labelMatcher2}
		assert.Equal(t, expOutMatchers, outMatchers, "unexpected outMatchers for test case %d", i)

		// Metric extracted
		assert.True(t, ok)
	}
}

func TestExtractMetricNameFromMatchersNoMetricLabel(t *testing.T) {
	// Create matcher with no metric label
	matcher, err := metric.NewLabelMatcher(metric.Equal, model.JobLabel, "testjob")
	if err != nil {
		t.Fatal(err)
	}
	matchers := []*metric.LabelMatcher{matcher}

	_, _, ok, err := ExtractMetricNameFromMatchers(matchers)
	if err != nil {
		t.Fatal(err)
	}

	// No metric extracted
	assert.False(t, ok)
}
