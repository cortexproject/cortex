package util

import (
	"reflect"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/stretchr/testify/assert"
)

func TestExtractMetricNameMatcherFromMatchers(t *testing.T) {
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
	nonEqualityMetricMatcher, err := metric.NewLabelMatcher(metric.NotEqual, model.MetricNameLabel, "testmetric")
	if err != nil {
		t.Fatal(err)
	}
	jobMatcher, err := metric.NewLabelMatcher(metric.Equal, model.JobLabel, "testjob")
	if err != nil {
		t.Fatal(err)
	}

	for i, tc := range []struct {
		matchers         []*metric.LabelMatcher
		expMetricMatcher *metric.LabelMatcher
		expOutMatchers   []*metric.LabelMatcher
		expOk            bool
	}{
		{
			matchers:         []*metric.LabelMatcher{metricMatcher},
			expMetricMatcher: metricMatcher,
			expOutMatchers:   []*metric.LabelMatcher{},
			expOk:            true,
		}, {
			matchers:         []*metric.LabelMatcher{metricMatcher, labelMatcher1, labelMatcher2},
			expMetricMatcher: metricMatcher,
			expOutMatchers:   []*metric.LabelMatcher{labelMatcher1, labelMatcher2},
			expOk:            true,
		}, {
			matchers:         []*metric.LabelMatcher{labelMatcher1, metricMatcher, labelMatcher2},
			expMetricMatcher: metricMatcher,
			expOutMatchers:   []*metric.LabelMatcher{labelMatcher1, labelMatcher2},
			expOk:            true,
		}, {
			matchers:         []*metric.LabelMatcher{labelMatcher1, labelMatcher2, metricMatcher},
			expMetricMatcher: metricMatcher,
			expOutMatchers:   []*metric.LabelMatcher{labelMatcher1, labelMatcher2},
			expOk:            true,
		}, {
			matchers:         []*metric.LabelMatcher{nonEqualityMetricMatcher},
			expMetricMatcher: nonEqualityMetricMatcher,
			expOutMatchers:   []*metric.LabelMatcher{},
			expOk:            true,
		}, {
			matchers:         []*metric.LabelMatcher{jobMatcher},
			expMetricMatcher: nil,
			expOutMatchers:   []*metric.LabelMatcher{jobMatcher},
			expOk:            false,
		}, {
			matchers:         []*metric.LabelMatcher{},
			expMetricMatcher: nil,
			expOutMatchers:   []*metric.LabelMatcher{},
			expOk:            false,
		},
	} {
		matchersCopy := make([]*metric.LabelMatcher, len(tc.matchers))
		copy(matchersCopy, tc.matchers)

		nameMatcher, outMatchers, ok := ExtractMetricNameMatcherFromMatchers(tc.matchers)

		if !reflect.DeepEqual(tc.matchers, matchersCopy) {
			t.Fatalf("%d. Matchers got mutated; want %v, got %v", i, matchersCopy, tc.matchers)
		}

		if !reflect.DeepEqual(tc.expMetricMatcher, nameMatcher) {
			t.Fatalf("%d. Wrong metric matcher; want '%v', got %v", i, tc.expMetricMatcher, nameMatcher)
		}

		assert.Equal(t, tc.expOutMatchers, outMatchers, "unexpected outMatchers for test case %d", i)

		assert.Equal(t, tc.expOk, ok, "unexpected ok for test case %d", i)
	}
}
