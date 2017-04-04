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
	nonEqualityMetricMatcher, err := metric.NewLabelMatcher(metric.NotEqual, model.MetricNameLabel, "testmetric")
	if err != nil {
		t.Fatal(err)
	}
	jobMatcher, err := metric.NewLabelMatcher(metric.Equal, model.JobLabel, "testjob")
	if err != nil {
		t.Fatal(err)
	}

	for i, tc := range []struct {
		matchers       []*metric.LabelMatcher
		expName        string
		expOutMatchers []*metric.LabelMatcher
		expOk          bool
	}{
		{
			matchers:       []*metric.LabelMatcher{metricMatcher, labelMatcher1, labelMatcher2},
			expName:        "testmetric",
			expOutMatchers: []*metric.LabelMatcher{labelMatcher1, labelMatcher2},
			expOk:          true,
		}, {
			matchers:       []*metric.LabelMatcher{labelMatcher1, metricMatcher, labelMatcher2},
			expName:        "testmetric",
			expOutMatchers: []*metric.LabelMatcher{labelMatcher1, labelMatcher2},
			expOk:          true,
		}, {
			matchers:       []*metric.LabelMatcher{labelMatcher1, labelMatcher2, metricMatcher},
			expName:        "testmetric",
			expOutMatchers: []*metric.LabelMatcher{labelMatcher1, labelMatcher2},
			expOk:          true,
		}, {
			matchers:       []*metric.LabelMatcher{nonEqualityMetricMatcher},
			expName:        "",
			expOutMatchers: nil,
			expOk:          false,
		}, {
			matchers:       []*metric.LabelMatcher{jobMatcher},
			expName:        "",
			expOutMatchers: nil,
			expOk:          false,
		},
	} {
		matchersCopy := make([]*metric.LabelMatcher, len(tc.matchers))
		copy(matchersCopy, tc.matchers)

		name, outMatchers, ok := ExtractMetricNameFromMatchers(tc.matchers)

		if !reflect.DeepEqual(tc.matchers, matchersCopy) {
			t.Fatalf("%d. Matchers got mutated; want %v, got %v", i, matchersCopy, tc.matchers)
		}

		if string(name) != tc.expName {
			t.Fatalf("%d. Wrong metric name; want '%q', got %q", i, tc.expName, name)
		}

		assert.Equal(t, tc.expOutMatchers, outMatchers, "unexpected outMatchers for test case %d", i)

		assert.Equal(t, tc.expOk, ok, "unexpected ok for test case %d", i)
	}
}
