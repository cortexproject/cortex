package tsdb

import (
	"regexp/syntax"
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	legacy_labels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_FromLabelAdaptersToLabels(t *testing.T) {
	t.Parallel()

	actual := FromLabelAdaptersToLabels([]client.LabelAdapter{
		{Name: "app", Value: "test"},
		{Name: "instance", Value: "i-1"},
	})

	expected := labels.Labels{
		{Name: "app", Value: "test"},
		{Name: "instance", Value: "i-1"},
	}

	assert.Equal(t, expected, actual)
}

func Test_FromLabelsToLabelAdapters(t *testing.T) {
	t.Parallel()

	actual := FromLabelsToLabelAdapters(labels.Labels{
		{Name: "app", Value: "test"},
		{Name: "instance", Value: "i-1"},
	})

	expected := []client.LabelAdapter{
		{Name: "app", Value: "test"},
		{Name: "instance", Value: "i-1"},
	}

	assert.Equal(t, expected, actual)
}

func Test_FromLegacyLabelMatchersToMatchers(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		input            []*legacy_labels.Matcher
		expectedMatchers []labels.Matcher
		expectedErr      error
	}{
		"empty matchers": {
			input:            []*legacy_labels.Matcher{},
			expectedMatchers: []labels.Matcher{},
			expectedErr:      nil,
		},
		"multi matchers": {
			input: []*legacy_labels.Matcher{
				{Type: legacy_labels.MatchEqual, Name: "metric", Value: "value"},
				{Type: legacy_labels.MatchNotEqual, Name: "metric", Value: "value"},
				{Type: legacy_labels.MatchRegexp, Name: "metric", Value: "value.*"},
				{Type: legacy_labels.MatchNotRegexp, Name: "metric", Value: "value.*"},
			},
			expectedMatchers: []labels.Matcher{
				labels.NewEqualMatcher("metric", "value"),
				labels.Not(labels.NewEqualMatcher("metric", "value")),
				labels.NewMustRegexpMatcher("metric", "^(?:value.*)$"),
				labels.Not(labels.NewMustRegexpMatcher("metric", "^(?:value.*)$")),
			},
			expectedErr: nil,
		},
		"invalid regex": {
			input: []*legacy_labels.Matcher{
				{Type: legacy_labels.MatchRegexp, Name: "metric", Value: "[0-9]++"},
			},
			expectedMatchers: nil,
			expectedErr:      &syntax.Error{Code: syntax.ErrInvalidRepeatOp, Expr: "++"},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual, err := FromLegacyLabelMatchersToMatchers(testData.input)
			require.Equal(t, testData.expectedErr, err)
			assert.Equal(t, testData.expectedMatchers, actual)
		})
	}
}
