package cortexpbv2

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func Test_GetLabelRefsFromLabelAdapters(t *testing.T) {
	tests := []struct {
		symbols            []string
		lbs                []cortexpb.LabelAdapter
		expectedSeriesRefs []uint32
	}{
		{
			symbols:            []string{"", "__name__", "test_metric", "foo", "bar", "baz", "qux"},
			lbs:                []cortexpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}, {Name: "foo", Value: "bar"}},
			expectedSeriesRefs: []uint32{1, 2, 3, 4},
		},
		{
			symbols:            []string{"", "__name__", "test_metric", "foo", "bar", "baz", "qux"},
			lbs:                []cortexpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}, {Name: "baz", Value: "qux"}},
			expectedSeriesRefs: []uint32{1, 2, 5, 6},
		},
		{
			symbols:            []string{"", "__name__", "test_metric", "foo", "bar", "baz", "qux", "1"},
			lbs:                []cortexpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}, {Name: "baz", Value: "qux"}, {Name: "qux", Value: "1"}},
			expectedSeriesRefs: []uint32{1, 2, 5, 6, 6, 7},
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expectedSeriesRefs, GetLabelRefsFromLabelAdapters(test.symbols, test.lbs))
	}
}

func Test_GetLabelsRefsFromLabels(t *testing.T) {
	tests := []struct {
		symbols            []string
		lbs                labels.Labels
		expectedSeriesRefs []uint32
	}{
		{
			symbols:            []string{"", "__name__", "test_metric", "foo", "bar", "baz", "qux"},
			lbs:                labels.Labels{labels.Label{Name: "__name__", Value: "test_metric"}, labels.Label{Name: "foo", Value: "bar"}},
			expectedSeriesRefs: []uint32{1, 2, 3, 4},
		},
		{
			symbols:            []string{"", "__name__", "test_metric", "foo", "bar", "baz", "qux"},
			lbs:                labels.Labels{labels.Label{Name: "__name__", Value: "test_metric"}, labels.Label{Name: "baz", Value: "qux"}},
			expectedSeriesRefs: []uint32{1, 2, 5, 6},
		},
		{
			symbols:            []string{"", "__name__", "test_metric", "foo", "bar", "baz", "qux", "1"},
			lbs:                labels.Labels{labels.Label{Name: "__name__", Value: "test_metric"}, labels.Label{Name: "baz", Value: "qux"}, labels.Label{Name: "qux", Value: "1"}},
			expectedSeriesRefs: []uint32{1, 2, 5, 6, 6, 7},
		},
		{
			symbols:            []string{"", "a help for testmetric", "a help for testmetric2"},
			lbs:                labels.Labels{},
			expectedSeriesRefs: nil,
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expectedSeriesRefs, GetLabelsRefsFromLabels(test.symbols, test.lbs))
	}
}
