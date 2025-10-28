package cortexpb

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test_ExemplarV2ToLabels(t *testing.T) {
	symbols := []string{"", "foo", "bar"}
	b := &labels.ScratchBuilder{}

	tests := []struct {
		desc           string
		e              *ExemplarV2
		isErr          bool
		expectedLabels labels.Labels
	}{
		{
			desc: "Success",
			e: &ExemplarV2{
				LabelsRefs: []uint32{1, 2},
			},
			expectedLabels: labels.FromStrings("foo", "bar"),
		},
		{
			desc: "Odd length of the label refs",
			e: &ExemplarV2{
				LabelsRefs: []uint32{1},
			},
			isErr: true,
		},
		{
			desc: "Label name refs out of ranges",
			e: &ExemplarV2{
				LabelsRefs: []uint32{10, 2},
			},
			isErr: true,
		},
		{
			desc: "Label value refs out of ranges",
			e: &ExemplarV2{
				LabelsRefs: []uint32{1, 10},
			},
			isErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			lbs, err := test.e.ToLabels(b, symbols)
			if test.isErr {
				require.Error(t, err)
			} else {
				require.Equal(t, test.expectedLabels.String(), lbs.String())
			}
		})
	}
}

func Test_TimeSeriesV2ToLabels(t *testing.T) {
	symbols := []string{"", "__name__", "test_metric", "job", "prometheus", "instance", "server-1"}
	b := &labels.ScratchBuilder{}

	tests := []struct {
		desc           string
		ts             *TimeSeriesV2
		isErr          bool
		expectedLabels labels.Labels
	}{
		{
			desc: "Success",
			ts: &TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 3, 4, 5, 6},
			},
			expectedLabels: labels.FromStrings("__name__", "test_metric", "job", "prometheus", "instance", "server-1"),
		},
		{
			desc: "Odd length of the label refs",
			ts: &TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 3},
			},
			isErr: true,
		},
		{
			desc: "Label name refs out of ranges",
			ts: &TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 10, 4},
			},
			isErr: true,
		},
		{
			desc: "Label value refs out of ranges",
			ts: &TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 3, 10},
			},
			isErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			lbs, err := test.ts.ToLabels(b, symbols)
			if test.isErr {
				require.Error(t, err)
			} else {
				require.Equal(t, test.expectedLabels.String(), lbs.String())
			}
		})
	}
}
