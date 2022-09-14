package queryrange

import (
	"math"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestGenLabelsCorrectness(t *testing.T) {
	ls := genLabels([]string{"a", "b"}, 2)
	for _, set := range ls {
		sort.Sort(set)
	}
	expected := []labels.Labels{
		{
			labels.Label{
				Name:  "a",
				Value: "0",
			},
			labels.Label{
				Name:  "b",
				Value: "0",
			},
		},
		{
			labels.Label{
				Name:  "a",
				Value: "0",
			},
			labels.Label{
				Name:  "b",
				Value: "1",
			},
		},
		{
			labels.Label{
				Name:  "a",
				Value: "1",
			},
			labels.Label{
				Name:  "b",
				Value: "0",
			},
		},
		{
			labels.Label{
				Name:  "a",
				Value: "1",
			},
			labels.Label{
				Name:  "b",
				Value: "1",
			},
		},
	}
	require.Equal(t, expected, ls)
}

func TestGenLabelsSize(t *testing.T) {
	for _, tc := range []struct {
		set     []string
		buckets int
	}{
		{
			set:     []string{"a", "b"},
			buckets: 5,
		},
		{
			set:     []string{"a", "b", "c"},
			buckets: 10,
		},
	} {
		sets := genLabels(tc.set, tc.buckets)
		require.Equal(
			t,
			math.Pow(float64(tc.buckets), float64(len(tc.set))),
			float64(len(sets)),
		)
	}
}
