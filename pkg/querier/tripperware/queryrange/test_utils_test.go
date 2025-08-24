package queryrange

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestGenLabelsCorrectness(t *testing.T) {
	t.Parallel()
	ls := genLabels([]string{"a", "b"}, 2)
	expected := []labels.Labels{
		labels.FromStrings("a", "0", "b", "0"),
		labels.FromStrings("a", "0", "b", "1"),
		labels.FromStrings("a", "1", "b", "0"),
		labels.FromStrings("a", "1", "b", "1"),
	}

	require.Equal(t, expected, ls)
}

func TestGenLabelsSize(t *testing.T) {
	t.Parallel()
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
