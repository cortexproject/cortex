package distributor

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func TestMergeSamplesIntoFirstDuplicates(t *testing.T) {
	a := []cortexpb.Sample{
		{Value: 1.084537996, TimestampMs: 1583946732744},
		{Value: 1.086111723, TimestampMs: 1583946750366},
		{Value: 1.086111723, TimestampMs: 1583946768623},
		{Value: 1.087776094, TimestampMs: 1583946795182},
		{Value: 1.089301187, TimestampMs: 1583946810018},
		{Value: 1.089301187, TimestampMs: 1583946825064},
		{Value: 1.089301187, TimestampMs: 1583946835547},
		{Value: 1.090722985, TimestampMs: 1583946846629},
		{Value: 1.090722985, TimestampMs: 1583946857608},
		{Value: 1.092038719, TimestampMs: 1583946882302},
	}

	b := []cortexpb.Sample{
		{Value: 1.084537996, TimestampMs: 1583946732744},
		{Value: 1.086111723, TimestampMs: 1583946750366},
		{Value: 1.086111723, TimestampMs: 1583946768623},
		{Value: 1.087776094, TimestampMs: 1583946795182},
		{Value: 1.089301187, TimestampMs: 1583946810018},
		{Value: 1.089301187, TimestampMs: 1583946825064},
		{Value: 1.089301187, TimestampMs: 1583946835547},
		{Value: 1.090722985, TimestampMs: 1583946846629},
		{Value: 1.090722985, TimestampMs: 1583946857608},
		{Value: 1.092038719, TimestampMs: 1583946882302},
	}

	a = mergeSamples(a, b)

	// should be the same
	require.Equal(t, a, b)
}

func TestMergeSamplesIntoFirst(t *testing.T) {
	a := []cortexpb.Sample{
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 45},
		{Value: 5, TimestampMs: 50},
	}

	b := []cortexpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 2, TimestampMs: 15},
		{Value: 3, TimestampMs: 25},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 35},
		{Value: 5, TimestampMs: 45},
		{Value: 6, TimestampMs: 55},
	}

	a = mergeSamples(a, b)

	require.Equal(t, []cortexpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 15},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 25},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 35},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 45},
		{Value: 5, TimestampMs: 50},
		{Value: 6, TimestampMs: 55},
	}, a)
}

func TestMergeSamplesIntoFirstNilA(t *testing.T) {
	b := []cortexpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 2, TimestampMs: 15},
		{Value: 3, TimestampMs: 25},
		{Value: 4, TimestampMs: 35},
		{Value: 5, TimestampMs: 45},
		{Value: 6, TimestampMs: 55},
	}

	a := mergeSamples(nil, b)

	require.Equal(t, b, a)
}

func TestMergeSamplesIntoFirstNilB(t *testing.T) {
	a := []cortexpb.Sample{
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 50},
	}

	b := mergeSamples(a, nil)

	require.Equal(t, b, a)
}

func TestMergeExemplarSets(t *testing.T) {
	now := timestamp.FromTime(time.Now())
	exemplar1 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-1")), TimestampMs: now, Value: 1}
	exemplar2 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-2")), TimestampMs: now + 1, Value: 2}
	exemplar3 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-3")), TimestampMs: now + 4, Value: 3}
	exemplar4 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-4")), TimestampMs: now + 8, Value: 7}
	exemplar5 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-4")), TimestampMs: now, Value: 7}

	for _, c := range []struct {
		exemplarsA []cortexpb.Exemplar
		exemplarsB []cortexpb.Exemplar
		expected   []cortexpb.Exemplar
	}{
		{
			exemplarsA: []cortexpb.Exemplar{},
			exemplarsB: []cortexpb.Exemplar{},
			expected:   []cortexpb.Exemplar{},
		},
		{
			exemplarsA: []cortexpb.Exemplar{exemplar1},
			exemplarsB: []cortexpb.Exemplar{},
			expected:   []cortexpb.Exemplar{exemplar1},
		},
		{
			exemplarsA: []cortexpb.Exemplar{},
			exemplarsB: []cortexpb.Exemplar{exemplar1},
			expected:   []cortexpb.Exemplar{exemplar1},
		},
		{
			exemplarsA: []cortexpb.Exemplar{exemplar1},
			exemplarsB: []cortexpb.Exemplar{exemplar1},
			expected:   []cortexpb.Exemplar{exemplar1},
		},
		{
			exemplarsA: []cortexpb.Exemplar{exemplar1, exemplar2, exemplar3},
			exemplarsB: []cortexpb.Exemplar{exemplar1, exemplar3, exemplar4},
			expected:   []cortexpb.Exemplar{exemplar1, exemplar2, exemplar3, exemplar4},
		},
		{ // Ensure that when there are exemplars with duplicate timestamps, the first one wins.
			exemplarsA: []cortexpb.Exemplar{exemplar1, exemplar2, exemplar3},
			exemplarsB: []cortexpb.Exemplar{exemplar5, exemplar3, exemplar4},
			expected:   []cortexpb.Exemplar{exemplar1, exemplar2, exemplar3, exemplar4},
		},
	} {
		e := mergeExemplarSets(c.exemplarsA, c.exemplarsB)
		require.Equal(t, c.expected, e)
	}
}
