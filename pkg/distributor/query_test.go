package distributor

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestMergeSamplesIntoFirstDuplicates(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestMergeExemplars(t *testing.T) {
	t.Parallel()
	now := timestamp.FromTime(time.Now())
	exemplar1 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-1")), TimestampMs: now, Value: 1}
	exemplar2 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-2")), TimestampMs: now + 1, Value: 2}
	exemplar3 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-3")), TimestampMs: now + 4, Value: 3}
	exemplar4 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-4")), TimestampMs: now + 8, Value: 7}
	exemplar5 := cortexpb.Exemplar{Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-4")), TimestampMs: now, Value: 7}
	labels1 := []cortexpb.LabelAdapter{{Name: "label1", Value: "foo1"}}
	labels2 := []cortexpb.LabelAdapter{{Name: "label1", Value: "foo2"}}

	for i, c := range []struct {
		seriesA       []cortexpb.TimeSeries
		seriesB       []cortexpb.TimeSeries
		expected      []cortexpb.TimeSeries
		nonReversible bool
	}{
		{
			seriesA:  []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{}}},
			seriesB:  []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{}}},
			expected: []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{}}},
		},
		{
			seriesA:  []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1}}},
			seriesB:  []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{}}},
			expected: []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1}}},
		},
		{
			seriesA:  []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1}}},
			seriesB:  []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1}}},
			expected: []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1}}},
		},
		{
			seriesA:  []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar2, exemplar3}}},
			seriesB:  []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar3, exemplar4}}},
			expected: []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar2, exemplar3, exemplar4}}},
		},
		{ // Ensure that when there are exemplars with duplicate timestamps, the first one wins.
			seriesA:       []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar2, exemplar3}}},
			seriesB:       []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar5, exemplar3, exemplar4}}},
			expected:      []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar2, exemplar3, exemplar4}}},
			nonReversible: true,
		},
		{ // Disjoint exemplars on two different series.
			seriesA: []cortexpb.TimeSeries{{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar2}}},
			seriesB: []cortexpb.TimeSeries{{Labels: labels2, Exemplars: []cortexpb.Exemplar{exemplar3, exemplar4}}},
			expected: []cortexpb.TimeSeries{
				{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []cortexpb.Exemplar{exemplar3, exemplar4}}},
		},
		{ // Second input adds to first on one series.
			seriesA: []cortexpb.TimeSeries{
				{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []cortexpb.Exemplar{exemplar3}}},
			seriesB: []cortexpb.TimeSeries{{Labels: labels2, Exemplars: []cortexpb.Exemplar{exemplar4}}},
			expected: []cortexpb.TimeSeries{
				{Labels: labels1, Exemplars: []cortexpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []cortexpb.Exemplar{exemplar3, exemplar4}}},
		},
	} {
		c := c
		t.Run(fmt.Sprint("test", i), func(t *testing.T) {
			t.Parallel()
			rA := &ingester_client.ExemplarQueryResponse{Timeseries: c.seriesA}
			rB := &ingester_client.ExemplarQueryResponse{Timeseries: c.seriesB}
			e := mergeExemplarQueryResponses([]interface{}{rA, rB})
			require.Equal(t, c.expected, e.Timeseries)
			if !c.nonReversible {
				// Check the other way round too
				e = mergeExemplarQueryResponses([]interface{}{rB, rA})
				require.Equal(t, c.expected, e.Timeseries)
			}
		})
	}
}
