package distributor

import (
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestMergeSamplesIntoFirstDuplicates(t *testing.T) {
	a := []ingester_client.Sample{
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

	b := []ingester_client.Sample{
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
	a := []ingester_client.Sample{
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 45},
		{Value: 5, TimestampMs: 50},
	}

	b := []ingester_client.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 2, TimestampMs: 15},
		{Value: 3, TimestampMs: 25},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 35},
		{Value: 5, TimestampMs: 45},
		{Value: 6, TimestampMs: 55},
	}

	a = mergeSamples(a, b)

	require.Equal(t, []ingester_client.Sample{
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
	b := []ingester_client.Sample{
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
	a := []ingester_client.Sample{
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 50},
	}

	b := mergeSamples(a, nil)

	require.Equal(t, b, a)
}

func BenchmarkSeriesMap(b *testing.B) {
	benchmarkSeriesMap(100000, b)
}

func benchmarkSeriesMap(numSeries int, b *testing.B) {
	series := makeSeries(numSeries)
	sm := map[string]int{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i, s := range series {
			sm[LabelsToKeyString(s)] = i
		}

		for _, s := range series {
			_, ok := sm[LabelsToKeyString(s)]
			if !ok {
				b.Fatal("element missing")
			}
		}

		if len(sm) != numSeries {
			b.Fatal("the number of series expected:", numSeries, "got:", len(sm))
		}
	}
}

func makeSeries(n int) []labels.Labels {
	series := make([]labels.Labels, 0, n)
	for i := 0; i < n; i++ {
		series = append(series, labels.FromMap(map[string]string{
			"label0": "value0",
			"label1": "value1",
			"label2": "value2",
			"label3": "value3",
			"label4": "value4",
			"label5": "value5",
			"label6": "value6",
			"label7": "value7",
			"label8": "value8",
			"label9": strconv.Itoa(i),
		}))
	}

	return series
}
