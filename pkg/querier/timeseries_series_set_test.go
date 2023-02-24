package querier

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func TestTimeSeriesSeriesSet(t *testing.T) {

	timeseries := []cortexpb.TimeSeries{
		{
			Labels: []cortexpb.LabelAdapter{
				{
					Name:  "label1",
					Value: "value1",
				},
			},
			Samples: []cortexpb.Sample{
				{
					Value:       3.14,
					TimestampMs: 1234,
				},
			},
		},
	}

	ss := newTimeSeriesSeriesSet(true, timeseries)

	require.True(t, ss.Next())
	series := ss.At()

	require.Equal(t, ss.ts[0].Labels[0].Name, series.Labels()[0].Name)
	require.Equal(t, ss.ts[0].Labels[0].Value, series.Labels()[0].Value)

	it := series.Iterator(nil)
	require.NotEqual(t, it.Next(), chunkenc.ValNone)
	ts, v := it.At()
	require.Equal(t, 3.14, v)
	require.Equal(t, int64(1234), ts)
	require.False(t, ss.Next())

	// Append a new sample to seek to
	timeseries[0].Samples = append(timeseries[0].Samples, cortexpb.Sample{
		Value:       1.618,
		TimestampMs: 2345,
	})
	ss = newTimeSeriesSeriesSet(true, timeseries)

	require.True(t, ss.Next())
	it = ss.At().Iterator(nil)
	require.NotEqual(t, it.Seek(2000), chunkenc.ValNone)
	ts, v = it.At()
	require.Equal(t, 1.618, v)
	require.Equal(t, int64(2345), ts)
}

func TestTimeSeriesIterator(t *testing.T) {
	ts := timeseries{
		series: cortexpb.TimeSeries{
			Labels: []cortexpb.LabelAdapter{
				{
					Name:  "label1",
					Value: "value1",
				},
			},
			Samples: []cortexpb.Sample{
				{
					Value:       3.14,
					TimestampMs: 1234,
				},
				{
					Value:       3.14,
					TimestampMs: 1235,
				},
				{
					Value:       3.14,
					TimestampMs: 1236,
				},
			},
		},
	}

	it := ts.Iterator(nil)
	require.NotEqual(t, it.Seek(1235), chunkenc.ValNone) // Seek to middle
	i, _ := it.At()
	require.EqualValues(t, 1235, i)
	require.NotEqual(t, it.Seek(1236), chunkenc.ValNone) // Seek to end
	i, _ = it.At()
	require.EqualValues(t, 1236, i)
	require.Equal(t, it.Seek(1238), chunkenc.ValNone) // Seek past end

	it = ts.Iterator(nil)
	require.NotEqual(t, it.Next(), chunkenc.ValNone)
	require.NotEqual(t, it.Next(), chunkenc.ValNone)
	i, _ = it.At()
	require.EqualValues(t, 1235, i)
	require.NotEqual(t, it.Seek(1234), chunkenc.ValNone) // Ensure seek doesn't do anything if already past seek target.
	i, _ = it.At()
	require.EqualValues(t, 1235, i)

	it = ts.Iterator(nil)
	for i := 0; it.Next() != chunkenc.ValNone; {
		j, _ := it.At()
		switch i {
		case 0:
			require.EqualValues(t, 1234, j)
		case 1:
			require.EqualValues(t, 1235, j)
		case 2:
			require.EqualValues(t, 1236, j)
		default:
			t.Fail()
		}
		i++
	}
	it.At() // Ensure an At after a full iteration, doesn't cause a panic
}
