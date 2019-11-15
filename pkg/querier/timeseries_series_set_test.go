package querier

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/stretchr/testify/require"
)

func TestTimeSeriesSeriesSet(t *testing.T) {

	timeseries := []client.TimeSeries{
		{
			Labels: []client.LabelAdapter{
				{
					Name:  "label1",
					Value: "value1",
				},
			},
			Samples: []client.Sample{
				{
					Value:       3.14,
					TimestampMs: 1234,
				},
			},
		},
	}

	ss := newTimeSeriesSeriesSet(timeseries)

	require.True(t, ss.Next())
	series := ss.At()

	require.Equal(t, ss.ts[0].Labels[0].Name, series.Labels()[0].Name)
	require.Equal(t, ss.ts[0].Labels[0].Value, series.Labels()[0].Value)

	it := series.Iterator()
	require.True(t, it.Next())
	ts, v := it.At()
	require.Equal(t, 3.14, v)
	require.Equal(t, int64(1234), ts)
	require.False(t, ss.Next())

	// Append a new sample to seek to
	timeseries[0].Samples = append(timeseries[0].Samples, client.Sample{
		Value:       1.618,
		TimestampMs: 2345,
	})
	ss = newTimeSeriesSeriesSet(timeseries)

	require.True(t, ss.Next())
	it = ss.At().Iterator()
	require.True(t, it.Seek(2000))
	ts, v = it.At()
	require.Equal(t, 1.618, v)
	require.Equal(t, int64(2345), ts)
}
