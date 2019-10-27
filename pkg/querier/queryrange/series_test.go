package queryrange

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func Test_ResponseToSeries(t *testing.T) {
	input := &PrometheusResponse{
		Data: PrometheusData{
			ResultType: promql.ValueTypeMatrix,
			Result: []SampleStream{
				{
					Labels: []client.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []client.Sample{
						{
							Value:       1,
							TimestampMs: 1,
						},
						{
							Value:       2,
							TimestampMs: 2,
						},
					},
				},
				{
					Labels: []client.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []client.Sample{
						{
							Value:       8,
							TimestampMs: 1,
						},
						{
							Value:       9,
							TimestampMs: 2,
						},
					},
				},
			},
		},
	}

	set, err := ResponseToSeries(input)
	require.Nil(t, err)

	setCt := 0

	for set.Next() {
		iter := set.At().Iterator()
		require.Nil(t, set.Err())

		sampleCt := 0
		for iter.Next() {
			ts, v := iter.At()
			require.Equal(t, input.Data.Result[setCt].Samples[sampleCt].TimestampMs, ts)
			require.Equal(t, input.Data.Result[setCt].Samples[sampleCt].Value, v)
			sampleCt++
		}
		require.Equal(t, len(input.Data.Result[setCt].Samples), sampleCt)
		setCt++
	}

	require.Equal(t, len(input.Data.Result), setCt)

}
