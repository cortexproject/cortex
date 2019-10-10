package querysharding

import (
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_ResponseToSeries(t *testing.T) {
	input := queryrange.Response{
		ResultType: promql.ValueTypeMatrix,
		Result: []queryrange.SampleStream{
			{
				Labels: []client.LabelAdapter{
					{"a", "a1"},
					{"b", "b1"},
				},
				Samples: []client.Sample{
					client.Sample{
						Value:       1,
						TimestampMs: 1,
					},
					client.Sample{
						Value:       2,
						TimestampMs: 2,
					},
				},
			},
			{
				Labels: []client.LabelAdapter{
					{"a", "a2"},
					{"b", "b2"},
				},
				Samples: []client.Sample{
					client.Sample{
						Value:       8,
						TimestampMs: 1,
					},
					client.Sample{
						Value:       9,
						TimestampMs: 2,
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
			require.Equal(t, input.Result[setCt].Samples[sampleCt].TimestampMs, ts)
			require.Equal(t, input.Result[setCt].Samples[sampleCt].Value, v)
			sampleCt++
		}
		require.Equal(t, len(input.Result[setCt].Samples), sampleCt)
		setCt++
	}

	require.Equal(t, len(input.Result), setCt)

}
