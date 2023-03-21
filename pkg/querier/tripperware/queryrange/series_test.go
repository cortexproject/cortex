package queryrange

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func Test_ResponseToSamples(t *testing.T) {
	t.Parallel()
	input := &PrometheusResponse{
		Data: PrometheusData{
			ResultType: string(parser.ValueTypeMatrix),
			Result: []tripperware.SampleStream{
				{
					Labels: []cortexpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []cortexpb.Sample{
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
					Labels: []cortexpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []cortexpb.Sample{
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

	streams, err := ResponseToSamples(input)
	require.Nil(t, err)
	set := NewSeriesSet(false, streams)

	setCt := 0

	var iter chunkenc.Iterator
	for set.Next() {
		iter = set.At().Iterator(iter)
		require.Nil(t, set.Err())

		sampleCt := 0
		for iter.Next() != chunkenc.ValNone {
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
