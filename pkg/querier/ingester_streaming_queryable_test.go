package querier

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/wire"
	"github.com/weaveworks/common/user"
)

func TestIngesterStreaming(t *testing.T) {
	d := &mockDistributor{
		r: []client.TimeSeriesChunk{
			{
				Labels: []client.LabelPair{
					{Name: wire.Bytes("bar"), Value: wire.Bytes("baz")},
				},
			},
			{
				Labels: []client.LabelPair{
					{Name: wire.Bytes("foo"), Value: wire.Bytes("bar")},
				},
			},
		},
	}
	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := newIngesterStreamingQueryable(d, mergeChunks)
	querier, err := queryable.Querier(ctx, mint, maxt)
	require.NoError(t, err)

	seriesSet, err, _ := querier.Select(nil)
	require.NoError(t, err)

	require.True(t, seriesSet.Next())
	series := seriesSet.At()
	require.Equal(t, labels.Labels{{Name: "bar", Value: "baz"}}, series.Labels())

	require.True(t, seriesSet.Next())
	series = seriesSet.At()
	require.Equal(t, labels.Labels{{Name: "foo", Value: "bar"}}, series.Labels())

	require.False(t, seriesSet.Next())
	require.NoError(t, seriesSet.Err())
}
