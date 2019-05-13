package querier

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/weaveworks/common/user"
)

func TestIngesterStreaming(t *testing.T) {
	// We need to make sure that there is atleast one chunk present,
	// else no series will be selected.
	promChunk, err := encoding.NewForEncoding(encoding.Bigchunk)
	require.NoError(t, err)

	clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{
		chunk.NewChunk("", 0, nil, promChunk, model.Earliest, model.Earliest),
	})
	require.NoError(t, err)

	d := &mockDistributor{
		r: []client.TimeSeriesChunk{
			{
				Labels: []client.LabelAdapter{
					{Name: "bar", Value: "baz"},
				},
				Chunks: clientChunks,
			},
			{
				Labels: []client.LabelAdapter{
					{Name: "foo", Value: "bar"},
				},
				Chunks: clientChunks,
			},
		},
	}
	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := newIngesterStreamingQueryable(d, mergeChunks)
	querier, err := queryable.Querier(ctx, mint, maxt)
	require.NoError(t, err)

	seriesSet, _, err := querier.Select(nil)
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
