package querier

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
)

const (
	maxt, mint = 0, 10
)

func TestDistributorQuerier(t *testing.T) {
	d := &mockDistributor{
		m: model.Matrix{
			// Matrixes are unsorted, so this tests that the labels get sorted.
			&model.SampleStream{
				Metric: model.Metric{
					"foo": "bar",
				},
			},
			&model.SampleStream{
				Metric: model.Metric{
					"bar": "baz",
				},
			},
		},
	}
	queryable := newDistributorQueryable(d, false, nil)
	querier, err := queryable.Querier(context.Background(), mint, maxt)
	require.NoError(t, err)

	seriesSet, _, err := querier.Select(&storage.SelectParams{Start: mint, End: maxt})
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
		r: &client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
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
		},
	}
	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := newDistributorQueryable(d, true, mergeChunks)
	querier, err := queryable.Querier(ctx, mint, maxt)
	require.NoError(t, err)

	seriesSet, _, err := querier.Select(&storage.SelectParams{Start: mint, End: maxt})
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

type mockDistributor struct {
	m model.Matrix
	r *client.QueryStreamResponse
}

func (m *mockDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return m.m, nil
}
func (m *mockDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	return m.r, nil
}
func (m *mockDistributor) LabelValuesForLabelName(context.Context, model.LabelName) ([]string, error) {
	return nil, nil
}
func (m *mockDistributor) LabelNames(context.Context) ([]string, error) {
	return nil, nil
}
func (m *mockDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}
