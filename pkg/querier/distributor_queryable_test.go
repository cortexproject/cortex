package querier

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"
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
	queryable := newDistributorQueryable(d)
	querier, err := queryable.Querier(context.Background(), mint, maxt)
	require.NoError(t, err)

	seriesSet, err := querier.Select(nil)
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
}

func (m *mockDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return m.m, nil
}
func (m *mockDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]*client.QueryStreamResponse, error) {
	return nil, nil
}
func (m *mockDistributor) LabelValuesForLabelName(context.Context, model.LabelName) ([]string, error) {
	return nil, nil
}
func (m *mockDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}
