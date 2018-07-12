package querier

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"
)

// Distributor is the read interface to the distributor, made an interface here
// to reduce package coupling.
type Distributor interface {
	Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error)
	LabelValuesForLabelName(context.Context, model.LabelName) (model.LabelValues, error)
	MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error)
}

func newDistributorQueryable(distributor Distributor) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &distributorQuerier{
			distributor: distributor,
			ctx:         ctx,
			mint:        mint,
			maxt:        maxt,
		}, nil
	})
}

type distributorQuerier struct {
	distributor Distributor
	ctx         context.Context
	mint, maxt  int64
}

func (q *distributorQuerier) Select(_ *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	matrix, err := q.distributor.Query(q.ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	return matrixToSeriesSet(matrix), nil
}

func (q *distributorQuerier) LabelValues(name string) ([]string, error) {
	values, err := q.distributor.LabelValuesForLabelName(q.ctx, model.LabelName(name))
	if err != nil {
		return nil, err
	}

	result := make([]string, len(values), len(values))
	for i := 0; i < len(values); i++ {
		result[i] = string(values[i])
	}
	return result, nil
}

func (q *distributorQuerier) Close() error {
	return nil
}
