package querier

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/querier/series"
)

// Distributor is the read interface to the distributor, made an interface here
// to reduce package coupling.
type Distributor interface {
	Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error)
	QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]client.TimeSeriesChunk, error)
	LabelValuesForLabelName(context.Context, model.LabelName) ([]string, error)
	LabelNames(context.Context) ([]string, error)
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

func (q *distributorQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	mint, maxt := q.mint, q.maxt
	if sp != nil {
		mint = sp.Start
		maxt = sp.End
	}

	matrix, err := q.distributor.Query(q.ctx, model.Time(mint), model.Time(maxt), matchers...)
	if err != nil {
		return nil, nil, promql.ErrStorage{Err: err}
	}

	return series.MatrixToSeriesSet(matrix), nil, nil
}

func (q *distributorQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	lv, err := q.distributor.LabelValuesForLabelName(q.ctx, model.LabelName(name))
	return lv, nil, err
}

func (q *distributorQuerier) LabelNames() ([]string, storage.Warnings, error) {
	ln, err := q.distributor.LabelNames(q.ctx)
	return ln, nil, err
}

func (q *distributorQuerier) Close() error {
	return nil
}
