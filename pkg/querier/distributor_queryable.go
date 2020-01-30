package querier

import (
	"context"
	"sort"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"
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

func newDistributorQueryable(distributor Distributor, streaming bool, iteratorFn chunkIteratorFunc) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &distributorQuerier{
			distributor: distributor,
			ctx:         ctx,
			mint:        mint,
			maxt:        maxt,
			streaming:   streaming,
			chunkIterFn: iteratorFn,
		}, nil
	})
}

type distributorQuerier struct {
	distributor Distributor
	ctx         context.Context
	mint, maxt  int64
	streaming   bool
	chunkIterFn chunkIteratorFunc
}

func (q *distributorQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	// Kludge: Prometheus passes nil SelectParams if it is doing a 'series' operation,
	// which needs only metadata.
	if sp == nil {
		ms, err := q.distributor.MetricsForLabelMatchers(q.ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
		if err != nil {
			return nil, nil, err
		}
		return metricsToSeriesSet(ms), nil, nil
	}

	mint, maxt := sp.Start, sp.End

	if q.streaming {
		return q.streamingSelect(*sp, matchers)
	}

	matrix, err := q.distributor.Query(q.ctx, model.Time(mint), model.Time(maxt), matchers...)
	if err != nil {
		return nil, nil, promql.ErrStorage{Err: err}
	}

	return matrixToSeriesSet(matrix), nil, nil
}

func (q *distributorQuerier) streamingSelect(sp storage.SelectParams, matchers []*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	userID, err := user.ExtractOrgID(q.ctx)
	if err != nil {
		return nil, nil, promql.ErrStorage{Err: err}
	}

	mint, maxt := sp.Start, sp.End

	results, err := q.distributor.QueryStream(q.ctx, model.Time(mint), model.Time(maxt), matchers...)
	if err != nil {
		return nil, nil, promql.ErrStorage{Err: err}
	}

	serieses := make([]storage.Series, 0, len(results))
	for _, result := range results {
		// Sometimes the ingester can send series that have no data.
		if len(result.Chunks) == 0 {
			continue
		}

		metric := client.FromLabelAdaptersToLabels(result.Labels)
		chunks, err := chunkcompat.FromChunks(userID, metric, result.Chunks)
		if err != nil {
			return nil, nil, promql.ErrStorage{Err: err}
		}

		ls := client.FromLabelAdaptersToLabels(result.Labels)
		sort.Sort(ls)
		series := &chunkSeries{
			labels:            ls,
			chunks:            chunks,
			chunkIteratorFunc: q.chunkIterFn,
		}
		serieses = append(serieses, series)
	}

	return newConcreteSeriesSet(serieses), nil, nil
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
