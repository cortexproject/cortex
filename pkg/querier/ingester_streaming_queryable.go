package querier

import (
	"context"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util/chunkcompat"
)

func newIngesterStreamingQueryable(distributor Distributor) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &ingesterStreamingQuerier{
			chunkIteratorFunc: newChunkMergeIterator,
			distributorQuerier: distributorQuerier{
				distributor: distributor,
				ctx:         ctx,
				mint:        mint,
				maxt:        maxt,
			},
		}, nil
	})
}

type ingesterStreamingQuerier struct {
	chunkIteratorFunc chunkIteratorFunc
	distributorQuerier
}

func (q *ingesterStreamingQuerier) Select(_ *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	userID, err := user.ExtractOrgID(q.ctx)
	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	results, err := q.distributor.QueryStream(q.ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	serieses := make([]storage.Series, 0, len(results))
	for _, result := range results {
		chunks, err := chunkcompat.FromChunks(userID, result.Chunks)
		if err != nil {
			return nil, promql.ErrStorage(err)
		}

		ls := fromLabelPairs(result.Labels)
		sort.Sort(ls)
		series := &chunkSeries{
			labels:            ls,
			chunks:            chunks,
			chunkIteratorFunc: q.chunkIteratorFunc,
		}
		serieses = append(serieses, series)
	}

	return newConcreteSeriesSet(serieses), nil
}

func fromLabelPairs(in []client.LabelPair) labels.Labels {
	out := make(labels.Labels, 0, len(in))
	for _, pair := range in {
		out = append(out, labels.Label{
			Name:  string(pair.Name),
			Value: string(pair.Value),
		})
	}
	return out
}
