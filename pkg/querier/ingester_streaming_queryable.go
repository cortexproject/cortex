package querier

import (
	"context"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/weaveworks/common/user"
)

func newIngesterStreamingQueryable(distributor Distributor, chunkIteratorFunc chunkIteratorFunc) *ingesterQueryable {
	return &ingesterQueryable{
		distributor:       distributor,
		chunkIteratorFunc: chunkIteratorFunc,
	}
}

type ingesterQueryable struct {
	distributor       Distributor
	chunkIteratorFunc chunkIteratorFunc
}

// Querier implements storage.Queryable.
func (i ingesterQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &ingesterStreamingQuerier{
		chunkIteratorFunc: i.chunkIteratorFunc,
		distributorQuerier: distributorQuerier{
			distributor: i.distributor,
			ctx:         ctx,
			mint:        mint,
			maxt:        maxt,
		},
	}, nil
}

// Get implements ChunkStore.
func (i ingesterQueryable) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	results, err := i.distributor.QueryStream(ctx, from, through, matchers...)
	if err != nil {
		return nil, promql.ErrStorage{Err: err}
	}

	chunks := make([]chunk.Chunk, 0, len(results))
	for _, result := range results {
		// Sometimes the ingester can send series that have no data.
		if len(result.Chunks) == 0 {
			continue
		}

		metric := client.FromLabelAdaptersToLabels(result.Labels)
		cs, err := chunkcompat.FromChunks(userID, metric, result.Chunks)
		if err != nil {
			return nil, promql.ErrStorage{Err: err}
		}
		chunks = append(chunks, cs...)
	}

	return chunks, nil
}

type ingesterStreamingQuerier struct {
	chunkIteratorFunc chunkIteratorFunc
	distributorQuerier
}

func (q *ingesterStreamingQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	userID, err := user.ExtractOrgID(q.ctx)
	if err != nil {
		return nil, nil, promql.ErrStorage{Err: err}
	}

	mint, maxt := q.mint, q.maxt
	if sp != nil {
		mint = sp.Start
		maxt = sp.End
	}

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

		chunks, err := chunkcompat.FromChunks(userID, nil, result.Chunks)
		if err != nil {
			return nil, nil, promql.ErrStorage{Err: err}
		}

		ls := client.FromLabelAdaptersToLabels(result.Labels)
		sort.Sort(ls)
		series := &chunkSeries{
			labels:            ls,
			chunks:            chunks,
			chunkIteratorFunc: q.chunkIteratorFunc,
		}
		serieses = append(serieses, series)
	}

	return NewConcreteSeriesSet(serieses), nil, nil
}
