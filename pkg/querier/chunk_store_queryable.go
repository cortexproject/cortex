package querier

import (
	"context"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
)

type chunkIteratorFunc func(chunks []chunk.Chunk, from, through model.Time) storage.SeriesIterator

func newChunkStoreQueryable(store ChunkStore, chunkIteratorFunc chunkIteratorFunc) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &chunkStoreQuerier{
			store:             store,
			chunkIteratorFunc: chunkIteratorFunc,
			ctx:               ctx,
			mint:              mint,
			maxt:              maxt,
		}, nil
	})
}

type chunkStoreQuerier struct {
	store             ChunkStore
	chunkIteratorFunc chunkIteratorFunc
	ctx               context.Context
	mint, maxt        int64
}

func (q *chunkStoreQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	userID, err := user.ExtractOrgID(q.ctx)
	if err != nil {
		return nil, nil, err
	}
	chunks, err := q.store.Get(q.ctx, userID, model.Time(sp.Start), model.Time(sp.End), matchers...)
	if err != nil {
		return nil, nil, promql.ErrStorage{Err: err}
	}

	return q.partitionChunks(chunks), nil, nil
}

func (q *chunkStoreQuerier) partitionChunks(chunks []chunk.Chunk) storage.SeriesSet {
	chunksBySeries := map[model.Fingerprint][]chunk.Chunk{}
	for _, c := range chunks {
		fp := client.Fingerprint(c.Metric)
		chunksBySeries[fp] = append(chunksBySeries[fp], c)
	}

	series := make([]storage.Series, 0, len(chunksBySeries))
	for i := range chunksBySeries {
		series = append(series, &chunkSeries{
			labels:            chunksBySeries[i][0].Metric,
			chunks:            chunksBySeries[i],
			chunkIteratorFunc: q.chunkIteratorFunc,
			mint:              q.mint,
			maxt:              q.maxt,
		})
	}

	return NewConcreteSeriesSet(series)
}

func (q *chunkStoreQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (q *chunkStoreQuerier) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (q *chunkStoreQuerier) Close() error {
	return nil
}

type chunkSeries struct {
	labels            labels.Labels
	chunks            []chunk.Chunk
	chunkIteratorFunc chunkIteratorFunc
	mint, maxt        int64
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.labels
}

// Iterator returns a new iterator of the data of the series.
func (s *chunkSeries) Iterator() storage.SeriesIterator {
	return s.chunkIteratorFunc(s.chunks, model.Time(s.mint), model.Time(s.maxt))
}
