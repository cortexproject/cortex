package querier

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/weaveworks/cortex/pkg/chunk"
)

func newIterChunkQueryable(store ChunkStore) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &iterChunkQuerier{
			store: store,
			ctx:   ctx,
			mint:  mint,
			maxt:  maxt,
		}, nil
	})
}

type iterChunkQuerier struct {
	store      ChunkStore
	ctx        context.Context
	mint, maxt int64
}

func (q *iterChunkQuerier) Select(_ *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	chunks, err := q.store.Get(q.ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	chunksBySeries := map[model.Fingerprint][]chunk.Chunk{}
	for _, c := range chunks {
		fp := c.Metric.Fingerprint()
		chunksBySeries[fp] = append(chunksBySeries[fp], c)
	}

	series := make([]storage.Series, 0, len(chunksBySeries))
	for i := range chunksBySeries {
		series = append(series, &chunkSeries{
			labels: metricToLabels(chunksBySeries[i][0].Metric),
			chunks: chunksBySeries[i],
		})
	}

	return newConcreteSeriesSet(series), nil
}

func (q *iterChunkQuerier) LabelValues(name string) ([]string, error) {
	return nil, nil
}

func (q *iterChunkQuerier) Close() error {
	return nil
}

type chunkSeries struct {
	labels labels.Labels
	chunks []chunk.Chunk
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.labels
}

// Iterator returns a new iterator of the data of the series.
func (s *chunkSeries) Iterator() storage.SeriesIterator {
	return newChunkMergeIteratorV2(s.chunks)
}
