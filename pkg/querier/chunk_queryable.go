package querier

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/cortex/pkg/chunk"
)

// ChunkStore is the read-interface to the Chunk Store.  Made an interface here
// to reduce package coupling.
type ChunkStore interface {
	Get(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error)
}

// NewQueryable creates a new Queryable for cortex.
func NewQueryable(distributor Distributor, chunkStore ChunkStore) storage.Queryable {
	dq := newDistributorQueryable(distributor)
	cq := newChunkQueryable(chunkStore)

	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		dqr, err := dq.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}

		cqr, err := cq.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}

		return querier{
			Querier:     storage.NewMergeQuerier([]storage.Querier{dqr, cqr}),
			distributor: distributor,
			ctx:         ctx,
			mint:        mint,
			maxt:        maxt,
		}, nil
	})
}

type querier struct {
	storage.Querier

	distributor Distributor
	ctx         context.Context
	mint, maxt  int64
}

func (q querier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	// Kludge: Prometheus passes nil SelectParams if it is doing a 'series' operation,
	// which needs only metadata.
	if sp != nil {
		return q.Querier.Select(sp, matchers...)
	}

	ms, err := q.distributor.MetricsForLabelMatchers(q.ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	if err != nil {
		return nil, err
	}
	return metricsToSeriesSet(ms), nil
}

func newChunkQueryable(store ChunkStore) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &chunkQuerier{
			store: store,
			ctx:   ctx,
			mint:  mint,
			maxt:  maxt,
		}, nil
	})
}

type chunkQuerier struct {
	store      ChunkStore
	ctx        context.Context
	mint, maxt int64
}

func (c chunkQuerier) Select(_ *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	chunks, err := c.store.Get(c.ctx, model.Time(c.mint), model.Time(c.maxt), matchers...)
	if err != nil {
		return nil, err
	}

	matrix, err := chunk.ChunksToMatrix(c.ctx, chunks, model.Time(c.mint), model.Time(c.maxt))
	if err != nil {
		return nil, err
	}

	return matrixToSeriesSet(matrix), nil
}

func (c chunkQuerier) LabelValues(name string) ([]string, error) {
	return nil, nil
}

func (c chunkQuerier) Close() error {
	return nil
}
