package querier

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/cortexproject/cortex/pkg/chunk"
)

func newUnifiedChunkQueryable(ds, cs ChunkStore, distributor Distributor, chunkIteratorFunc chunkIteratorFunc) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &unifiedChunkQuerier{
			stores: []ChunkStore{ds, cs},
			querier: querier{
				ctx:         ctx,
				mint:        mint,
				maxt:        maxt,
				distributor: distributor,
			},
			csq: chunkStoreQuerier{
				chunkIteratorFunc: chunkIteratorFunc,
				ctx:               ctx,
				mint:              mint,
				maxt:              maxt,
			},
		}, nil
	})
}

type unifiedChunkQuerier struct {
	stores []ChunkStore

	// We reuse metadataQuery, LabelValues and Close from querier.
	querier

	// We reuse partitionChunks from chunkStoreQuerier.
	csq chunkStoreQuerier
}

func (q *unifiedChunkQuerier) Get(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	css := make(chan []chunk.Chunk, len(q.stores))
	errs := make(chan error, len(q.stores))
	for _, store := range q.stores {
		go func(store ChunkStore) {
			cs, err := store.Get(ctx, from, through, matchers...)
			if err != nil {
				errs <- err
			} else {
				css <- cs
			}
		}(store)
	}

	chunks := []chunk.Chunk{}
	for range q.stores {
		select {
		case err := <-errs:
			return nil, err
		case cs := <-css:
			chunks = append(chunks, cs...)
		}
	}
	return chunks, nil
}

// Select implements storage.Querier.
func (q *unifiedChunkQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	if sp == nil {
		return q.metadataQuery(matchers...)
	}

	chunks, err := q.Get(q.ctx, model.Time(sp.Start), model.Time(sp.End), matchers...)
	if err != nil {
		return nil, nil, err
	}

	return q.csq.partitionChunks(chunks), nil, nil
}
