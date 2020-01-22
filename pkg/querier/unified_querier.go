package querier

import (
	"context"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier/chunkstore"
)

func newUnifiedChunkQueryable(ds, cs chunkstore.ChunkStore, distributor Distributor, chunkIteratorFunc chunkIteratorFunc, ingesterMaxQueryLookback time.Duration) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		ucq := &unifiedChunkQuerier{
			stores: []chunkstore.ChunkStore{cs},
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
		}

		// Include ingester only if maxt is within ingesterMaxQueryLookback w.r.t. current time.
		if ingesterMaxQueryLookback == 0 || maxt >= time.Now().Add(-ingesterMaxQueryLookback).UnixNano()/1e6 {
			ucq.stores = append(ucq.stores, ds)
		}

		return ucq, nil
	})
}

type unifiedChunkQuerier struct {
	stores []chunkstore.ChunkStore

	// We reuse metadataQuery, LabelValues and Close from querier.
	querier

	// We reuse partitionChunks from chunkStoreQuerier.
	csq chunkStoreQuerier
}

func (q *unifiedChunkQuerier) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	css := make(chan []chunk.Chunk, len(q.stores))
	errs := make(chan error, len(q.stores))
	for _, store := range q.stores {
		go func(store chunkstore.ChunkStore) {
			cs, err := store.Get(ctx, userID, from, through, matchers...)
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
	userID, err := user.ExtractOrgID(q.ctx)
	if err != nil {
		return nil, nil, err
	}

	if sp == nil {
		return q.metadataQuery(matchers...)
	}

	chunks, err := q.Get(q.ctx, userID, model.Time(sp.Start), model.Time(sp.End), matchers...)
	if err != nil {
		return nil, nil, err
	}

	return q.csq.partitionChunks(chunks), nil, nil
}
