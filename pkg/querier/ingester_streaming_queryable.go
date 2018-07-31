package querier

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	prom_chunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

func newIngesterStreamingQueryable(distributor Distributor) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &ingesterStreamingQuerier{
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
		chunks, err := fromChunks(userID, result.Chunks)
		if err != nil {
			return nil, promql.ErrStorage(err)
		}

		serieses = append(serieses, &chunkSeries{
			labels:            fromLabelPairs(result.Labels),
			chunks:            chunks,
			chunkIteratorFunc: q.chunkIteratorFunc,
		})
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

func fromChunks(userID string, in []client.Chunk) ([]chunk.Chunk, error) {
	out := make([]chunk.Chunk, 0, len(in))
	for _, i := range in {
		o, err := prom_chunk.NewForEncoding(prom_chunk.Encoding(byte(i.Encoding)))
		if err != nil {
			return nil, err
		}

		if err := o.UnmarshalFromBuf(i.Data); err != nil {
			return nil, err
		}

		firstTime, lastTime := model.Time(i.StartTimestampMs), model.Time(i.EndTimestampMs)
		// As the lifetime of this chunk is scopes to this request, we don't need
		// to supply a fingerprint or
		out = append(out, chunk.NewChunk(userID, 0, nil, o, firstTime, lastTime))
	}
	return out, nil
}
