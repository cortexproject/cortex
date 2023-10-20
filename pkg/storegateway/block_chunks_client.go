package storegateway

import (
	"context"

	"github.com/cortexproject/cortex/pkg/storegateway/storepb"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Loads a batch of chunks
type blockChunkClient struct {
	ctx    context.Context
	logger log.Logger

	mint           int64
	maxt           int64
	chunkr         *bucketChunkReader
	loadAggregates []storepb.Aggr
	chunksLimiter  ChunksLimiter
	bytesLimiter   BytesLimiter

	calculateChunkHash     bool
	seriesFetchDurationSum *prometheus.HistogramVec
	chunkFetchDuration     *prometheus.HistogramVec
	chunkFetchDurationSum  *prometheus.HistogramVec
	tenant                 string

	batchSize int
}

func newBlockChunksClient(
	ctx context.Context,
	logger log.Logger,
	b *bucketBlock,
	req *storepb.SeriesRequest,
	limiter ChunksLimiter,
	bytesLimiter BytesLimiter,
	calculateChunkHash bool,
	batchSize int,
	chunkFetchDuration *prometheus.HistogramVec,
	chunkFetchDurationSum *prometheus.HistogramVec,
	tenant string,
) *blockChunkClient {
	return &blockChunkClient{
		ctx:    ctx,
		logger: logger,
		mint:   req.MinTime,
		maxt:   req.MaxTime,
		chunkr: b.chunkReader(),
	}
}

func (b *blockChunkClient) loadChunks(entries []seriesEntry) error {
	b.chunkr.reset()

	for i, s := range entries {
		for j := range s.chks {
			if err := b.chunkr.addLoad(s.refs[j], i, j); err != nil {
				return errors.Wrap(err, "add chunk load")
			}
		}
	}

	if err := b.chunkr.load(b.ctx, entries, b.loadAggregates, b.calculateChunkHash, b.bytesLimiter, b.tenant); err != nil {
		return errors.Wrap(err, "load chunks")
	}

	return nil
}
