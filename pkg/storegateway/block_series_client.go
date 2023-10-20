package storegateway

import (
	"context"
	"io"

	"github.com/cortexproject/cortex/pkg/storegateway/storepb"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"google.golang.org/grpc"
)

type seriesEntry struct {
	lset labels.Labels
	refs []chunks.ChunkRef
	chks []storepb.AggrChunk
}

// blockSeriesClient is a storepb.Store_SeriesClient for a
// single TSDB block in object storage.
type blockSeriesClient struct {
	grpc.ClientStream
	ctx             context.Context
	logger          log.Logger
	extLset         labels.Labels
	extLsetToRemove map[string]struct{}

	mint           int64
	maxt           int64
	indexr         *bucketIndexReader
	chunkr         *bucketChunkReader
	loadAggregates []storepb.Aggr
	chunksLimiter  ChunksLimiter
	bytesLimiter   BytesLimiter

	lazyExpandedPostingEnabled                    bool
	lazyExpandedPostingsCount                     prometheus.Counter
	lazyExpandedPostingSizeBytes                  prometheus.Counter
	lazyExpandedPostingSeriesOverfetchedSizeBytes prometheus.Counter

	skipChunks             bool
	shardMatcher           *storepb.ShardMatcher
	blockMatchers          []*labels.Matcher
	calculateChunkHash     bool
	seriesFetchDurationSum *prometheus.HistogramVec
	chunkFetchDuration     *prometheus.HistogramVec
	chunkFetchDurationSum  *prometheus.HistogramVec
	tenant                 string

	entries   []seriesEntry
	batchSize int

	// New stuff
	blockIndexClient blockIndexClient
}

func newBlockSeriesClient(
	ctx context.Context,
	logger log.Logger,
	b *bucketBlock,
	req *storepb.SeriesRequest,
	limiter ChunksLimiter,
	bytesLimiter BytesLimiter,
	blockMatchers []*labels.Matcher,
	shardMatcher *storepb.ShardMatcher,
	calculateChunkHash bool,
	batchSize int,
	seriesFetchDurationSum *prometheus.HistogramVec,
	chunkFetchDuration *prometheus.HistogramVec,
	chunkFetchDurationSum *prometheus.HistogramVec,
	extLsetToRemove map[string]struct{},
	lazyExpandedPostingEnabled bool,
	lazyExpandedPostingsCount prometheus.Counter,
	lazyExpandedPostingSizeBytes prometheus.Counter,
	lazyExpandedPostingSeriesOverfetchedSizeBytes prometheus.Counter,
	tenant string,
) *blockSeriesClient {
	var chunkr *bucketChunkReader
	if !req.SkipChunks {
		chunkr = b.chunkReader()
	}

	extLset := b.extLset
	if extLsetToRemove != nil {
		extLset = rmLabels(extLset.Copy(), extLsetToRemove)
	}

	blockIndexClient := newBlockIndexClient(
		ctx,
		logger,
		b,
		blockIndexRequest{minTime: req.MinTime, maxTime: req.MaxTime},
		bytesLimiter,
		blockMatchers,
		shardMatcher,
		batchSize,
		seriesFetchDurationSum,
		extLsetToRemove,
		lazyExpandedPostingEnabled,
		lazyExpandedPostingsCount,
		lazyExpandedPostingSizeBytes,
		lazyExpandedPostingSeriesOverfetchedSizeBytes,
		tenant,
	)

	return &blockSeriesClient{
		ctx:             ctx,
		logger:          logger,
		extLset:         extLset,
		extLsetToRemove: extLsetToRemove,

		mint:                   req.MinTime,
		maxt:                   req.MaxTime,
		indexr:                 b.indexReader(),
		chunkr:                 chunkr,
		chunksLimiter:          limiter,
		bytesLimiter:           bytesLimiter,
		skipChunks:             req.SkipChunks,
		seriesFetchDurationSum: seriesFetchDurationSum,
		chunkFetchDuration:     chunkFetchDuration,
		chunkFetchDurationSum:  chunkFetchDurationSum,

		lazyExpandedPostingEnabled:                    lazyExpandedPostingEnabled,
		lazyExpandedPostingsCount:                     lazyExpandedPostingsCount,
		lazyExpandedPostingSizeBytes:                  lazyExpandedPostingSizeBytes,
		lazyExpandedPostingSeriesOverfetchedSizeBytes: lazyExpandedPostingSeriesOverfetchedSizeBytes,

		loadAggregates:     req.Aggregates,
		shardMatcher:       shardMatcher,
		blockMatchers:      blockMatchers,
		calculateChunkHash: calculateChunkHash,
		batchSize:          batchSize,
		tenant:             tenant,

		// New
		blockIndexClient: *blockIndexClient,
	}
}

func (b *blockSeriesClient) Close() {
	if !b.skipChunks {
		runutil.CloseWithLogOnErr(b.logger, b.chunkr, "series block")
	}

	b.blockIndexClient.Close()
}

func (b *blockSeriesClient) MergeStats(stats *queryStats) *queryStats {
	b.blockIndexClient.MergeStats(stats)
	if !b.skipChunks {
		stats.merge(b.chunkr.stats)
	}
	return stats
}

func (b *blockSeriesClient) ExpandPostings(
	matchers sortedMatchers,
	seriesLimiter SeriesLimiter,
) error {
	return b.blockIndexClient.ExpandPostings(matchers, seriesLimiter)
}

func (b *blockSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	for len(b.entries) == 0 && b.blockIndexClient.hasNext() {
		if err := b.nextBatch(b.tenant); err != nil {
			return nil, err
		}
	}

	if len(b.entries) == 0 {
		b.seriesFetchDurationSum.WithLabelValues(b.tenant).Observe(b.indexr.stats.SeriesDownloadLatencySum.Seconds())
		if b.chunkr != nil {
			b.chunkFetchDuration.WithLabelValues(b.tenant).Observe(b.chunkr.stats.ChunksFetchDurationSum.Seconds())
			b.chunkFetchDurationSum.WithLabelValues(b.tenant).Observe(b.chunkr.stats.ChunksDownloadLatencySum.Seconds())
		}
		return nil, io.EOF
	}

	next := b.entries[0]
	b.entries = b.entries[1:]

	return storepb.NewSeriesResponse(&storepb.Series{
		Labels: labelpb.ZLabelsFromPromLabels(next.lset),
		Chunks: next.chks,
	}), nil
}

func (b *blockSeriesClient) nextBatch(tenant string) error {
	seriesEntries, err := b.blockIndexClient.nextBatch(tenant)
	if err != nil {
		return err
	}

	b.entries = seriesEntries

	if b.skipChunks {
		return nil
	}

	b.chunkr.reset()

	for i, s := range b.entries {
		for j := range s.chks {
			if err := b.chunkr.addLoad(s.refs[j], i, j); err != nil {
				return errors.Wrap(err, "add chunk load")
			}
		}
	}

	if err := b.chunkr.load(b.ctx, b.entries, b.loadAggregates, b.calculateChunkHash, b.bytesLimiter, b.tenant); err != nil {
		return errors.Wrap(err, "load chunks")
	}

	return nil
}
