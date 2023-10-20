package storegateway

import (
	"context"

	"github.com/cortexproject/cortex/pkg/storegateway/storepb"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/codes"
)

type blockIndexRequest struct {
	maxTime    int64
	minTime    int64
	skipChunks bool
}

type blockIndexClient struct {
	ctx             context.Context
	logger          log.Logger
	extLset         labels.Labels
	extLsetToRemove map[string]struct{}

	mint                                          int64
	maxt                                          int64
	indexr                                        *bucketIndexReader
	bytesLimiter                                  BytesLimiter
	lazyExpandedPostingEnabled                    bool
	lazyExpandedPostingsCount                     prometheus.Counter
	lazyExpandedPostingSizeBytes                  prometheus.Counter
	lazyExpandedPostingSeriesOverfetchedSizeBytes prometheus.Counter

	skipChunks    bool
	shardMatcher  *storepb.ShardMatcher
	blockMatchers []*labels.Matcher

	seriesFetchDurationSum *prometheus.HistogramVec

	tenant string

	// Internal state.
	i                uint64
	lazyPostings     *lazyExpandedPostings
	expandedPostings []storage.SeriesRef
	chkMetas         []chunks.Meta
	lset             labels.Labels
	symbolizedLset   []symbolizedLabel
	entries          []seriesEntry
	hasMorePostings  bool
	batchSize        int
}

func newBlockIndexClient(
	ctx context.Context,
	logger log.Logger,
	b *bucketBlock,
	req blockIndexRequest,
	bytesLimiter BytesLimiter,
	blockMatchers []*labels.Matcher,
	shardMatcher *storepb.ShardMatcher,
	batchSize int,
	seriesFetchDurationSum *prometheus.HistogramVec,
	extLsetToRemove map[string]struct{},
	lazyExpandedPostingEnabled bool,
	lazyExpandedPostingsCount prometheus.Counter,
	lazyExpandedPostingSizeBytes prometheus.Counter,
	lazyExpandedPostingSeriesOverfetchedSizeBytes prometheus.Counter,
	tenant string,
) *blockIndexClient {
	extLset := b.extLset
	if extLsetToRemove != nil {
		extLset = rmLabels(extLset.Copy(), extLsetToRemove)
	}

	return &blockIndexClient{
		ctx:             ctx,
		logger:          logger,
		extLset:         extLset,
		extLsetToRemove: extLsetToRemove,

		mint:                   req.minTime,
		maxt:                   req.maxTime,
		indexr:                 b.indexReader(),
		bytesLimiter:           bytesLimiter,
		skipChunks:             req.skipChunks,
		seriesFetchDurationSum: seriesFetchDurationSum,

		lazyExpandedPostingEnabled:                    lazyExpandedPostingEnabled,
		lazyExpandedPostingsCount:                     lazyExpandedPostingsCount,
		lazyExpandedPostingSizeBytes:                  lazyExpandedPostingSizeBytes,
		lazyExpandedPostingSeriesOverfetchedSizeBytes: lazyExpandedPostingSeriesOverfetchedSizeBytes,

		shardMatcher:    shardMatcher,
		blockMatchers:   blockMatchers,
		hasMorePostings: true,
		batchSize:       batchSize,
		tenant:          tenant,
	}
}

func (b *blockIndexClient) ExpandPostings(
	matchers sortedMatchers,
	seriesLimiter SeriesLimiter,
) error {
	ps, err := b.indexr.ExpandedPostings(b.ctx, matchers, b.bytesLimiter, b.lazyExpandedPostingEnabled, b.lazyExpandedPostingSizeBytes, b.tenant)
	if err != nil {
		return errors.Wrap(err, "expanded matching posting")
	}

	if ps == nil || len(ps.postings) == 0 {
		b.lazyPostings = emptyLazyPostings
		return nil
	}
	b.lazyPostings = ps

	// If lazy expanded posting enabled, it is possible to fetch more series
	//  so easier to hit the series limit.
	if err := seriesLimiter.Reserve(uint64(len(ps.postings))); err != nil {
		return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded series limit: %s", err)
	}

	if b.batchSize > len(ps.postings) {
		b.batchSize = len(ps.postings)
	}
	if b.lazyPostings.lazyExpanded() {
		// Assume lazy expansion could cut actual expanded postings length to 50%.
		b.expandedPostings = make([]storage.SeriesRef, 0, len(b.lazyPostings.postings)/2)
		b.lazyExpandedPostingsCount.Inc()
	}
	b.entries = make([]seriesEntry, 0, b.batchSize)
	return nil
}

func (b *blockIndexClient) MergeStats(stats *queryStats) *queryStats {
	stats.merge(b.indexr.stats)
	return stats
}

func (b *blockIndexClient) Close() {
	runutil.CloseWithLogOnErr(b.logger, b.indexr, "series block")
}

func (b *blockIndexClient) hasNext() bool {
	return b.hasMorePostings
}

func (b *blockIndexClient) nextBatch(tenant string) ([]seriesEntry, error) {
	start := b.i
	end := start + uint64(b.batchSize)
	if end > uint64(len(b.lazyPostings.postings)) {
		end = uint64(len(b.lazyPostings.postings))
	}
	b.i = end

	postingsBatch := b.lazyPostings.postings[start:end]
	if len(postingsBatch) == 0 {
		b.hasMorePostings = false
		if b.lazyPostings.lazyExpanded() {
			v, err := b.indexr.IndexVersion()
			if err != nil {
				return nil, errors.Wrap(err, "get index version")
			}
			if v >= 2 {
				for i := range b.expandedPostings {
					b.expandedPostings[i] = b.expandedPostings[i] / 16
				}
			}
			b.indexr.storeExpandedPostingsToCache(b.blockMatchers, index.NewListPostings(b.expandedPostings), len(b.expandedPostings), tenant)
		}
		return nil, nil
	}

	b.indexr.reset(len(postingsBatch))

	if err := b.indexr.PreloadSeries(b.ctx, postingsBatch, b.bytesLimiter, b.tenant); err != nil {
		return nil, errors.Wrap(err, "preload series")
	}

	b.entries = b.entries[:0]
OUTER:
	for i := 0; i < len(postingsBatch); i++ {
		if err := b.ctx.Err(); err != nil {
			return nil, err
		}
		ok, err := b.indexr.LoadSeriesForTime(postingsBatch[i], &b.symbolizedLset, &b.chkMetas, b.skipChunks, b.mint, b.maxt)
		if err != nil {
			return nil, errors.Wrap(err, "read series")
		}
		if !ok {
			continue
		}

		if err := b.indexr.LookupLabelsSymbols(b.ctx, b.symbolizedLset, &b.lset); err != nil {
			return nil, errors.Wrap(err, "Lookup labels symbols")
		}

		for _, matcher := range b.lazyPostings.matchers {
			val := b.lset.Get(matcher.Name)
			if !matcher.Matches(val) {
				// Series not matched means series we overfetched due to lazy posting expansion.
				seriesBytes := b.indexr.loadedSeries[postingsBatch[i]]
				b.lazyExpandedPostingSeriesOverfetchedSizeBytes.Add(float64(len(seriesBytes)))
				continue OUTER
			}
		}
		if b.lazyPostings.lazyExpanded() {
			b.expandedPostings = append(b.expandedPostings, postingsBatch[i])
		}

		completeLabelset := labelpb.ExtendSortedLabels(b.lset, b.extLset)
		if b.extLsetToRemove != nil {
			completeLabelset = rmLabels(completeLabelset, b.extLsetToRemove)
		}

		if !b.shardMatcher.MatchesLabels(completeLabelset) {
			continue
		}

		s := seriesEntry{lset: completeLabelset}
		if b.skipChunks {
			b.entries = append(b.entries, s)
			continue
		}

		// Schedule loading chunks.
		s.refs = make([]chunks.ChunkRef, 0, len(b.chkMetas))
		s.chks = make([]storepb.AggrChunk, 0, len(b.chkMetas))

		for _, meta := range b.chkMetas {
			s.refs = append(s.refs, meta.Ref)
			s.chks = append(s.chks, storepb.AggrChunk{
				MinTime: meta.MinTime,
				MaxTime: meta.MaxTime,
			})
		}

		b.entries = append(b.entries, s)
	}

	return b.entries, nil
}
