package storegateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

type parquetBucketStore struct {
	logger      log.Logger
	bucket      objstore.Bucket
	limits      *validation.Overrides
	concurrency int

	chunksDecoder *schema.PrometheusParquetChunksDecoder

	matcherCache storecache.MatchersCache
}

func (p *parquetBucketStore) Close() error {
	return p.bucket.Close()
}

func (p *parquetBucketStore) SyncBlocks(ctx context.Context) error {
	// TODO: Implement it
	return nil
}

func (p *parquetBucketStore) InitialSync(ctx context.Context) error {
	// TODO: Implement it
	return nil
}

func (p *parquetBucketStore) findParquetBlocks(ctx context.Context, blockMatchers []storepb.LabelMatcher) ([]*parquetBlock, error) {
	if len(blockMatchers) != 1 || blockMatchers[0].Type != storepb.LabelMatcher_RE || blockMatchers[0].Name != block.BlockIDLabel {
		return nil, status.Error(codes.InvalidArgument, "only one block matcher is supported")
	}

	blockIDs := strings.Split(blockMatchers[0].Value, "|")
	blocks := make([]*parquetBlock, 0, len(blockIDs))
	bucketOpener := parquet_storage.NewParquetBucketOpener(p.bucket)
	noopQuota := search.NewQuota(search.NoopQuotaLimitFunc(ctx))
	for _, blockID := range blockIDs {
		block, err := p.newParquetBlock(ctx, blockID, bucketOpener, bucketOpener, p.chunksDecoder, noopQuota, noopQuota, noopQuota)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

// Series implements the store interface for a single parquet bucket store
func (p *parquetBucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
	matchers, err := storecache.MatchersToPromMatchersCached(p.matcherCache, req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	ctx := srv.Context()
	resHints := &hintspb.SeriesResponseHints{}
	var anyHints *types.Any

	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.SeriesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal series request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}
	ctx = injectShardInfoIntoContext(ctx, req.ShardInfo)

	// Find parquet shards for the time range
	shards, err := p.findParquetBlocks(ctx, blockMatchers)
	if err != nil {
		return fmt.Errorf("failed to find parquet shards: %w", err)
	}

	seriesSet := make([]prom_storage.ChunkSeriesSet, len(shards))
	errGroup, ctx := errgroup.WithContext(srv.Context())
	errGroup.SetLimit(p.concurrency)

	for i, shard := range shards {
		resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
			Id: shard.name,
		})
		errGroup.Go(func() error {
			ss, err := shard.Query(ctx, req.MinTime, req.MaxTime, req.SkipChunks, matchers)
			seriesSet[i] = ss
			return err
		})
	}

	if err = errGroup.Wait(); err != nil {
		return err
	}

	ss := convert.NewMergeChunkSeriesSet(seriesSet, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger())
	for ss.Next() {
		cs := ss.At()
		cIter := cs.Iterator(nil)
		chunks := make([]storepb.AggrChunk, 0)
		for cIter.Next() {
			chunk := cIter.At()
			chunks = append(chunks, storepb.AggrChunk{
				MinTime: chunk.MinTime,
				MaxTime: chunk.MaxTime,
				Raw: &storepb.Chunk{
					Type: chunkToStoreEncoding(chunk.Chunk.Encoding()),
					Data: chunk.Chunk.Bytes(),
				},
			})
		}

		if err = srv.Send(storepb.NewSeriesResponse(&storepb.Series{
			Labels: labelpb.ZLabelsFromPromLabels(cs.Labels()),
			Chunks: chunks,
		})); err != nil {
			err = status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
			return
		}
	}

	if anyHints, err = types.MarshalAny(resHints); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "marshal series response hints").Error())
		return
	}

	if err = srv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
		return
	}

	return nil
}

// LabelNames implements the store interface for a single parquet bucket store
func (p *parquetBucketStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	matchers, err := storecache.MatchersToPromMatchersCached(p.matcherCache, req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resHints := &hintspb.LabelNamesResponseHints{}

	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelNamesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label names request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}

	// Find parquet shards for the time range
	shards, err := p.findParquetBlocks(ctx, blockMatchers)
	if err != nil {
		return nil, fmt.Errorf("failed to find parquet shards: %w", err)
	}

	resNameSets := make([][]string, len(shards))
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(p.concurrency)

	for i, s := range shards {
		resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
			Id: s.name,
		})
		errGroup.Go(func() error {
			r, err := s.LabelNames(ctx, req.Limit, matchers)
			resNameSets[i] = r
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label names response hints").Error())
	}
	result := util.MergeUnsortedSlices(int(req.Limit), resNameSets...)

	return &storepb.LabelNamesResponse{
		Names: result,
		Hints: anyHints,
	}, nil
}

// LabelValues implements the store interface for a single parquet bucket store
func (p *parquetBucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	matchers, err := storecache.MatchersToPromMatchersCached(p.matcherCache, req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resHints := &hintspb.LabelValuesResponseHints{}
	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelValuesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label values request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}

	// Find parquet shards for the time range
	shards, err := p.findParquetBlocks(ctx, blockMatchers)
	if err != nil {
		return nil, fmt.Errorf("failed to find parquet shards: %w", err)
	}

	resNameValues := make([][]string, len(shards))
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(p.concurrency)

	for i, s := range shards {
		resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
			Id: s.name,
		})
		errGroup.Go(func() error {
			r, err := s.LabelValues(ctx, req.Label, req.Limit, matchers)
			resNameValues[i] = r
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label values response hints").Error())
	}
	result := util.MergeUnsortedSlices(int(req.Limit), resNameValues...)

	return &storepb.LabelValuesResponse{
		Values: result,
		Hints:  anyHints,
	}, nil
}
