package storegateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid/v2"
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

	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
	"github.com/cortexproject/cortex/pkg/util/parquetutil"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type parquetBucketStore struct {
	logger      log.Logger
	bucket      objstore.InstrumentedBucket
	limits      *validation.Overrides
	concurrency int

	chunksDecoder *schema.PrometheusParquetChunksDecoder

	matcherCache      storecache.MatchersCache
	parquetShardCache parquetutil.CacheInterface[parquet_storage.ParquetShard]
	rowRangesCache    search.RowRangesForConstraintsCache
}

func (p *parquetBucketStore) Close() error {
	p.parquetShardCache.Close()
	return p.bucket.Close()
}

func (p *parquetBucketStore) SyncBlocks(ctx context.Context) error {
	return nil
}

func (p *parquetBucketStore) InitialSync(ctx context.Context) error {
	return nil
}

func (p *parquetBucketStore) findParquetBlocks(ctx context.Context, blockMatchers []storepb.LabelMatcher) ([]*parquetBlock, error) {
	if len(blockMatchers) != 1 || blockMatchers[0].Type != storepb.LabelMatcher_RE || blockMatchers[0].Name != block.BlockIDLabel {
		return nil, status.Error(codes.InvalidArgument, "only one block matcher is supported")
	}

	blockIDs := strings.Split(blockMatchers[0].Value, "|")
	bucketOpener := parquet_storage.NewParquetBucketOpener(p.bucket)
	noopQuota := search.NewQuota(search.NoopQuotaLimitFunc(ctx))

	// Read converter marks and expand to per-shard (blockID, shardID) lists.
	// TODO(Sungjin1212): Read the shard count from the bucket index instead of reading the converter mark for each block.
	var shardBlockIDs []string
	var shardIDs []int
	for _, blockID := range blockIDs {
		uid, err := ulid.Parse(blockID)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse block ID %s", blockID)
		}
		marker, err := cortex_parquet.ReadConverterMark(ctx, uid, p.bucket, p.logger)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read converter mark for block %s", blockID)
		}
		numShards := marker.Shards
		if numShards <= 0 {
			// backward compatibility: blocks without a shard count have one shard
			numShards = 1
		}
		for shardID := 0; shardID < numShards; shardID++ {
			shardBlockIDs = append(shardBlockIDs, blockID)
			shardIDs = append(shardIDs, shardID)
		}
	}

	// Open all shards in parallel.
	parquetBlocks := make([]*parquetBlock, len(shardBlockIDs))
	errGroup, egCtx := errgroup.WithContext(ctx)
	errGroup.SetLimit(p.concurrency)
	for i := range shardBlockIDs {
		errGroup.Go(func() error {
			blk, err := p.newParquetBlock(egCtx, shardBlockIDs[i], shardIDs[i], bucketOpener, bucketOpener, p.chunksDecoder, p.rowRangesCache, noopQuota, noopQuota, noopQuota)
			if err != nil {
				return err
			}
			parquetBlocks[i] = blk
			return nil
		})
	}
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return parquetBlocks, nil
}

// Series implements the store interface for a single parquet bucket store
func (p *parquetBucketStore) Series(req *storepb.SeriesRequest, seriesSrv storepb.Store_SeriesServer) (err error) {
	spanLog, ctx := spanlogger.New(seriesSrv.Context(), "ParquetBucketStore.Series")
	defer spanLog.Finish()

	srv := newFlushableServer(newBatchableServer(seriesSrv, int(req.ResponseBatchSize)))

	matchers, err := storecache.MatchersToPromMatchersCached(p.matcherCache, req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

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

	seenBlocks := make(map[string]struct{}, len(shards))
	for i, shard := range shards {
		if _, seen := seenBlocks[shard.name]; !seen {
			seenBlocks[shard.name] = struct{}{}
			resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
				Id: shard.name,
			})
		}
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

	return srv.Flush()
}

// LabelNames implements the store interface for a single parquet bucket store
func (p *parquetBucketStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	spanLog, ctx := spanlogger.New(ctx, "ParquetBucketStore.LabelNames")
	defer spanLog.Finish()

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

	seenBlocks := make(map[string]struct{}, len(shards))
	for i, s := range shards {
		if _, seen := seenBlocks[s.name]; !seen {
			seenBlocks[s.name] = struct{}{}
			resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
				Id: s.name,
			})
		}
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
	spanLog, ctx := spanlogger.New(ctx, "ParquetBucketStore.LabelValues")
	defer spanLog.Finish()

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

	seenBlocks := make(map[string]struct{}, len(shards))
	for i, s := range shards {
		if _, seen := seenBlocks[s.name]; !seen {
			seenBlocks[s.name] = struct{}{}
			resHints.QueriedBlocks = append(resHints.QueriedBlocks, hintspb.Block{
				Id: s.name,
			})
		}
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
