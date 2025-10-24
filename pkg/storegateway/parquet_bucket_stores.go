package storegateway

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/httpgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/querysharding"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	cortex_util "github.com/cortexproject/cortex/pkg/util"
	cortex_errors "github.com/cortexproject/cortex/pkg/util/errors"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// ParquetBucketStores is a multi-tenant wrapper for parquet bucket stores
type ParquetBucketStores struct {
	logger log.Logger
	cfg    tsdb.BlocksStorageConfig
	limits *validation.Overrides
	bucket objstore.Bucket

	storesMu sync.RWMutex
	stores   map[string]*parquetBucketStore

	// Keeps the last sync error for the bucket store for each tenant.
	storesErrorsMu sync.RWMutex
	storesErrors   map[string]error

	chunksDecoder *schema.PrometheusParquetChunksDecoder

	matcherCache storecache.MatchersCache

	inflightRequests *cortex_util.InflightRequestTracker
}

// parquetBucketStore represents a single tenant's parquet store
type parquetBucketStore struct {
	logger      log.Logger
	bucket      objstore.Bucket
	limits      *validation.Overrides
	concurrency int

	chunksDecoder *schema.PrometheusParquetChunksDecoder

	matcherCache storecache.MatchersCache
}

// newParquetBucketStores creates a new multi-tenant parquet bucket stores
func newParquetBucketStores(cfg tsdb.BlocksStorageConfig, bucketClient objstore.InstrumentedBucket, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer) (*ParquetBucketStores, error) {
	// Create caching bucket client for parquet bucket stores
	cachingBucket, err := createCachingBucketClientForParquet(cfg, bucketClient, "parquet-storegateway", logger, reg)
	if err != nil {
		return nil, err
	}

	u := &ParquetBucketStores{
		logger:           logger,
		cfg:              cfg,
		limits:           limits,
		bucket:           cachingBucket,
		stores:           map[string]*parquetBucketStore{},
		storesErrors:     map[string]error{},
		chunksDecoder:    schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool()),
		inflightRequests: cortex_util.NewInflightRequestTracker(),
	}

	if cfg.BucketStore.MatchersCacheMaxItems > 0 {
		r := prometheus.NewRegistry()
		reg.MustRegister(tsdb.NewMatchCacheMetrics("cortex_storegateway", r, logger))
		u.matcherCache, err = storecache.NewMatchersCache(storecache.WithSize(cfg.BucketStore.MatchersCacheMaxItems), storecache.WithPromRegistry(r))
		if err != nil {
			return nil, err
		}
	} else {
		u.matcherCache = storecache.NoopMatchersCache
	}

	return u, nil
}

// Series implements BucketStores
func (u *ParquetBucketStores) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	spanLog, spanCtx := spanlogger.New(srv.Context(), "ParquetBucketStores.Series")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return fmt.Errorf("no userID")
	}

	err := u.getStoreError(userID)
	userBkt := bucket.NewUserBucketClient(userID, u.bucket, u.limits)
	if err != nil {
		if cortex_errors.ErrorIs(err, userBkt.IsAccessDeniedErr) {
			return httpgrpc.Errorf(int(codes.PermissionDenied), "store error: %s", err)
		}

		return err
	}

	store, err := u.getOrCreateStore(userID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	maxInflightRequests := u.cfg.BucketStore.MaxInflightRequests
	if maxInflightRequests > 0 {
		if u.inflightRequests.Count() >= maxInflightRequests {
			return ErrTooManyInflightRequests
		}

		u.inflightRequests.Inc()
		defer u.inflightRequests.Dec()
	}

	return store.Series(req, spanSeriesServer{
		Store_SeriesServer: srv,
		ctx:                spanCtx,
	})
}

// LabelNames implements BucketStores
func (u *ParquetBucketStores) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	userID := getUserIDFromGRPCContext(ctx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}

	err := u.getStoreError(userID)
	userBkt := bucket.NewUserBucketClient(userID, u.bucket, u.limits)
	if err != nil {
		if cortex_errors.ErrorIs(err, userBkt.IsAccessDeniedErr) {
			return nil, httpgrpc.Errorf(int(codes.PermissionDenied), "store error: %s", err)
		}

		return nil, err
	}

	store, err := u.getOrCreateStore(userID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return store.LabelNames(ctx, req)
}

// LabelValues implements BucketStores
func (u *ParquetBucketStores) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	userID := getUserIDFromGRPCContext(ctx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}

	err := u.getStoreError(userID)
	userBkt := bucket.NewUserBucketClient(userID, u.bucket, u.limits)
	if err != nil {
		if cortex_errors.ErrorIs(err, userBkt.IsAccessDeniedErr) {
			return nil, httpgrpc.Errorf(int(codes.PermissionDenied), "store error: %s", err)
		}

		return nil, err
	}

	store, err := u.getOrCreateStore(userID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return store.LabelValues(ctx, req)
}

// SyncBlocks implements BucketStores
func (u *ParquetBucketStores) SyncBlocks(ctx context.Context) error {
	return nil
}

// InitialSync implements BucketStores
func (u *ParquetBucketStores) InitialSync(ctx context.Context) error {
	return nil
}

func (u *ParquetBucketStores) getStoreError(userID string) error {
	u.storesErrorsMu.RLock()
	defer u.storesErrorsMu.RUnlock()
	return u.storesErrors[userID]
}

// getOrCreateStore gets or creates a parquet bucket store for the given user
func (u *ParquetBucketStores) getOrCreateStore(userID string) (*parquetBucketStore, error) {
	u.storesMu.RLock()
	store, exists := u.stores[userID]
	u.storesMu.RUnlock()

	if exists {
		return store, nil
	}

	u.storesMu.Lock()
	defer u.storesMu.Unlock()

	// Double-check after acquiring write lock
	if store, exists = u.stores[userID]; exists {
		return store, nil
	}

	// Check if there was an error creating this store
	if err, exists := u.storesErrors[userID]; exists {
		return nil, err
	}

	// Create new store
	userLogger := log.With(u.logger, "user", userID)
	store, err := u.createParquetBucketStore(userID, userLogger)
	if err != nil {
		u.storesErrors[userID] = err
		return nil, err
	}

	u.stores[userID] = store
	return store, nil
}

// createParquetBucketStore creates a new parquet bucket store for a user
func (u *ParquetBucketStores) createParquetBucketStore(userID string, userLogger log.Logger) (*parquetBucketStore, error) {
	level.Info(userLogger).Log("msg", "creating parquet bucket store")

	// Create user-specific bucket client
	userBucket := bucket.NewUserBucketClient(userID, u.bucket, u.limits)

	store := &parquetBucketStore{
		logger:        userLogger,
		bucket:        userBucket,
		limits:        u.limits,
		concurrency:   4, // TODO: make this configurable
		chunksDecoder: u.chunksDecoder,
		matcherCache:  u.matcherCache,
	}

	return store, nil
}

// findParquetBlocks finds parquet shards for the given user and time range
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

type parquetBlock struct {
	name        string
	shard       parquet_storage.ParquetShard
	m           *search.Materializer
	concurrency int
}

func (p *parquetBucketStore) newParquetBlock(ctx context.Context, name string, labelsFileOpener, chunksFileOpener parquet_storage.ParquetOpener, d *schema.PrometheusParquetChunksDecoder, rowCountQuota *search.Quota, chunkBytesQuota *search.Quota, dataBytesQuota *search.Quota) (*parquetBlock, error) {
	shard, err := parquet_storage.NewParquetShardOpener(
		context.WithoutCancel(ctx),
		name,
		labelsFileOpener,
		chunksFileOpener,
		0,
		parquet_storage.WithFileOptions(
			parquet.SkipMagicBytes(true),
			parquet.ReadBufferSize(100*1024),
			parquet.SkipBloomFilters(true),
			parquet.OptimisticRead(true),
		),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open parquet shard. block: %v", name)
	}

	s, err := shard.TSDBSchema()
	if err != nil {
		return nil, err
	}
	m, err := search.NewMaterializer(s, d, shard, p.concurrency, rowCountQuota, chunkBytesQuota, dataBytesQuota, search.NoopMaterializedSeriesFunc, materializedLabelsFilterCallback)
	if err != nil {
		return nil, err
	}

	return &parquetBlock{
		shard:       shard,
		m:           m,
		concurrency: p.concurrency,
		name:        name,
	}, nil
}

type contextKey int

var (
	shardInfoCtxKey contextKey = 1
)

func injectShardInfoIntoContext(ctx context.Context, si *storepb.ShardInfo) context.Context {
	return context.WithValue(ctx, shardInfoCtxKey, si)
}

func extractShardInfoFromContext(ctx context.Context) (*storepb.ShardInfo, bool) {
	if si := ctx.Value(shardInfoCtxKey); si != nil {
		return si.(*storepb.ShardInfo), true
	}

	return nil, false
}

func materializedLabelsFilterCallback(ctx context.Context, _ *prom_storage.SelectHints) (search.MaterializedLabelsFilter, bool) {
	shardInfo, exists := extractShardInfoFromContext(ctx)
	if !exists {
		return nil, false
	}
	sm := shardInfo.Matcher(&querysharding.Buffers)
	if !sm.IsSharded() {
		return nil, false
	}
	return &shardMatcherLabelsFilter{shardMatcher: sm}, true
}

type shardMatcherLabelsFilter struct {
	shardMatcher *storepb.ShardMatcher
}

func (f *shardMatcherLabelsFilter) Filter(lbls labels.Labels) bool {
	return f.shardMatcher.MatchesLabels(lbls)
}

func (f *shardMatcherLabelsFilter) Close() {
	f.shardMatcher.Close()
}

func (b *parquetBlock) Query(ctx context.Context, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (prom_storage.ChunkSeriesSet, error) {
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	rowGroupCount := len(b.shard.LabelsFile().RowGroups())
	results := make([][]prom_storage.ChunkSeries, rowGroupCount)
	for i := range results {
		results[i] = make([]prom_storage.ChunkSeries, 0, 1024/rowGroupCount)
	}

	for rgi := range rowGroupCount {
		errGroup.Go(func() error {
			cs, err := search.MatchersToConstraints(matchers...)
			if err != nil {
				return err
			}
			err = search.Initialize(b.shard.LabelsFile(), cs...)
			if err != nil {
				return err
			}
			rr, err := search.Filter(ctx, b.shard, rgi, cs...)
			if err != nil {
				return err
			}

			if len(rr) == 0 {
				return nil
			}

			seriesSetIter, err := b.m.Materialize(ctx, nil, rgi, mint, maxt, skipChunks, rr)
			if err != nil {
				return err
			}
			defer func() { _ = seriesSetIter.Close() }()
			for seriesSetIter.Next() {
				results[rgi] = append(results[rgi], seriesSetIter.At())
			}
			sort.Sort(byLabels(results[rgi]))
			return seriesSetIter.Err()
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	totalResults := 0
	for _, res := range results {
		totalResults += len(res)
	}

	resultsFlattened := make([]prom_storage.ChunkSeries, 0, totalResults)
	for _, res := range results {
		resultsFlattened = append(resultsFlattened, res...)
	}
	sort.Sort(byLabels(resultsFlattened))

	return convert.NewChunksSeriesSet(resultsFlattened), nil
}

func (b *parquetBlock) LabelNames(ctx context.Context, limit int64, matchers []*labels.Matcher) ([]string, error) {
	if len(matchers) == 0 {
		return b.m.MaterializeAllLabelNames(), nil
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))

	for rgi := range b.shard.LabelsFile().RowGroups() {
		errGroup.Go(func() error {
			cs, err := search.MatchersToConstraints(matchers...)
			if err != nil {
				return err
			}
			err = search.Initialize(b.shard.LabelsFile(), cs...)
			if err != nil {
				return err
			}
			rr, err := search.Filter(ctx, b.shard, rgi, cs...)
			if err != nil {
				return err
			}
			series, err := b.m.MaterializeLabelNames(ctx, rgi, rr)
			if err != nil {
				return err
			}
			results[rgi] = series
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return util.MergeUnsortedSlices(int(limit), results...), nil
}

func (b *parquetBlock) LabelValues(ctx context.Context, name string, limit int64, matchers []*labels.Matcher) ([]string, error) {
	if len(matchers) == 0 {
		return b.allLabelValues(ctx, name, limit)
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))

	for rgi := range b.shard.LabelsFile().RowGroups() {
		errGroup.Go(func() error {
			cs, err := search.MatchersToConstraints(matchers...)
			if err != nil {
				return err
			}
			err = search.Initialize(b.shard.LabelsFile(), cs...)
			if err != nil {
				return err
			}
			rr, err := search.Filter(ctx, b.shard, rgi, cs...)
			if err != nil {
				return err
			}
			series, err := b.m.MaterializeLabelValues(ctx, name, rgi, rr)
			if err != nil {
				return err
			}
			results[rgi] = series
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return util.MergeUnsortedSlices(int(limit), results...), nil
}

func (b *parquetBlock) allLabelValues(ctx context.Context, name string, limit int64) ([]string, error) {
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))

	for i := range b.shard.LabelsFile().RowGroups() {
		errGroup.Go(func() error {
			series, err := b.m.MaterializeAllLabelValues(ctx, name, i)
			if err != nil {
				return err
			}
			results[i] = series
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return util.MergeUnsortedSlices(int(limit), results...), nil
}

type byLabels []prom_storage.ChunkSeries

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }

func chunkToStoreEncoding(in chunkenc.Encoding) storepb.Chunk_Encoding {
	switch in {
	case chunkenc.EncXOR:
		return storepb.Chunk_XOR
	case chunkenc.EncHistogram:
		return storepb.Chunk_HISTOGRAM
	case chunkenc.EncFloatHistogram:
		return storepb.Chunk_FLOAT_HISTOGRAM
	default:
		panic("unknown chunk encoding")
	}
}

// createCachingBucketClientForParquet creates a caching bucket client for parquet bucket stores
func createCachingBucketClientForParquet(storageCfg tsdb.BlocksStorageConfig, bucketClient objstore.InstrumentedBucket, name string, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	// Create caching bucket using the existing infrastructure
	matchers := tsdb.NewMatchers()
	cachingBucket, err := tsdb.CreateCachingBucket(storageCfg.BucketStore.ChunksCache, storageCfg.BucketStore.MetadataCache, storageCfg.BucketStore.ParquetLabelsCache, matchers, bucketClient, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create caching bucket for parquet")
	}
	return cachingBucket, nil
}
