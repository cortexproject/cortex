package storegateway

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
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
	"github.com/cortexproject/cortex/pkg/util/parquetutil"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/users"
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

	matcherCache      storecache.MatchersCache
	parquetShardCache parquetutil.CacheInterface[parquet_storage.ParquetShard]

	inflightRequests *cortex_util.InflightRequestTracker
}

// newParquetBucketStores creates a new multi-tenant parquet bucket stores
func newParquetBucketStores(cfg tsdb.BlocksStorageConfig, bucketClient objstore.InstrumentedBucket, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer) (*ParquetBucketStores, error) {
	// Create caching bucket client for parquet bucket stores
	cachingBucket, err := createCachingBucketClientForParquet(cfg, bucketClient, "parquet-storegateway", logger, reg)
	if err != nil {
		return nil, err
	}

	parquetShardCache, err := parquetutil.NewParquetShardCache[parquet_storage.ParquetShard](&cfg.BucketStore.ParquetShardCache, "parquet-shards", reg)
	if err != nil {
		return nil, err
	}

	u := &ParquetBucketStores{
		logger:            logger,
		cfg:               cfg,
		limits:            limits,
		bucket:            cachingBucket,
		stores:            map[string]*parquetBucketStore{},
		storesErrors:      map[string]error{},
		chunksDecoder:     schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool()),
		inflightRequests:  cortex_util.NewInflightRequestTracker(),
		parquetShardCache: parquetShardCache,
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
	spanLog, spanCtx := spanlogger.New(ctx, "ParquetBucketStores.LabelNames")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
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
	spanLog, spanCtx := spanlogger.New(ctx, "ParquetBucketStores.LabelValues")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
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
		logger:            userLogger,
		bucket:            userBucket,
		limits:            u.limits,
		concurrency:       4, // TODO: make this configurable
		chunksDecoder:     u.chunksDecoder,
		matcherCache:      u.matcherCache,
		parquetShardCache: u.parquetShardCache,
	}

	return store, nil
}

type parquetBlock struct {
	name        string
	shard       parquet_storage.ParquetShard
	m           *search.Materializer
	concurrency int
}

func (p *parquetBucketStore) newParquetBlock(ctx context.Context, name string, shardID int, labelsFileOpener, chunksFileOpener parquet_storage.ParquetOpener, d *schema.PrometheusParquetChunksDecoder, rowCountQuota *search.Quota, chunkBytesQuota *search.Quota, dataBytesQuota *search.Quota) (*parquetBlock, error) {
	userID, err := users.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	cacheKey := fmt.Sprintf("%v-%v-%v", userID, name, shardID)
	shard := p.parquetShardCache.Get(cacheKey)

	if shard == nil {
		// cache miss, open parquet files
		shard, err = parquet_storage.NewParquetShardOpener(
			context.WithoutCancel(ctx),
			name,
			labelsFileOpener,
			chunksFileOpener,
			0, // we always only have 1 shard - shard 0
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

		// set shard to cache
		p.parquetShardCache.Set(cacheKey, shard)
	}

	s, err := shard.TSDBSchema()
	if err != nil {
		return nil, err
	}
	m, err := search.NewMaterializer(s, d, shard, p.concurrency, rowCountQuota, chunkBytesQuota, dataBytesQuota, search.NoopMaterializedSeriesFunc, materializedLabelsFilterCallback, false)
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
			// TODO: Add cache.
			rr, err := search.Filter(ctx, b.shard, rgi, nil, cs...)
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
			// TODO: Add cache.
			rr, err := search.Filter(ctx, b.shard, rgi, nil, cs...)
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
			// TODO: Add cache.
			rr, err := search.Filter(ctx, b.shard, rgi, nil, cs...)
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
