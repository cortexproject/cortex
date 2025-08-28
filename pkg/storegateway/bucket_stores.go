package storegateway

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	thanos_metadata "github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	thanos_model "github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/users"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	cortex_errors "github.com/cortexproject/cortex/pkg/util/errors"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// BucketStores defines the methods that any bucket stores implementation must provide
type BucketStores interface {
	storepb.StoreServer
	SyncBlocks(ctx context.Context) error
	InitialSync(ctx context.Context) error
}

// ThanosBucketStores is a multi-tenant wrapper of Thanos BucketStore.
type ThanosBucketStores struct {
	logger             log.Logger
	cfg                tsdb.BlocksStorageConfig
	limits             *validation.Overrides
	bucket             objstore.Bucket
	logLevel           logging.Level
	bucketStoreMetrics *BucketStoreMetrics
	metaFetcherMetrics *MetadataFetcherMetrics
	shardingStrategy   ShardingStrategy

	// Index cache shared across all tenants.
	indexCache storecache.IndexCache

	// Matchers cache shared across all tenants
	matcherCache storecache.MatchersCache

	// Chunks bytes pool shared across all tenants.
	chunksPool pool.Pool[byte]

	// Partitioner shared across all tenants.
	partitioner store.Partitioner

	// Gate used to limit query concurrency across all tenants.
	queryGate gate.Gate

	// Keeps a bucket store for each tenant.
	storesMu sync.RWMutex
	stores   map[string]*store.BucketStore

	// Keeps the last sync error for the bucket store for each tenant.
	storesErrorsMu sync.RWMutex
	storesErrors   map[string]error

	instanceTokenBucket *util.TokenBucket

	userTokenBucketsMu sync.RWMutex
	userTokenBuckets   map[string]*util.TokenBucket

	userScanner users.Scanner

	// Keeps number of inflight requests
	inflightRequests *util.InflightRequestTracker

	// Metrics.
	syncTimes         prometheus.Histogram
	syncLastSuccess   prometheus.Gauge
	tenantsDiscovered prometheus.Gauge
	tenantsSynced     prometheus.Gauge
}

var ErrTooManyInflightRequests = status.Error(codes.ResourceExhausted, "too many inflight requests in store gateway")

// NewBucketStores makes a new BucketStores.
func NewBucketStores(cfg tsdb.BlocksStorageConfig, shardingStrategy ShardingStrategy, bucketClient objstore.InstrumentedBucket, limits *validation.Overrides, logLevel logging.Level, logger log.Logger, reg prometheus.Registerer) (BucketStores, error) {
	switch cfg.BucketStore.BucketStoreType {
	case string(tsdb.ParquetBucketStore):
		return newParquetBucketStores(cfg, bucketClient, limits, logger, reg)
	case string(tsdb.TSDBBucketStore):
		return newThanosBucketStores(cfg, shardingStrategy, bucketClient, limits, logLevel, logger, reg)
	default:
		return nil, fmt.Errorf("unsupported bucket store type: %s", cfg.BucketStore.BucketStoreType)
	}
}

// newThanosBucketStores creates a new TSDB-based bucket stores
func newThanosBucketStores(cfg tsdb.BlocksStorageConfig, shardingStrategy ShardingStrategy, bucketClient objstore.InstrumentedBucket, limits *validation.Overrides, logLevel logging.Level, logger log.Logger, reg prometheus.Registerer) (*ThanosBucketStores, error) {
	matchers := tsdb.NewMatchers()
	cachingBucket, err := tsdb.CreateCachingBucket(cfg.BucketStore.ChunksCache, cfg.BucketStore.MetadataCache, tsdb.ParquetLabelsCacheConfig{}, matchers, bucketClient, logger, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "create caching bucket")
	}

	// The number of concurrent queries against the tenants BucketStores are limited.
	queryGateReg := extprom.WrapRegistererWithPrefix("cortex_bucket_stores_", reg)
	queryGate := gate.New(queryGateReg, cfg.BucketStore.MaxConcurrent, gate.Queries)
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_bucket_stores_gate_queries_concurrent_max",
		Help: "Number of maximum concurrent queries allowed.",
	}).Set(float64(cfg.BucketStore.MaxConcurrent))

	u := &ThanosBucketStores{
		logger:             logger,
		cfg:                cfg,
		limits:             limits,
		bucket:             cachingBucket,
		shardingStrategy:   shardingStrategy,
		stores:             map[string]*store.BucketStore{},
		storesErrors:       map[string]error{},
		logLevel:           logLevel,
		bucketStoreMetrics: NewBucketStoreMetrics(),
		metaFetcherMetrics: NewMetadataFetcherMetrics(),
		queryGate:          queryGate,
		partitioner:        newGapBasedPartitioner(cfg.BucketStore.PartitionerMaxGapBytes, reg),
		userTokenBuckets:   make(map[string]*util.TokenBucket),
		inflightRequests:   util.NewInflightRequestTracker(),
		syncTimes: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_bucket_stores_blocks_sync_seconds",
			Help:    "The total time it takes to perform a sync stores",
			Buckets: []float64{0.1, 1, 10, 30, 60, 120, 300, 600, 900},
		}),
		syncLastSuccess: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_bucket_stores_blocks_last_successful_sync_timestamp_seconds",
			Help: "Unix timestamp of the last successful blocks sync.",
		}),
		tenantsDiscovered: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_bucket_stores_tenants_discovered",
			Help: "Number of tenants discovered in the bucket.",
		}),
		tenantsSynced: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_bucket_stores_tenants_synced",
			Help: "Number of tenants synced.",
		}),
	}
	u.userScanner, err = users.NewScanner(cfg.UsersScanner, bucketClient, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create users scanner")
	}

	u.matcherCache = storecache.NoopMatchersCache

	if cfg.BucketStore.MatchersCacheMaxItems > 0 {
		r := prometheus.NewRegistry()
		reg.MustRegister(tsdb.NewMatchCacheMetrics("cortex_storegateway", r, logger))
		u.matcherCache, err = storecache.NewMatchersCache(storecache.WithSize(cfg.BucketStore.MatchersCacheMaxItems), storecache.WithPromRegistry(r))
		if err != nil {
			return nil, err
		}
	}

	// Init the index cache.
	if u.indexCache, err = tsdb.NewIndexCache(cfg.BucketStore.IndexCache, logger, reg); err != nil {
		return nil, errors.Wrap(err, "create index cache")
	}

	// Init the chunks bytes pool.
	if u.chunksPool, err = newChunkBytesPool(cfg.BucketStore.ChunkPoolMinBucketSizeBytes, cfg.BucketStore.ChunkPoolMaxBucketSizeBytes, cfg.BucketStore.MaxChunkPoolBytes, reg); err != nil {
		return nil, errors.Wrap(err, "create chunks bytes pool")
	}

	if u.cfg.BucketStore.TokenBucketBytesLimiter.Mode != string(tsdb.TokenBucketBytesLimiterDisabled) {
		u.instanceTokenBucket = util.NewTokenBucket(cfg.BucketStore.TokenBucketBytesLimiter.InstanceTokenBucketSize, promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_bucket_stores_instance_token_bucket_remaining",
			Help: "Number of tokens left in instance token bucket.",
		}))
	}

	if reg != nil {
		reg.MustRegister(u.bucketStoreMetrics, u.metaFetcherMetrics)
	}

	return u, nil
}

// InitialSync does an initial synchronization of blocks for all users.
func (u *ThanosBucketStores) InitialSync(ctx context.Context) error {
	level.Info(u.logger).Log("msg", "synchronizing TSDB blocks for all users")

	if err := u.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, s *store.BucketStore) error {
		return s.InitialSync(ctx)
	}); err != nil {
		level.Warn(u.logger).Log("msg", "failed to synchronize TSDB blocks", "err", err)
		return err
	}

	level.Info(u.logger).Log("msg", "successfully synchronized TSDB blocks for all users")
	return nil
}

// SyncBlocks synchronizes the stores state with the Bucket store for every user.
func (u *ThanosBucketStores) SyncBlocks(ctx context.Context) error {
	return u.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, s *store.BucketStore) error {
		return s.SyncBlocks(ctx)
	})
}

func (u *ThanosBucketStores) syncUsersBlocksWithRetries(ctx context.Context, f func(context.Context, *store.BucketStore) error) error {
	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 3,
	})

	var lastErr error
	for retries.Ongoing() {
		lastErr = u.syncUsersBlocks(ctx, f)
		if lastErr == nil {
			return nil
		}

		retries.Wait()
	}

	if lastErr == nil {
		return retries.Err()
	}

	return lastErr
}

func (u *ThanosBucketStores) syncUsersBlocks(ctx context.Context, f func(context.Context, *store.BucketStore) error) (returnErr error) {
	defer func(start time.Time) {
		u.syncTimes.Observe(time.Since(start).Seconds())
		if returnErr == nil {
			u.syncLastSuccess.SetToCurrentTime()
		}
	}(time.Now())

	type job struct {
		userID string
		store  *store.BucketStore
	}

	wg := &sync.WaitGroup{}
	jobs := make(chan job)
	errs := tsdb_errors.NewMulti()
	errsMx := sync.Mutex{}

	// Scan users in the bucket.
	userIDs, err := u.scanUsers(ctx)
	if err != nil {
		return err
	}

	includeUserIDs := make(map[string]struct{})
	for _, userID := range u.shardingStrategy.FilterUsers(ctx, userIDs) {
		includeUserIDs[userID] = struct{}{}
	}

	u.tenantsDiscovered.Set(float64(len(userIDs)))
	u.tenantsSynced.Set(float64(len(includeUserIDs)))

	// Create a pool of workers which will synchronize blocks. The pool size
	// is limited in order to avoid to concurrently sync a lot of tenants in
	// a large cluster.
	for i := 0; i < u.cfg.BucketStore.TenantSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				if err := f(ctx, job.store); err != nil {
					if errors.Is(err, bucket.ErrCustomerManagedKeyAccessDenied) {
						u.storesErrorsMu.Lock()
						u.storesErrors[job.userID] = httpgrpc.Errorf(int(codes.PermissionDenied), "store error: %s", err)
						u.storesErrorsMu.Unlock()
					} else {
						errsMx.Lock()
						errs.Add(errors.Wrapf(err, "failed to synchronize TSDB blocks for user %s", job.userID))
						errsMx.Unlock()
					}
				} else {
					u.storesErrorsMu.Lock()
					delete(u.storesErrors, job.userID)
					u.storesErrorsMu.Unlock()
				}
			}
		}()
	}

	// Lazily create a bucket store for each new user found
	// and submit a sync job for each user.
	for _, userID := range userIDs {
		// If we don't have a store for the tenant yet, then we should skip it if it's not
		// included in the store-gateway shard. If we already have it, we need to sync it
		// anyway to make sure all its blocks are unloaded and metrics updated correctly
		// (but bucket API calls are skipped thanks to the objstore client adapter).
		if _, included := includeUserIDs[userID]; !included && u.getStore(userID) == nil {
			continue
		}

		bs, err := u.getOrCreateStore(userID)
		if err != nil {
			errsMx.Lock()
			errs.Add(err)
			errsMx.Unlock()

			continue
		}

		select {
		case jobs <- job{userID: userID, store: bs}:
			// Nothing to do. Will loop to push more jobs.
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Wait until all workers completed.
	close(jobs)
	wg.Wait()

	u.deleteLocalFilesForExcludedTenants(includeUserIDs)

	return errs.Err()
}

// Series makes a series request to the underlying user bucket store.
func (u *ThanosBucketStores) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	spanLog, spanCtx := spanlogger.New(srv.Context(), "BucketStores.Series")
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

	store := u.getStore(userID)
	if store == nil {
		return nil
	}

	maxInflightRequests := u.cfg.BucketStore.MaxInflightRequests
	if maxInflightRequests > 0 {
		if u.inflightRequests.Count() >= maxInflightRequests {
			return ErrTooManyInflightRequests
		}

		u.inflightRequests.Inc()
		defer u.inflightRequests.Dec()
	}

	err = store.Series(req, spanSeriesServer{
		Store_SeriesServer: srv,
		ctx:                spanCtx,
	})

	return err
}

// LabelNames implements the Storegateway proto service.
func (u *ThanosBucketStores) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	spanLog, spanCtx := spanlogger.New(ctx, "BucketStores.LabelNames")
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

	store := u.getStore(userID)
	if store == nil {
		return &storepb.LabelNamesResponse{}, nil
	}

	resp, err := store.LabelNames(ctx, req)

	return resp, err
}

// LabelValues implements the Storegateway proto service.
func (u *ThanosBucketStores) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	spanLog, spanCtx := spanlogger.New(ctx, "BucketStores.LabelValues")
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

	store := u.getStore(userID)
	if store == nil {
		return &storepb.LabelValuesResponse{}, nil
	}

	return store.LabelValues(ctx, req)
}

// scanUsers in the bucket and return the list of found users. It includes active and deleting users
// but not deleted users.
func (u *ThanosBucketStores) scanUsers(ctx context.Context) ([]string, error) {
	activeUsers, deletingUsers, _, err := u.userScanner.ScanUsers(ctx)
	if err != nil {
		return nil, err
	}
	users := make([]string, 0, len(activeUsers)+len(deletingUsers))
	users = append(users, activeUsers...)
	users = append(users, deletingUsers...)
	users = deduplicateUsers(users)

	return users, err
}

func deduplicateUsers(users []string) []string {
	seen := make(map[string]struct{}, len(users))
	var uniqueUsers []string

	for _, user := range users {
		if _, ok := seen[user]; !ok {
			seen[user] = struct{}{}
			uniqueUsers = append(uniqueUsers, user)
		}
	}

	return uniqueUsers
}

func (u *ThanosBucketStores) getStore(userID string) *store.BucketStore {
	u.storesMu.RLock()
	defer u.storesMu.RUnlock()
	return u.stores[userID]
}

func (u *ThanosBucketStores) getStoreError(userID string) error {
	u.storesErrorsMu.RLock()
	defer u.storesErrorsMu.RUnlock()
	return u.storesErrors[userID]
}

var (
	errBucketStoreNotEmpty = errors.New("bucket store not empty")
	errBucketStoreNotFound = errors.New("bucket store not found")
)

// closeEmptyBucketStore closes bucket store for given user, if it is empty,
// and removes it from bucket stores map and metrics.
// If bucket store doesn't exist, returns errBucketStoreNotFound.
// If bucket store is not empty, returns errBucketStoreNotEmpty.
// Otherwise returns error from closing the bucket store.
func (u *ThanosBucketStores) closeEmptyBucketStore(userID string) error {
	u.storesMu.Lock()
	unlockInDefer := true
	defer func() {
		if unlockInDefer {
			u.storesMu.Unlock()
		}
	}()

	bs := u.stores[userID]
	if bs == nil {
		return errBucketStoreNotFound
	}

	if !isEmptyBucketStore(bs) {
		return errBucketStoreNotEmpty
	}

	delete(u.stores, userID)
	unlockInDefer = false
	u.storesMu.Unlock()

	if u.cfg.BucketStore.TokenBucketBytesLimiter.Mode != string(tsdb.TokenBucketBytesLimiterDisabled) {
		u.userTokenBucketsMu.Lock()
		delete(u.userTokenBuckets, userID)
		u.userTokenBucketsMu.Unlock()
	}

	u.metaFetcherMetrics.RemoveUserRegistry(userID)
	u.bucketStoreMetrics.RemoveUserRegistry(userID)
	return bs.Close()
}

func isEmptyBucketStore(bs *store.BucketStore) bool {
	min, max := bs.TimeRange()
	return min == math.MaxInt64 && max == math.MinInt64
}

func (u *ThanosBucketStores) syncDirForUser(userID string) string {
	return filepath.Join(u.cfg.BucketStore.SyncDir, userID)
}

func (u *ThanosBucketStores) getOrCreateStore(userID string) (*store.BucketStore, error) {
	// Check if the store already exists.
	bs := u.getStore(userID)
	if bs != nil {
		return bs, nil
	}

	u.storesMu.Lock()
	defer u.storesMu.Unlock()

	// Check again for the store in the event it was created in-between locks.
	bs = u.stores[userID]
	if bs != nil {
		return bs, nil
	}

	userLogger := util_log.WithUserID(userID, u.logger)

	level.Info(userLogger).Log("msg", "creating user bucket store")

	userBkt := bucket.NewUserBucketClient(userID, u.bucket, u.limits)
	fetcherReg := prometheus.NewRegistry()

	// The sharding strategy filter MUST be before the ones we create here (order matters).
	filters := []block.MetadataFilter{NewShardingMetadataFilterAdapter(userID, u.shardingStrategy)}

	if u.cfg.BucketStore.IgnoreBlocksBefore > 0 {
		// We don't want to filter out any blocks for max time.
		// Set a positive duration so we can always load blocks till now.
		// IgnoreBlocksWithin
		filterMaxTimeDuration := model.Duration(time.Second)
		filterMinTime := thanos_model.TimeOrDurationValue{}
		ignoreBlocksBefore := -model.Duration(u.cfg.BucketStore.IgnoreBlocksBefore)
		filterMinTime.Dur = &ignoreBlocksBefore
		filters = append(filters, block.NewTimePartitionMetaFilter(filterMinTime, thanos_model.TimeOrDurationValue{Dur: &filterMaxTimeDuration}))
	}

	filters = append(filters, []block.MetadataFilter{
		block.NewConsistencyDelayMetaFilter(userLogger, u.cfg.BucketStore.ConsistencyDelay, fetcherReg),
		// Use our own custom implementation.
		NewIgnoreDeletionMarkFilter(userLogger, userBkt, u.cfg.BucketStore.IgnoreDeletionMarksDelay, u.cfg.BucketStore.MetaSyncConcurrency),
		// The duplicate filter has been intentionally omitted because it could cause troubles with
		// the consistency check done on the querier. The duplicate filter removes redundant blocks
		// but if the store-gateway removes redundant blocks before the querier discovers them, the
		// consistency check on the querier will fail.
		NewReplicaLabelRemover(userLogger, []string{
			tsdb.TenantIDExternalLabel,
			tsdb.IngesterIDExternalLabel,
		}),
		// Remove Cortex external labels so that they're not injected when querying blocks.
	}...)

	if u.cfg.BucketStore.IgnoreBlocksWithin > 0 {
		// Filter out blocks that are too new to be queried.
		filters = append(filters, NewIgnoreNonQueryableBlocksFilter(userLogger, u.cfg.BucketStore.IgnoreBlocksWithin))
	}

	// Instantiate a different blocks metadata fetcher based on whether bucket index is enabled or not.
	var fetcher block.MetadataFetcher
	if u.cfg.BucketStore.BucketIndex.Enabled {
		fetcher = NewBucketIndexMetadataFetcher(
			userID,
			u.bucket,
			u.shardingStrategy,
			u.limits,
			u.logger,
			fetcherReg,
			filters)
	} else {
		// Wrap the bucket reader to skip iterating the bucket at all if the user doesn't
		// belong to the store-gateway shard. We need to run the BucketStore syncing anyway
		// in order to unload previous tenants in case of a resharding leading to tenants
		// moving out from the store-gateway shard and also make sure both MetaFetcher and
		// BucketStore metrics are correctly updated.
		fetcherBkt := NewShardingBucketReaderAdapter(userID, u.shardingStrategy, userBkt)

		var (
			err         error
			blockLister block.Lister
		)
		switch tsdb.BlockDiscoveryStrategy(u.cfg.BucketStore.BlockDiscoveryStrategy) {
		case tsdb.ConcurrentDiscovery:
			blockLister = block.NewConcurrentLister(userLogger, userBkt)
		case tsdb.RecursiveDiscovery:
			blockLister = block.NewRecursiveLister(userLogger, userBkt)
		case tsdb.BucketIndexDiscovery:
			return nil, tsdb.ErrInvalidBucketIndexBlockDiscoveryStrategy
		default:
			return nil, tsdb.ErrBlockDiscoveryStrategy
		}
		fetcher, err = block.NewMetaFetcher(
			userLogger,
			u.cfg.BucketStore.MetaSyncConcurrency,
			fetcherBkt,
			blockLister,
			u.syncDirForUser(userID), // The fetcher stores cached metas in the "meta-syncer/" sub directory
			fetcherReg,
			filters,
		)
		if err != nil {
			return nil, err
		}
	}

	bucketStoreReg := prometheus.NewRegistry()

	bucketStoreOpts := []store.BucketStoreOption{
		store.WithLogger(userLogger),
		store.WithMatchersCache(u.matcherCache),
		store.WithRequestLoggerFunc(func(ctx context.Context, logger log.Logger) log.Logger {
			return util_log.HeadersFromContext(ctx, logger)
		}),
		store.WithRegistry(bucketStoreReg),
		store.WithIndexCache(u.indexCache),
		store.WithQueryGate(u.queryGate),
		store.WithChunkPool(u.chunksPool),
		store.WithSeriesBatchSize(u.cfg.BucketStore.SeriesBatchSize),
		store.WithBlockEstimatedMaxChunkFunc(func(m thanos_metadata.Meta) uint64 {
			if m.Thanos.IndexStats.ChunkMaxSize > 0 &&
				uint64(m.Thanos.IndexStats.ChunkMaxSize) < u.cfg.BucketStore.EstimatedMaxChunkSizeBytes {
				return uint64(m.Thanos.IndexStats.ChunkMaxSize)
			}
			return u.cfg.BucketStore.EstimatedMaxChunkSizeBytes
		}),
		store.WithBlockEstimatedMaxSeriesFunc(func(m thanos_metadata.Meta) uint64 {
			if m.Thanos.IndexStats.SeriesMaxSize > 0 &&
				uint64(m.Thanos.IndexStats.SeriesMaxSize) < u.cfg.BucketStore.EstimatedMaxSeriesSizeBytes {
				return uint64(m.Thanos.IndexStats.SeriesMaxSize)
			}
			return u.cfg.BucketStore.EstimatedMaxSeriesSizeBytes
		}),
		store.WithLazyExpandedPostings(u.cfg.BucketStore.LazyExpandedPostingsEnabled),
		store.WithPostingGroupMaxKeySeriesRatio(u.cfg.BucketStore.LazyExpandedPostingGroupMaxKeySeriesRatio),
		store.WithSeriesMatchRatio(0.5), // TODO: expose this as a config.
		store.WithDontResort(true),      // Cortex doesn't need to resort series in store gateway.
		store.WithBlockLifecycleCallback(&shardingBlockLifecycleCallbackAdapter{
			userID:   userID,
			strategy: u.shardingStrategy,
			logger:   userLogger,
		}),
	}
	if u.logLevel.String() == "debug" {
		bucketStoreOpts = append(bucketStoreOpts, store.WithDebugLogging())
	}

	if u.cfg.BucketStore.TokenBucketBytesLimiter.Mode != string(tsdb.TokenBucketBytesLimiterDisabled) {
		u.userTokenBucketsMu.Lock()
		u.userTokenBuckets[userID] = util.NewTokenBucket(u.cfg.BucketStore.TokenBucketBytesLimiter.UserTokenBucketSize, nil)
		u.userTokenBucketsMu.Unlock()
	}

	bs, err := store.NewBucketStore(
		userBkt,
		fetcher,
		u.syncDirForUser(userID),
		newChunksLimiterFactory(u.limits, userID),
		newSeriesLimiterFactory(u.limits, userID),
		newBytesLimiterFactory(u.limits, userID, u.getUserTokenBucket(userID), u.instanceTokenBucket, u.cfg.BucketStore.TokenBucketBytesLimiter, u.getTokensToRetrieve),
		u.partitioner,
		u.cfg.BucketStore.BlockSyncConcurrency,
		false, // No need to enable backward compatibility with Thanos pre 0.8.0 queriers
		u.cfg.BucketStore.PostingOffsetsInMemSampling,
		true, // Enable series hints.
		u.cfg.BucketStore.IndexHeaderLazyLoadingEnabled,
		u.cfg.BucketStore.IndexHeaderLazyLoadingIdleTimeout,
		bucketStoreOpts...,
	)
	if err != nil {
		return nil, err
	}

	u.stores[userID] = bs
	u.metaFetcherMetrics.AddUserRegistry(userID, fetcherReg)
	u.bucketStoreMetrics.AddUserRegistry(userID, bucketStoreReg)

	return bs, nil
}

// deleteLocalFilesForExcludedTenants removes local "sync" directories for tenants that are not included in the current
// shard.
func (u *ThanosBucketStores) deleteLocalFilesForExcludedTenants(includeUserIDs map[string]struct{}) {
	files, err := os.ReadDir(u.cfg.BucketStore.SyncDir)
	if err != nil {
		return
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		userID := f.Name()
		if _, included := includeUserIDs[userID]; included {
			// Preserve directory for users owned by this shard.
			continue
		}

		err := u.closeEmptyBucketStore(userID)
		switch {
		case errors.Is(err, errBucketStoreNotEmpty):
			continue
		case errors.Is(err, errBucketStoreNotFound):
			// This is OK, nothing was closed.
		case err == nil:
			level.Info(u.logger).Log("msg", "closed bucket store for user", "user", userID)
		default:
			level.Warn(u.logger).Log("msg", "failed to close bucket store for user", "user", userID, "err", err)
		}

		userSyncDir := u.syncDirForUser(userID)
		err = os.RemoveAll(userSyncDir)
		if err == nil {
			level.Info(u.logger).Log("msg", "deleted user sync directory", "dir", userSyncDir)
		} else {
			level.Warn(u.logger).Log("msg", "failed to delete user sync directory", "dir", userSyncDir, "err", err)
		}
	}
}

func (u *ThanosBucketStores) getUserTokenBucket(userID string) *util.TokenBucket {
	u.userTokenBucketsMu.RLock()
	defer u.userTokenBucketsMu.RUnlock()
	return u.userTokenBuckets[userID]
}

func (u *ThanosBucketStores) getTokensToRetrieve(tokens uint64, dataType store.StoreDataType) int64 {
	tokensToRetrieve := float64(tokens)
	switch dataType {
	case store.PostingsFetched:
		tokensToRetrieve *= u.cfg.BucketStore.TokenBucketBytesLimiter.FetchedPostingsTokenFactor
	case store.PostingsTouched:
		tokensToRetrieve *= u.cfg.BucketStore.TokenBucketBytesLimiter.TouchedPostingsTokenFactor
	case store.SeriesFetched:
		tokensToRetrieve *= u.cfg.BucketStore.TokenBucketBytesLimiter.FetchedSeriesTokenFactor
	case store.SeriesTouched:
		tokensToRetrieve *= u.cfg.BucketStore.TokenBucketBytesLimiter.TouchedSeriesTokenFactor
	case store.ChunksFetched:
		tokensToRetrieve *= u.cfg.BucketStore.TokenBucketBytesLimiter.FetchedChunksTokenFactor
	case store.ChunksTouched:
		tokensToRetrieve *= u.cfg.BucketStore.TokenBucketBytesLimiter.TouchedChunksTokenFactor
	}
	return int64(tokensToRetrieve)
}

func getUserIDFromGRPCContext(ctx context.Context) string {
	values := metadata.ValueFromIncomingContext(ctx, tsdb.TenantIDExternalLabel)
	if values == nil || len(values) != 1 {
		return ""
	}
	return values[0]
}

// ReplicaLabelRemover is a BaseFetcher modifier modifies external labels of existing blocks, it removes given replica labels from the metadata of blocks that have it.
type ReplicaLabelRemover struct {
	logger log.Logger

	replicaLabels []string
}

// NewReplicaLabelRemover creates a ReplicaLabelRemover.
func NewReplicaLabelRemover(logger log.Logger, replicaLabels []string) *ReplicaLabelRemover {
	return &ReplicaLabelRemover{logger: logger, replicaLabels: replicaLabels}
}

// Filter implements block.MetadataFilter.
func (r *ReplicaLabelRemover) Filter(_ context.Context, metas map[ulid.ULID]*thanos_metadata.Meta, _ block.GaugeVec, _ block.GaugeVec) error {
	for u, meta := range metas {
		l := meta.Thanos.Labels
		for _, replicaLabel := range r.replicaLabels {
			if _, exists := l[replicaLabel]; exists {
				level.Debug(r.logger).Log("msg", "replica label removed", "label", replicaLabel)
				delete(l, replicaLabel)
			}
		}
		metas[u].Thanos.Labels = l
	}
	return nil
}

type spanSeriesServer struct {
	storepb.Store_SeriesServer

	ctx context.Context
}

func (s spanSeriesServer) Context() context.Context {
	return s.ctx
}
