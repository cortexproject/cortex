package storegateway

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

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
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
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
	"github.com/cortexproject/cortex/pkg/storage/tsdb/users"
	cortex_util "github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/backoff"
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

	parquetBucketStoreMetrics *ParquetBucketStoreMetrics
	cortexBucketStoreMetrics  *CortexBucketStoreMetrics
	userScanner               users.Scanner
	shardingStrategy          ShardingStrategy

	userTokenBucketsMu sync.RWMutex
	userTokenBuckets   map[string]*cortex_util.TokenBucket
}

// newParquetBucketStores creates a new multi-tenant parquet bucket stores
func newParquetBucketStores(cfg tsdb.BlocksStorageConfig, shardingStrategy ShardingStrategy, bucketClient objstore.InstrumentedBucket, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer) (*ParquetBucketStores, error) {
	// Create caching bucket client for parquet bucket stores
	cachingBucket, err := createCachingBucketClientForParquet(cfg, bucketClient, "parquet-storegateway", logger, reg)
	if err != nil {
		return nil, err
	}

	u := &ParquetBucketStores{
		logger:                    logger,
		cfg:                       cfg,
		limits:                    limits,
		bucket:                    cachingBucket,
		stores:                    map[string]*parquetBucketStore{},
		storesErrors:              map[string]error{},
		chunksDecoder:             schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool()),
		inflightRequests:          cortex_util.NewInflightRequestTracker(),
		cortexBucketStoreMetrics:  NewCortexBucketStoreMetrics(reg),
		shardingStrategy:          shardingStrategy,
		userTokenBuckets:          make(map[string]*cortex_util.TokenBucket),
		parquetBucketStoreMetrics: NewParquetBucketStoreMetrics(),
	}
	u.userScanner, err = users.NewScanner(cfg.UsersScanner, bucketClient, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create users scanner")
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
	return u.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, p *parquetBucketStore) error {
		return p.SyncBlocks(ctx)
	})
}

// InitialSync implements BucketStores
func (u *ParquetBucketStores) InitialSync(ctx context.Context) error {
	level.Info(u.logger).Log("msg", "synchronizing Parquet blocks for all users")

	if err := u.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, p *parquetBucketStore) error {
		return p.InitialSync(ctx)
	}); err != nil {
		level.Warn(u.logger).Log("msg", "failed to synchronize Parquet blocks", "err", err)
		return err
	}

	level.Info(u.logger).Log("msg", "successfully synchronized Parquet blocks for all users")
	return nil
}

func (u *ParquetBucketStores) syncUsersBlocksWithRetries(ctx context.Context, f func(context.Context, *parquetBucketStore) error) error {
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

func (u *ParquetBucketStores) syncUsersBlocks(ctx context.Context, f func(context.Context, *parquetBucketStore) error) (returnErr error) {
	defer func(start time.Time) {
		u.cortexBucketStoreMetrics.syncTimes.Observe(time.Since(start).Seconds())
		if returnErr == nil {
			u.cortexBucketStoreMetrics.syncLastSuccess.SetToCurrentTime()
		}
	}(time.Now())

	type job struct {
		userID string
		store  *parquetBucketStore
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

	u.cortexBucketStoreMetrics.tenantsDiscovered.Set(float64(len(userIDs)))
	u.cortexBucketStoreMetrics.tenantsSynced.Set(float64(len(includeUserIDs)))

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
						errs.Add(errors.Wrapf(err, "failed to synchronize Parquet blocks for user %s", job.userID))
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

// deleteLocalFilesForExcludedTenants removes local "sync" directories for tenants that are not included in the current
// shard.
func (u *ParquetBucketStores) deleteLocalFilesForExcludedTenants(includeUserIDs map[string]struct{}) {
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

// closeEmptyBucketStore closes bucket store for given user, if it is empty,
// and removes it from bucket stores map and metrics.
// If bucket store doesn't exist, returns errBucketStoreNotFound.
// Otherwise returns error from closing the bucket store.
func (u *ParquetBucketStores) closeEmptyBucketStore(userID string) error {
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

	delete(u.stores, userID)
	unlockInDefer = false
	u.storesMu.Unlock()

	if u.cfg.BucketStore.TokenBucketBytesLimiter.Mode != string(tsdb.TokenBucketBytesLimiterDisabled) {
		u.userTokenBucketsMu.Lock()
		delete(u.userTokenBuckets, userID)
		u.userTokenBucketsMu.Unlock()
	}

	u.parquetBucketStoreMetrics.RemoveUserRegistry(userID)
	return bs.Close()
}

func (u *ParquetBucketStores) syncDirForUser(userID string) string {
	return filepath.Join(u.cfg.BucketStore.SyncDir, userID)
}

// scanUsers in the bucket and return the list of found users. It includes active and deleting users
// but not deleted users.
func (u *ParquetBucketStores) scanUsers(ctx context.Context) ([]string, error) {
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

func (u *ParquetBucketStores) getStore(userID string) *parquetBucketStore {
	u.storesMu.RLock()
	defer u.storesMu.RUnlock()
	return u.stores[userID]
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
	reg := prometheus.NewRegistry()
	u.parquetBucketStoreMetrics.AddUserRegistry(userID, reg)
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
