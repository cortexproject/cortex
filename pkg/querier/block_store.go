package querier

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// UserStore is a multi-tenant version of Thanos BucketStore
type UserStore struct {
	services.Service

	logger             log.Logger
	cfg                tsdb.Config
	bucket             objstore.Bucket
	logLevel           logging.Level
	bucketStoreMetrics *tsdbBucketStoreMetrics
	indexCacheMetrics  prometheus.Collector

	// Index cache shared across all tenants.
	indexCache storecache.IndexCache

	// Keeps a bucket store for each tenant.
	storesMu sync.RWMutex
	stores   map[string]*store.BucketStore

	// Metrics.
	syncTimes prometheus.Histogram
}

// NewUserStore returns a new UserStore
func NewUserStore(cfg tsdb.Config, bucketClient objstore.Bucket, logLevel logging.Level, logger log.Logger, registerer prometheus.Registerer) (*UserStore, error) {
	indexCacheRegistry := prometheus.NewRegistry()

	u := &UserStore{
		logger:             logger,
		cfg:                cfg,
		bucket:             bucketClient,
		stores:             map[string]*store.BucketStore{},
		logLevel:           logLevel,
		bucketStoreMetrics: newTSDBBucketStoreMetrics(),
		indexCacheMetrics:  tsdb.MustNewIndexCacheMetrics(cfg.BucketStore.IndexCache.Backend, indexCacheRegistry),
		syncTimes: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_querier_blocks_sync_seconds",
			Help:    "The total time it takes to perform a sync stores",
			Buckets: []float64{0.1, 1, 10, 30, 60, 120, 300, 600, 900},
		}),
	}

	// Init the index cache.
	var err error
	if u.indexCache, err = tsdb.NewIndexCache(cfg.BucketStore.IndexCache, logger, indexCacheRegistry); err != nil {
		return nil, errors.Wrap(err, "create index cache")
	}

	if registerer != nil {
		registerer.MustRegister(u.bucketStoreMetrics, u.indexCacheMetrics)
	}

	u.Service = services.NewBasicService(u.starting, u.syncStoresLoop, nil)
	return u, nil
}

func (u *UserStore) starting(ctx context.Context) error {
	if u.cfg.BucketStore.SyncInterval > 0 {
		// Run an initial blocks sync, required in order to be able to serve queries.
		if err := u.initialSync(ctx); err != nil {
			return err
		}
	}

	return nil
}

// initialSync iterates over the storage bucket creating user bucket stores, and calling initialSync on each of them
func (u *UserStore) initialSync(ctx context.Context) error {
	level.Info(u.logger).Log("msg", "synchronizing TSDB blocks for all users")

	if err := u.syncUserStores(ctx, func(ctx context.Context, s *store.BucketStore) error {
		return s.InitialSync(ctx)
	}); err != nil {
		level.Warn(u.logger).Log("msg", "failed to synchronize TSDB blocks", "err", err)
		return err
	}

	level.Info(u.logger).Log("msg", "successfully synchronized TSDB blocks for all users")
	return nil
}

// syncStoresLoop periodically calls syncStores() to synchronize the blocks for all tenants.
func (u *UserStore) syncStoresLoop(ctx context.Context) error {
	// If the sync is disabled we never sync blocks, which means the bucket store
	// will be empty and no series will be returned once queried.
	if u.cfg.BucketStore.SyncInterval <= 0 {
		<-ctx.Done()
		return nil
	}

	syncInterval := u.cfg.BucketStore.SyncInterval

	// Since we've just run the initial sync, we should wait the next
	// sync interval before resynching.
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(syncInterval):
	}

	err := runutil.Repeat(syncInterval, ctx.Done(), func() error {
		level.Info(u.logger).Log("msg", "synchronizing TSDB blocks for all users")
		if err := u.syncStores(ctx); err != nil && err != io.EOF {
			level.Warn(u.logger).Log("msg", "failed to synchronize TSDB blocks", "err", err)
		} else {
			level.Info(u.logger).Log("msg", "successfully synchronized TSDB blocks for all users")
		}

		return nil
	})

	// This should never occur because the rununtil.Repeat() returns error
	// only if the callback function returns error (which doesn't), but since
	// we have to handle the error because of the linter, it's better to log it.
	return errors.Wrap(err, "blocks synchronization has been halted due to an unexpected error")
}

// syncStores iterates over the storage bucket creating user bucket stores
func (u *UserStore) syncStores(ctx context.Context) error {
	if err := u.syncUserStores(ctx, func(ctx context.Context, s *store.BucketStore) error {
		return s.SyncBlocks(ctx)
	}); err != nil {
		return err
	}

	return nil
}

func (u *UserStore) syncUserStores(ctx context.Context, f func(context.Context, *store.BucketStore) error) error {
	defer func(start time.Time) {
		u.syncTimes.Observe(time.Since(start).Seconds())
	}(time.Now())

	type job struct {
		userID string
		store  *store.BucketStore
	}

	wg := &sync.WaitGroup{}
	jobs := make(chan job)

	// Create a pool of workers which will synchronize blocks. The pool size
	// is limited in order to avoid to concurrently sync a lot of tenants in
	// a large cluster.
	for i := 0; i < u.cfg.BucketStore.TenantSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				if err := f(ctx, job.store); err != nil {
					level.Warn(u.logger).Log("msg", "failed to synchronize TSDB blocks for user", "user", job.userID, "err", err)
				}
			}
		}()
	}

	// Iterate the bucket, lazily create a bucket store for each new user found
	// and submit a sync job for each user.
	err := u.bucket.Iter(ctx, "", func(s string) error {
		user := strings.TrimSuffix(s, "/")

		bs, err := u.getOrCreateStore(user)
		if err != nil {
			return err
		}

		jobs <- job{
			userID: user,
			store:  bs,
		}

		return nil
	})

	// Wait until all workers completed.
	close(jobs)
	wg.Wait()

	return err
}

// Series makes a series request to the underlying user store.
func (u *UserStore) Series(ctx context.Context, userID string, req *storepb.SeriesRequest) ([]*storepb.Series, storage.Warnings, error) {
	log, ctx := spanlogger.New(ctx, "UserStore.Series")
	defer log.Span.Finish()

	store := u.getStore(userID)
	if store == nil {
		return nil, nil, nil
	}

	srv := newBucketStoreSeriesServer(ctx)
	err := store.Series(req, srv)
	if err != nil {
		return nil, nil, err
	}

	return srv.SeriesSet, srv.Warnings, nil
}

func (u *UserStore) getStore(userID string) *store.BucketStore {
	u.storesMu.RLock()
	store := u.stores[userID]
	u.storesMu.RUnlock()

	return store
}

func (u *UserStore) getOrCreateStore(userID string) (*store.BucketStore, error) {
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

	userLogger := util.WithUserID(userID, u.logger)

	level.Info(userLogger).Log("msg", "creating user bucket store")

	userBkt := tsdb.NewUserBucketClient(userID, u.bucket)

	reg := prometheus.NewRegistry()
	fetcher, err := block.NewMetaFetcher(
		userLogger,
		u.cfg.BucketStore.MetaSyncConcurrency,
		userBkt,
		filepath.Join(u.cfg.BucketStore.SyncDir, userID), // The fetcher stores cached metas in the "meta-syncer/" sub directory
		reg,
		// List of filters to apply (order matters).
		block.NewConsistencyDelayMetaFilter(userLogger, u.cfg.BucketStore.ConsistencyDelay, reg).Filter,
		// Filters out duplicate blocks that can be formed from two or more overlapping
		// blocks that fully submatches the source blocks of the older blocks.
		block.NewDeduplicateFilter().Filter,
	)
	if err != nil {
		return nil, err
	}

	bs, err = store.NewBucketStore(
		userLogger,
		reg,
		userBkt,
		fetcher,
		filepath.Join(u.cfg.BucketStore.SyncDir, userID),
		u.indexCache,
		uint64(u.cfg.BucketStore.MaxChunkPoolBytes),
		u.cfg.BucketStore.MaxSampleCount,
		u.cfg.BucketStore.MaxConcurrent,
		u.logLevel.String() == "debug", // Turn on debug logging, if the log level is set to debug
		u.cfg.BucketStore.BlockSyncConcurrency,
		nil,   // Do not limit timerange.
		false, // No need to enable backward compatibility with Thanos pre 0.8.0 queriers
		u.cfg.BucketStore.BinaryIndexHeader,
	)
	if err != nil {
		return nil, err
	}

	u.stores[userID] = bs
	u.bucketStoreMetrics.addUserRegistry(userID, reg)

	return bs, nil
}
