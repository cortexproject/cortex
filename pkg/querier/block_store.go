package querier

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// UserStore is a multi-tenant version of Thanos BucketStore
type UserStore struct {
	logger      log.Logger
	cfg         tsdb.Config
	bucket      objstore.Bucket
	client      storepb.StoreClient
	logLevel    logging.Level
	tsdbMetrics *tsdbBucketStoreMetrics

	syncMint model.TimeOrDurationValue
	syncMaxt model.TimeOrDurationValue

	// Keeps a bucket store for each tenant.
	storesMu sync.RWMutex
	stores   map[string]*store.BucketStore

	// Used to cancel workers and wait until done.
	workers       sync.WaitGroup
	workersCancel context.CancelFunc

	// Metrics.
	syncTimes prometheus.Histogram
}

// NewUserStore returns a new UserStore
func NewUserStore(cfg tsdb.Config, bucketClient objstore.Bucket, logLevel logging.Level, logger log.Logger, registerer prometheus.Registerer) (*UserStore, error) {
	workersCtx, workersCancel := context.WithCancel(context.Background())

	u := &UserStore{
		logger:        logger,
		cfg:           cfg,
		bucket:        bucketClient,
		stores:        map[string]*store.BucketStore{},
		logLevel:      logLevel,
		tsdbMetrics:   newTSDBBucketStoreMetrics(),
		workersCancel: workersCancel,
		syncTimes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_querier_blocks_sync_seconds",
			Help:    "The total time it takes to perform a sync stores",
			Buckets: []float64{0.1, 1, 10, 30, 60, 120, 300, 600, 900},
		}),
	}

	// Configure the time range to sync all blocks.
	if err := u.syncMint.Set("0000-01-01T00:00:00Z"); err != nil {
		return nil, err
	}
	if err := u.syncMaxt.Set("9999-12-31T23:59:59Z"); err != nil {
		return nil, err
	}

	if registerer != nil {
		registerer.MustRegister(u.syncTimes, u.tsdbMetrics)
	}

	serv := grpc.NewServer()
	storepb.RegisterStoreServer(serv, u)
	l, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}
	go serv.Serve(l)

	cc, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	u.client = storepb.NewStoreClient(cc)

	// If the sync is disabled we never sync blocks, which means the bucket store
	// will be empty and no series will be returned once queried.
	if u.cfg.BucketStore.SyncInterval > 0 {
		// Run an initial blocks sync, required in order to be able to serve queries.
		if err := u.initialSync(workersCtx); err != nil {
			return nil, err
		}

		// Periodically sync the blocks.
		u.workers.Add(1)
		go u.syncStoresLoop(workersCtx)
	}

	return u, nil
}

// Stop the blocks sync and waits until done.
func (u *UserStore) Stop() {
	u.workersCancel()
	u.workers.Wait()
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
func (u *UserStore) syncStoresLoop(ctx context.Context) {
	defer u.workers.Done()

	syncInterval := u.cfg.BucketStore.SyncInterval

	// Since we've just run the initial sync, we should wait the next
	// sync interval before resynching.
	select {
	case <-ctx.Done():
		return
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

	if err != nil {
		// This should never occur because the rununtil.Repeat() returns error
		// only if the callback function returns error (which doesn't), but since
		// we have to handle the error because of the linter, it's better to log it.
		level.Error(u.logger).Log("msg", "blocks synchronization has been halted due to an unexpected error", "err", err)
	}
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

// Info makes an info request to the underlying user store
func (u *UserStore) Info(ctx context.Context, req *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata")
	}

	v := md.Get("user")
	if len(v) == 0 {
		return nil, fmt.Errorf("no userID")
	}

	store := u.getStore(v[0])
	if store == nil {
		return nil, nil
	}

	return store.Info(ctx, req)
}

// Series makes a series request to the underlying user store
func (u *UserStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return fmt.Errorf("no metadata")
	}

	v := md.Get("user")
	if len(v) == 0 {
		return fmt.Errorf("no userID")
	}

	store := u.getStore(v[0])
	if store == nil {
		return nil
	}

	return store.Series(req, srv)
}

// LabelNames makes a labelnames request to the underlying user store
func (u *UserStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata")
	}

	v := md.Get("user")
	if len(v) == 0 {
		return nil, fmt.Errorf("no userID")
	}

	store := u.getStore(v[0])
	if store == nil {
		return nil, nil
	}

	return store.LabelNames(ctx, req)
}

// LabelValues makes a labelvalues request to the underlying user store
func (u *UserStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata")
	}

	v := md.Get("user")
	if len(v) == 0 {
		return nil, fmt.Errorf("no userID")
	}

	store := u.getStore(v[0])
	if store == nil {
		return nil, nil
	}

	return store.LabelValues(ctx, req)
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

	level.Info(u.logger).Log("msg", "creating user bucket store", "user", userID)

	reg := prometheus.NewRegistry()
	indexCacheSizeBytes := u.cfg.BucketStore.IndexCacheSizeBytes
	maxItemSizeBytes := indexCacheSizeBytes / 2
	indexCache, err := storecache.NewInMemoryIndexCache(u.logger, reg, storecache.Opts{
		MaxSizeBytes:     indexCacheSizeBytes,
		MaxItemSizeBytes: maxItemSizeBytes,
	})
	if err != nil {
		return nil, err
	}

	bs, err = store.NewBucketStore(
		u.logger,
		reg,
		tsdb.NewUserBucketClient(userID, u.bucket),
		filepath.Join(u.cfg.BucketStore.SyncDir, userID),
		indexCache,
		uint64(u.cfg.BucketStore.MaxChunkPoolBytes),
		u.cfg.BucketStore.MaxSampleCount,
		u.cfg.BucketStore.MaxConcurrent,
		u.logLevel.String() == "debug", // Turn on debug logging, if the log level is set to debug
		u.cfg.BucketStore.BlockSyncConcurrency,
		&store.FilterConfig{
			MinTime: u.syncMint,
			MaxTime: u.syncMaxt,
		},
		nil,   // No relabelling config
		false, // No need to enable backward compatibility with Thanos pre 0.8.0 queriers
	)
	if err != nil {
		return nil, err
	}

	u.stores[userID] = bs
	u.tsdbMetrics.addUserRegistry(userID, reg)

	return bs, nil
}
