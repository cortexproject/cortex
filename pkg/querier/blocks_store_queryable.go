package querier

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/pool"
	thanosquery "github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	grpc_metadata "google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/querysharding"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/storegateway"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/multierror"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	// The maximum number of times we attempt fetching missing blocks from different
	// store-gateways. If no more store-gateways are left (ie. due to lower replication
	// factor) than we'll end the retries earlier.
	maxFetchSeriesAttempts = 3
)

var (
	errNoStoreGatewayAddress  = errors.New("no store-gateway address configured")
	errMaxChunksPerQueryLimit = "the query hit the max number of chunks limit while fetching chunks from store-gateways for %s (limit: %d)"
	defaultAggrs              = []storepb.Aggr{storepb.Aggr_COUNT, storepb.Aggr_SUM}
)

// BlocksStoreSet is the interface used to get the clients to query series on a set of blocks.
type BlocksStoreSet interface {
	services.Service

	// GetClientsFor returns the store gateway clients that should be used to
	// query the set of blocks in input. The exclude parameter is the map of
	// blocks -> store-gateway addresses that should be excluded.
	GetClientsFor(userID string, blockIDs []ulid.ULID, exclude map[ulid.ULID][]string, attemptedBlocksZones map[ulid.ULID]map[string]int) (map[BlocksStoreClient][]ulid.ULID, error)
}

// BlocksFinder is the interface used to find blocks for a given user and time range.
type BlocksFinder interface {
	services.Service

	// GetBlocks returns known blocks for userID containing samples within the range minT
	// and maxT (milliseconds, both included). Returned blocks are sorted by MaxTime descending.
	GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error)
}

// BlocksStoreClient is the interface that should be implemented by any client used
// to query a backend store-gateway.
type BlocksStoreClient interface {
	storegatewaypb.StoreGatewayClient

	// RemoteAddress returns the address of the remote store-gateway and is used to uniquely
	// identify a store-gateway backend instance.
	RemoteAddress() string
}

// BlocksStoreLimits is the interface that should be implemented by the limits provider.
type BlocksStoreLimits interface {
	bucket.TenantConfigProvider

	MaxChunksPerQueryFromStore(userID string) int
	StoreGatewayTenantShardSize(userID string) float64
}

type blocksStoreQueryableMetrics struct {
	storesHit prometheus.Histogram
	refetches prometheus.Histogram
}

func newBlocksStoreQueryableMetrics(reg prometheus.Registerer) *blocksStoreQueryableMetrics {
	return &blocksStoreQueryableMetrics{
		storesHit: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "querier_storegateway_instances_hit_per_query",
			Help:      "Number of store-gateway instances hit for a single query.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}),
		refetches: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "querier_storegateway_refetches_per_query",
			Help:      "Number of re-fetches attempted while querying store-gateway instances due to missing blocks.",
			Buckets:   []float64{0, 1, 2},
		}),
	}
}

// BlocksStoreQueryable is a queryable which queries blocks storage via
// the store-gateway.
type BlocksStoreQueryable struct {
	services.Service

	stores          BlocksStoreSet
	finder          BlocksFinder
	consistency     *BlocksConsistencyChecker
	logger          log.Logger
	queryStoreAfter time.Duration
	metrics         *blocksStoreQueryableMetrics
	limits          BlocksStoreLimits

	storeGatewayQueryStatsEnabled           bool
	storeGatewayConsistencyCheckMaxAttempts int

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewBlocksStoreQueryable(
	stores BlocksStoreSet,
	finder BlocksFinder,
	consistency *BlocksConsistencyChecker,
	limits BlocksStoreLimits,
	config Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlocksStoreQueryable, error) {
	manager, err := services.NewManager(stores, finder)
	if err != nil {
		return nil, errors.Wrap(err, "register blocks storage queryable subservices")
	}

	q := &BlocksStoreQueryable{
		stores:                                  stores,
		finder:                                  finder,
		consistency:                             consistency,
		queryStoreAfter:                         config.QueryStoreAfter,
		logger:                                  logger,
		subservices:                             manager,
		subservicesWatcher:                      services.NewFailureWatcher(),
		metrics:                                 newBlocksStoreQueryableMetrics(reg),
		limits:                                  limits,
		storeGatewayQueryStatsEnabled:           config.StoreGatewayQueryStatsEnabled,
		storeGatewayConsistencyCheckMaxAttempts: config.StoreGatewayConsistencyCheckMaxAttempts,
	}

	q.Service = services.NewBasicService(q.starting, q.running, q.stopping)

	return q, nil
}

func NewBlocksStoreQueryableFromConfig(querierCfg Config, gatewayCfg storegateway.Config, storageCfg cortex_tsdb.BlocksStorageConfig, limits BlocksStoreLimits, logger log.Logger, reg prometheus.Registerer) (*BlocksStoreQueryable, error) {
	var stores BlocksStoreSet

	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, gatewayCfg.HedgedRequest.GetHedgedRoundTripper(), "querier", logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bucket client")
	}

	// Blocks finder doesn't use chunks, but we pass config for consistency.
	matchers := cortex_tsdb.NewMatchers()
	cachingBucket, err := cortex_tsdb.CreateCachingBucket(storageCfg.BucketStore.ChunksCache, storageCfg.BucketStore.MetadataCache, matchers, bucketClient, logger, extprom.WrapRegistererWith(prometheus.Labels{"component": "querier"}, reg))
	if err != nil {
		return nil, errors.Wrap(err, "create caching bucket")
	}
	bucketClient = cachingBucket

	// Create the blocks finder.
	var finder BlocksFinder
	if storageCfg.BucketStore.BucketIndex.Enabled {
		finder = NewBucketIndexBlocksFinder(BucketIndexBlocksFinderConfig{
			IndexLoader: bucketindex.LoaderConfig{
				CheckInterval:         time.Minute,
				UpdateOnStaleInterval: storageCfg.BucketStore.SyncInterval,
				UpdateOnErrorInterval: storageCfg.BucketStore.BucketIndex.UpdateOnErrorInterval,
				IdleTimeout:           storageCfg.BucketStore.BucketIndex.IdleTimeout,
			},
			MaxStalePeriod:           storageCfg.BucketStore.BucketIndex.MaxStalePeriod,
			IgnoreDeletionMarksDelay: storageCfg.BucketStore.IgnoreDeletionMarksDelay,
			IgnoreBlocksWithin:       storageCfg.BucketStore.IgnoreBlocksWithin,
		}, bucketClient, limits, logger, reg)
	} else {
		finder = NewBucketScanBlocksFinder(BucketScanBlocksFinderConfig{
			ScanInterval:             storageCfg.BucketStore.SyncInterval,
			TenantsConcurrency:       storageCfg.BucketStore.TenantSyncConcurrency,
			MetasConcurrency:         storageCfg.BucketStore.MetaSyncConcurrency,
			CacheDir:                 storageCfg.BucketStore.SyncDir,
			IgnoreDeletionMarksDelay: storageCfg.BucketStore.IgnoreDeletionMarksDelay,
			IgnoreBlocksWithin:       storageCfg.BucketStore.IgnoreBlocksWithin,
			BlockDiscoveryStrategy:   storageCfg.BucketStore.BlockDiscoveryStrategy,
		}, bucketClient, limits, logger, reg)
	}

	if gatewayCfg.ShardingEnabled {
		storesRingCfg := gatewayCfg.ShardingRing.ToRingConfig()
		storesRingBackend, err := kv.NewClient(
			storesRingCfg.KVStore,
			ring.GetCodec(),
			kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", reg), "querier-store-gateway"),
			logger,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create store-gateway ring backend")
		}

		storesRing, err := ring.NewWithStoreClientAndStrategy(storesRingCfg, storegateway.RingNameForClient, storegateway.RingKey, storesRingBackend, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", reg), logger)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create store-gateway ring client")
		}

		stores, err = newBlocksStoreReplicationSet(storesRing, gatewayCfg.ShardingStrategy, randomLoadBalancing, limits, querierCfg.StoreGatewayClient, logger, reg, storesRingCfg.ZoneAwarenessEnabled, gatewayCfg.ShardingRing.ZoneStableShuffleSharding)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create store set")
		}
	} else {
		if len(querierCfg.GetStoreGatewayAddresses()) == 0 {
			return nil, errNoStoreGatewayAddress
		}

		stores = newBlocksStoreBalancedSet(querierCfg.GetStoreGatewayAddresses(), querierCfg.StoreGatewayClient, logger, reg)
	}

	consistency := NewBlocksConsistencyChecker(
		// Exclude blocks which have been recently uploaded, in order to give enough time to store-gateways
		// to discover and load them (3 times the sync interval).
		storageCfg.BucketStore.ConsistencyDelay+(3*storageCfg.BucketStore.SyncInterval),
		// To avoid any false positive in the consistency check, we do exclude blocks which have been
		// recently marked for deletion, until the "ignore delay / 2". This means the consistency checker
		// exclude such blocks about 50% of the time before querier and store-gateway stops querying them.
		storageCfg.BucketStore.IgnoreDeletionMarksDelay/2,
		logger,
		reg,
	)

	return NewBlocksStoreQueryable(stores, finder, consistency, limits, querierCfg, logger, reg)
}

func (q *BlocksStoreQueryable) starting(ctx context.Context) error {
	q.subservicesWatcher.WatchManager(q.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, q.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks storage queryable subservices")
	}

	return nil
}

func (q *BlocksStoreQueryable) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-q.subservicesWatcher.Chan():
			return errors.Wrap(err, "block storage queryable subservice failed")
		}
	}
}

func (q *BlocksStoreQueryable) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), q.subservices)
}

// Querier returns a new Querier on the storage.
func (q *BlocksStoreQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	if s := q.State(); s != services.Running {
		return nil, errors.Errorf("BlocksStoreQueryable is not running: %v", s)
	}

	return &blocksStoreQuerier{
		minT:                                    mint,
		maxT:                                    maxt,
		finder:                                  q.finder,
		stores:                                  q.stores,
		metrics:                                 q.metrics,
		limits:                                  q.limits,
		consistency:                             q.consistency,
		logger:                                  q.logger,
		queryStoreAfter:                         q.queryStoreAfter,
		storeGatewayQueryStatsEnabled:           q.storeGatewayQueryStatsEnabled,
		storeGatewayConsistencyCheckMaxAttempts: q.storeGatewayConsistencyCheckMaxAttempts,
	}, nil
}

type blocksStoreQuerier struct {
	minT, maxT  int64
	finder      BlocksFinder
	stores      BlocksStoreSet
	metrics     *blocksStoreQueryableMetrics
	consistency *BlocksConsistencyChecker
	limits      BlocksStoreLimits
	logger      log.Logger

	// If set, the querier manipulates the max time to not be greater than
	// "now - queryStoreAfter" so that most recent blocks are not queried.
	queryStoreAfter time.Duration

	// If enabled, query stats of store gateway requests will be logged
	// using `info` level.
	storeGatewayQueryStatsEnabled bool

	// The maximum number of times we attempt fetching missing blocks from different Store Gateways.
	storeGatewayConsistencyCheckMaxAttempts int
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *blocksStoreQuerier) Select(ctx context.Context, _ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return q.selectSorted(ctx, sp, matchers...)
}

func (q *blocksStoreQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanLog, spanCtx := spanlogger.New(ctx, "blocksStoreQuerier.LabelNames")
	defer spanLog.Span.Finish()

	minT, maxT, limit := q.minT, q.maxT, int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	var (
		resMtx            sync.Mutex
		resNameSets       = [][]string{}
		resWarnings       = annotations.Annotations(nil)
		convertedMatchers = convertMatchersToLabelMatcher(matchers)
	)

	queryFunc := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64) ([]ulid.ULID, error, error) {
		nameSets, warnings, queriedBlocks, err, retryableError := q.fetchLabelNamesFromStore(spanCtx, userID, clients, minT, maxT, limit, convertedMatchers)
		if err != nil {
			return nil, err, retryableError
		}

		resMtx.Lock()
		resNameSets = append(resNameSets, nameSets...)
		resWarnings.Merge(warnings)
		resMtx.Unlock()

		return queriedBlocks, nil, retryableError
	}

	if err := q.queryWithConsistencyCheck(spanCtx, spanLog, minT, maxT, userID, queryFunc); err != nil {
		return nil, nil, err
	}

	return strutil.MergeSlices(int(limit), resNameSets...), resWarnings, nil
}

func (q *blocksStoreQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanLog, spanCtx := spanlogger.New(ctx, "blocksStoreQuerier.LabelValues")
	defer spanLog.Span.Finish()

	minT, maxT, limit := q.minT, q.maxT, int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	var (
		resValueSets = [][]string{}
		resWarnings  = annotations.Annotations(nil)

		resultMtx sync.Mutex
	)

	queryFunc := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64) ([]ulid.ULID, error, error) {
		valueSets, warnings, queriedBlocks, err, retryableError := q.fetchLabelValuesFromStore(spanCtx, userID, name, clients, minT, maxT, limit, matchers...)
		if err != nil {
			return nil, err, retryableError
		}

		resultMtx.Lock()
		resValueSets = append(resValueSets, valueSets...)
		resWarnings.Merge(warnings)
		resultMtx.Unlock()

		return queriedBlocks, nil, retryableError
	}

	if err := q.queryWithConsistencyCheck(spanCtx, spanLog, minT, maxT, userID, queryFunc); err != nil {
		return nil, nil, err
	}

	return strutil.MergeSlices(int(limit), resValueSets...), resWarnings, nil
}

func (q *blocksStoreQuerier) Close() error {
	return nil
}

func (q *blocksStoreQuerier) selectSorted(ctx context.Context, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	spanLog, spanCtx := spanlogger.New(ctx, "blocksStoreQuerier.selectSorted")
	defer spanLog.Span.Finish()

	minT, maxT, limit := q.minT, q.maxT, int64(0)
	if sp != nil {
		minT, maxT, limit = sp.Start, sp.End, int64(sp.Limit)
	}

	var (
		resSeriesSets = []storage.SeriesSet(nil)
		resWarnings   = annotations.Annotations(nil)

		maxChunksLimit  = q.limits.MaxChunksPerQueryFromStore(userID)
		leftChunksLimit = maxChunksLimit

		resultMtx sync.Mutex
	)

	queryFunc := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64) ([]ulid.ULID, error, error) {
		seriesSets, queriedBlocks, warnings, numChunks, err, retryableError := q.fetchSeriesFromStores(spanCtx, sp, userID, clients, minT, maxT, limit, matchers, maxChunksLimit, leftChunksLimit)
		if err != nil {
			return nil, err, retryableError
		}

		resultMtx.Lock()

		resSeriesSets = append(resSeriesSets, seriesSets...)
		resWarnings.Merge(warnings)

		// Given a single block is guaranteed to not be queried twice, we can safely decrease the number of
		// chunks we can still read before hitting the limit (max == 0 means disabled).
		if maxChunksLimit > 0 {
			leftChunksLimit -= numChunks
		}
		resultMtx.Unlock()

		return queriedBlocks, nil, retryableError
	}

	if err := q.queryWithConsistencyCheck(spanCtx, spanLog, minT, maxT, userID, queryFunc); err != nil {
		return storage.ErrSeriesSet(err)
	}

	if len(resSeriesSets) == 0 {
		storage.EmptySeriesSet()
	}

	// TODO(johrry): pass limit when merging.
	return series.NewSeriesSetWithWarnings(
		storage.NewMergeSeriesSet(resSeriesSets, storage.ChainedSeriesMerge),
		resWarnings)
}

func (q *blocksStoreQuerier) queryWithConsistencyCheck(ctx context.Context, logger log.Logger, minT, maxT int64, userID string,
	queryFunc func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64) ([]ulid.ULID, error, error)) error {
	// If queryStoreAfter is enabled, we do manipulate the query maxt to query samples up until
	// now - queryStoreAfter, because the most recent time range is covered by ingesters. This
	// optimization is particularly important for the blocks storage because can be used to skip
	// querying most recent not-compacted-yet blocks from the storage.
	if q.queryStoreAfter > 0 {
		now := time.Now()
		origMaxT := maxT
		maxT = min(maxT, util.TimeToMillis(now.Add(-q.queryStoreAfter)))

		if origMaxT != maxT {
			level.Debug(logger).Log("msg", "the max time of the query to blocks storage has been manipulated", "original", origMaxT, "updated", maxT)
		}

		if maxT < minT {
			q.metrics.storesHit.Observe(0)
			level.Debug(logger).Log("msg", "empty query time range after max time manipulation")
			return nil
		}
	}

	// Find the list of blocks we need to query given the time range.
	knownBlocks, knownDeletionMarks, err := q.finder.GetBlocks(ctx, userID, minT, maxT)
	if err != nil {
		return err
	}

	if len(knownBlocks) == 0 {
		q.metrics.storesHit.Observe(0)
		level.Debug(logger).Log("msg", "no blocks found")
		return nil
	}

	level.Debug(logger).Log("msg", "found blocks to query", "expected", knownBlocks.String())

	var (
		// At the beginning the list of blocks to query are all known blocks.
		remainingBlocks = knownBlocks.GetULIDs()
		attemptedBlocks = map[ulid.ULID][]string{}
		touchedStores   = map[string]struct{}{}

		resQueriedBlocks     = []ulid.ULID(nil)
		attemptedBlocksZones = make(map[ulid.ULID]map[string]int, len(remainingBlocks))

		queriedBlocks  []ulid.ULID
		retryableError error
	)

	for attempt := 1; attempt <= q.storeGatewayConsistencyCheckMaxAttempts; attempt++ {
		// Find the set of store-gateway instances having the blocks. The exclude parameter is the
		// map of blocks queried so far, with the list of store-gateway addresses for each block.
		clients, err := q.stores.GetClientsFor(userID, remainingBlocks, attemptedBlocks, attemptedBlocksZones)
		if err != nil {
			// If it's a retry and we get an error, it means there are no more store-gateways left
			// from which running another attempt, so we're just stopping retrying.
			if attempt > 1 {
				level.Warn(logger).Log("msg", "unable to get store-gateway clients while retrying to fetch missing blocks", "err", err)
				break
			}

			return err
		}
		level.Debug(logger).Log("msg", "found store-gateway instances to query", "num instances", len(clients), "attempt", attempt)

		// Fetch series from stores. If an error occur we do not retry because retries
		// are only meant to cover missing blocks.
		queriedBlocks, err, retryableError = queryFunc(clients, minT, maxT)
		if err != nil {
			return err
		}
		level.Debug(logger).Log("msg", "received series from all store-gateways", "queried blocks", strings.Join(convertULIDsToString(queriedBlocks), " "))

		resQueriedBlocks = append(resQueriedBlocks, queriedBlocks...)

		// Update the map of blocks we attempted to query.
		for client, blockIDs := range clients {
			touchedStores[client.RemoteAddress()] = struct{}{}

			for _, blockID := range blockIDs {
				attemptedBlocks[blockID] = append(attemptedBlocks[blockID], client.RemoteAddress())
			}
		}

		// Ensure all expected blocks have been queried (during all tries done so far).
		missingBlocks := q.consistency.Check(knownBlocks, knownDeletionMarks, resQueriedBlocks)
		if len(missingBlocks) == 0 {
			q.metrics.storesHit.Observe(float64(len(touchedStores)))
			q.metrics.refetches.Observe(float64(attempt - 1))

			return nil
		}

		level.Debug(logger).Log("msg", "consistency check failed", "attempt", attempt, "missing blocks", strings.Join(convertULIDsToString(missingBlocks), " "))

		// The next attempt should just query the missing blocks.
		remainingBlocks = missingBlocks
	}

	// After we exhausted retries, if retryable error is not nil return the retryable error.
	// It can be helpful to know whether we need to retry more or not.
	if retryableError != nil {
		return retryableError
	}

	// We've not been able to query all expected blocks after all retries.
	err = fmt.Errorf("consistency check failed because some blocks were not queried: %s", strings.Join(convertULIDsToString(remainingBlocks), " "))
	level.Warn(util_log.WithContext(ctx, logger)).Log("msg", "failed consistency check", "err", err)
	return err
}

func (q *blocksStoreQuerier) fetchSeriesFromStores(
	ctx context.Context,
	sp *storage.SelectHints,
	userID string,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	limit int64,
	matchers []*labels.Matcher,
	maxChunksLimit int,
	leftChunksLimit int,
) ([]storage.SeriesSet, []ulid.ULID, annotations.Annotations, int, error, error) {
	var (
		reqCtx        = grpc_metadata.AppendToOutgoingContext(ctx, cortex_tsdb.TenantIDExternalLabel, userID)
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		seriesSets    = []storage.SeriesSet(nil)
		warnings      = annotations.Annotations(nil)
		queriedBlocks = []ulid.ULID(nil)
		numChunks     = atomic.NewInt32(0)
		spanLog       = spanlogger.FromContext(ctx)
		queryLimiter  = limiter.QueryLimiterFromContextWithFallback(ctx)
		reqStats      = stats.FromContext(ctx)
		merrMtx       = sync.Mutex{}
		merr          = multierror.MultiError{}
	)
	matchers, shardingInfo, err := querysharding.ExtractShardingInfo(matchers)

	if err != nil {
		return nil, nil, nil, 0, err, merr.Err()
	}
	convertedMatchers := convertMatchersToLabelMatcher(matchers)

	// Concurrently fetch series from all clients.
	for c, blockIDs := range clients {
		// Change variables scope since it will be used in a goroutine.
		c := c
		blockIDs := blockIDs

		g.Go(func() error {
			// See: https://github.com/prometheus/prometheus/pull/8050
			// TODO(goutham): we should ideally be passing the hints down to the storage layer
			// and let the TSDB return us data with no chunks as in prometheus#8050.
			// But this is an acceptable workaround for now.

			// Only fail the function if we have validation error. We should return blocks that were successfully
			// retrieved.
			seriesQueryStats := &hintspb.QueryStats{}
			skipChunks := sp != nil && sp.Func == "series"

			req, err := createSeriesRequest(minT, maxT, limit, convertedMatchers, shardingInfo, skipChunks, blockIDs, defaultAggrs)
			if err != nil {
				return errors.Wrapf(err, "failed to create series request")
			}

			begin := time.Now()
			stream, err := c.Series(gCtx, req)
			if err != nil {
				if isRetryableError(err) {
					level.Warn(spanLog).Log("err", errors.Wrapf(err, "failed to fetch series from %s due to retryable error", c.RemoteAddress()))
					merrMtx.Lock()
					merr.Add(err)
					merrMtx.Unlock()
					return nil
				}
				return errors.Wrapf(err, "failed to fetch series from %s", c.RemoteAddress())
			}

			mySeries := []*storepb.Series(nil)
			myWarnings := annotations.Annotations(nil)
			myQueriedBlocks := []ulid.ULID(nil)

			for {
				// Ensure the context hasn't been canceled in the meanwhile (eg. an error occurred
				// in another goroutine).
				if gCtx.Err() != nil {
					return gCtx.Err()
				}

				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}

				if isRetryableError(err) {
					level.Warn(spanLog).Log("err", errors.Wrapf(err, "failed to receive series from %s due to retryable error", c.RemoteAddress()))
					merrMtx.Lock()
					merr.Add(err)
					merrMtx.Unlock()
					return nil
				}

				if err != nil {
					s, ok := status.FromError(err)
					if !ok {
						s, ok = status.FromError(errors.Cause(err))
					}

					if ok {
						if s.Code() == codes.ResourceExhausted {
							return validation.LimitError(s.Message())
						}

						if s.Code() == codes.PermissionDenied {
							return validation.AccessDeniedError(s.Message())
						}
					}
					return errors.Wrapf(err, "failed to receive series from %s", c.RemoteAddress())
				}

				// Response may either contain series, warning or hints.
				if s := resp.GetSeries(); s != nil {
					mySeries = append(mySeries, s)

					// Add series fingerprint to query limiter; will return error if we are over the limit
					limitErr := queryLimiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(s.PromLabels()))
					if limitErr != nil {
						return validation.LimitError(limitErr.Error())
					}

					// Ensure the max number of chunks limit hasn't been reached (max == 0 means disabled).
					if maxChunksLimit > 0 {
						actual := numChunks.Add(int32(len(s.Chunks)))
						if actual > int32(leftChunksLimit) {
							return validation.LimitError(fmt.Sprintf(errMaxChunksPerQueryLimit, util.LabelMatchersToString(matchers), maxChunksLimit))
						}
					}
					chunksSize := countChunkBytes(s)
					dataSize := countDataBytes(s)
					if chunkBytesLimitErr := queryLimiter.AddChunkBytes(chunksSize); chunkBytesLimitErr != nil {
						return validation.LimitError(chunkBytesLimitErr.Error())
					}
					if chunkLimitErr := queryLimiter.AddChunks(len(s.Chunks)); chunkLimitErr != nil {
						return validation.LimitError(chunkLimitErr.Error())
					}
					if dataBytesLimitErr := queryLimiter.AddDataBytes(dataSize); dataBytesLimitErr != nil {
						return validation.LimitError(dataBytesLimitErr.Error())
					}
				}

				if w := resp.GetWarning(); w != "" {
					myWarnings.Add(errors.New(w))
				}

				if h := resp.GetHints(); h != nil {
					hints := hintspb.SeriesResponseHints{}
					if err := types.UnmarshalAny(h, &hints); err != nil {
						return errors.Wrapf(err, "failed to unmarshal series hints from %s", c.RemoteAddress())
					}

					ids, err := convertBlockHintsToULIDs(hints.QueriedBlocks)
					if err != nil {
						return errors.Wrapf(err, "failed to parse queried block IDs from received hints")
					}

					myQueriedBlocks = append(myQueriedBlocks, ids...)
					if hints.QueryStats != nil {
						seriesQueryStats.Merge(hints.QueryStats)
					}
				}
			}

			numSeries := len(mySeries)
			numSamples, chunksCount := countSamplesAndChunks(mySeries...)
			chunkBytes := countChunkBytes(mySeries...)
			dataBytes := countDataBytes(mySeries...)

			reqStats.AddFetchedSeries(uint64(numSeries))
			reqStats.AddFetchedChunks(chunksCount)
			reqStats.AddFetchedSamples(numSamples)
			reqStats.AddFetchedChunkBytes(uint64(chunkBytes))
			reqStats.AddFetchedDataBytes(uint64(dataBytes))
			reqStats.AddStoreGatewayTouchedPostings(uint64(seriesQueryStats.PostingsTouched))
			reqStats.AddStoreGatewayTouchedPostingBytes(uint64(seriesQueryStats.PostingsTouchedSizeSum))

			level.Debug(spanLog).Log("msg", "received series from store-gateway",
				"instance", c.RemoteAddress(),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			// Use number of blocks queried to check whether we should log the query
			// or not. It might be logging too much but good to understand per request
			// performance.
			if q.storeGatewayQueryStatsEnabled && seriesQueryStats.BlocksQueried > 0 {
				level.Info(spanLog).Log("msg", "store gateway series request stats",
					"instance", c.RemoteAddress(),
					"queryable_chunk_bytes_fetched", chunkBytes,
					"queryable_data_bytes_fetched", dataBytes,
					"blocks_queried", seriesQueryStats.BlocksQueried,
					"series_merged_count", seriesQueryStats.MergedSeriesCount,
					"chunks_merged_count", seriesQueryStats.MergedChunksCount,
					"postings_touched", seriesQueryStats.PostingsTouched,
					"postings_touched_size_sum", seriesQueryStats.PostingsTouchedSizeSum,
					"postings_to_fetch", seriesQueryStats.PostingsToFetch,
					"postings_fetched", seriesQueryStats.PostingsFetched,
					"postings_fetch_count", seriesQueryStats.PostingsFetchCount,
					"postings_fetched_size_sum", seriesQueryStats.PostingsFetchedSizeSum,
					"series_touched", seriesQueryStats.SeriesTouched,
					"series_touched_size_sum", seriesQueryStats.SeriesTouchedSizeSum,
					"series_fetched", seriesQueryStats.SeriesFetched,
					"series_fetch_count", seriesQueryStats.SeriesFetchCount,
					"series_fetched_size_sum", seriesQueryStats.SeriesFetchedSizeSum,
					"chunks_touched", seriesQueryStats.ChunksTouched,
					"chunks_touched_size_sum", seriesQueryStats.ChunksTouchedSizeSum,
					"chunks_fetched", seriesQueryStats.ChunksFetched,
					"chunks_fetch_count", seriesQueryStats.ChunksFetchCount,
					"chunks_fetched_size_sum", seriesQueryStats.ChunksFetchedSizeSum,
					"data_downloaded_size_sum", seriesQueryStats.DataDownloadedSizeSum,
					"get_all_duration", seriesQueryStats.GetAllDuration,
					"merge_duration", seriesQueryStats.MergeDuration,
					"response_time", time.Since(begin),
				)
			}

			// Store the result.
			mtx.Lock()
			// TODO: change other aggregations when downsampling is enabled.
			seriesSets = append(seriesSets, thanosquery.NewPromSeriesSet(newStoreSeriesSet(mySeries), minT, maxT, defaultAggrs, nil))
			warnings.Merge(myWarnings)
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	// Wait until all client requests complete.
	if err := g.Wait(); err != nil {
		return nil, nil, nil, 0, err, merr.Err()
	}

	return seriesSets, queriedBlocks, warnings, int(numChunks.Load()), nil, merr.Err()
}

func (q *blocksStoreQuerier) fetchLabelNamesFromStore(
	ctx context.Context,
	userID string,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	limit int64,
	matchers []storepb.LabelMatcher,
) ([][]string, annotations.Annotations, []ulid.ULID, error, error) {
	var (
		reqCtx        = grpc_metadata.AppendToOutgoingContext(ctx, cortex_tsdb.TenantIDExternalLabel, userID)
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		nameSets      = [][]string{}
		warnings      = annotations.Annotations(nil)
		queriedBlocks = []ulid.ULID(nil)
		spanLog       = spanlogger.FromContext(ctx)
		merrMtx       = sync.Mutex{}
		merr          = multierror.MultiError{}
		queryLimiter  = limiter.QueryLimiterFromContextWithFallback(ctx)
	)

	// Concurrently fetch series from all clients.
	for c, blockIDs := range clients {
		// Change variables scope since it will be used in a goroutine.
		c := c
		blockIDs := blockIDs

		g.Go(func() error {
			req, err := createLabelNamesRequest(minT, maxT, limit, blockIDs, matchers)
			if err != nil {
				return errors.Wrapf(err, "failed to create label names request")
			}

			namesResp, err := c.LabelNames(gCtx, req)
			if err != nil {
				if isRetryableError(err) {
					level.Warn(spanLog).Log("err", errors.Wrapf(err, "failed to fetch label names from %s due to retryable error", c.RemoteAddress()))
					merrMtx.Lock()
					merr.Add(err)
					merrMtx.Unlock()
					return nil
				}

				s, ok := status.FromError(err)
				if !ok {
					s, ok = status.FromError(errors.Cause(err))
				}

				if ok {
					if s.Code() == codes.ResourceExhausted {
						return validation.LimitError(s.Message())
					}
				}

				if s.Code() == codes.PermissionDenied {
					return validation.AccessDeniedError(s.Message())
				}
				return errors.Wrapf(err, "failed to fetch label names from %s", c.RemoteAddress())
			}
			if dataBytesLimitErr := queryLimiter.AddDataBytes(namesResp.Size()); dataBytesLimitErr != nil {
				return validation.LimitError(dataBytesLimitErr.Error())
			}

			myQueriedBlocks := []ulid.ULID(nil)
			if namesResp.Hints != nil {
				hints := hintspb.LabelNamesResponseHints{}
				if err := types.UnmarshalAny(namesResp.Hints, &hints); err != nil {
					return errors.Wrapf(err, "failed to unmarshal label names hints from %s", c.RemoteAddress())
				}

				ids, err := convertBlockHintsToULIDs(hints.QueriedBlocks)
				if err != nil {
					return errors.Wrapf(err, "failed to parse queried block IDs from received hints")
				}

				myQueriedBlocks = ids
			}

			level.Debug(spanLog).Log("msg", "received label names from store-gateway",
				"instance", c,
				"num labels", len(namesResp.Names),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			// Store the result.
			mtx.Lock()
			nameSets = append(nameSets, namesResp.Names)
			for _, w := range namesResp.Warnings {
				warnings.Add(errors.New(w))
			}
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	// Wait until all client requests complete.
	if err := g.Wait(); err != nil {
		return nil, nil, nil, err, merr.Err()
	}

	return nameSets, warnings, queriedBlocks, nil, merr.Err()
}

func (q *blocksStoreQuerier) fetchLabelValuesFromStore(
	ctx context.Context,
	userID string,
	name string,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	limit int64,
	matchers ...*labels.Matcher,
) ([][]string, annotations.Annotations, []ulid.ULID, error, error) {
	var (
		reqCtx        = grpc_metadata.AppendToOutgoingContext(ctx, cortex_tsdb.TenantIDExternalLabel, userID)
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		valueSets     = [][]string{}
		warnings      = annotations.Annotations(nil)
		queriedBlocks = []ulid.ULID(nil)
		spanLog       = spanlogger.FromContext(ctx)
		merrMtx       = sync.Mutex{}
		merr          = multierror.MultiError{}
		queryLimiter  = limiter.QueryLimiterFromContextWithFallback(ctx)
	)

	// Concurrently fetch series from all clients.
	for c, blockIDs := range clients {
		// Change variables scope since it will be used in a goroutine.
		c := c
		blockIDs := blockIDs

		g.Go(func() error {
			req, err := createLabelValuesRequest(minT, maxT, limit, name, blockIDs, matchers...)
			if err != nil {
				return errors.Wrapf(err, "failed to create label values request")
			}

			valuesResp, err := c.LabelValues(gCtx, req)
			if err != nil {
				if isRetryableError(err) {
					level.Warn(spanLog).Log("err", errors.Wrapf(err, "failed to fetch label values from %s due to retryable error", c.RemoteAddress()))
					merrMtx.Lock()
					merr.Add(err)
					merrMtx.Unlock()
					return nil
				}

				s, ok := status.FromError(err)
				if !ok {
					s, ok = status.FromError(errors.Cause(err))
				}

				if ok {
					if s.Code() == codes.ResourceExhausted {
						return validation.LimitError(s.Message())
					}

					if s.Code() == codes.PermissionDenied {
						return validation.AccessDeniedError(s.Message())
					}
				}
				return errors.Wrapf(err, "failed to fetch label values from %s", c.RemoteAddress())
			}
			if dataBytesLimitErr := queryLimiter.AddDataBytes(valuesResp.Size()); dataBytesLimitErr != nil {
				return validation.LimitError(dataBytesLimitErr.Error())
			}

			myQueriedBlocks := []ulid.ULID(nil)
			if valuesResp.Hints != nil {
				hints := hintspb.LabelValuesResponseHints{}
				if err := types.UnmarshalAny(valuesResp.Hints, &hints); err != nil {
					return errors.Wrapf(err, "failed to unmarshal label values hints from %s", c.RemoteAddress())
				}

				ids, err := convertBlockHintsToULIDs(hints.QueriedBlocks)
				if err != nil {
					return errors.Wrapf(err, "failed to parse queried block IDs from received hints")
				}

				myQueriedBlocks = ids
			}

			level.Debug(spanLog).Log("msg", "received label values from store-gateway",
				"instance", c.RemoteAddress(),
				"num values", len(valuesResp.Values),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			// Values returned need not be sorted, but we need them to be sorted so we can merge.
			sort.Strings(valuesResp.Values)

			// Store the result.
			mtx.Lock()
			valueSets = append(valueSets, valuesResp.Values)
			for _, w := range valuesResp.Warnings {
				warnings.Add(errors.New(w))
			}
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	// Wait until all client requests complete.
	if err := g.Wait(); err != nil {
		return nil, nil, nil, err, merr.Err()
	}

	return valueSets, warnings, queriedBlocks, nil, merr.Err()
}

func createSeriesRequest(minT, maxT, limit int64, matchers []storepb.LabelMatcher, shardingInfo *storepb.ShardInfo, skipChunks bool, blockIDs []ulid.ULID, aggrs []storepb.Aggr) (*storepb.SeriesRequest, error) {
	// Selectively query only specific blocks.
	hints := &hintspb.SeriesRequestHints{
		BlockMatchers: []storepb.LabelMatcher{
			{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(convertULIDsToString(blockIDs), "|"),
			},
		},
		EnableQueryStats: true,
	}

	anyHints, err := types.MarshalAny(hints)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal series request hints")
	}

	return &storepb.SeriesRequest{
		MinTime:                 minT,
		MaxTime:                 maxT,
		Limit:                   limit,
		Matchers:                matchers,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		Hints:                   anyHints,
		SkipChunks:              skipChunks,
		ShardInfo:               shardingInfo,
		Aggregates:              aggrs,
		// TODO: support more downsample levels when downsampling is supported.
		MaxResolutionWindow: downsample.ResLevel0,
	}, nil
}

func createLabelNamesRequest(minT, maxT, limit int64, blockIDs []ulid.ULID, matchers []storepb.LabelMatcher) (*storepb.LabelNamesRequest, error) {
	req := &storepb.LabelNamesRequest{
		Start:    minT,
		End:      maxT,
		Limit:    limit,
		Matchers: matchers,
	}

	// Selectively query only specific blocks.
	hints := &hintspb.LabelNamesRequestHints{
		BlockMatchers: []storepb.LabelMatcher{
			{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(convertULIDsToString(blockIDs), "|"),
			},
		},
	}

	anyHints, err := types.MarshalAny(hints)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal label names request hints")
	}

	req.Hints = anyHints

	return req, nil
}

func createLabelValuesRequest(minT, maxT, limit int64, label string, blockIDs []ulid.ULID, matchers ...*labels.Matcher) (*storepb.LabelValuesRequest, error) {
	req := &storepb.LabelValuesRequest{
		Start:    minT,
		End:      maxT,
		Limit:    limit,
		Label:    label,
		Matchers: convertMatchersToLabelMatcher(matchers),
	}

	// Selectively query only specific blocks.
	hints := &hintspb.LabelValuesRequestHints{
		BlockMatchers: []storepb.LabelMatcher{
			{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(convertULIDsToString(blockIDs), "|"),
			},
		},
	}

	anyHints, err := types.MarshalAny(hints)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal label values request hints")
	}

	req.Hints = anyHints

	return req, nil
}

func convertULIDsToString(ids []ulid.ULID) []string {
	res := make([]string, len(ids))
	for idx, id := range ids {
		res[idx] = id.String()
	}
	return res
}

func convertBlockHintsToULIDs(hints []hintspb.Block) ([]ulid.ULID, error) {
	res := make([]ulid.ULID, len(hints))

	for idx, hint := range hints {
		blockID, err := ulid.Parse(hint.Id)
		if err != nil {
			return nil, err
		}

		res[idx] = blockID
	}

	return res, nil
}

// countChunkBytes returns the size of the chunks making up the provided series in bytes
func countChunkBytes(series ...*storepb.Series) (count int) {
	for _, s := range series {
		for _, c := range s.Chunks {
			count += c.Size()
		}
	}

	return count
}

// countDataBytes returns the combined size of the all series
func countDataBytes(series ...*storepb.Series) (count int) {
	for _, s := range series {
		count += s.Size()
	}

	return count
}

// countSamplesAndChunks counts the number of samples and number counts from the series.
func countSamplesAndChunks(series ...*storepb.Series) (samplesCount, chunksCount uint64) {
	for _, s := range series {
		chunksCount += uint64(len(s.Chunks))
		for _, c := range s.Chunks {
			if c.Raw != nil {
				switch c.Raw.Type {
				case storepb.Chunk_XOR, storepb.Chunk_HISTOGRAM, storepb.Chunk_FLOAT_HISTOGRAM:
					samplesCount += uint64(binary.BigEndian.Uint16(c.Raw.Data))
				}
			}
		}
	}
	return
}

// only retry connection issues
func isRetryableError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable:
		return true
	case codes.ResourceExhausted:
		return errors.Is(err, storegateway.ErrTooManyInflightRequests)
	// Client side connection closing, this error happens during store gateway deployment.
	// https://github.com/grpc/grpc-go/blob/03172006f5d168fc646d87928d85cb9c4a480291/clientconn.go#L67
	case codes.Canceled:
		return strings.Contains(err.Error(), "grpc: the client connection is closing")
	case codes.Unknown:
		// Catch chunks pool exhaustion error only.
		return strings.Contains(err.Error(), pool.ErrPoolExhausted.Error())
	default:
		return false
	}
}
