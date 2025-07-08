package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/queryable"
	"github.com/prometheus-community/parquet-common/schema"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/strutil"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/multierror"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type blockStorageType struct{}

var blockStorageKey = blockStorageType{}

const BlockStoreTypeHeader = "X-Cortex-BlockStore-Type"

type blockStoreType string

const (
	tsdbBlockStore    blockStoreType = "tsdb"
	parquetBlockStore blockStoreType = "parquet"
)

var validBlockStoreTypes = []blockStoreType{tsdbBlockStore, parquetBlockStore}

// AddBlockStoreTypeToContext checks HTTP header and set block store key to context if
// relevant header is set.
func AddBlockStoreTypeToContext(ctx context.Context, storeType string) context.Context {
	ng := blockStoreType(storeType)
	switch ng {
	case tsdbBlockStore, parquetBlockStore:
		return context.WithValue(ctx, blockStorageKey, ng)
	}
	return ctx
}

func getBlockStoreType(ctx context.Context, defaultBlockStoreType blockStoreType) blockStoreType {
	if ng, ok := ctx.Value(blockStorageKey).(blockStoreType); ok {
		return ng
	}
	return defaultBlockStoreType
}

type parquetQueryableFallbackMetrics struct {
	blocksQueriedTotal *prometheus.CounterVec
	operationsTotal    *prometheus.CounterVec
}

func newParquetQueryableFallbackMetrics(reg prometheus.Registerer) *parquetQueryableFallbackMetrics {
	return &parquetQueryableFallbackMetrics{
		blocksQueriedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_queryable_blocks_queried_total",
			Help: "Total number of blocks found to query.",
		}, []string{"type"}),
		operationsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_queryable_operations_total",
			Help: "Total number of Operations.",
		}, []string{"type", "method"}),
	}
}

type parquetQueryableWithFallback struct {
	services.Service

	queryStoreAfter       time.Duration
	parquetQueryable      storage.Queryable
	blockStorageQueryable *BlocksStoreQueryable

	finder BlocksFinder

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	// metrics
	metrics *parquetQueryableFallbackMetrics

	limits *validation.Overrides
	logger log.Logger

	defaultBlockStoreType blockStoreType
}

func NewParquetQueryable(
	config Config,
	storageCfg cortex_tsdb.BlocksStorageConfig,
	limits *validation.Overrides,
	blockStorageQueryable *BlocksStoreQueryable,
	logger log.Logger,
	reg prometheus.Registerer,
) (storage.Queryable, error) {
	bucketClient, err := createCachingBucketClient(context.Background(), storageCfg, nil, "parquet-querier", logger, reg)
	if err != nil {
		return nil, err
	}

	manager, err := services.NewManager(blockStorageQueryable)
	if err != nil {
		return nil, err
	}

	cache, err := newCache[parquet_storage.ParquetShard]("parquet-shards", config.ParquetQueryableShardCacheSize, newCacheMetrics(reg))
	if err != nil {
		return nil, err
	}

	cDecoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())

	parquetQueryable, err := queryable.NewParquetQueryable(cDecoder, func(ctx context.Context, mint, maxt int64) ([]parquet_storage.ParquetShard, error) {
		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		blocks, ok := ExtractBlocksFromContext(ctx)
		if !ok {
			return nil, errors.Errorf("failed to extract blocks from context")
		}
		userBkt := bucket.NewUserBucketClient(userID, bucketClient, limits)
		bucketOpener := parquet_storage.NewParquetBucketOpener(userBkt)
		shards := make([]parquet_storage.ParquetShard, len(blocks))
		errGroup := &errgroup.Group{}

		span, ctx := opentracing.StartSpanFromContext(ctx, "parquetQuerierWithFallback.OpenShards")
		defer span.Finish()

		for i, block := range blocks {
			errGroup.Go(func() error {
				cacheKey := fmt.Sprintf("%v-%v", userID, block.ID)
				shard := cache.Get(cacheKey)
				if shard == nil {
					// we always only have 1 shard - shard 0
					// Use context.Background() here as the file can be cached and live after the request ends.
					shard, err = parquet_storage.NewParquetShardOpener(
						context.WithoutCancel(ctx),
						block.ID.String(),
						bucketOpener,
						bucketOpener,
						0,
						parquet_storage.WithFileOptions(
							parquet.SkipMagicBytes(true),
							parquet.ReadBufferSize(100*1024),
							parquet.SkipBloomFilters(true),
							parquet.OptimisticRead(true),
						),
					)
					if err != nil {
						return errors.Wrapf(err, "failed to open parquet shard. block: %v", block.ID.String())
					}
					cache.Set(cacheKey, shard)
				}

				shards[i] = shard
				return nil
			})
		}

		return shards, errGroup.Wait()
	})

	p := &parquetQueryableWithFallback{
		subservices:           manager,
		blockStorageQueryable: blockStorageQueryable,
		parquetQueryable:      parquetQueryable,
		queryStoreAfter:       config.QueryStoreAfter,
		subservicesWatcher:    services.NewFailureWatcher(),
		finder:                blockStorageQueryable.finder,
		metrics:               newParquetQueryableFallbackMetrics(reg),
		limits:                limits,
		logger:                logger,
		defaultBlockStoreType: blockStoreType(config.ParquetQueryableDefaultBlockStore),
	}

	p.Service = services.NewBasicService(p.starting, p.running, p.stopping)

	return p, err
}

func (p *parquetQueryableWithFallback) starting(ctx context.Context) error {
	p.subservicesWatcher.WatchManager(p.subservices)
	if err := services.StartManagerAndAwaitHealthy(ctx, p.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks storage queryable subservices")
	}
	return nil
}

func (p *parquetQueryableWithFallback) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-p.subservicesWatcher.Chan():
			return errors.Wrap(err, "block storage queryable subservice failed")
		}
	}
}

func (p *parquetQueryableWithFallback) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), p.subservices)
}

func (p *parquetQueryableWithFallback) Querier(mint, maxt int64) (storage.Querier, error) {
	pq, err := p.parquetQueryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}

	bsq, err := p.blockStorageQueryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}

	return &parquetQuerierWithFallback{
		minT:                  mint,
		maxT:                  maxt,
		parquetQuerier:        pq,
		queryStoreAfter:       p.queryStoreAfter,
		blocksStoreQuerier:    bsq,
		finder:                p.finder,
		metrics:               p.metrics,
		limits:                p.limits,
		logger:                p.logger,
		defaultBlockStoreType: p.defaultBlockStoreType,
	}, nil
}

type parquetQuerierWithFallback struct {
	minT, maxT int64

	finder BlocksFinder

	parquetQuerier     storage.Querier
	blocksStoreQuerier storage.Querier

	// If set, the querier manipulates the max time to not be greater than
	// "now - queryStoreAfter" so that most recent blocks are not queried.
	queryStoreAfter time.Duration

	// metrics
	metrics *parquetQueryableFallbackMetrics

	limits *validation.Overrides
	logger log.Logger

	defaultBlockStoreType blockStoreType
}

func (q *parquetQuerierWithFallback) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "parquetQuerierWithFallback.LabelValues")
	defer span.Finish()

	remaining, parquet, err := q.getBlocks(ctx, q.minT, q.maxT)
	defer q.incrementOpsMetric("LabelValues", remaining, parquet)
	if err != nil {
		return nil, nil, err
	}
	limit := 0

	if hints != nil {
		limit = hints.Limit
	}

	var (
		result       []string
		rAnnotations annotations.Annotations
	)

	if len(parquet) > 0 {
		res, ann, qErr := q.parquetQuerier.LabelValues(InjectBlocksIntoContext(ctx, parquet...), name, hints, matchers...)
		if qErr != nil {
			return nil, nil, err
		}
		result = res
		rAnnotations = ann
	}

	if len(remaining) > 0 {
		res, ann, qErr := q.blocksStoreQuerier.LabelValues(InjectBlocksIntoContext(ctx, remaining...), name, hints, matchers...)
		if qErr != nil {
			return nil, nil, err
		}

		if len(result) == 0 {
			result = res
		} else {
			result = strutil.MergeSlices(limit, result, res)
		}

		if rAnnotations != nil {
			rAnnotations = rAnnotations.Merge(ann)
		}
	}

	return result, rAnnotations, nil
}

func (q *parquetQuerierWithFallback) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "parquetQuerierWithFallback.LabelNames")
	defer span.Finish()

	remaining, parquet, err := q.getBlocks(ctx, q.minT, q.maxT)
	defer q.incrementOpsMetric("LabelNames", remaining, parquet)
	if err != nil {
		return nil, nil, err
	}

	limit := 0

	if hints != nil {
		limit = hints.Limit
	}

	var (
		result       []string
		rAnnotations annotations.Annotations
	)

	if len(parquet) > 0 {
		res, ann, qErr := q.parquetQuerier.LabelNames(InjectBlocksIntoContext(ctx, parquet...), hints, matchers...)
		if qErr != nil {
			return nil, nil, err
		}
		result = res
		rAnnotations = ann
	}

	if len(remaining) > 0 {
		res, ann, qErr := q.blocksStoreQuerier.LabelNames(InjectBlocksIntoContext(ctx, remaining...), hints, matchers...)
		if qErr != nil {
			return nil, nil, err
		}

		if len(result) == 0 {
			result = res
		} else {
			result = strutil.MergeSlices(limit, result, res)
		}

		if rAnnotations != nil {
			rAnnotations = rAnnotations.Merge(ann)
		}
	}

	return result, rAnnotations, nil
}

func (q *parquetQuerierWithFallback) Select(ctx context.Context, sortSeries bool, h *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	span, ctx := opentracing.StartSpanFromContext(ctx, "parquetQuerierWithFallback.Select")
	defer span.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		storage.ErrSeriesSet(err)
	}

	if q.limits.QueryVerticalShardSize(userID) > 1 {
		uLogger := util_log.WithUserID(userID, q.logger)
		level.Warn(uLogger).Log("msg", "parquet queryable enabled but vertical sharding > 1. Falling back to the block storage")

		return q.blocksStoreQuerier.Select(ctx, sortSeries, h, matchers...)
	}

	hints := storage.SelectHints{
		Start: q.minT,
		End:   q.maxT,
	}

	mint, maxt, limit := q.minT, q.maxT, 0
	if h != nil {
		// let copy the hints here as we wanna potentially modify it
		hints = *h
		mint, maxt, limit = hints.Start, hints.End, hints.Limit
	}

	maxt = q.adjustMaxT(maxt)
	hints.End = maxt

	if maxt < mint {
		return storage.EmptySeriesSet()
	}

	remaining, parquet, err := q.getBlocks(ctx, mint, maxt)
	defer q.incrementOpsMetric("Select", remaining, parquet)

	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Lets sort the series to merge
	if len(parquet) > 0 && len(remaining) > 0 {
		sortSeries = true
	}

	promises := make([]chan storage.SeriesSet, 0, 2)

	if len(parquet) > 0 {
		p := make(chan storage.SeriesSet, 1)
		promises = append(promises, p)
		go func() {
			span, _ := opentracing.StartSpanFromContext(ctx, "parquetQuerier.Select")
			defer span.Finish()
			p <- q.parquetQuerier.Select(InjectBlocksIntoContext(ctx, parquet...), sortSeries, &hints, matchers...)
		}()
	}

	if len(remaining) > 0 {
		p := make(chan storage.SeriesSet, 1)
		promises = append(promises, p)
		go func() {
			p <- q.blocksStoreQuerier.Select(InjectBlocksIntoContext(ctx, remaining...), sortSeries, &hints, matchers...)
		}()
	}

	if len(promises) == 1 {
		return <-promises[0]
	}

	seriesSets := make([]storage.SeriesSet, len(promises))
	for i, promise := range promises {
		seriesSets[i] = <-promise
	}

	return storage.NewMergeSeriesSet(seriesSets, limit, storage.ChainedSeriesMerge)
}

func (q *parquetQuerierWithFallback) adjustMaxT(maxt int64) int64 {
	// If queryStoreAfter is enabled, we do manipulate the query maxt to query samples up until
	// now - queryStoreAfter, because the most recent time range is covered by ingesters. This
	// optimization is particularly important for the blocks storage because can be used to skip
	// querying most recent not-compacted-yet blocks from the storage.
	if q.queryStoreAfter > 0 {
		now := time.Now()
		maxt = min(maxt, util.TimeToMillis(now.Add(-q.queryStoreAfter)))
	}
	return maxt
}

func (q *parquetQuerierWithFallback) Close() error {
	mErr := multierror.MultiError{}
	mErr.Add(q.parquetQuerier.Close())
	mErr.Add(q.blocksStoreQuerier.Close())
	return mErr.Err()
}

func (q *parquetQuerierWithFallback) getBlocks(ctx context.Context, minT, maxT int64) ([]*bucketindex.Block, []*bucketindex.Block, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	maxT = q.adjustMaxT(maxT)

	if maxT < minT {
		return nil, nil, nil
	}

	blocks, _, err := q.finder.GetBlocks(ctx, userID, minT, maxT)
	if err != nil {
		return nil, nil, err
	}

	useParquet := getBlockStoreType(ctx, q.defaultBlockStoreType) == parquetBlockStore
	parquetBlocks := make([]*bucketindex.Block, 0, len(blocks))
	remaining := make([]*bucketindex.Block, 0, len(blocks))
	for _, b := range blocks {
		if useParquet && b.Parquet != nil {
			parquetBlocks = append(parquetBlocks, b)
			continue
		}
		remaining = append(remaining, b)
	}

	q.metrics.blocksQueriedTotal.WithLabelValues("parquet").Add(float64(len(parquetBlocks)))
	q.metrics.blocksQueriedTotal.WithLabelValues("tsdb").Add(float64(len(remaining)))
	return remaining, parquetBlocks, nil
}

func (q *parquetQuerierWithFallback) incrementOpsMetric(method string, remaining []*bucketindex.Block, parquetBlocks []*bucketindex.Block) {
	switch {
	case len(remaining) > 0 && len(parquetBlocks) > 0:
		q.metrics.operationsTotal.WithLabelValues("mixed", method).Inc()
	case len(remaining) > 0 && len(parquetBlocks) == 0:
		q.metrics.operationsTotal.WithLabelValues("tsdb", method).Inc()
	case len(remaining) == 0 && len(parquetBlocks) > 0:
		q.metrics.operationsTotal.WithLabelValues("parquet", method).Inc()
	}
}

type cacheInterface[T any] interface {
	Get(path string) T
	Set(path string, reader T)
}

type cacheMetrics struct {
	hits      *prometheus.CounterVec
	misses    *prometheus.CounterVec
	evictions *prometheus.CounterVec
	size      *prometheus.GaugeVec
}

func newCacheMetrics(reg prometheus.Registerer) *cacheMetrics {
	return &cacheMetrics{
		hits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_queryable_cache_hits_total",
			Help: "Total number of parquet cache hits",
		}, []string{"name"}),
		misses: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_queryable_cache_misses_total",
			Help: "Total number of parquet cache misses",
		}, []string{"name"}),
		evictions: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_queryable_cache_evictions_total",
			Help: "Total number of parquet cache evictions",
		}, []string{"name"}),
		size: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_parquet_queryable_cache_item_count",
			Help: "Current number of cached parquet items",
		}, []string{"name"}),
	}
}

type Cache[T any] struct {
	cache   *lru.Cache[string, T]
	name    string
	metrics *cacheMetrics
}

func newCache[T any](name string, size int, metrics *cacheMetrics) (cacheInterface[T], error) {
	if size <= 0 {
		return &noopCache[T]{}, nil
	}
	cache, err := lru.NewWithEvict(size, func(key string, value T) {
		metrics.evictions.WithLabelValues(name).Inc()
		metrics.size.WithLabelValues(name).Dec()
	})
	if err != nil {
		return nil, err
	}

	return &Cache[T]{
		cache:   cache,
		name:    name,
		metrics: metrics,
	}, nil
}

func (c *Cache[T]) Get(path string) (r T) {
	if reader, ok := c.cache.Get(path); ok {
		c.metrics.hits.WithLabelValues(c.name).Inc()
		return reader
	}
	c.metrics.misses.WithLabelValues(c.name).Inc()
	return
}

func (c *Cache[T]) Set(path string, reader T) {
	if !c.cache.Contains(path) {
		c.metrics.size.WithLabelValues(c.name).Inc()
	}
	c.metrics.misses.WithLabelValues(c.name).Inc()
	c.cache.Add(path, reader)
}

type noopCache[T any] struct {
}

func (n noopCache[T]) Get(_ string) (r T) {
	return
}

func (n noopCache[T]) Set(_ string, _ T) {

}
