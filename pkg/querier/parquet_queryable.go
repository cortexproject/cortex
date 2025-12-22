package querier

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/queryable"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querysharding"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/multierror"
	"github.com/cortexproject/cortex/pkg/util/parquetutil"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/users"
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

var (
	validBlockStoreTypes = []blockStoreType{tsdbBlockStore, parquetBlockStore}
)

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

	fallbackDisabled      bool
	queryStoreAfter       time.Duration
	parquetQueryable      storage.Queryable
	cache                 parquetutil.CacheInterface[parquet_storage.ParquetShard]
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

	honorProjectionHints bool
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

	cache, err := parquetutil.NewParquetShardCache[parquet_storage.ParquetShard](&config.ParquetShardCache, "parquet-shards", extprom.WrapRegistererWith(prometheus.Labels{"component": "querier"}, reg))
	if err != nil {
		return nil, err
	}

	cDecoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	// This is a noop cache for now as we didn't expose any config to enable this cache.
	rrConstraintsCache := search.NewConstraintRowRangeCacheSyncMap()
	constraintCacheFunc := func(ctx context.Context) (search.RowRangesForConstraintsCache, error) {
		return rrConstraintsCache, nil
	}

	parquetQueryableOpts := []queryable.QueryableOpts{
		queryable.WithHonorProjectionHints(config.HonorProjectionHints),
		queryable.WithRowCountLimitFunc(func(ctx context.Context) int64 {
			// Ignore error as this shouldn't happen.
			// If failed to resolve tenant we will just use the default limit value.
			userID, _ := users.TenantID(ctx)
			return int64(limits.ParquetMaxFetchedRowCount(userID))
		}),
		queryable.WithChunkBytesLimitFunc(func(ctx context.Context) int64 {
			// Ignore error as this shouldn't happen.
			// If failed to resolve tenant we will just use the default limit value.
			userID, _ := users.TenantID(ctx)
			return int64(limits.ParquetMaxFetchedChunkBytes(userID))
		}),
		queryable.WithDataBytesLimitFunc(func(ctx context.Context) int64 {
			// Ignore error as this shouldn't happen.
			// If failed to resolve tenant we will just use the default limit value.
			userID, _ := users.TenantID(ctx)
			return int64(limits.ParquetMaxFetchedDataBytes(userID))
		}),
		queryable.WithMaterializedLabelsFilterCallback(materializedLabelsFilterCallback),
		queryable.WithMaterializedSeriesCallback(func(ctx context.Context, series storage.ChunkSeries) error {
			queryLimiter := limiter.QueryLimiterFromContextWithFallback(ctx)
			chkCount := 0
			chunkSize := 0
			lblSize := 0
			lblAdapter := cortexpb.FromLabelsToLabelAdapters(series.Labels())
			for _, lbl := range lblAdapter {
				lblSize += lbl.Size()
			}
			iter := series.Iterator(nil)
			for iter.Next() {
				chk := iter.At()
				chunkSize += len(chk.Chunk.Bytes())
				chkCount++
			}
			if chkCount > 0 {
				if err := queryLimiter.AddChunks(chkCount); err != nil {
					return validation.LimitError(err.Error())
				}
				if err := queryLimiter.AddChunkBytes(chunkSize); err != nil {
					return validation.LimitError(err.Error())
				}
			}

			if err := queryLimiter.AddDataBytes(chunkSize + lblSize); err != nil {
				return validation.LimitError(err.Error())
			}
			if err := queryLimiter.AddSeries(lblAdapter); err != nil {
				return validation.LimitError(err.Error())
			}
			return nil
		}),
	}
	parquetQueryable, err := queryable.NewParquetQueryable(func(ctx context.Context, mint, maxt int64) ([]parquet_storage.ParquetShard, error) {
		userID, err := users.TenantID(ctx)
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
	}, constraintCacheFunc, cDecoder, parquetQueryableOpts...)

	p := &parquetQueryableWithFallback{
		subservices:           manager,
		blockStorageQueryable: blockStorageQueryable,
		parquetQueryable:      parquetQueryable,
		cache:                 cache,
		queryStoreAfter:       config.QueryStoreAfter,
		subservicesWatcher:    services.NewFailureWatcher(),
		finder:                blockStorageQueryable.finder,
		metrics:               newParquetQueryableFallbackMetrics(reg),
		limits:                limits,
		logger:                logger,
		defaultBlockStoreType: blockStoreType(config.ParquetQueryableDefaultBlockStore),
		fallbackDisabled:      config.ParquetQueryableFallbackDisabled,
		honorProjectionHints:  config.HonorProjectionHints,
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
	if p.cache != nil {
		p.cache.Close()
	}

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
		fallbackDisabled:      p.fallbackDisabled,
		honorProjectionHints:  p.honorProjectionHints,
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

	fallbackDisabled bool

	honorProjectionHints bool
}

func (q *parquetQuerierWithFallback) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "parquetQuerierWithFallback.LabelValues")
	defer span.Finish()

	remaining, parquet, err := q.getBlocks(ctx, q.minT, q.maxT, matchers)
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

	if len(remaining) > 0 && q.fallbackDisabled {
		return nil, nil, parquetConsistencyCheckError(remaining)
	}

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

	remaining, parquet, err := q.getBlocks(ctx, q.minT, q.maxT, matchers)
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

	if len(remaining) > 0 && q.fallbackDisabled {
		return nil, nil, parquetConsistencyCheckError(remaining)
	}

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

	newMatchers, shardInfo, err := querysharding.ExtractShardingInfo(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
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

	remaining, parquet, err := q.getBlocks(ctx, mint, maxt, matchers)
	defer q.incrementOpsMetric("Select", remaining, parquet)

	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	if len(remaining) > 0 && q.fallbackDisabled {
		err = parquetConsistencyCheckError(remaining)
		return storage.ErrSeriesSet(err)
	}

	// Lets sort the series to merge
	if len(parquet) > 0 && len(remaining) > 0 {
		sortSeries = true
	}

	// Reset projection hints if:
	// - there are mixed blocks (both parquet and non-parquet)
	// - not all parquet blocks have hash column (version < 2)
	if q.honorProjectionHints {
		if len(remaining) > 0 || !allParquetBlocksHaveHashColumn(parquet) {
			hints.ProjectionLabels = nil
			hints.ProjectionInclude = false
		}
		if hints.ProjectionInclude && !slices.Contains(hints.ProjectionLabels, schema.SeriesHashColumn) {
			// Series hash column is always required for projection unless duplicate label check is disabled.
			hints.ProjectionLabels = append(hints.ProjectionLabels, schema.SeriesHashColumn)

		}
	}

	promises := make([]chan storage.SeriesSet, 0, 2)

	if len(parquet) > 0 {
		p := make(chan storage.SeriesSet, 1)
		promises = append(promises, p)
		go func() {
			span, _ := opentracing.StartSpanFromContext(ctx, "parquetQuerier.Select")
			defer span.Finish()
			parquetCtx := InjectBlocksIntoContext(ctx, parquet...)
			if shardInfo != nil {
				parquetCtx = injectShardInfoIntoContext(parquetCtx, shardInfo)
			}
			p <- q.parquetQuerier.Select(parquetCtx, sortSeries, &hints, newMatchers...)
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

func (q *parquetQuerierWithFallback) getBlocks(ctx context.Context, minT, maxT int64, matchers []*labels.Matcher) ([]*bucketindex.Block, []*bucketindex.Block, error) {
	userID, err := users.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	maxT = q.adjustMaxT(maxT)

	if maxT < minT {
		return nil, nil, nil
	}

	blocks, _, err := q.finder.GetBlocks(ctx, userID, minT, maxT, matchers)
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

// allParquetBlocksHaveHashColumn checks if all parquet blocks have version >= 2, which means they have the hash column.
// Parquet blocks with version 1 don't have the hash column, so projection cannot be enabled for them.
func allParquetBlocksHaveHashColumn(blocks []*bucketindex.Block) bool {
	for _, b := range blocks {
		if b.Parquet == nil || b.Parquet.Version < cortex_parquet.ParquetConverterMarkVersion2 {
			return false
		}
	}
	return true
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

func materializedLabelsFilterCallback(ctx context.Context, _ *storage.SelectHints) (search.MaterializedLabelsFilter, bool) {
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

func parquetConsistencyCheckError(blocks []*bucketindex.Block) error {
	return fmt.Errorf("consistency check failed because some blocks were not available as parquet files: %s", strings.Join(convertBlockULIDToString(blocks), " "))
}

func convertBlockULIDToString(blocks []*bucketindex.Block) []string {
	res := make([]string, len(blocks))
	for idx, b := range blocks {
		res[idx] = b.ID.String()
	}
	return res
}
