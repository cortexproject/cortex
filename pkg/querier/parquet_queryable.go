package querier

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
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

type parquetQueryableFallbackMetrics struct {
	blocksQueriedTotal *prometheus.CounterVec
	selectCount        *prometheus.CounterVec
}

func newParquetQueryableFallbackMetrics(reg prometheus.Registerer) *parquetQueryableFallbackMetrics {
	return &parquetQueryableFallbackMetrics{
		blocksQueriedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_queryable_blocks_queried_total",
			Help: "Total number of blocks found to query.",
		}, []string{"type"}),
		selectCount: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_queryable_selects_queried_total",
			Help: "Total number of selects.",
		}, []string{"type"}),
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
}

func NewParquetQueryable(
	config Config,
	storageCfg cortex_tsdb.BlocksStorageConfig,
	limits *validation.Overrides,
	blockStorageQueryable *BlocksStoreQueryable,
	logger log.Logger,
	reg prometheus.Registerer,
) (storage.Queryable, error) {
	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, nil, "parquet-querier", logger, reg)

	if err != nil {
		return nil, err
	}
	manager, err := services.NewManager(blockStorageQueryable)
	if err != nil {
		return nil, err
	}

	cDecoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())

	parquetQueryable, err := search.NewParquetQueryable(cDecoder, func(ctx context.Context, mint, maxt int64) ([]*parquet_storage.ParquetShard, error) {
		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		blocks, ok := ExtractBlocksFromContext(ctx)
		if !ok {
			return nil, errors.Errorf("failed to extract blocks from context")
		}
		userBkt := bucket.NewUserBucketClient(userID, bucketClient, limits)

		shards := make([]*parquet_storage.ParquetShard, len(blocks))
		errGroup := &errgroup.Group{}

		for i, block := range blocks {
			errGroup.Go(func() error {
				// we always only have 1 shard - shard 0
				shard, err := parquet_storage.OpenParquetShard(ctx,
					userBkt,
					block.ID.String(),
					0,
					parquet_storage.WithFileOptions(
						parquet.SkipMagicBytes(true),
						parquet.ReadBufferSize(100*1024),
						parquet.SkipBloomFilters(true),
					),
					parquet_storage.WithOptimisticReader(true),
				)
				shards[i] = shard
				return err
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
		minT:               mint,
		maxT:               maxt,
		parquetQuerier:     pq,
		queryStoreAfter:    p.queryStoreAfter,
		blocksStoreQuerier: bsq,
		finder:             p.finder,
		metrics:            p.metrics,
		limits:             p.limits,
		logger:             p.logger,
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
}

func (q *parquetQuerierWithFallback) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	remaining, parquet, err := q.getBlocks(ctx, q.minT, q.maxT)
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
	remaining, parquet, err := q.getBlocks(ctx, q.minT, q.maxT)
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

func (q *parquetQuerierWithFallback) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		storage.ErrSeriesSet(err)
	}

	if q.limits.QueryVerticalShardSize(userID) > 1 {
		uLogger := util_log.WithUserID(userID, q.logger)
		level.Warn(uLogger).Log("msg", "parquet queryable enabled but vertical sharding > 1. Falling back to the block storage")

		return q.blocksStoreQuerier.Select(ctx, sortSeries, hints, matchers...)
	}

	mint, maxt, limit := q.minT, q.maxT, 0

	if hints != nil {
		mint, maxt, limit = hints.Start, hints.End, hints.Limit
	}

	remaining, parquet, err := q.getBlocks(ctx, mint, maxt)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	serieSets := []storage.SeriesSet{}

	// Lets sort the series to merge
	if len(parquet) > 0 && len(remaining) > 0 {
		sortSeries = true
	}

	if len(parquet) > 0 {
		serieSets = append(serieSets, q.parquetQuerier.Select(InjectBlocksIntoContext(ctx, parquet...), sortSeries, hints, matchers...))
	}

	if len(remaining) > 0 {
		serieSets = append(serieSets, q.blocksStoreQuerier.Select(InjectBlocksIntoContext(ctx, remaining...), sortSeries, hints, matchers...))
	}

	if len(serieSets) == 1 {
		return serieSets[0]
	}

	return storage.NewMergeSeriesSet(serieSets, limit, storage.ChainedSeriesMerge)
}

func (q *parquetQuerierWithFallback) Close() error {
	mErr := multierror.MultiError{}
	mErr.Add(q.parquetQuerier.Close())
	mErr.Add(q.blocksStoreQuerier.Close())
	return mErr.Err()
}

func (q *parquetQuerierWithFallback) getBlocks(ctx context.Context, minT, maxT int64) ([]*bucketindex.Block, []*bucketindex.Block, error) {
	// If queryStoreAfter is enabled, we do manipulate the query maxt to query samples up until
	// now - queryStoreAfter, because the most recent time range is covered by ingesters. This
	// optimization is particularly important for the blocks storage because can be used to skip
	// querying most recent not-compacted-yet blocks from the storage.
	if q.queryStoreAfter > 0 {
		now := time.Now()
		maxT = min(maxT, util.TimeToMillis(now.Add(-q.queryStoreAfter)))

		if maxT < minT {
			return nil, nil, nil
		}
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	blocks, _, err := q.finder.GetBlocks(ctx, userID, minT, maxT)
	if err != nil {
		return nil, nil, err
	}

	parquetBlocks := make([]*bucketindex.Block, 0, len(blocks))
	remaining := make([]*bucketindex.Block, 0, len(blocks))
	for _, b := range blocks {
		if b.Parquet != nil {
			parquetBlocks = append(parquetBlocks, b)
			continue
		}
		remaining = append(remaining, b)
	}

	q.metrics.blocksQueriedTotal.WithLabelValues("parquet").Add(float64(len(parquetBlocks)))
	q.metrics.blocksQueriedTotal.WithLabelValues("tsdb").Add(float64(len(remaining)))

	switch {
	case len(remaining) > 0 && len(parquetBlocks) > 0:
		q.metrics.selectCount.WithLabelValues("mixed").Inc()
	case len(remaining) > 0 && len(parquetBlocks) == 0:
		q.metrics.selectCount.WithLabelValues("tsdb").Inc()
	case len(remaining) == 0 && len(parquetBlocks) > 0:
		q.metrics.selectCount.WithLabelValues("parquet").Inc()
	}

	return remaining, parquetBlocks, nil
}
