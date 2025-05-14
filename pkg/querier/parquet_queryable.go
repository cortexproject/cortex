package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/search"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/strutil"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/multierror"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type contextKey int

var (
	blockIdsCtxKey contextKey = 0
)

type parquetQueryableWithFallback struct {
	services.Service

	queryStoreAfter       time.Duration
	parquetQueryable      storage.Queryable
	blockStorageQueryable BlocksStoreQueryable

	finder BlocksFinder

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func newParquetQueryable(
	storageCfg cortex_tsdb.BlocksStorageConfig,
	limits BlocksStoreLimits,
	config Config,
	blockStorageQueryable BlocksStoreQueryable,
	logger log.Logger,
	reg prometheus.Registerer,
) (storage.Queryable, error) {
	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, nil, "parquet-querier", logger, reg)

	if err != nil {
		return nil, err
	}

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

	manager, err := services.NewManager(finder)
	if err != nil {
		return nil, err
	}

	pq, err := search.NewParquetQueryable(nil, func(ctx context.Context, mint, maxt int64) ([]*parquet_storage.ParquetShard, error) {
		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		blocks := ctx.Value(blockIdsCtxKey).([]*bucketindex.Block)
		userBkt := bucket.NewUserBucketClient(userID, bucketClient, limits)

		shards := make([]*parquet_storage.ParquetShard, 0, len(blocks))

		for _, block := range blocks {
			blockName := fmt.Sprintf("%v/block", block.ID.String())
			shard, err := parquet_storage.OpenParquetShard(ctx, userBkt, blockName, 0)
			if err != nil {
				return nil, err
			}
			shards = append(shards, shard)
		}

		return shards, nil
	})

	q := &parquetQueryableWithFallback{
		subservices:           manager,
		blockStorageQueryable: blockStorageQueryable,
		parquetQueryable:      pq,
		queryStoreAfter:       config.QueryStoreAfter,
		subservicesWatcher:    services.NewFailureWatcher(),
		finder:                finder,
	}

	q.Service = services.NewBasicService(q.starting, q.running, q.stopping)

	return pq, nil
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

	return &parquetQuerier{
		minT:               mint,
		maxT:               maxt,
		parquetQuerier:     pq,
		queryStoreAfter:    p.queryStoreAfter,
		blocksStoreQuerier: bsq,
		finder:             p.finder,
	}, nil
}

type parquetQuerier struct {
	minT, maxT int64

	finder BlocksFinder

	parquetQuerier     storage.Querier
	blocksStoreQuerier storage.Querier

	// If set, the querier manipulates the max time to not be greater than
	// "now - queryStoreAfter" so that most recent blocks are not queried.
	queryStoreAfter time.Duration
}

func (q *parquetQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
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
		res, ann, qErr := q.parquetQuerier.LabelValues(context.WithValue(ctx, blockIdsCtxKey, parquet), name, hints, matchers...)
		if qErr != nil {
			return nil, nil, err
		}
		result = res
		rAnnotations = ann
	}

	if len(remaining) > 0 {
		res, ann, qErr := q.blocksStoreQuerier.LabelValues(context.WithValue(ctx, blockIdsCtxKey, remaining), name, hints, matchers...)
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

func (q *parquetQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
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
		res, ann, qErr := q.parquetQuerier.LabelNames(context.WithValue(ctx, blockIdsCtxKey, parquet), hints, matchers...)
		if qErr != nil {
			return nil, nil, err
		}
		result = res
		rAnnotations = ann
	}

	if len(remaining) > 0 {
		res, ann, qErr := q.blocksStoreQuerier.LabelNames(context.WithValue(ctx, blockIdsCtxKey, remaining), hints, matchers...)
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

func (q *parquetQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	mint, maxt, limit := q.minT, q.maxT, 0

	if hints != nil {
		mint, maxt, limit = hints.Start, hints.End, hints.Limit
	}

	remaining, parquet, err := q.getBlocks(ctx, mint, maxt)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	serieSets := []storage.SeriesSet{}

	if len(parquet) > 0 {
		serieSets = append(serieSets, q.parquetQuerier.Select(context.WithValue(ctx, blockIdsCtxKey, parquet), sortSeries, hints, matchers...))
	}

	if len(remaining) > 0 {
		serieSets = append(serieSets, q.blocksStoreQuerier.Select(context.WithValue(ctx, blockIdsCtxKey, remaining), sortSeries, hints, matchers...))
	}

	if len(serieSets) == 1 {
		return serieSets[0]
	}

	return storage.NewMergeSeriesSet(serieSets, limit, storage.ChainedSeriesMerge)
}

func (q *parquetQuerier) Close() error {
	mErr := multierror.MultiError{}
	mErr.Add(q.parquetQuerier.Close())
	mErr.Add(q.blocksStoreQuerier.Close())
	return mErr.Err()
}

func (q *parquetQuerier) getBlocks(ctx context.Context, minT, maxT int64) ([]*bucketindex.Block, []*bucketindex.Block, error) {
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

	return remaining, parquetBlocks, nil
}
