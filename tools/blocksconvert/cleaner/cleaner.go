package cleaner

import (
	"context"
	"flag"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/weaveworks/common/user"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
	"github.com/cortexproject/cortex/tools/blocksconvert/plan_processor"
)

type Config struct {
	PlansDirectory string
	Concurrency    int

	PlanProcessorConfig plan_processor.Config
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.PlanProcessorConfig.RegisterFlags("cleaner", f)

	f.StringVar(&cfg.PlansDirectory, "cleaner.plans-dir", "", "Local directory used for storing temporary plan files.")
	f.IntVar(&cfg.Concurrency, "cleaner.concurrency", 128, "Number of concurrent series cleaners.")
}

func NewCleaner(cfg Config, scfg blocksconvert.SharedConfig, l log.Logger, reg prometheus.Registerer) (services.Service, error) {
	err := scfg.SchemaConfig.Load()
	if err != nil {
		return nil, errors.Wrap(err, "failed to load schema")
	}

	bucketClient, err := scfg.GetBucket(l, reg)
	if err != nil {
		return nil, err
	}

	// No registerer, to avoid problems with registering same metrics multiple times.
	store, err := storage.NewStore(scfg.StorageConfig, chunk.StoreConfig{
		ChunkCacheConfig:       cache.Config{Cache: noCache{}},
		WriteDedupeCacheConfig: cache.Config{Cache: noCache{}},
	}, scfg.SchemaConfig, cleanerStoreLimits{}, reg, nil, l)
	if err != nil {
		return nil, err
	}

	c := &Cleaner{
		cfg: cfg,

		bucketClient:  bucketClient,
		schemaConfig:  scfg.SchemaConfig,
		storageConfig: scfg.StorageConfig,
		store:         store,

		deletedSeries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_cleaner_deleted_series_total",
			Help: "Deleted series",
		}),
		deletedSeriesErrors: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_cleaner_delete_series_errors_total",
			Help: "Number of errors while deleting series.",
		}),
		deletedChunks: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_cleaner_deleted_chunks_total",
			Help: "Deleted chunks",
		}),
		deletedChunksErrors: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_cleaner_delete_chunks_errors_total",
			Help: "Number of errors while deleting individual chunks.",
		}),
	}

	cfg.PlanProcessorConfig.PlansDirectory = cfg.PlansDirectory
	cfg.PlanProcessorConfig.Bucket = bucketClient
	cfg.PlanProcessorConfig.Factory = c.planProcessorFactory

	return plan_processor.NewPlanProcessorService(cfg.PlanProcessorConfig, l, reg)
}

type Cleaner struct {
	cfg Config

	bucketClient  objstore.Bucket
	schemaConfig  chunk.SchemaConfig
	storageConfig storage.Config
	store         chunk.Store

	deletedChunks       prometheus.Counter
	deletedChunksErrors prometheus.Counter

	deletedSeries       prometheus.Counter
	deletedSeriesErrors prometheus.Counter
}

func (c *Cleaner) planProcessorFactory(planLog log.Logger, userID string, start time.Time, end time.Time) plan_processor.PlanProcessor {
	return &cleanerProcessor{
		cleaner:  c,
		log:      planLog,
		userID:   userID,
		dayStart: start,
		dayEnd:   end,
	}
}

type cleanerProcessor struct {
	cleaner *Cleaner

	log      log.Logger
	userID   string
	dayStart time.Time
	dayEnd   time.Time
}

func (cp *cleanerProcessor) ProcessPlanEntries(ctx context.Context, planEntryCh chan blocksconvert.PlanEntry) (string, error) {
	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < cp.cleaner.cfg.Concurrency; i++ {
		g.Go(func() error {
			return cp.fetchAndClean(gctx, planEntryCh)
		})
	}

	if err := g.Wait(); err != nil {
		return "", errors.Wrap(err, "failed to cleanup series")
	}

	// "cleaned" will be appended as "block ID" to finished status file.
	return "cleaned", nil
}

func (cp *cleanerProcessor) fetchAndClean(ctx context.Context, input chan blocksconvert.PlanEntry) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case e, ok := <-input:
			if !ok {
				// End of input.
				return nil
			}

			err := cp.deleteChunksForSeries(ctx, e)
			if err != nil {
				return err
			}
		}
	}
}

func (cp *cleanerProcessor) deleteChunksForSeries(ctx context.Context, e blocksconvert.PlanEntry) error {
	// Interaction with chunks.Store needs this.
	ctx = user.InjectOrgID(ctx, cp.userID)

	fetcher := cp.cleaner.store.GetChunkFetcher(model.TimeFromUnixNano(cp.dayStart.UnixNano()))

	var c *chunk.Chunk
	var err error

	b := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 5 * time.Second,
		MaxRetries: 5,
	})
	for ; b.Ongoing(); b.Wait() {
		c, err = fetchSingleChunk(ctx, cp.userID, fetcher, e.Chunks)
		if err == nil {
			break
		}

		level.Warn(cp.log).Log("msg", "failed to fetch chunk for series", "series", e.SeriesID, "err", err, "retries", b.NumRetries()+1)
	}

	if err == nil {
		err = b.Err()
	}
	if err != nil {
		return errors.Wrapf(err, "error while fetching chunk for series %s", e.SeriesID)
	}

	if c == nil {
		cp.cleaner.deletedSeriesErrors.Inc()
		level.Warn(cp.log).Log("msg", "no chunk found for series, unable to delete series", "series", e.SeriesID)
		return nil
	}

	start := model.TimeFromUnixNano(cp.dayStart.UnixNano())
	end := model.TimeFromUnixNano(cp.dayEnd.UnixNano())

	// All chunks belonging to the series use the same metric.
	metric := c.Metric

	for _, cid := range e.Chunks {
		cleaned := false

		for b.Reset(); b.Ongoing(); b.Wait() {
			err = cp.cleaner.store.DeleteChunk(ctx, start, end, cp.userID, cid, metric, nil)
			if err == nil {
				cleaned = true
				break
			}

			level.Warn(cp.log).Log("msg", "failed to delete chunk", "series", e.SeriesID, "chunk", cid, "err", err)
		}

		if cleaned {
			cp.cleaner.deletedChunks.Inc()
		} else {
			cp.cleaner.deletedChunksErrors.Inc()
		}
	}

	if err := cp.cleaner.store.DeleteSeriesIDs(ctx, start, end, cp.userID, metric); err != nil {
		cp.cleaner.deletedSeriesErrors.Inc()
		level.Warn(cp.log).Log("msg", "failed to delete series", "series", e.SeriesID, "err", err)
	} else {
		cp.cleaner.deletedSeries.Inc()
	}
	return nil
}

func fetchSingleChunk(ctx context.Context, userID string, fetcher *chunk.Fetcher, chunksIds []string) (*chunk.Chunk, error) {
	// Fetch single chunk
	for _, cid := range chunksIds {
		c, err := chunk.ParseExternalKey(userID, cid)
		if err != nil {
			return nil, errors.Wrap(err, "fetching chunks")
		}

		cs, err := fetcher.FetchChunks(ctx, []chunk.Chunk{c}, []string{cid})

		if errors.Is(err, chunk.ErrStorageObjectNotFound) {
			continue
		}
		if err != nil {
			return nil, errors.Wrap(err, "fetching chunks")
		}

		if len(cs) > 0 {
			return &cs[0], nil
		}
	}

	return nil, nil
}

type cleanerStoreLimits struct{}

func (c cleanerStoreLimits) CardinalityLimit(_ string) int         { return 0 }
func (c cleanerStoreLimits) MaxChunksPerQuery(_ string) int        { return 0 }
func (c cleanerStoreLimits) MaxQueryLength(_ string) time.Duration { return 0 }

type noCache struct{}

func (n noCache) Store(ctx context.Context, key []string, buf [][]byte) {}
func (n noCache) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string) {
	return nil, nil, keys
}

func (n noCache) Stop() {}
