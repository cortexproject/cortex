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
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
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

	c := &Cleaner{
		cfg: cfg,

		bucketClient:  bucketClient,
		schemaConfig:  scfg.SchemaConfig,
		storageConfig: scfg.StorageConfig,

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
		deletedChunksMissing: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_cleaner_delete_chunks_missing_total",
			Help: "Chunks that were missing when trying to delete them.",
		}),
		deletedChunksSkipped: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_cleaner_delete_chunks_skipped_total",
			Help: "Number of skipped chunks during deletion.",
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

	deletedChunks        prometheus.Counter
	deletedChunksSkipped prometheus.Counter
	deletedChunksMissing prometheus.Counter
	deletedChunksErrors  prometheus.Counter

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
	schema, chunkClient, indexClient, err := cp.cleaner.createClientsForDay(cp.dayStart)
	if err != nil {
		return "", errors.Wrap(err, "failed to create clients")
	}

	defer chunkClient.Stop()
	defer indexClient.Stop()

	seriesSchema, ok := schema.(chunk.SeriesStoreSchema)
	if !ok || seriesSchema == nil {
		return "", errors.Errorf("invalid schema, expected v9 or later")
	}

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < cp.cleaner.cfg.Concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil

				case e, ok := <-planEntryCh:
					if !ok {
						// End of input.
						return nil
					}

					err := cp.deleteChunksForSeries(gctx, seriesSchema, chunkClient, indexClient, e)
					if err != nil {
						return err
					}
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return "", errors.Wrap(err, "failed to cleanup series")
	}

	// "cleaned" will be appended as "block ID" to finished status file.
	return "cleaned", nil
}

func (cp *cleanerProcessor) deleteChunksForSeries(ctx context.Context, schema chunk.SeriesStoreSchema, chunkClient chunk.Client, indexClient chunk.IndexClient, e blocksconvert.PlanEntry) error {
	var c *chunk.Chunk
	var err error

	b := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 5 * time.Second,
		MaxRetries: 5,
	})
	for ; b.Ongoing(); b.Wait() {
		c, err = fetchSingleChunk(ctx, cp.userID, chunkClient, e.Chunks)
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

	// All chunks belonging to the series use the same metric.
	metric := c.Metric

	metricName := metric.Get(model.MetricNameLabel)
	if metricName == "" {
		return errors.Errorf("cannot find metric name for series %s", metric.String())
	}

	start := model.TimeFromUnixNano(cp.dayStart.UnixNano())
	// End is inclusive in GetChunkWriteEntries, but we don't want to delete chunks from next day.
	end := model.TimeFromUnixNano(cp.dayEnd.UnixNano()) - 1

	var chunksToDelete []string

	// With metric, we find out which index entries to remove.
	batch := indexClient.NewWriteBatch()
	for _, cid := range e.Chunks {
		c, err := chunk.ParseExternalKey(cp.userID, cid)
		if err != nil {
			return errors.Wrap(err, "failed to parse chunk key")
		}

		ents, err := schema.GetChunkWriteEntries(start, end, cp.userID, metricName, metric, cid)
		if err != nil {
			return errors.Wrapf(err, "getting index entries to delete for chunkID=%s", cid)
		}

		for i := range ents {
			batch.Delete(ents[i].TableName, ents[i].HashValue, ents[i].RangeValue)
		}

		// Only delete this chunk if it *starts* in specified date-period.
		if c.From >= start {
			chunksToDelete = append(chunksToDelete, cid)
		} else {
			cp.cleaner.deletedChunksSkipped.Inc()
			continue
		}
	}

	// Delete index entries first. If we delete chunks first, and then cleaner is interrupted,
	// chunks won't be find upon restart, and it won't be possible to clean up index entries.
	if err := indexClient.BatchWrite(ctx, batch); err != nil {
		level.Warn(cp.log).Log("msg", "failed to delete index entries for series", "series", e.SeriesID, "err", err)
		cp.cleaner.deletedSeriesErrors.Inc()
	} else {
		cp.cleaner.deletedSeries.Inc()
	}

	for _, cid := range chunksToDelete {
		if err := chunkClient.DeleteChunk(ctx, cp.userID, cid); err != nil {
			if errors.Is(err, chunk.ErrStorageObjectNotFound) {
				cp.cleaner.deletedChunksMissing.Inc()
			} else {
				level.Warn(cp.log).Log("msg", "failed to delete chunk for series", "series", e.SeriesID, "chunk", cid, "err", err)
				cp.cleaner.deletedChunksErrors.Inc()
			}
		} else {
			cp.cleaner.deletedChunks.Inc()
		}
	}

	return nil
}

func fetchSingleChunk(ctx context.Context, userID string, chunkClient chunk.Client, chunksIds []string) (*chunk.Chunk, error) {
	// Fetch single chunk
	for _, cid := range chunksIds {
		c, err := chunk.ParseExternalKey(userID, cid)
		if err != nil {
			return nil, errors.Wrap(err, "fetching chunks")
		}

		cs, err := chunkClient.GetChunks(ctx, []chunk.Chunk{c})

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

func (c *Cleaner) createClientsForDay(dayStart time.Time) (chunk.BaseSchema, chunk.Client, chunk.IndexClient, error) {
	for ix, s := range c.schemaConfig.Configs {
		if dayStart.Unix() < s.From.Unix() {
			continue
		}

		if ix+1 < len(c.schemaConfig.Configs) && dayStart.Unix() > c.schemaConfig.Configs[ix+1].From.Unix() {
			continue
		}

		schema, err := s.CreateSchema()
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to create schema")
		}

		// No registerer, to avoid problems with registering same metrics multiple times.
		index, err := storage.NewIndexClient(s.IndexType, c.storageConfig, c.schemaConfig, nil)
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "error creating index client")
		}

		objectStoreType := s.ObjectType
		if objectStoreType == "" {
			objectStoreType = s.IndexType
		}

		// No registerer, to avoid problems with registering same metrics multiple times.
		chunks, err := storage.NewChunkClient(objectStoreType, c.storageConfig, c.schemaConfig, nil)
		if err != nil {
			index.Stop()
			return nil, nil, nil, errors.Wrap(err, "error creating object client")
		}

		return schema, chunks, index, nil
	}

	return nil, nil, nil, errors.Errorf("no schema for day %v", dayStart.Format("2006-01-02"))
}
