package cleaner

import (
	"context"
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/tools/blocksconvert"
	"github.com/cortexproject/cortex/tools/blocksconvert/planprocessor"
)

type Config struct {
	PlansDirectory string
	Concurrency    int

	PlanProcessorConfig planprocessor.Config
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
		deletedIndexEntries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_cleaner_deleted_index_entries_total",
			Help: "Deleted index entries",
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

	return planprocessor.NewService(cfg.PlanProcessorConfig, cfg.PlansDirectory, bucketClient, nil, c.planProcessorFactory, l, reg)
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

	deletedIndexEntries prometheus.Counter
	deletedSeries       prometheus.Counter
	deletedSeriesErrors prometheus.Counter
}

func (c *Cleaner) planProcessorFactory(planLog log.Logger, userID string, start time.Time, end time.Time) planprocessor.PlanProcessor {
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
	tableName, schema, chunkClient, indexClient, err := cp.cleaner.createClientsForDay(cp.dayStart)
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

					err := cp.deleteChunksForSeries(gctx, tableName, seriesSchema, chunkClient, indexClient, e)
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

func (cp *cleanerProcessor) deleteChunksForSeries(ctx context.Context, tableName string, schema chunk.SeriesStoreSchema, chunkClient chunk.Client, indexClient chunk.IndexClient, e blocksconvert.PlanEntry) error {
	var c *chunk.Chunk
	var err error

	b := backoff.New(ctx, backoff.Config{
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
		// This can happen also when cleaner is restarted. Chunks deleted previously cannot be found anymore,
		// but index entries should not exist.
		// level.Warn(cp.log).Log("msg", "no chunk found for series, unable to delete series", "series", e.SeriesID)
		return nil
	}

	// All chunks belonging to the series use the same metric.
	metric := c.Metric

	metricName := metric.Get(model.MetricNameLabel)
	if metricName == "" {
		return errors.Errorf("cannot find metric name for series %s", metric.String())
	}

	start := model.TimeFromUnixNano(cp.dayStart.UnixNano())
	end := model.TimeFromUnixNano(cp.dayEnd.UnixNano())

	var chunksToDelete []string

	indexEntries := 0

	// With metric, we find out which index entries to remove.
	batch := indexClient.NewWriteBatch()
	for _, cid := range e.Chunks {
		c, err := chunk.ParseExternalKey(cp.userID, cid)
		if err != nil {
			return errors.Wrap(err, "failed to parse chunk key")
		}

		// ChunkWriteEntries returns entries not only for this day-period, but all days that chunk covers.
		// Since we process plans "backwards", more recent entries should already be cleaned up.
		ents, err := schema.GetChunkWriteEntries(c.From, c.Through, cp.userID, metricName, metric, cid)
		if err != nil {
			return errors.Wrapf(err, "getting index entries to delete for chunkID=%s", cid)
		}
		for i := range ents {
			// To avoid deleting entries from older tables, we check for table. This can still delete entries
			// from different buckets in the same table, but we just accept that.
			if tableName == ents[i].TableName {
				batch.Delete(ents[i].TableName, ents[i].HashValue, ents[i].RangeValue)
			}
		}
		indexEntries += len(ents)

		// Label entries in v9, v10 and v11 don't use from/through in encoded values, so instead of chunk From/Through values,
		// we only pass start/end for current day, to avoid deleting entries in other buckets.
		// As "end" is inclusive, we make it exclusive by -1.
		_, perKeyEnts, err := schema.GetCacheKeysAndLabelWriteEntries(start, end-1, cp.userID, metricName, metric, cid)
		if err != nil {
			return errors.Wrapf(err, "getting index entries to delete for chunkID=%s", cid)
		}
		for _, ents := range perKeyEnts {
			for i := range ents {
				batch.Delete(ents[i].TableName, ents[i].HashValue, ents[i].RangeValue)
			}
			indexEntries += len(ents)
		}

		// Only delete this chunk if it *starts* in plans' date-period. In general we process plans from most-recent
		// to older, so if chunk starts in current plan's period, its index entries were already removed in later plans.
		// This breaks when running multiple cleaners or cleaner crashes.
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
		cp.cleaner.deletedIndexEntries.Add(float64(indexEntries))
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

func (c *Cleaner) createClientsForDay(dayStart time.Time) (string, chunk.BaseSchema, chunk.Client, chunk.IndexClient, error) {
	for ix, s := range c.schemaConfig.Configs {
		if dayStart.Unix() < s.From.Unix() {
			continue
		}

		if ix+1 < len(c.schemaConfig.Configs) && dayStart.Unix() > c.schemaConfig.Configs[ix+1].From.Unix() {
			continue
		}

		tableName := s.IndexTables.TableFor(model.TimeFromUnixNano(dayStart.UnixNano()))

		schema, err := s.CreateSchema()
		if err != nil {
			return "", nil, nil, nil, errors.Wrap(err, "failed to create schema")
		}

		// No registerer, to avoid problems with registering same metrics multiple times.
		index, err := storage.NewIndexClient(s.IndexType, c.storageConfig, c.schemaConfig, nil)
		if err != nil {
			return "", nil, nil, nil, errors.Wrap(err, "error creating index client")
		}

		objectStoreType := s.ObjectType
		if objectStoreType == "" {
			objectStoreType = s.IndexType
		}

		// No registerer, to avoid problems with registering same metrics multiple times.
		chunks, err := storage.NewChunkClient(objectStoreType, c.storageConfig, c.schemaConfig, nil)
		if err != nil {
			index.Stop()
			return "", nil, nil, nil, errors.Wrap(err, "error creating object client")
		}

		return tableName, schema, chunks, index, nil
	}

	return "", nil, nil, nil, errors.Errorf("no schema for day %v", dayStart.Format("2006-01-02"))
}
