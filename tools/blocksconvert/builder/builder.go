package builder

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

type Config struct {
	PlanFile string

	OutputDirectory  string
	Concurrency      int
	StorageConfig    storage.Config
	ChunkCacheConfig cache.Config
	HeartbeatPeriod  time.Duration
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ChunkCacheConfig.RegisterFlagsWithPrefix("chunks.", "Chunks cache", f)
	cfg.StorageConfig.RegisterFlags(f)

	f.StringVar(&cfg.PlanFile, "builder.plan-file", "", "Plan file to process. (Test only)")

	f.StringVar(&cfg.OutputDirectory, "builder.local-dir", "", "Local directory used for storing temporary plan files (will be deleted and recreated!).")
	f.IntVar(&cfg.Concurrency, "builder.concurrency", 16, "Number of concurrent index processors.")
	f.DurationVar(&cfg.HeartbeatPeriod, "builder.heartbeat", 5*time.Minute, "How often to update plan status file.")
}

func NewBuilder(cfg Config, scfg blocksconvert.SharedConfig, l log.Logger, reg prometheus.Registerer) (*Builder, error) {
	err := scfg.SchemaConfig.Load()
	if err != nil {
		return nil, errors.Wrap(err, "failed to load schema")
	}

	cfg.ChunkCacheConfig.Prefix = "chunks"
	chunksCache, err := cache.New(cfg.ChunkCacheConfig, reg, l)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create chunks cache")
	}

	bucketClient, err := scfg.GetBucket(l, reg)
	if err != nil {
		return nil, err
	}

	b := &Builder{
		cfg: cfg,
		log: l,

		bucketClient: bucketClient,
		chunksCache:  chunksCache,
		schemaConfig: scfg.SchemaConfig,

		fetchedChunks: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "builder_fetched_chunks_total",
			Help: "Fetched chunks",
		}),
		fetchedChunksSize: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "builder_fetched_chunks_bytes_total",
			Help: "Fetched chunks bytes",
		}),

		processedSeries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "builder_series_total",
			Help: "Processed series",
		}),
		writtenSamples: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "builder_written_samples_total",
			Help: "Written samples",
		}),
	}
	b.Service = services.NewBasicService(nil, b.running, nil)
	return b, err
}

type Builder struct {
	services.Service

	cfg Config
	log log.Logger
	reg prometheus.Registerer

	bucketClient objstore.Bucket
	chunksCache  cache.Cache
	schemaConfig chunk.SchemaConfig

	fetchedChunks     prometheus.Counter
	fetchedChunksSize prometheus.Counter
	processedSeries   prometheus.Counter
	writtenSamples    prometheus.Counter
}

func (b *Builder) running(ctx context.Context) error {
	return b.processPlanFile(ctx, b.cfg.PlanFile)
}

func (b *Builder) processPlanFile(ctx context.Context, planFile string) error {
	f, err := b.bucketClient.Get(ctx, planFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read plan file %s", planFile)
	}
	defer func() {
		_ = f.Close()
	}()

	r, _, err := preparePlanFileAndGetBaseName(planFile, f)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(r)

	header := blocksconvert.PlanEntry{}
	if err := dec.Decode(&header); err != nil {
		return errors.Wrapf(err, "failed to read plan file header %s", planFile)
	}
	if header.User == "" || header.DayIndex == 0 {
		return errors.New("failed to read plan file header: no user or day index found")
	}

	dayStart := time.Unix(int64(header.DayIndex)*int64(24*time.Hour/time.Second), 0).UTC()
	dayEnd := dayStart.Add(24 * time.Hour)

	level.Info(b.log).Log("msg", "processing plan file", "plan", planFile, "user", header.User, "dayStart", dayStart, "dayEnd", dayEnd)
	chunkClient, err := b.createChunkClientForDay(dayStart)
	if err != nil {
		return errors.Wrap(err, "failed to create chunk client")
	}
	defer chunkClient.Stop()

	fetcher, err := newFetcher(header.User, chunkClient, b.chunksCache, b.fetchedChunks, b.fetchedChunksSize)
	if err != nil {
		return errors.Wrap(err, "failed to create chunk fetcher")
	}
	defer fetcher.stop()

	tsdbBuilder, err := newTsdbBuilder(b.cfg.OutputDirectory, dayStart, dayEnd, b.log, b.processedSeries, b.writtenSamples)
	if err != nil {
		return errors.Wrap(err, "failed to create TSDB builder")
	}

	planEntryCh := make(chan blocksconvert.PlanEntry)

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < b.cfg.Concurrency; i++ {
		g.Go(func() error {
			return fetchAndBuild(gctx, fetcher, planEntryCh, tsdbBuilder)
		})
	}

	g.Go(func() error {
		return parsePlanEntries(dec, planEntryCh)
	})

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "failed to build block")
	}

	// Finish block.
	ulid, err := tsdbBuilder.finishBlock()
	if err != nil {
		return errors.Wrap(err, "failed to finish block building")
	}

	level.Info(b.log).Log("msg", "successfully built block for a plan", "plan", planFile, "ulid", ulid.String())

	// TODO: Upload block to storage.

	return nil
}

func parsePlanEntries(dec *json.Decoder, planEntryCh chan blocksconvert.PlanEntry) error {
	defer close(planEntryCh)

	var err error
	complete := false
	entry := &blocksconvert.PlanEntry{}
	for err = dec.Decode(&entry); err == nil; err = dec.Decode(&entry) {
		if entry.Complete {
			complete = true
			continue
		}

		if complete {
			return errors.New("plan entries found after plan footer")
		}

		if entry.SeriesID != "" && len(entry.Chunks) > 0 {
			planEntryCh <- *entry
		}

		entry.Reset()
	}

	if err == io.EOF {
		if !complete {
			return errors.New("plan is not complete")
		}
		err = nil
	}
	return err
}

func fetchAndBuild(ctx context.Context, f *fetcher, input chan blocksconvert.PlanEntry, tb *tsdbBuilder) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case e, ok := <-input:
			if !ok {
				// End of input.
				return nil
			}

			m, cs, err := fetchAndBuildSingleSeries(ctx, f, e.Chunks)
			if err != nil {
				return errors.Wrapf(err, "failed to fetch chunks for series %s", e.SeriesID)
			}

			if len(cs) == 0 {
				continue
			}

			err = tb.buildSingleSeries(m, cs)
			if err != nil {
				return errors.Wrapf(err, "failed to build series %s", e.SeriesID)
			}
		}
	}
}

func fetchAndBuildSingleSeries(ctx context.Context, fetcher *fetcher, chunksIds []string) (labels.Labels, []chunk.Chunk, error) {
	cs, err := fetcher.fetchChunks(ctx, chunksIds)
	if err != nil {
		return nil, nil, errors.Wrap(err, "fetching chunks")
	}

	if len(cs) == 0 {
		return nil, nil, nil
	}

	m := cs[0].Metric
	// Verify that all chunks belong to the same series.
	for _, c := range cs {
		if !labels.Equal(m, c.Metric) {
			return nil, nil, errors.Errorf("chunks for multiple metrics: %v, %v", m.String(), c.Metric.String())
		}
	}

	return m, cs, nil
}

func preparePlanFileAndGetBaseName(planFile string, in io.Reader) (io.Reader, string, error) {
	baseFilename := planFile

	var r = in

	if strings.HasSuffix(baseFilename, ".snappy") {
		r = snappy.NewReader(r)
		baseFilename = baseFilename[:len(baseFilename)-len(".snappy")]
	}

	if strings.HasSuffix(baseFilename, ".gz") {
		var err error
		r, err = gzip.NewReader(r)
		if err != nil {
			return nil, "", fmt.Errorf("failed to open plan file %s: %w", planFile, err)
		}
		baseFilename = baseFilename[:len(baseFilename)-len(".gz")]
	}

	if !strings.HasSuffix(baseFilename, ".plan") {
		return nil, "", fmt.Errorf("failed to determine base plan file name: %s", planFile)
	}

	baseFilename = baseFilename[:len(baseFilename)-len(".plan")]
	return r, baseFilename, nil
}

// Finds storage configuration for given day, and builds a client.
func (b *Builder) createChunkClientForDay(dayStart time.Time) (chunk.Client, error) {
	for ix, s := range b.schemaConfig.Configs {
		if dayStart.Unix() < s.From.Unix() {
			continue
		}

		if ix+1 < len(b.schemaConfig.Configs) && dayStart.Unix() > b.schemaConfig.Configs[ix+1].From.Unix() {
			continue
		}

		objectStoreType := s.ObjectType
		if objectStoreType == "" {
			objectStoreType = s.IndexType
		}
		// No registerer, to avoid problems with registering same metrics multiple times.
		chunks, err := storage.NewChunkClient(objectStoreType, b.cfg.StorageConfig, b.schemaConfig, nil)
		if err != nil {
			return nil, fmt.Errorf("error creating object client: %w", err)
		}
		return chunks, nil
	}

	return nil, fmt.Errorf("no schema for day %v", dayStart.Format("2006-01-02"))
}
