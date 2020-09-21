package builder

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

// How many series are kept in the memory before sorting and writing them to the file.
const defaultSeriesBatchSize = 250000

type Config struct {
	BuilderName     string
	OutputDirectory string
	Concurrency     int

	ChunkCacheConfig cache.Config
	HeartbeatPeriod  time.Duration
	UploadBlock      bool
	DeleteLocalBlock bool
	SeriesBatchSize  int

	SchedulerEndpoint string
	NextPlanInterval  time.Duration
	GrpcConfig        grpcclient.ConfigWithTLS
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ChunkCacheConfig.RegisterFlagsWithPrefix("chunks.", "Chunks cache", f)
	cfg.GrpcConfig.RegisterFlagsWithPrefix("builder.client", f)

	host, _ := os.Hostname()
	f.StringVar(&cfg.BuilderName, "builder.name", host, "Builder name, defaults to hostname.")
	f.StringVar(&cfg.OutputDirectory, "builder.output-dir", "", "Local directory used for storing temporary plan files (will be created, if missing).")
	f.IntVar(&cfg.Concurrency, "builder.concurrency", 128, "Number of concurrent series processors.")
	f.DurationVar(&cfg.HeartbeatPeriod, "builder.heartbeat", 5*time.Minute, "How often to update plan progress file.")
	f.BoolVar(&cfg.UploadBlock, "builder.upload", true, "Upload generated blocks to storage.")
	f.BoolVar(&cfg.DeleteLocalBlock, "builder.delete-local-blocks", true, "Delete local files after uploading block.")
	f.StringVar(&cfg.SchedulerEndpoint, "builder.scheduler-endpoint", "", "Scheduler endpoint, where builder will ask for more plans to work on.")
	f.DurationVar(&cfg.NextPlanInterval, "builder.next-plan-interval", 1*time.Minute, "How often to ask for next plan (when idle)")
	f.IntVar(&cfg.SeriesBatchSize, "builder.series-batch-size", defaultSeriesBatchSize, "Number of series to keep in memory before batch-write to temp file. Lower to decrease memory usage during the block building.")
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

	if cfg.SchedulerEndpoint == "" {
		return nil, errors.New("no scheduler endpoint")
	}

	bucketClient, err := scfg.GetBucket(l, reg)
	if err != nil {
		return nil, err
	}

	if cfg.OutputDirectory == "" {
		return nil, errors.New("no output directory")
	}
	plansDir := filepath.Join(cfg.OutputDirectory, "plans")
	if err := os.MkdirAll(plansDir, os.FileMode(0700)); err != nil {
		return nil, errors.Wrap(err, "failed to create plans directory")
	}

	b := &Builder{
		cfg: cfg,
		log: l,

		bucketClient:  bucketClient,
		chunksCache:   chunksCache,
		schemaConfig:  scfg.SchemaConfig,
		storageConfig: scfg.StorageConfig,
		plansDir:      plansDir,

		fetchedChunks: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_builder_fetched_chunks_total",
			Help: "Fetched chunks",
		}),
		fetchedChunksSize: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_builder_fetched_chunks_bytes_total",
			Help: "Fetched chunks bytes",
		}),
		processedSeries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_builder_series_total",
			Help: "Processed series",
		}),
		writtenSamples: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_builder_written_samples_total",
			Help: "Written samples",
		}),
		buildInProgress: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_builder_in_progress",
			Help: "Build in progress",
		}),
		currentPlanStartTime: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_builder_plan_start_time_seconds",
			Help: "Start time of current plan's time range (unix timestamp).",
		}),
		planFileReadPosition: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_builder_plan_file_position",
			Help: "Read bytes from the plan file.",
		}),
		planFileSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_builder_plan_size",
			Help: "Total size of plan file.",
		}),
		chunksNotFound: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_builder_chunks_not_found_total",
			Help: "Number of chunks that were not found on the storage.",
		}),
		blocksSize: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_builder_block_size_bytes_total",
			Help: "Total size of blocks generated by this builder.",
		}),
		seriesInMemory: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_builder_series_in_memory",
			Help: "Number of series kept in memory at the moment. (Builder writes series to temp files in order to reduce memory usage.)",
		}),
	}
	b.Service = services.NewBasicService(b.cleanup, b.running, nil)
	return b, err
}

type Builder struct {
	services.Service

	cfg Config
	log log.Logger

	bucketClient  objstore.Bucket
	chunksCache   cache.Cache
	schemaConfig  chunk.SchemaConfig
	storageConfig storage.Config
	plansDir      string

	fetchedChunks     prometheus.Counter
	fetchedChunksSize prometheus.Counter
	processedSeries   prometheus.Counter
	writtenSamples    prometheus.Counter
	blocksSize        prometheus.Counter

	planFileReadPosition prometheus.Gauge
	planFileSize         prometheus.Gauge
	buildInProgress      prometheus.Gauge
	chunksNotFound       prometheus.Counter
	seriesInMemory       prometheus.Gauge
	currentPlanStartTime prometheus.Gauge
}

func (b *Builder) cleanup(_ context.Context) error {
	files, err := ioutil.ReadDir(b.cfg.OutputDirectory)
	if err != nil {
		return err
	}

	// Delete directories with .tmp suffix (unfinished blocks).
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".tmp") && f.IsDir() {
			toRemove := filepath.Join(b.cfg.OutputDirectory, f.Name())

			level.Info(b.log).Log("msg", "deleting unfinished block", "dir", toRemove)

			err := os.RemoveAll(toRemove)
			if err != nil {
				return errors.Wrapf(err, "removing %s", toRemove)
			}
		}
	}

	files, err = ioutil.ReadDir(b.plansDir)
	if err != nil {
		return err
	}

	for _, f := range files {
		toRemove := filepath.Join(b.plansDir, f.Name())

		level.Info(b.log).Log("msg", "deleting unfinished local plan file", "file", toRemove)
		err = os.Remove(toRemove)
		if err != nil {
			return errors.Wrapf(err, "removing %s", toRemove)
		}
	}

	return nil
}

func (b *Builder) running(ctx context.Context) error {
	ticker := time.NewTicker(b.cfg.NextPlanInterval)
	defer ticker.Stop()

	var schedulerClient blocksconvert.SchedulerClient
	var conn *grpc.ClientConn

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			// We may get "tick" even when we should stop.
			if ctx.Err() != nil {
				return nil
			}

			if conn == nil {
				opts, err := b.cfg.GrpcConfig.DialOption(nil, nil)
				if err != nil {
					return err
				}

				conn, err = grpc.Dial(b.cfg.SchedulerEndpoint, opts...)
				if err != nil {
					level.Error(b.log).Log("msg", "failed to dial", "endpoint", b.cfg.SchedulerEndpoint, "err", err)
					continue
				}

				schedulerClient = blocksconvert.NewSchedulerClient(conn)
			}

			resp, err := schedulerClient.NextPlan(ctx, &blocksconvert.NextPlanRequest{BuilderName: b.cfg.BuilderName})
			if err != nil {
				level.Error(b.log).Log("msg", "failed to get next plan due to error, closing connection", "err", err)
				_ = conn.Close()
				conn = nil
				schedulerClient = nil
				continue
			}

			// No plan to work on, ignore.
			if resp.PlanFile == "" {
				continue
			}

			isPlanFile, planBaseName := blocksconvert.IsPlanFilename(resp.PlanFile)
			if !isPlanFile {
				level.Error(b.log).Log("msg", "got invalid plan file", "planFile", resp.PlanFile)
				continue
			}

			ok, base, _ := blocksconvert.IsProgressFilename(resp.ProgressFile)
			if !ok || base != planBaseName {
				level.Error(b.log).Log("msg", "got invalid progress file", "progressFile", resp.ProgressFile)
				continue
			}

			level.Info(b.log).Log("msg", "received plan file", "planFile", resp.PlanFile, "progressFile", resp.ProgressFile)

			err = b.processPlanFile(ctx, resp.PlanFile, planBaseName, resp.ProgressFile)
			if err != nil {
				level.Error(b.log).Log("msg", "failed to process plan file", "planFile", resp.PlanFile, "err", err)

				// If context is canceled (either builder is shutting down, or due to hearbeating failure), don't upload error.
				if !errors.Is(err, context.Canceled) {
					errorFile := blocksconvert.ErrorFilename(planBaseName)
					err = b.bucketClient.Upload(ctx, errorFile, strings.NewReader(err.Error()))
					if err != nil {
						level.Error(b.log).Log("msg", "failed to upload error file", "errorFile", errorFile, "err", err)
					}
				}
			}

			err = b.cleanup(ctx)
			if err != nil {
				level.Error(b.log).Log("msg", "failed to cleanup working directory", "err", err)
			}
		}
	}
}

func (b *Builder) processPlanFile(ctx context.Context, planFile, planBaseName, lastProgressFile string) error {
	b.buildInProgress.Set(1)
	defer b.buildInProgress.Set(0)
	defer b.planFileSize.Set(0)
	defer b.planFileReadPosition.Set(0)
	defer b.currentPlanStartTime.Set(0)
	defer b.seriesInMemory.Set(0)

	planLog := log.With(b.log, "plan", planFile)

	// Start heartbeating (updating of progress file). We setup new context used for the rest of the function.
	// If hearbeating fails, we cancel this new context to abort quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	hb := newHeartbeat(planLog, b.bucketClient, b.cfg.HeartbeatPeriod, planBaseName, lastProgressFile)
	hb.AddListener(services.NewListener(nil, nil, nil, nil, func(from services.State, failure error) {
		level.Error(planLog).Log("msg", "heartbeating failed, aborting build", "failure", failure)
		cancel()
	}))
	if err := services.StartAndAwaitRunning(ctx, hb); err != nil {
		return errors.Wrap(err, "failed to start heartbeating")
	}

	localPlanFile := filepath.Join(b.plansDir, filepath.Base(planFile))
	planSize, err := downloadPlanFile(ctx, b.bucketClient, planFile, localPlanFile)
	if err != nil {
		return errors.Wrapf(err, "failed to download plan file %s to %s", planFile, localPlanFile)
	}
	level.Info(planLog).Log("msg", "downloaded plan file", "localPlanFile", localPlanFile, "size", planSize)

	b.planFileSize.Set(float64(planSize))

	f, err := os.Open(localPlanFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read local plan file %s", localPlanFile)
	}
	defer func() {
		_ = f.Close()
	}()

	// Use a buffer for reading plan file.
	r, err := blocksconvert.PreparePlanFileReader(planFile, bufio.NewReaderSize(&readPositionReporter{r: f, g: b.planFileReadPosition}, 1*1024*1024))
	if err != nil {
		return err
	}

	dec := json.NewDecoder(r)

	userID, dayStart, dayEnd, err := parsePlanHeader(dec)
	if err != nil {
		return err
	}

	b.currentPlanStartTime.Set(float64(dayStart.Unix()))

	level.Info(planLog).Log("msg", "processing plan file", "user", userID, "dayStart", dayStart, "dayEnd", dayEnd)
	chunkClient, err := b.createChunkClientForDay(dayStart)
	if err != nil {
		return errors.Wrap(err, "failed to create chunk client")
	}
	defer chunkClient.Stop()

	fetcher, err := newFetcher(userID, chunkClient, b.chunksCache, b.fetchedChunks, b.fetchedChunksSize)
	if err != nil {
		return errors.Wrap(err, "failed to create chunk fetcher")
	}

	tsdbBuilder, err := newTsdbBuilder(b.cfg.OutputDirectory, dayStart, dayEnd, b.cfg.SeriesBatchSize, planLog, b.processedSeries, b.writtenSamples, b.seriesInMemory)
	if err != nil {
		return errors.Wrap(err, "failed to create TSDB builder")
	}

	planEntryCh := make(chan blocksconvert.PlanEntry)

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < b.cfg.Concurrency; i++ {
		g.Go(func() error {
			return fetchAndBuild(gctx, fetcher, planEntryCh, tsdbBuilder, planLog, b.chunksNotFound)
		})
	}

	g.Go(func() error {
		return parsePlanEntries(gctx, dec, planEntryCh)
	})

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "failed to build block")
	}

	// Finish block.
	ulid, err := tsdbBuilder.finishBlock("blocksconvert", map[string]string{
		cortex_tsdb.TenantIDExternalLabel: userID,
	})

	if err != nil {
		return errors.Wrap(err, "failed to finish block building")
	}

	blockDir := filepath.Join(b.cfg.OutputDirectory, ulid.String())
	blockSize, err := getBlockSize(blockDir)
	if err != nil {
		return errors.Wrap(err, "block size")
	}

	level.Info(planLog).Log("msg", "successfully built block for a plan", "ulid", ulid.String(), "size", blockSize)
	b.blocksSize.Add(float64(blockSize))

	if b.cfg.UploadBlock {
		userBucket := cortex_tsdb.NewUserBucketClient(userID, b.bucketClient)
		if err := block.Upload(ctx, planLog, userBucket, blockDir); err != nil {
			return errors.Wrap(err, "uploading block")
		}

		level.Info(planLog).Log("msg", "block uploaded", "ulid", ulid.String())

		if b.cfg.DeleteLocalBlock {
			if err := os.RemoveAll(blockDir); err != nil {
				level.Warn(planLog).Log("msg", "failed to delete local block", "err", err)
			}
		}
	}

	err = os.Remove(localPlanFile)
	if err != nil {
		level.Warn(planLog).Log("msg", "failed to delete local plan file", "err", err)
	}

	// Upload finished status file
	finishedFile := blocksconvert.FinishedFilename(planBaseName, ulid)
	if err := b.bucketClient.Upload(ctx, finishedFile, strings.NewReader(ulid.String())); err != nil {
		return errors.Wrap(err, "failed to upload finished status file")
	}
	level.Info(planLog).Log("msg", "uploaded finished file", "file", finishedFile)

	// Stop heartbeating.
	if err := services.StopAndAwaitTerminated(ctx, hb); err != nil {
		// No need to report this error to caller to avoid generating error file.
		level.Warn(planLog).Log("msg", "hearbeating failed", "err", err)
	}

	// All OK
	return nil
}

func downloadPlanFile(ctx context.Context, bucket objstore.Bucket, planFile string, localPlanFile string) (int64, error) {
	f, err := os.Create(localPlanFile)
	if err != nil {
		return 0, err
	}

	r, err := bucket.Get(ctx, planFile)
	if err != nil {
		_ = f.Close()
		return 0, err
	}
	// Copy will read `r` until EOF, or error is returned. Any possible error from Close is irrelevant.
	defer func() { _ = r.Close() }()

	n, err := io.Copy(f, r)
	if err != nil {
		_ = f.Close()
		return 0, err
	}

	return n, f.Close()
}

func getBlockSize(dir string) (int64, error) {
	size := int64(0)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			size += info.Size()
		}

		// Ignore directory with temporary series files.
		if info.IsDir() && info.Name() == "series" {
			return filepath.SkipDir
		}

		return nil
	})
	return size, err
}

func parsePlanHeader(dec *json.Decoder) (userID string, startTime, endTime time.Time, err error) {
	header := blocksconvert.PlanEntry{}
	if err = dec.Decode(&header); err != nil {
		return
	}
	if header.User == "" || header.DayIndex == 0 {
		err = errors.New("failed to read plan file header: no user or day index found")
		return
	}

	dayStart := time.Unix(int64(header.DayIndex)*int64(24*time.Hour/time.Second), 0).UTC()
	dayEnd := dayStart.Add(24 * time.Hour)
	return header.User, dayStart, dayEnd, nil
}

func parsePlanEntries(ctx context.Context, dec *json.Decoder, planEntryCh chan blocksconvert.PlanEntry) error {
	defer close(planEntryCh)

	var err error
	complete := false
	entry := blocksconvert.PlanEntry{}
	for err = dec.Decode(&entry); err == nil; err = dec.Decode(&entry) {
		if entry.Complete {
			complete = true
			entry.Reset()
			continue
		}

		if complete {
			return errors.New("plan entries found after plan footer")
		}

		if entry.SeriesID != "" && len(entry.Chunks) > 0 {
			select {
			case planEntryCh <- entry:
				// ok
			case <-ctx.Done():
				return nil
			}

		}

		entry.Reset()
	}

	if err == io.EOF {
		if !complete {
			return errors.New("plan is not complete")
		}
		err = nil
	}
	return errors.Wrap(err, "parsing plan entries")
}

func fetchAndBuild(ctx context.Context, f *fetcher, input chan blocksconvert.PlanEntry, tb *tsdbBuilder, log log.Logger, chunksNotFound prometheus.Counter) error {
	b := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 5 * time.Second,
		MaxRetries: 5,
	})

	for {
		select {
		case <-ctx.Done():
			return nil

		case e, ok := <-input:
			if !ok {
				// End of input.
				return nil
			}

			var m labels.Labels
			var cs []chunk.Chunk
			var err error

			// Rather than aborting entire block build due to temporary errors ("connection reset by peer", "http2: client conn not usable"),
			// try to fetch chunks multiple times.
			for b.Reset(); b.Ongoing(); {
				m, cs, err = fetchAndBuildSingleSeries(ctx, f, e.Chunks)
				if err == nil {
					break
				}

				if b.Ongoing() {
					level.Warn(log).Log("msg", "failed to fetch chunks for series", "series", e.SeriesID, "err", err, "retries", b.NumRetries()+1)
					b.Wait()
				}
			}

			if err == nil {
				err = b.Err()
			}
			if err != nil {
				return errors.Wrapf(err, "failed to fetch chunks for series %s", e.SeriesID)
			}

			if len(e.Chunks) > len(cs) {
				chunksNotFound.Add(float64(len(e.Chunks) - len(cs)))
				level.Warn(log).Log("msg", "chunks for series not found", "seriesID", e.SeriesID, "expected", len(e.Chunks), "got", len(cs))
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
	if err != nil && !errors.Is(err, chunk.ErrStorageObjectNotFound) {
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
		chunks, err := storage.NewChunkClient(objectStoreType, b.storageConfig, b.schemaConfig, nil)
		if err != nil {
			return nil, errors.Wrap(err, "error creating object client")
		}
		return chunks, nil
	}

	return nil, errors.Errorf("no schema for day %v", dayStart.Format("2006-01-02"))
}

type readPositionReporter struct {
	r   io.Reader
	g   prometheus.Gauge
	pos int64
}

func (r *readPositionReporter) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.pos += int64(n)
		r.g.Set(float64(r.pos))
	}
	return n, err
}
