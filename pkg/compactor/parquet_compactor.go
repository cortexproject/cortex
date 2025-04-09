package compactor

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	storage "github.com/cortexproject/cortex/pkg/storage/parquet"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/logutil"
	"golang.org/x/sync/errgroup"
)

const (
	batchSize                = 50000
	parquetFileName          = "block.parquet"
	compactorRingKey         = "compactor-parquet"
	maxParquetIndexSizeLimit = 100
)

type ParquetCompactorService struct {
	services.Service

	bucket      objstore.InstrumentedBucket
	loader      *bucketindex.Loader
	Cfg         Config
	registerer  prometheus.Registerer
	logger      log.Logger
	limits      *validation.Overrides
	blockRanges []int64

	// Ring used for sharding compactions.
	ringLifecycler         *ring.Lifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher
}

func NewParquetCompactor(compactorCfg Config, storageCfg cortex_tsdb.BlocksStorageConfig, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides, ingestionReplicationFactor int) (*ParquetCompactorService, error) {
	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, nil, "parquet-compactor", util_log.Logger, prometheus.DefaultRegisterer)

	if err != nil {
		return nil, err
	}
	indexLoaderConfig := bucketindex.LoaderConfig{
		CheckInterval:         time.Minute,
		UpdateOnStaleInterval: storageCfg.BucketStore.SyncInterval,
		UpdateOnErrorInterval: storageCfg.BucketStore.BucketIndex.UpdateOnErrorInterval,
		IdleTimeout:           storageCfg.BucketStore.BucketIndex.IdleTimeout,
	}

	loader := bucketindex.NewLoader(indexLoaderConfig, bucketClient, limits, util_log.Logger, prometheus.DefaultRegisterer)

	c := &ParquetCompactorService{
		Cfg:         compactorCfg,
		bucket:      bucketClient,
		loader:      loader,
		logger:      log.With(logger, "component", "compactor"),
		registerer:  registerer,
		blockRanges: compactorCfg.BlockRanges.ToMilliseconds(),
		limits:      limits,
	}

	c.Service = services.NewBasicService(c.starting, c.run, nil)
	return c, nil
}

func (c *ParquetCompactorService) starting(ctx context.Context) error {
	var err error
	if c.Cfg.ShardingEnabled {
		lifecyclerCfg := c.Cfg.ShardingRing.ToLifecyclerConfig()
		c.ringLifecycler, err = ring.NewLifecycler(lifecyclerCfg, ring.NewNoopFlushTransferer(), "compactor-parquet", compactorRingKey, true, false, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.registerer))
		if err != nil {
			return errors.Wrap(err, "unable to initialize compactor ring lifecycler")
		}

		c.ring, err = ring.New(lifecyclerCfg.RingConfig, "compactor", compactorRingKey, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.registerer))
		if err != nil {
			return errors.Wrap(err, "unable to initialize compactor ring")
		}

		c.ringSubservices, err = services.NewManager(c.ringLifecycler, c.ring)
		if err == nil {
			c.ringSubservicesWatcher = services.NewFailureWatcher()
			c.ringSubservicesWatcher.WatchManager(c.ringSubservices)

			err = services.StartManagerAndAwaitHealthy(ctx, c.ringSubservices)
		}
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.Cfg.ShardingRing.WaitActiveInstanceTimeout)
	defer cancel()
	if err := ring.WaitInstanceState(ctxWithTimeout, c.ring, c.ringLifecycler.ID, ring.ACTIVE); err != nil {
		level.Error(c.logger).Log("msg", "compactor failed to become ACTIVE in the ring", "err", err)
		return err
	}
	level.Info(c.logger).Log("msg", "compactor is ACTIVE in the ring")
	if err := c.loader.StartAsync(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start loader")
	}

	if err := c.loader.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to start loader")
	}
	return nil
}

func (c *ParquetCompactorService) run(ctx context.Context) error {

	go func() {
		updateIndexTicker := time.NewTicker(time.Second * 60)
		for {
			select {
			case <-ctx.Done():
				return
			case <-updateIndexTicker.C:
				users, err := c.discoverUsers(ctx)
				if err != nil {
					level.Error(util_log.Logger).Log("msg", "Error scanning users", "err", err)
					break
				}
				for _, u := range users {
					if ok, _ := c.ownBlock(u); ok {
						err := c.updateParquetIndex(ctx, u)
						if err != nil {
							level.Error(util_log.Logger).Log("msg", "Error updating index", "err", err)
						}
					}
				}
			}
		}
	}()

	t := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			u, err := c.discoverUsers(ctx)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "Error scanning users", "err", err)
				return err
			}

			for _, u := range u {
				uBucket := bucket.NewUserBucketClient(u, c.bucket, c.limits)

				pIdx, err := bucketindex.ReadParquetIndex(ctx, uBucket, util_log.Logger)
				if err != nil {
					level.Error(util_log.Logger).Log("msg", "Error loading index", "err", err)
					break
				}

				for {
					if ctx.Err() != nil {
						return ctx.Err()
					}

					level.Info(util_log.Logger).Log("msg", "Scanning User", "user", u)

					idx, _, err := c.loader.GetIndex(ctx, u)
					if err != nil {
						level.Error(util_log.Logger).Log("msg", "Error loading index", "err", err)
						break
					}

					level.Info(util_log.Logger).Log("msg", "Loaded index", "user", u, "totalBlocks", len(idx.Blocks), "deleteBlocks", len(idx.BlockDeletionMarks))

					level.Info(util_log.Logger).Log("msg", "Loaded Parquet index", "user", u, "totalBlocks", len(pIdx.Blocks))

					ownedBlocks, totalBlocks := c.findNextBlockToCompact(ctx, uBucket, idx, pIdx)
					if len(ownedBlocks) == 0 {
						level.Info(util_log.Logger).Log("msg", "No owned blocks to compact", "numBlocks", len(pIdx.Blocks), "totalBlocks", totalBlocks)
						break
					}

					b := ownedBlocks[0]

					if err := os.RemoveAll(c.compactRootDir()); err != nil {
						level.Error(util_log.Logger).Log("msg", "failed to remove compaction work directory", "path", c.compactRootDir(), "err", err)
					}
					bdir := filepath.Join(c.compactDirForUser(u), b.ID.String())
					level.Info(util_log.Logger).Log("msg", "Downloading block", "block", b.ID.String(), "maxTime", b.MaxTime, "dir", bdir, "ownedBlocks", len(ownedBlocks), "totalBlocks", totalBlocks)
					if err := block.Download(ctx, util_log.Logger, uBucket, b.ID, bdir, objstore.WithFetchConcurrency(10)); err != nil {
						level.Error(util_log.Logger).Log("msg", "Error downloading block", "err", err)
						continue
					}
					err = c.doImport(ctx, b.ID, uBucket, bdir)
					if err != nil {
						level.Error(util_log.Logger).Log("msg", "failed to import block", "block", b.String(), "err", err)
					}
					// Add the blocks
					pIdx.Blocks[b.ID] = bucketindex.BlockWithExtension{Block: b}
				}
			}
		}
	}
}

func (c *ParquetCompactorService) updateParquetIndex(ctx context.Context, u string) error {
	level.Info(util_log.Logger).Log("msg", "Updating index", "user", u)
	uBucket := bucket.NewUserBucketClient(u, c.bucket, c.limits)
	deleted := map[ulid.ULID]struct{}{}
	idx, _, err := c.loader.GetIndex(ctx, u)

	if err != nil {
		return err
	}

	for _, b := range idx.BlockDeletionMarks {
		deleted[b.ID] = struct{}{}
	}

	pIdx, err := bucketindex.ReadParquetIndex(ctx, uBucket, util_log.Logger)
	if err != nil {
		return errors.Wrap(err, "failed to read parquet index")
	}

	for _, b := range idx.Blocks {
		if _, ok := deleted[b.ID]; ok {
			continue
		}

		if _, ok := pIdx.Blocks[b.ID]; ok {
			continue
		}

		marker, err := ReadCompactMark(ctx, b.ID, uBucket, c.logger)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to check if file exists", "err", err)
			continue
		}

		if marker.Version == CurrentVersion {
			m, err := block.DownloadMeta(ctx, util_log.Logger, uBucket, b.ID)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "failed to download block", "block", b.ID, "err", err)
			}
			extensions := bucketindex.Extensions{}
			_, err = metadata.ConvertExtensions(m.Thanos.Extensions, &extensions)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "failed to convert extensions", "err", err)
			}
			pIdx.Blocks[b.ID] = bucketindex.BlockWithExtension{Block: b, Extensions: extensions}
		}
	}
	c.removeDeletedBlocks(idx, pIdx)
	//// Remove block from bucket index if marker version is outdated.
	//c.removeOutdatedBlocks(ctx, uBucket, pIdx)
	return bucketindex.WriteParquetIndex(ctx, uBucket, pIdx)
}

func (c *ParquetCompactorService) removeDeletedBlocks(idx *bucketindex.Index, pIdx *bucketindex.ParquetIndex) {
	blocks := map[ulid.ULID]struct{}{}
	deleted := map[ulid.ULID]struct{}{}

	for _, b := range idx.BlockDeletionMarks {
		deleted[b.ID] = struct{}{}
	}

	for _, b := range idx.Blocks {
		if _, ok := deleted[b.ID]; !ok {
			blocks[b.ID] = struct{}{}
		}
	}

	for _, b := range pIdx.Blocks {
		if _, ok := blocks[b.ID]; !ok {
			delete(pIdx.Blocks, b.ID)
		}
	}
}

func (c *ParquetCompactorService) removeOutdatedBlocks(ctx context.Context, uBucket objstore.InstrumentedBucket, pIdx *bucketindex.ParquetIndex) {
	for _, b := range pIdx.Blocks {
		marker, err := ReadCompactMark(ctx, b.ID, uBucket, c.logger)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to check if file exists", "err", err)
			continue
		}

		if marker.Version < CurrentVersion {
			delete(pIdx.Blocks, b.ID)
		}
	}
}

func (c *ParquetCompactorService) findNextBlockToCompact(ctx context.Context, uBucket objstore.InstrumentedBucket, idx *bucketindex.Index, pIdx *bucketindex.ParquetIndex) ([]*bucketindex.Block, int) {
	deleted := map[ulid.ULID]struct{}{}
	result := make([]*bucketindex.Block, 0, len(idx.Blocks))
	totalBlocks := 0

	for _, b := range idx.BlockDeletionMarks {
		deleted[b.ID] = struct{}{}
	}

	for _, b := range idx.Blocks {
		// Do not compact if deleted
		if _, ok := deleted[b.ID]; ok {
			continue
		}

		// Do not compact if is already compacted
		if _, ok := pIdx.Blocks[b.ID]; ok {
			continue
		}

		// Don't try to compact 2h block. Compact 12h and 24h.
		if getBlockTimeRange(b, c.blockRanges) == c.blockRanges[0] {
			continue
		}

		totalBlocks++

		// Do not compact block if is not owned
		if ok, err := c.ownBlock(b.ID.String()); err != nil || !ok {
			continue
		}

		marker, err := ReadCompactMark(ctx, b.ID, uBucket, c.logger)

		if err == nil && marker.Version == CurrentVersion {
			continue
		}

		result = append(result, b)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].MinTime > result[j].MinTime
	})

	return result, totalBlocks
}

func (c *ParquetCompactorService) discoverUsers(ctx context.Context) ([]string, error) {
	var users []string

	err := c.bucket.Iter(ctx, "", func(entry string) error {
		u := strings.TrimSuffix(entry, "/")
		users = append(users, u)
		return nil
	})

	return users, err
}

// compactDirForUser returns the directory to be used to download and compact the blocks for a user
func (c *ParquetCompactorService) compactDirForUser(userID string) string {
	return filepath.Join(c.compactRootDir(), userID)
}

func (c *ParquetCompactorService) compactRootDir() string {
	return filepath.Join(c.Cfg.DataDir, "compact")
}

func (c *ParquetCompactorService) ownBlock(blockId string) (bool, error) {
	// Hash the user ID.
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(blockId))
	userHash := hasher.Sum32()

	// Check whether this compactor instance owns the user.
	rs, err := c.ring.Get(userHash, RingOp, nil, nil, nil)
	if err != nil {
		return false, err
	}

	if len(rs.Instances) != 1 {
		return false, fmt.Errorf("unexpected number of compactors in the shard (expected 1, got %d)", len(rs.Instances))
	}

	return rs.Instances[0].Addr == c.ringLifecycler.Addr, nil
}

func (c *ParquetCompactorService) doImport(ctx context.Context, id ulid.ULID, bucket objstore.Bucket, bDir string) (err error) {
	r, w := io.Pipe()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer r.Close()
		err = bucket.Upload(context.Background(), path.Join(id.String(), parquetFileName), r)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to upload file", "err", err)
		}
		if err == nil {
			err = WriteCompactMark(ctx, id, bucket)
		}
	}()

	sc, ln, totalMetrics, err := c.readTsdb(ctx, bDir)
	if err != nil {
		return err
	}

	writer := storage.NewParquetWriter(w, 1e6, maxParquetIndexSizeLimit, storage.ChunkColumnsPerDay, ln, labels.MetricName)
	if err != nil {
		return err
	}

	total := 0
	for b := range sc {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		fmt.Printf("Writing Metrics [%v] [%v]\n", 100*(float64(total)/float64(totalMetrics)), b[0].Columns[labels.MetricName])
		writer.WriteRows(b)
		total += len(b)
	}

	if e := writer.Close(); err == nil {
		err = e
	}

	if e := w.Close(); err == nil {
		err = e
	}

	wg.Wait()
	return err
}

func (c *ParquetCompactorService) readTsdb(ctx context.Context, path string) (chan []storage.ParquetRow, []string, int, error) {
	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(c.logger), path, nil, tsdb.DefaultPostingsDecoderFactory)
	if err != nil {
		return nil, nil, 0, err
	}
	idx, err := b.Index()
	if err != nil {
		return nil, nil, 0, err
	}

	cReader, err := b.Chunks()
	if err != nil {
		return nil, nil, 0, err
	}

	metricNames, err := idx.LabelValues(ctx, labels.MetricName)
	if err != nil {
		return nil, nil, 0, err
	}
	k, v := index.AllPostingsKey()
	all, err := idx.Postings(ctx, k, v)
	if err != nil {
		return nil, nil, 0, err
	}

	total := 0
	for all.Next() {
		total++
	}

	labelNames, err := idx.LabelNames(ctx)
	if err != nil {
		return nil, nil, 0, err
	}

	slices.SortFunc(metricNames, func(a, b string) int {
		return bytes.Compare(
			truncateByteArray([]byte(a), maxParquetIndexSizeLimit),
			truncateByteArray([]byte(b), maxParquetIndexSizeLimit),
		)
	})
	rc := make(chan []storage.ParquetRow, 10)
	chunksEncoder := storage.NewPrometheusParquetChunksEncoder()

	go func() {
		defer close(rc)
		batch := make([]storage.ParquetRow, 0, batchSize)
		batchMutex := &sync.Mutex{}
		for _, metricName := range metricNames {
			if ctx.Err() != nil {
				return
			}
			p, err := idx.Postings(ctx, labels.MetricName, metricName)
			if err != nil {
				return
			}
			eg := &errgroup.Group{}
			eg.SetLimit(runtime.GOMAXPROCS(0))

			for p.Next() {
				chks := []chunks.Meta{}
				builder := labels.ScratchBuilder{}

				at := p.At()
				err = idx.Series(at, &builder, &chks)
				if err != nil {
					return
				}
				eg.Go(func() error {
					for i := range chks {
						chks[i].Chunk, _, err = cReader.ChunkOrIterable(chks[i])
						if err != nil {
							return err
						}
					}

					data, err := chunksEncoder.Encode(chks)
					if err != nil {
						return err
					}

					promLbls := builder.Labels()
					lbsls := make(map[string]string)

					promLbls.Range(func(l labels.Label) {
						lbsls[l.Name] = l.Value
					})

					row := storage.ParquetRow{
						Hash:    promLbls.Hash(),
						Columns: lbsls,
						Data:    data,
					}

					batchMutex.Lock()
					batch = append(batch, row)
					if len(batch) >= batchSize {
						rc <- batch
						batch = make([]storage.ParquetRow, 0, batchSize)
					}
					batchMutex.Unlock()
					return nil
				})
			}

			err = eg.Wait()
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "failed to process chunk", "err", err)
				return
			}
		}
		if len(batch) > 0 {
			rc <- batch
		}
	}()

	return rc, labelNames, total, nil
}

func getBlockTimeRange(b *bucketindex.Block, timeRanges []int64) int64 {
	timeRange := int64(0)
	// fallback logic to guess block time range based
	// on MaxTime and MinTime
	blockRange := b.MaxTime - b.MinTime
	for _, tr := range timeRanges {
		rangeStart := getStart(b.MinTime, tr)
		rangeEnd := rangeStart + tr
		if tr >= blockRange && rangeEnd >= b.MaxTime {
			timeRange = tr
			break
		}
	}
	return timeRange
}

func getStart(mint int64, tr int64) int64 {
	// Compute start of aligned time range of size tr closest to the current block's start.
	// This code has been copied from TSDB.
	if mint >= 0 {
		return tr * (mint / tr)
	}

	return tr * ((mint - tr + 1) / tr)
}

func truncateByteArray(value []byte, sizeLimit int) []byte {
	if len(value) > sizeLimit {
		value = value[:sizeLimit]
	}
	return value
}
