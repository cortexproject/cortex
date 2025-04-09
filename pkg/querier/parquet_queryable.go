package querier

import (
	"context"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_storage "github.com/cortexproject/cortex/pkg/storage/parquet"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	parquetFileName = "block.parquet"
	ModuleName      = "parquet-queryable"
	seriesHash      = "__series_hash__"
)

type ParquetQueryable struct {
	services.Service

	finder               *ParquetBucketIndexBlocksFinder
	logger               log.Logger
	queryStoreAfter      time.Duration
	queryIngestersWithin time.Duration
	limits               BlocksStoreLimits
	bucket               objstore.InstrumentedBucket
	chunkDecoder         *cortex_storage.PrometheusParquetChunksDecoder
	asyncRead            bool
	dictionaryCacheSize  int
	projectionPushdown   bool

	metrics *metrics

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	readerCache  *cortex_storage.Cache[*cortex_storage.ParquetReader]
	cacheMetrics *cortex_storage.CacheMetrics
}

type metrics struct {
	parquetFileOpenDuration prometheus.Histogram
	pagesSkippedPageIndex   *prometheus.CounterVec
	projectionPushdown      prometheus.Counter
	dataFetchDuration       *prometheus.HistogramVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		parquetFileOpenDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_parquet_querier_parquet_file_open_duration_seconds",
			Help:                            "native histogram for parquet file open latency",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}),
		pagesSkippedPageIndex: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_querier_pages_skipped_page_index_total",
			Help: "total number of pages skipped using page index for each column",
		}, []string{"column"}),
		projectionPushdown: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_parquet_querier_projection_pushdowns_total",
			Help: "total number of projection pushdowns",
		}),
		dataFetchDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_parquet_querier_data_fetch_duration",
			Help:                            "Time spent to fetch data",
			Buckets:                         instrument.DefBuckets,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"method"}),
	}
}

func NewParquetStoreQueryable(
	limits BlocksStoreLimits,
	config Config,
	storageCfg cortex_tsdb.BlocksStorageConfig,
	logger log.Logger,
	reg prometheus.Registerer,
) (*ParquetQueryable, error) {
	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, nil, ModuleName, logger, reg)
	if err != nil {
		return nil, err
	}

	cacheMetrics := cortex_storage.NewCacheMetrics(reg)
	cache, err := cortex_storage.NewCache[*cortex_storage.ParquetReader]("files", 1000, cacheMetrics)
	if err != nil {
		return nil, err
	}

	indexLoaderConfig := bucketindex.LoaderConfig{
		CheckInterval:         time.Minute,
		UpdateOnStaleInterval: storageCfg.BucketStore.SyncInterval,
		UpdateOnErrorInterval: storageCfg.BucketStore.BucketIndex.UpdateOnErrorInterval,
		IdleTimeout:           storageCfg.BucketStore.BucketIndex.IdleTimeout,
	}
	bucketIndexBlocksFinder := BucketIndexBlocksFinderConfig{
		IndexLoader: indexLoaderConfig,
	}
	finder := NewParquetBucketIndexBlocksFinder(bucketIndexBlocksFinder, bucketClient, limits, util_log.Logger, prometheus.DefaultRegisterer)
	manager, err := services.NewManager(finder)

	q := &ParquetQueryable{
		bucket:               bucketClient,
		finder:               finder,
		queryStoreAfter:      config.QueryStoreAfter,
		queryIngestersWithin: config.QueryIngestersWithin,
		logger:               logger,
		subservices:          manager,
		subservicesWatcher:   services.NewFailureWatcher(),
		limits:               limits,
		chunkDecoder:         cortex_storage.NewPrometheusParquetChunksDecoder(),
		asyncRead:            true,
		dictionaryCacheSize:  1024,
		metrics:              newMetrics(reg),
		readerCache:          cache,
		cacheMetrics:         cacheMetrics,
		projectionPushdown:   false,
	}

	q.Service = services.NewBasicService(q.starting, q.running, q.stopping)

	return q, nil
}

func (q *ParquetQueryable) starting(ctx context.Context) error {
	q.subservicesWatcher.WatchManager(q.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, q.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks storage queryable subservices")
	}

	return nil
}

func (q *ParquetQueryable) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-q.subservicesWatcher.Chan():
			return errors.Wrap(err, "block storage queryable subservice failed")
		}
	}
}

func (q *ParquetQueryable) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), q.subservices)
}

// Querier returns a new Querier on the storage.
func (q *ParquetQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &parquetQuerier{
		minT:                 mint,
		maxT:                 maxt,
		finder:               q.finder,
		limits:               q.limits,
		logger:               q.logger,
		queryStoreAfter:      q.queryStoreAfter,
		queryIngestersWithin: q.queryIngestersWithin,
		bucket:               q.bucket,
		chunkDecoder:         q.chunkDecoder,
		asyncRead:            q.asyncRead,
		dictionaryCacheSize:  q.dictionaryCacheSize,
		metrics:              q.metrics,
		readerCache:          q.readerCache,
		cacheMetrics:         q.cacheMetrics,
		projectionPushdown:   q.projectionPushdown,
	}, nil
}

type parquetQuerier struct {
	minT, maxT int64
	finder     *ParquetBucketIndexBlocksFinder
	limits     BlocksStoreLimits
	logger     log.Logger
	bucket     objstore.InstrumentedBucket

	metrics *metrics

	// If set, the querier manipulates the max time to not be greater than
	// "now - queryStoreAfter" so that most recent blocks are not queried.
	queryStoreAfter      time.Duration
	queryIngestersWithin time.Duration
	asyncRead            bool
	dictionaryCacheSize  int
	projectionPushdown   bool

	readerCache  *cortex_storage.Cache[*cortex_storage.ParquetReader]
	cacheMetrics *cortex_storage.CacheMetrics

	chunkDecoder *cortex_storage.PrometheusParquetChunksDecoder
}

func (q *parquetQuerier) findBlocks(ctx context.Context, userID string, logger log.Logger, minT, maxT int64, matchers []*labels.Matcher) (bucketindex.Blocks, error) {
	// If queryStoreAfter is enabled, we do manipulate the query maxt to query samples up until
	// now - queryStoreAfter, because the most recent time range is covered by ingesters. This
	// optimization is particularly important for the blocks storage because can be used to skip
	// querying most recent not-compacted-yet blocks from the storage.
	if q.queryStoreAfter > 0 {
		now := time.Now()
		origMaxT := maxT
		maxT = min(maxT, util.TimeToMillis(now.Add(-q.queryStoreAfter)))

		if origMaxT != maxT {
			level.Debug(logger).Log("msg", "the max time of the query to blocks storage has been manipulated", "original", origMaxT, "updated", maxT)
		}

		if maxT < minT {
			return nil, nil
		}
	}
	// No deleted block for now.
	blocks, _, err := q.finder.GetBlocks(ctx, userID, minT, maxT, matchers)
	return blocks, err
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *parquetQuerier) Select(ctx context.Context, _ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	n := time.Now()
	ss := q.selectSorted(ctx, sp, matchers...)
	q.metrics.dataFetchDuration.WithLabelValues("select").Observe(time.Since(n).Seconds())
	return ss
}

func (q *parquetQuerier) searchWithMatchers(ctx context.Context, reader *cortex_storage.ParquetReader, sts *cortex_storage.Stats, matchers ...*labels.Matcher) ([][]int64, error) {
	start := time.Now()
	defer func() {
		sts.AddSearchWallTime(time.Since(start).Milliseconds())
	}()
	span, ctx := opentracing.StartSpanFromContext(ctx, "parquetQuerier.searchWithMatchers")
	defer span.Finish()
	nameMatcher, matchers := equalNameMatcher(matchers...)
	var (
		rows        [][]int64
		err         error
		fullScanned bool
	)
	if nameMatcher != nil {
		rows, err = reader.SearchRows(ctx, nameMatcher.Name, nameMatcher.Value, sts)
		if err != nil {
			return nil, err
		}
		sts.AddColumnSearched(nameMatcher.Name, len(rows))

		if len(rows) == 0 {
			return nil, nil
		}
		fullScanned = true
	}

	for _, matcher := range matchers {
		// .* regexp matches any string.
		if matcher.Type == labels.MatchRegexp && matcher.Value == ".*" {
			sts.AddColumnSearched(matcher.Name, len(rows))
			continue
		}

		// .* not regexp doesn't match any string.
		// TODO: skip such matcher earlier.
		if matcher.Type == labels.MatchNotRegexp && matcher.Value == ".*" {
			sts.AddColumnSearched(matcher.Name, 0)
			return nil, nil
		}

		// Only full scan if we never full scan before.
		rows, err = reader.ScanRows(ctx, rows, !fullScanned, matcher, sts)
		if err != nil {
			return nil, err
		}
		sts.AddColumnSearched(matcher.Name, len(rows))
		if len(rows) == 0 {
			return nil, nil
		}

		if !fullScanned {
			fullScanned = true
		}
	}
	return rows, nil
}

func (q *parquetQuerier) selectSorted(ctx context.Context, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	if len(matchers) == 0 {
		return storage.ErrSeriesSet(errors.New("no matchers"))
	}

	spanLog, ctx := spanlogger.New(ctx, "ParquetQuerier.selectSorted")
	defer spanLog.Span.Finish()
	requestStart := time.Now()

	var (
		queryLimiter = limiter.QueryLimiterFromContextWithFallback(ctx)
		reqStats     = stats.FromContext(ctx)
	)

	minT, maxT, limit := q.minT, q.maxT, int64(0)
	if sp != nil {
		minT, maxT, limit = sp.Start, sp.End, int64(sp.Limit)
	}
	spanLog.SetTag("minT", minT)
	spanLog.SetTag("maxT", maxT)
	skipChunks := sp != nil && sp.Func == "series"
	// Only pushdown if we know in the we are not querying ingesters. Add 1 hour buffer to avoid accidental overalp.
	queryIngesters := q.queryIngestersWithin == 0 || maxT >= util.TimeToMillis(time.Now().Add(-q.queryIngestersWithin).Add(-time.Hour))

	var (
		grouping []string
		by       bool
	)
	// For now, we simplify things and only think about pruning columns if not querying ingesters
	projectionPushdown := false
	if q.projectionPushdown && !queryIngesters && sp != nil {
		projectionPushdown = true
		grouping = sp.Grouping
		by = sp.By
	}

	var (
		resSeriesSets = []storage.SeriesSet(nil)
		resultMtx     sync.Mutex
	)

	sts := cortex_storage.NewStats()

	blocks, err := q.findBlocks(ctx, userID, q.logger, minT, maxT, matchers)
	spanLog.SetTag("blocks", len(blocks))

	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if len(blocks) == 0 {
		return storage.EmptySeriesSet()
	}
	prs, err := q.blocksToParquetReader(ctx, userID, blocks, sts)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	defer func() {
		sts.AddRequestDuration(time.Since(requestStart).Milliseconds())
		extraFields := []interface{}{
			"mint", minT,
			"maxt", maxT,
			"matchers", util.LabelMatchersToString(matchers),
			"projectionPushdown", projectionPushdown,
			"by", by,
			"step", sp.Step,
			"range", sp.Range,
			"func", sp.Func,
			"fetchedSeries", reqStats.LoadFetchedSeries(),
			"fetchedChunks", reqStats.LoadFetchedChunks(),
			"fetchedChunkBytes", reqStats.LoadFetchedChunkBytes(),
			"fetchedSamples", reqStats.LoadFetchedSamples(),
			"fetchedDataBytes", reqStats.LoadFetchedDataBytes(),
		}
		if len(grouping) > 0 {
			extraFields = append(extraFields, "grouping", strings.Join(grouping, ","))
		}
		sts.Display(spanLog, extraFields...)
	}()

	eg := &errgroup.Group{}
	for i, reader := range prs {
		if reader == nil {
			continue
		}
		sts.AddFilesScanned(1)
		b := blocks[i]
		reader := reader
		eg.Go(func() error {
			start := time.Now()
			defer func() {
				sts.AddStorageWallTime(time.Since(start).Milliseconds())
			}()
			rows, err := q.searchWithMatchers(ctx, reader, sts, matchers...)
			if err != nil {
				level.Error(q.logger).Log("msg", "error querying series", "err", err, "block", b.ID)
				return errors.Wrapf(err, "block id %s", b.ID)
			}
			dataColsIdx := q.getDataColsIdx(skipChunks, reader.DataColsSize(), b)
			prs, err := reader.Materialize(ctx, rows, dataColsIdx, grouping, by, sts)
			if err != nil {
				level.Error(q.logger).Log("msg", "error materializing series", "err", err, "block", b.ID)
				return errors.Wrapf(err, "block id %s", b.ID)
			}
			sts.AddRowsFetched(len(prs))
			ss, err := newParquetRowsSeriesSet(true, prs, q.minT, q.maxT, skipChunks, q.chunkDecoder, projectionPushdown && by, queryLimiter, reqStats, sts)
			if err != nil {
				return err
			}
			resultMtx.Lock()
			resSeriesSets = append(resSeriesSets, ss)
			defer resultMtx.Unlock()
			return nil
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return storage.ErrSeriesSet(err)
	}

	if len(resSeriesSets) == 0 {
		return storage.EmptySeriesSet()
	}

	return storage.NewMergeSeriesSet(resSeriesSets, int(limit), storage.ChainedSeriesMerge)
}

func (q *parquetQuerier) getDataColsIdx(skipChunks bool, dataCols int, b *bucketindex.Block) []int {
	if skipChunks {
		return []int{}
	}
	idxInit := 0
	idxEnd := dataCols - 1
	dataColsIdx := make([]int, 0, dataCols)
	dataColLength := time.Hour * 24 / time.Duration(dataCols)

	if q.minT > b.MinTime {
		hour := time.UnixMilli(q.minT).UTC().Hour()

		idxInit = (hour / int(dataColLength.Hours())) % dataCols
	}

	if q.maxT < b.MaxTime {
		hour := time.UnixMilli(q.maxT).UTC().Hour()
		idxEnd = (hour / int(dataColLength.Hours())) % dataCols
	}

	for i := idxInit; i <= idxEnd; i++ {
		dataColsIdx = append(dataColsIdx, i)
	}
	return dataColsIdx
}

func equalNameMatcher(matchers ...*labels.Matcher) (*labels.Matcher, []*labels.Matcher) {
	others := make([]*labels.Matcher, 0, len(matchers))
	var metricNameMatcher *labels.Matcher
	for _, m := range matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			metricNameMatcher = m
		} else {
			others = append(others, m)
		}
	}
	return metricNameMatcher, others
}

func (q *parquetQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanLog, ctx := spanlogger.New(ctx, "ParquetQuerier.LabelNames")
	defer spanLog.Span.Finish()

	minT, maxT, limit := q.minT, q.maxT, int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	var (
		resMtx      sync.Mutex
		resNameSets = [][]string{}
		resWarnings = annotations.Annotations(nil)
	)
	sts := cortex_storage.NewStats()
	defer func() {
		extraFields := []interface{}{
			"mint", minT,
			"maxt", maxT,
			"matchers", util.LabelMatchersToString(matchers),
		}
		sts.Display(spanLog, extraFields...)
	}()

	blocks, err := q.findBlocks(ctx, userID, q.logger, minT, maxT, matchers)
	if err != nil {
		return nil, nil, err
	}

	prs, err := q.blocksToParquetReader(ctx, userID, blocks, sts)
	if err != nil {
		return nil, nil, err
	}
	eg := &errgroup.Group{}
	// Happy path.
	for _, reader := range prs {
		if reader == nil {
			continue
		}
		sts.AddFilesScanned(1)
		eg.Go(func() error {
			if len(matchers) == 0 {
				resMtx.Lock()
				resNameSets = append(resNameSets, reader.ColumnNames())
				defer resMtx.Unlock()
			} else {
				rows, err := q.searchWithMatchers(ctx, reader, sts, matchers...)
				if err != nil {
					return err
				}
				out, err := reader.MaterializeColumnNames(ctx, rows, sts)
				if err != nil {
					return err
				}
				resMtx.Lock()
				resNameSets = append(resNameSets, out)
				defer resMtx.Unlock()
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	return strutil.MergeSlices(int(limit), resNameSets...), resWarnings, nil
}

func (q *parquetQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanLog, ctx := spanlogger.New(ctx, "ParquetQuerier.LabelValues")
	defer spanLog.Span.Finish()

	minT, maxT, limit := q.minT, q.maxT, int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	var (
		resValueSets = [][]string{}
		resWarnings  = annotations.Annotations(nil)

		resultMtx sync.Mutex
	)

	sts := cortex_storage.NewStats()
	defer func() {
		extraFields := []interface{}{
			"name", name,
			"mint", minT,
			"maxt", maxT,
			"matchers", util.LabelMatchersToString(matchers),
		}
		sts.Display(spanLog, extraFields...)
	}()

	blocks, err := q.findBlocks(ctx, userID, q.logger, minT, maxT, matchers)
	if err != nil {
		return nil, nil, err
	}

	prs, err := q.blocksToParquetReader(ctx, userID, blocks, sts)
	if err != nil {
		return nil, nil, err
	}
	eg := &errgroup.Group{}
	// Happy path.
	for _, reader := range prs {
		if reader == nil {
			continue
		}
		sts.AddFilesScanned(1)
		eg.Go(func() error {
			if !reader.ColumnExists(name) {
				return nil
			}

			var (
				values []string
				err    error
			)
			if len(matchers) == 0 {
				values, err = reader.ColumnValues(ctx, name)
				if err != nil {
					return err
				}
			} else {
				rows, err := q.searchWithMatchers(ctx, reader, sts, matchers...)
				if err != nil {
					return err
				}
				vals, err := reader.MaterializeColumn(ctx, rows, sts, name)
				if err != nil {
					return err
				}
				values = parquetValuesToStrings(vals[0])
			}

			resultMtx.Lock()
			resValueSets = append(resValueSets, values)
			defer resultMtx.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	return strutil.MergeSlices(int(limit), resValueSets...), resWarnings, nil
}

func (q *parquetQuerier) Close() error {
	return nil
}

type bReadAt struct {
	path string
	obj  objstore.InstrumentedBucketReader
	ctx  context.Context

	m *metrics
}

func (b *bReadAt) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := b.obj.GetRange(b.ctx, b.path, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err = rc.Read(p)
	if err == io.EOF {
		err = nil
	}
	return
}

func (b *bReadAt) CreateReadAtWithContext(ctx context.Context) io.ReaderAt {
	return &bReadAt{
		path: b.path,
		obj:  b.obj,
		m:    b.m,
		ctx:  ctx,
	}
}

func (q *parquetQuerier) blocksToParquetReader(ctx context.Context, uname string, blocks bucketindex.Blocks, sts *cortex_storage.Stats) ([]*cortex_storage.ParquetReader, error) {
	start := time.Now()
	defer func() {
		sts.AddOpenParquetReaderDuration(time.Since(start).Milliseconds())
	}()

	span, _ := opentracing.StartSpanFromContext(ctx, "parquetQuerier.blocksToParquetReader")
	defer span.Finish()
	output := make([]*cortex_storage.ParquetReader, len(blocks))
	mtx := &sync.Mutex{}
	eg := &errgroup.Group{}
	readerCacheMiss := 0
	defer func() {
		sts.AddParquetReaderCacheMiss(readerCacheMiss)
	}()
	for i, block := range blocks {
		id := block.ID.String()
		name := path.Join(uname, id, parquetFileName)

		// Try to get from cache.
		if reader := q.readerCache.Get(name); reader != nil {
			output[i] = reader
			continue
		}
		readerCacheMiss++

		eg.Go(func() error {
			attr, err := q.bucket.Attributes(ctx, name)
			if err != nil {
				if q.bucket.IsObjNotFoundErr(err) {
					return nil
				}
				return err
			}
			t := time.Now()
			r := &bReadAt{path: name, obj: q.bucket, m: q.metrics}
			pr, err := cortex_storage.NewParquetReader(r.CreateReadAtWithContext, attr.Size, q.asyncRead, q.cacheMetrics, q.dictionaryCacheSize, q.metrics.pagesSkippedPageIndex, q.metrics.projectionPushdown)
			if err != nil {
				return err
			}
			mtx.Lock()
			output[i] = pr
			mtx.Unlock()
			q.readerCache.Set(name, pr)
			q.metrics.parquetFileOpenDuration.Observe(time.Since(t).Seconds())
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return output, nil
}

func parquetValuesToStrings(values []parquet.Value) []string {
	s := make(map[string]struct{}, len(values))
	for _, value := range values {
		s[value.String()] = struct{}{}
	}
	result := make([]string, 0, len(s))
	for k := range s {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

type RemoveHashLabelSeriesSet struct {
	s       storage.SeriesSet
	builder *labels.Builder
}

func newRemoveHashLabelSeriesSet(s storage.SeriesSet) *RemoveHashLabelSeriesSet {
	return &RemoveHashLabelSeriesSet{s: s, builder: labels.NewBuilder(labels.EmptyLabels())}
}

func (ss *RemoveHashLabelSeriesSet) Next() bool {
	return ss.s.Next()
}

// At returns full series. Returned series should be iterable even after Next is called.
func (ss *RemoveHashLabelSeriesSet) At() storage.Series {
	series := ss.s.At()
	ss.builder.Reset(series.Labels())

	return &storage.SeriesEntry{
		Lset:             ss.builder.Del(seriesHash).Labels(),
		SampleIteratorFn: series.Iterator,
	}
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (ss *RemoveHashLabelSeriesSet) Err() error {
	return ss.s.Err()
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (ss *RemoveHashLabelSeriesSet) Warnings() annotations.Annotations {
	return ss.s.Warnings()
}

type ParquetRowsSeriesSet struct {
	series []Series
	curr   Series
	err    error
}

type Series struct {
	lbls       labels.Labels
	chks       []chunks.Meta
	mint, maxt int64
}

func (s Series) Labels() labels.Labels {
	return s.lbls
}

func (s Series) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	iters := make([]chunkenc.Iterator, 0, len(s.chks))
	for _, chk := range s.chks {
		iters = append(iters, chk.Chunk.Iterator(nil))
	}
	return dedup.NewBoundedSeriesIterator(newChunkSeriesIterator(iters), s.mint, s.maxt)
}

func newParquetRowsSeriesSet(sorted bool, rows []cortex_storage.ParquetRow, mint, maxt int64, skipChunks bool, chunksDecoder *cortex_storage.PrometheusParquetChunksDecoder, projectionPushdown bool, queryLimiter *limiter.QueryLimiter, reqStats *stats.QueryStats, sts *cortex_storage.Stats) (*ParquetRowsSeriesSet, error) {
	start := time.Now()
	defer func() {
		sts.AddCreateSeriesSetWallTime(time.Since(start).Milliseconds())
	}()
	series := make([]Series, 0, 1024)
	lblsBuilder := labels.NewScratchBuilder(10)
	var (
		chunksCount     uint64
		chunksSizeBytes uint64
		labelsSizeBytes uint64
		samples         uint64
		overfetchedRows int
	)
	for i := range rows {
		var (
			chks []chunks.Meta
			err  error
			skip bool
		)
		if !skipChunks {
			chks, skip, err = getChunksFromParquetRow(chunksDecoder, rows[i], mint, maxt)
			if err != nil {
				return nil, err
			}
			if skip {
				overfetchedRows++
				continue
			}
		}

		lblsBuilder.Reset()
		for n, v := range rows[i].Columns {
			lblsBuilder.Add(n, v)
		}
		// If we are not pruning columns then there is no need to add series hash as label.
		if projectionPushdown {
			lblsBuilder.Add(seriesHash, strconv.FormatUint(rows[i].Hash, 10))
		}

		lblsBuilder.Sort()
		lbls := lblsBuilder.Labels()
		if err := queryLimiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(lbls)); err != nil {
			return nil, validation.LimitError(err.Error())
		}

		chksCount := 0
		chunksBytes := 0
		for _, meta := range chks {
			chunksBytes += len(meta.Chunk.Bytes())
			samples += uint64(meta.Chunk.NumSamples())
		}

		if chunkBytesLimitErr := queryLimiter.AddChunkBytes(chunksBytes); chunkBytesLimitErr != nil {
			return nil, validation.LimitError(chunkBytesLimitErr.Error())
		}
		if chunkLimitErr := queryLimiter.AddChunks(chksCount); chunkLimitErr != nil {
			return nil, validation.LimitError(chunkLimitErr.Error())
		}

		labelsBytes := 0
		lbls.Range(func(l labels.Label) {
			labelsBytes += len(l.Name) + len(l.Value)
		})

		if dataBytesLimitErr := queryLimiter.AddDataBytes(chunksBytes + labelsBytes); dataBytesLimitErr != nil {
			return nil, validation.LimitError(dataBytesLimitErr.Error())
		}

		chunksCount += uint64(chksCount)
		chunksSizeBytes += uint64(chunksBytes)
		labelsSizeBytes += uint64(labelsBytes)
		series = append(series, Series{
			lbls: lbls,
			chks: chks,
			mint: mint,
			maxt: maxt,
		})
	}

	sts.AddOverfetchedRows(overfetchedRows)

	reqStats.AddFetchedSeries(uint64(len(series)))
	reqStats.AddFetchedChunks(chunksCount)
	reqStats.AddFetchedChunkBytes(chunksSizeBytes)
	reqStats.AddFetchedSamples(samples)
	reqStats.AddFetchedDataBytes(chunksSizeBytes + labelsSizeBytes)

	if sorted {
		sort.Sort(byLabels(series))
	}
	return &ParquetRowsSeriesSet{
		series: series,
	}, nil
}

func getChunksFromParquetRow(chunksDecoder *cortex_storage.PrometheusParquetChunksDecoder, row cortex_storage.ParquetRow, mint, maxt int64) ([]chunks.Meta, bool, error) {
	chks, err := chunksDecoder.Decode(row.Data, mint, maxt)
	if err != nil {
		return nil, false, err
	}
	if len(chks) == 0 {
		return nil, true, nil
	}
	return chks, false, nil
}

type byLabels []Series

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }

func (ss *ParquetRowsSeriesSet) Next() bool {
	if ss.err != nil {
		return false
	}
	if len(ss.series) == 0 {
		return false
	}
	ss.curr, ss.series = ss.series[0], ss.series[1:]
	return true
}

// At returns full series. Returned series should be iterable even after Next is called.
func (ss *ParquetRowsSeriesSet) At() storage.Series {
	return ss.curr
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (ss *ParquetRowsSeriesSet) Err() error {
	return ss.err
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (ss *ParquetRowsSeriesSet) Warnings() annotations.Annotations {
	return nil
}

type chunkSeriesIterator struct {
	chunks  []chunkenc.Iterator
	i       int
	lastVal chunkenc.ValueType
}

func newChunkSeriesIterator(cs []chunkenc.Iterator) chunkenc.Iterator {
	if len(cs) == 0 {
		// This should not happen. StoreAPI implementations should not send empty results.
		return errSeriesIterator{err: errors.Errorf("store returned an empty result")}
	}
	return &chunkSeriesIterator{chunks: cs}
}

func (it *chunkSeriesIterator) Seek(t int64) chunkenc.ValueType {
	// We generally expect the chunks already to be cut down
	// to the range we are interested in. There's not much to be gained from
	// hopping across chunks so we just call next until we reach t.
	for {
		ct := it.AtT()
		if ct >= t {
			return it.lastVal
		}
		it.lastVal = it.Next()
		if it.lastVal == chunkenc.ValNone {
			return chunkenc.ValNone
		}
	}
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.chunks[it.i].At()
}

func (it *chunkSeriesIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return it.chunks[it.i].AtHistogram(h)
}

func (it *chunkSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.chunks[it.i].AtFloatHistogram(fh)
}

func (it *chunkSeriesIterator) AtT() int64 {
	return it.chunks[it.i].AtT()
}

func (it *chunkSeriesIterator) Next() chunkenc.ValueType {
	lastT := it.AtT()

	if valueType := it.chunks[it.i].Next(); valueType != chunkenc.ValNone {
		it.lastVal = valueType
		return valueType
	}
	if it.Err() != nil {
		return chunkenc.ValNone
	}
	if it.i >= len(it.chunks)-1 {
		return chunkenc.ValNone
	}
	// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
	// We must ensure to skip any overlapping range between adjacent chunks.
	it.i++
	return it.Seek(lastT + 1)
}

func (it *chunkSeriesIterator) Err() error {
	return it.chunks[it.i].Err()
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) chunkenc.ValueType { return chunkenc.ValNone }
func (errSeriesIterator) Next() chunkenc.ValueType      { return chunkenc.ValNone }
func (errSeriesIterator) At() (int64, float64)          { return 0, 0 }
func (errSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}
func (errSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}
func (errSeriesIterator) AtT() int64    { return 0 }
func (it errSeriesIterator) Err() error { return it.err }
