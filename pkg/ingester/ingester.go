package ingester

import (
	"context"
	"flag"
	"fmt"
	"html"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/zeropool"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/shipper"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querysharding"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	"github.com/cortexproject/cortex/pkg/util/extract"
	logutil "github.com/cortexproject/cortex/pkg/util/log"
	util_math "github.com/cortexproject/cortex/pkg/util/math"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	// RingKey is the key under which we store the ingesters ring in the KVStore.
	RingKey = "ring"
)

const (
	errTSDBCreateIncompatibleState = "cannot create a new TSDB while the ingester is not in active state (current state: %s)"
	errTSDBIngestWithTimestamp     = "err: %v. series=%s"               // Using error.Wrap puts the message before the error and if the series is too long, its truncated.
	errTSDBIngest                  = "err: %v. timestamp=%s, series=%s" // Using error.Wrap puts the message before the error and if the series is too long, its truncated.
	errTSDBIngestExemplar          = "err: %v. timestamp=%s, series=%s, exemplar=%s"

	// Jitter applied to the idle timeout to prevent compaction in all ingesters concurrently.
	compactionIdleTimeoutJitter = 0.25

	instanceIngestionRateTickInterval = time.Second

	// Number of timeseries to return in each batch of a QueryStream.
	queryStreamBatchSize    = 128
	metadataStreamBatchSize = 128

	// Discarded Metadata metric labels.
	perUserMetadataLimit   = "per_user_metadata_limit"
	perMetricMetadataLimit = "per_metric_metadata_limit"

	// Period at which to attempt purging metadata from memory.
	metadataPurgePeriod = 5 * time.Minute

	// Period at which we should reset the max inflight query requests counter.
	maxInflightRequestResetPeriod = 1 * time.Minute

	labelSetMetricsTickInterval = 30 * time.Second
)

var (
	errExemplarRef      = errors.New("exemplars not ingested because series not already present")
	errIngesterStopping = errors.New("ingester stopping")
	errNoUserDb         = errors.New("no user db")

	tsChunksPool zeropool.Pool[[]client.TimeSeriesChunk]
)

// Config for an Ingester.
type Config struct {
	LifecyclerConfig ring.LifecyclerConfig `yaml:"lifecycler"`

	// Config for metadata purging.
	MetadataRetainPeriod time.Duration `yaml:"metadata_retain_period"`

	RateUpdatePeriod            time.Duration `yaml:"rate_update_period"`
	UserTSDBConfigsUpdatePeriod time.Duration `yaml:"user_tsdb_configs_update_period"`

	ActiveSeriesMetricsEnabled      bool          `yaml:"active_series_metrics_enabled"`
	ActiveSeriesMetricsUpdatePeriod time.Duration `yaml:"active_series_metrics_update_period"`
	ActiveSeriesMetricsIdleTimeout  time.Duration `yaml:"active_series_metrics_idle_timeout"`

	// Use blocks storage.
	BlocksStorageConfig cortex_tsdb.BlocksStorageConfig `yaml:"-"`

	// UploadCompactedBlocksEnabled enables uploading compacted blocks.
	UploadCompactedBlocksEnabled bool `yaml:"upload_compacted_blocks_enabled"`

	// Injected at runtime and read from the distributor config, required
	// to accurately apply global limits.
	DistributorShardingStrategy string `yaml:"-"`
	DistributorShardByAllLabels bool   `yaml:"-"`

	// Injected at runtime and read from querier config.
	QueryIngestersWithin time.Duration `yaml:"-"`

	DefaultLimits    InstanceLimits         `yaml:"instance_limits"`
	InstanceLimitsFn func() *InstanceLimits `yaml:"-"`

	IgnoreSeriesLimitForMetricNames string `yaml:"ignore_series_limit_for_metric_names"`

	// For testing, you can override the address and ID of this ingester.
	ingesterClientFactory func(addr string, cfg client.Config) (client.HealthAndIngesterClient, error)

	// For admin contact details
	AdminLimitMessage string `yaml:"admin_limit_message"`

	LabelsStringInterningEnabled bool `yaml:"labels_string_interning_enabled"`

	// DisableChunkTrimming allows to disable trimming of matching series chunks based on query Start and End time.
	// When disabled, the result may contain samples outside the queried time range but Select() performances
	// may be improved.
	DisableChunkTrimming bool `yaml:"disable_chunk_trimming"`

	// Maximum number of entries in the matchers cache. 0 to disable.
	MatchersCacheMaxItems int `yaml:"matchers_cache_max_items"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.LifecyclerConfig.RegisterFlags(f)

	f.DurationVar(&cfg.MetadataRetainPeriod, "ingester.metadata-retain-period", 10*time.Minute, "Period at which metadata we have not seen will remain in memory before being deleted.")

	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-user ingestion rates.")
	f.DurationVar(&cfg.UserTSDBConfigsUpdatePeriod, "ingester.user-tsdb-configs-update-period", 15*time.Second, "Period with which to update the per-user tsdb config.")
	f.BoolVar(&cfg.ActiveSeriesMetricsEnabled, "ingester.active-series-metrics-enabled", true, "Enable tracking of active series and export them as metrics.")
	f.DurationVar(&cfg.ActiveSeriesMetricsUpdatePeriod, "ingester.active-series-metrics-update-period", 1*time.Minute, "How often to update active series metrics.")
	f.DurationVar(&cfg.ActiveSeriesMetricsIdleTimeout, "ingester.active-series-metrics-idle-timeout", 10*time.Minute, "After what time a series is considered to be inactive.")

	f.BoolVar(&cfg.UploadCompactedBlocksEnabled, "ingester.upload-compacted-blocks-enabled", true, "Enable uploading compacted blocks.")
	f.Float64Var(&cfg.DefaultLimits.MaxIngestionRate, "ingester.instance-limits.max-ingestion-rate", 0, "Max ingestion rate (samples/sec) that ingester will accept. This limit is per-ingester, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInMemoryTenants, "ingester.instance-limits.max-tenants", 0, "Max users that this ingester can hold. Requests from additional users will be rejected. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInMemorySeries, "ingester.instance-limits.max-series", 0, "Max series that this ingester can hold (across all tenants). Requests to create additional series will be rejected. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInflightPushRequests, "ingester.instance-limits.max-inflight-push-requests", 0, "Max inflight push requests that this ingester can handle (across all tenants). Additional requests will be rejected. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInflightQueryRequests, "ingester.instance-limits.max-inflight-query-requests", 0, "Max inflight query requests that this ingester can handle (across all tenants). Additional requests will be rejected. 0 = unlimited.")

	f.StringVar(&cfg.IgnoreSeriesLimitForMetricNames, "ingester.ignore-series-limit-for-metric-names", "", "Comma-separated list of metric names, for which -ingester.max-series-per-metric and -ingester.max-global-series-per-metric limits will be ignored. Does not affect max-series-per-user or max-global-series-per-metric limits.")

	f.StringVar(&cfg.AdminLimitMessage, "ingester.admin-limit-message", "please contact administrator to raise it", "Customize the message contained in limit errors")

	f.BoolVar(&cfg.LabelsStringInterningEnabled, "ingester.labels-string-interning-enabled", false, "Experimental: Enable string interning for metrics labels.")

	f.BoolVar(&cfg.DisableChunkTrimming, "ingester.disable-chunk-trimming", false, "Disable trimming of matching series chunks based on query Start and End time. When disabled, the result may contain samples outside the queried time range but select performances may be improved. Note that certain query results might change by changing this option.")
	f.IntVar(&cfg.MatchersCacheMaxItems, "ingester.matchers-cache-max-items", 0, "Maximum number of entries in the regex matchers cache. 0 to disable.")
}

func (cfg *Config) Validate() error {
	if err := cfg.LifecyclerConfig.Validate(); err != nil {
		return err
	}

	if cfg.LabelsStringInterningEnabled {
		logutil.WarnExperimentalUse("String interning for metrics labels Enabled")
	}

	return nil
}

func (cfg *Config) getIgnoreSeriesLimitForMetricNamesMap() map[string]struct{} {
	if cfg.IgnoreSeriesLimitForMetricNames == "" {
		return nil
	}

	result := map[string]struct{}{}

	for _, s := range strings.Split(cfg.IgnoreSeriesLimitForMetricNames, ",") {
		tr := strings.TrimSpace(s)
		if tr != "" {
			result[tr] = struct{}{}
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// Ingester deals with "in flight" chunks.  Based on Prometheus 1.x
// MemorySeriesStorage.
type Ingester struct {
	*services.BasicService

	cfg Config

	metrics         *ingesterMetrics
	validateMetrics *validation.ValidateMetrics

	logger log.Logger

	lifecycler         *ring.Lifecycler
	limits             *validation.Overrides
	limiter            *Limiter
	subservicesWatcher *services.FailureWatcher

	stoppedMtx sync.RWMutex // protects stopped
	stopped    bool         // protected by stoppedMtx

	// For storing metadata ingested.
	usersMetadataMtx sync.RWMutex
	usersMetadata    map[string]*userMetricsMetadata

	// Prometheus block storage
	TSDBState TSDBState

	// Rate of pushed samples. Only used by V2-ingester to limit global samples push rate.
	ingestionRate           *util_math.EwmaRate
	inflightPushRequests    atomic.Int64
	maxInflightPushRequests util_math.MaxTracker

	inflightQueryRequests    atomic.Int64
	maxInflightQueryRequests util_math.MaxTracker

	matchersCache                storecache.MatchersCache
	expandedPostingsCacheFactory *cortex_tsdb.ExpandedPostingsCacheFactory
}

// Shipper interface is used to have an easy way to mock it in tests.
type Shipper interface {
	Sync(ctx context.Context) (uploaded int, err error)
}

type tsdbState int

const (
	active          tsdbState = iota // Pushes are allowed.
	activeShipping                   // Pushes are allowed. Blocks shipping is in progress.
	forceCompacting                  // TSDB is being force-compacted.
	closing                          // Used while closing idle TSDB.
	closed                           // Used to avoid setting closing back to active in closeAndDeleteIdleUsers method.
)

// Describes result of TSDB-close check. String is used as metric label.
type tsdbCloseCheckResult string

const (
	tsdbIdle                    tsdbCloseCheckResult = "idle" // Not reported via metrics. Metrics use tsdbIdleClosed on success.
	tsdbShippingDisabled        tsdbCloseCheckResult = "shipping_disabled"
	tsdbNotIdle                 tsdbCloseCheckResult = "not_idle"
	tsdbNotCompacted            tsdbCloseCheckResult = "not_compacted"
	tsdbNotShipped              tsdbCloseCheckResult = "not_shipped"
	tsdbCheckFailed             tsdbCloseCheckResult = "check_failed"
	tsdbCloseFailed             tsdbCloseCheckResult = "close_failed"
	tsdbNotActive               tsdbCloseCheckResult = "not_active"
	tsdbDataRemovalFailed       tsdbCloseCheckResult = "data_removal_failed"
	tsdbTenantMarkedForDeletion tsdbCloseCheckResult = "tenant_marked_for_deletion"
	tsdbIdleClosed              tsdbCloseCheckResult = "idle_closed" // Success.
)

func (r tsdbCloseCheckResult) shouldClose() bool {
	return r == tsdbIdle || r == tsdbTenantMarkedForDeletion
}

type userTSDB struct {
	db              *tsdb.DB
	userID          string
	activeSeries    *ActiveSeries
	seriesInMetric  *metricCounter
	labelSetCounter *labelSetCounter
	limiter         *Limiter

	instanceSeriesCount *atomic.Int64 // Shared across all userTSDB instances created by ingester.
	instanceLimitsFn    func() *InstanceLimits

	stateMtx       sync.RWMutex
	state          tsdbState
	pushesInFlight sync.WaitGroup // Increased with stateMtx read lock held, only if state == active or activeShipping.
	readInFlight   sync.WaitGroup // Increased with stateMtx read lock held, only if state == active, activeShipping or forceCompacting.

	// Used to detect idle TSDBs.
	lastUpdate atomic.Int64

	// Thanos shipper used to ship blocks to the storage.
	shipper                 Shipper
	shipperMetadataFilePath string

	// When deletion marker is found for the tenant (checked before shipping),
	// shipping stops and TSDB is closed before reaching idle timeout time (if enabled).
	deletionMarkFound atomic.Bool

	// Unix timestamp of last deletion mark check.
	lastDeletionMarkCheck atomic.Int64

	// for statistics
	ingestedAPISamples  *util_math.EwmaRate
	ingestedRuleSamples *util_math.EwmaRate

	// Cached shipped blocks.
	shippedBlocksMtx sync.Mutex
	shippedBlocks    map[ulid.ULID]struct{}

	// Used to dedup strings and keep a single reference in memory
	labelsStringInterningEnabled bool
	interner                     util.Interner

	blockRetentionPeriod int64

	postingCache cortex_tsdb.ExpandedPostingsCache
}

// Explicitly wrapping the tsdb.DB functions that we use.

func (u *userTSDB) Appender(ctx context.Context) storage.Appender {
	return u.db.Appender(ctx)
}

func (u *userTSDB) Querier(mint, maxt int64) (storage.Querier, error) {
	return u.db.Querier(mint, maxt)
}

func (u *userTSDB) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return u.db.ChunkQuerier(mint, maxt)
}

func (u *userTSDB) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return u.db.ExemplarQuerier(ctx)
}

func (u *userTSDB) Head() *tsdb.Head {
	return u.db.Head()
}

func (u *userTSDB) Blocks() []*tsdb.Block {
	return u.db.Blocks()
}

func (u *userTSDB) Close() error {
	return u.db.Close()
}

func (u *userTSDB) Compact(ctx context.Context) error {
	return u.db.Compact(ctx)
}

func (u *userTSDB) StartTime() (int64, error) {
	return u.db.StartTime()
}

func (u *userTSDB) casState(from, to tsdbState) bool {
	u.stateMtx.Lock()
	defer u.stateMtx.Unlock()

	if u.state != from {
		return false
	}
	u.state = to
	return true
}

// compactHead compacts the Head block at specified block durations avoiding a single huge block.
func (u *userTSDB) compactHead(ctx context.Context, blockDuration int64) error {
	if !u.casState(active, forceCompacting) {
		return errors.New("TSDB head cannot be compacted because it is not in active state (possibly being closed or blocks shipping in progress)")
	}

	defer u.casState(forceCompacting, active)

	// Ingestion of samples in parallel with forced compaction can lead to overlapping blocks,
	// and possible invalidation of the references returned from Appender.GetRef().
	// So we wait for existing in-flight requests to finish. Future push requests would fail until compaction is over.
	u.pushesInFlight.Wait()

	h := u.Head()

	minTime, maxTime := h.MinTime(), h.MaxTime()

	for (minTime/blockDuration)*blockDuration != (maxTime/blockDuration)*blockDuration {
		// Data in Head spans across multiple block ranges, so we break it into blocks here.
		// Block max time is exclusive, so we do a -1 here.
		blockMaxTime := ((minTime/blockDuration)+1)*blockDuration - 1
		if err := u.db.CompactHead(tsdb.NewRangeHead(h, minTime, blockMaxTime)); err != nil {
			return err
		}

		// Get current min/max times after compaction.
		minTime, maxTime = h.MinTime(), h.MaxTime()
	}

	if err := u.db.CompactHead(tsdb.NewRangeHead(h, minTime, maxTime)); err != nil {
		return err
	}
	return u.db.CompactOOOHead(ctx)
}

// PreCreation implements SeriesLifecycleCallback interface.
func (u *userTSDB) PreCreation(metric labels.Labels) error {
	if u.limiter == nil {
		return nil
	}

	// Verify ingester's global limit
	gl := u.instanceLimitsFn()
	if gl != nil && gl.MaxInMemorySeries > 0 {
		if series := u.instanceSeriesCount.Load(); series >= gl.MaxInMemorySeries {
			return errMaxSeriesLimitReached
		}
	}

	// Total series limit.
	if err := u.limiter.AssertMaxSeriesPerUser(u.userID, int(u.Head().NumSeries())); err != nil {
		return err
	}

	// Series per metric name limit.
	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		return err
	}
	if err := u.seriesInMetric.canAddSeriesFor(u.userID, metricName); err != nil {
		return err
	}

	if err := u.labelSetCounter.canAddSeriesForLabelSet(context.TODO(), u, metric); err != nil {
		return err
	}

	if u.labelsStringInterningEnabled {
		metric.InternStrings(u.interner.Intern)
	}

	return nil
}

// PostCreation implements SeriesLifecycleCallback interface.
func (u *userTSDB) PostCreation(metric labels.Labels) {
	u.instanceSeriesCount.Inc()

	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		// This should never happen because it has already been checked in PreCreation().
		return
	}
	u.seriesInMetric.increaseSeriesForMetric(metricName)
	u.labelSetCounter.increaseSeriesLabelSet(u, metric)

	if u.postingCache != nil {
		u.postingCache.ExpireSeries(metric)
	}
}

// PostDeletion implements SeriesLifecycleCallback interface.
func (u *userTSDB) PostDeletion(metrics map[chunks.HeadSeriesRef]labels.Labels) {
	u.instanceSeriesCount.Sub(int64(len(metrics)))

	for _, metric := range metrics {
		metricName, err := extract.MetricNameFromLabels(metric)
		if err != nil {
			// This should never happen because it has already been checked in PreCreation().
			continue
		}
		u.seriesInMetric.decreaseSeriesForMetric(metricName)
		u.labelSetCounter.decreaseSeriesLabelSet(u, metric)
		if u.postingCache != nil {
			u.postingCache.ExpireSeries(metric)
		}
	}
}

// blocksToDelete filters the input blocks and returns the blocks which are safe to be deleted from the ingester.
func (u *userTSDB) blocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	if u.db == nil {
		return nil
	}
	deletable := tsdb.DefaultBlocksToDelete(u.db)(blocks)

	now := time.Now().UnixMilli()
	for _, b := range blocks {
		if now-b.MaxTime() >= u.blockRetentionPeriod {
			deletable[b.Meta().ULID] = struct{}{}
		}
	}

	if u.shipper == nil {
		return deletable
	}

	shippedBlocks := u.getCachedShippedBlocks()

	result := map[ulid.ULID]struct{}{}
	for shippedID := range shippedBlocks {
		if _, ok := deletable[shippedID]; ok {
			result[shippedID] = struct{}{}
		}
	}
	return result
}

// updateCachedShippedBlocks reads the shipper meta file and updates the cached shipped blocks.
func (u *userTSDB) updateCachedShippedBlocks() error {
	shipperMeta, err := shipper.ReadMetaFile(u.shipperMetadataFilePath)
	if os.IsNotExist(err) || os.IsNotExist(errors.Cause(err)) {
		// If the meta file doesn't exist it means the shipper hasn't run yet.
		shipperMeta = &shipper.Meta{}
	} else if err != nil {
		return err
	}

	// Build a map.
	shippedBlocks := make(map[ulid.ULID]struct{}, len(shipperMeta.Uploaded))
	for _, blockID := range shipperMeta.Uploaded {
		shippedBlocks[blockID] = struct{}{}
	}

	// Cache it.
	u.shippedBlocksMtx.Lock()
	u.shippedBlocks = shippedBlocks
	u.shippedBlocksMtx.Unlock()

	return nil
}

// getCachedShippedBlocks returns the cached shipped blocks.
func (u *userTSDB) getCachedShippedBlocks() map[ulid.ULID]struct{} {
	u.shippedBlocksMtx.Lock()
	defer u.shippedBlocksMtx.Unlock()

	// It's safe to directly return the map because it's never updated in-place.
	return u.shippedBlocks
}

// getOldestUnshippedBlockTime returns the unix timestamp with milliseconds precision of the oldest
// TSDB block not shipped to the storage yet, or 0 if all blocks have been shipped.
func (u *userTSDB) getOldestUnshippedBlockTime() uint64 {
	shippedBlocks := u.getCachedShippedBlocks()
	oldestTs := uint64(0)

	for _, b := range u.Blocks() {
		if _, ok := shippedBlocks[b.Meta().ULID]; ok {
			continue
		}

		if oldestTs == 0 || b.Meta().ULID.Time() < oldestTs {
			oldestTs = b.Meta().ULID.Time()
		}
	}

	return oldestTs
}

func (u *userTSDB) isIdle(now time.Time, idle time.Duration) bool {
	lu := u.lastUpdate.Load()

	return time.Unix(lu, 0).Add(idle).Before(now)
}

func (u *userTSDB) setLastUpdate(t time.Time) {
	u.lastUpdate.Store(t.Unix())
}

// Checks if TSDB can be closed.
func (u *userTSDB) shouldCloseTSDB(idleTimeout time.Duration) tsdbCloseCheckResult {
	if u.deletionMarkFound.Load() {
		return tsdbTenantMarkedForDeletion
	}

	if !u.isIdle(time.Now(), idleTimeout) {
		return tsdbNotIdle
	}

	// If head is not compacted, we cannot close this yet.
	if u.Head().NumSeries() > 0 {
		return tsdbNotCompacted
	}

	// Ensure that all blocks have been shipped.
	if oldest := u.getOldestUnshippedBlockTime(); oldest > 0 {
		return tsdbNotShipped
	}

	return tsdbIdle
}

// TSDBState holds data structures used by the TSDB storage engine
type TSDBState struct {
	dbs    map[string]*userTSDB // tsdb sharded by userID
	bucket objstore.Bucket

	// Value used by shipper as external label.
	shipperIngesterID string

	subservices *services.Manager

	tsdbMetrics *tsdbMetrics

	forceCompactTrigger chan requestWithUsersAndCallback
	shipTrigger         chan requestWithUsersAndCallback

	// Timeout chosen for idle compactions.
	compactionIdleTimeout time.Duration

	// Number of series in memory, across all tenants.
	seriesCount atomic.Int64

	// Head compactions metrics.
	compactionsTriggered   prometheus.Counter
	compactionsFailed      prometheus.Counter
	walReplayTime          prometheus.Histogram
	appenderAddDuration    prometheus.Histogram
	appenderCommitDuration prometheus.Histogram
	idleTsdbChecks         *prometheus.CounterVec
}

type requestWithUsersAndCallback struct {
	users    *util.AllowedTenants // if nil, all tenants are allowed.
	callback chan<- struct{}      // when compaction/shipping is finished, this channel is closed
}

func newTSDBState(bucketClient objstore.Bucket, registerer prometheus.Registerer) TSDBState {
	idleTsdbChecks := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_idle_tsdb_checks_total",
		Help: "The total number of various results for idle TSDB checks.",
	}, []string{"result"})

	idleTsdbChecks.WithLabelValues(string(tsdbShippingDisabled))
	idleTsdbChecks.WithLabelValues(string(tsdbNotIdle))
	idleTsdbChecks.WithLabelValues(string(tsdbNotCompacted))
	idleTsdbChecks.WithLabelValues(string(tsdbNotShipped))
	idleTsdbChecks.WithLabelValues(string(tsdbCheckFailed))
	idleTsdbChecks.WithLabelValues(string(tsdbCloseFailed))
	idleTsdbChecks.WithLabelValues(string(tsdbNotActive))
	idleTsdbChecks.WithLabelValues(string(tsdbDataRemovalFailed))
	idleTsdbChecks.WithLabelValues(string(tsdbTenantMarkedForDeletion))
	idleTsdbChecks.WithLabelValues(string(tsdbIdleClosed))

	return TSDBState{
		dbs:                 make(map[string]*userTSDB),
		bucket:              bucketClient,
		tsdbMetrics:         newTSDBMetrics(registerer),
		forceCompactTrigger: make(chan requestWithUsersAndCallback),
		shipTrigger:         make(chan requestWithUsersAndCallback),

		compactionsTriggered: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_tsdb_compactions_triggered_total",
			Help: "Total number of triggered compactions.",
		}),

		compactionsFailed: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_tsdb_compactions_failed_total",
			Help: "Total number of compactions that failed.",
		}),
		walReplayTime: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_tsdb_wal_replay_duration_seconds",
			Help:    "The total time it takes to open and replay a TSDB WAL.",
			Buckets: prometheus.DefBuckets,
		}),
		appenderAddDuration: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_tsdb_appender_add_duration_seconds",
			Help:    "The total time it takes for a push request to add samples to the TSDB appender.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		appenderCommitDuration: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_tsdb_appender_commit_duration_seconds",
			Help:    "The total time it takes for a push request to commit samples appended to TSDB.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),

		idleTsdbChecks: idleTsdbChecks,
	}
}

// New returns a new Ingester that uses Cortex block storage instead of chunks storage.
func New(cfg Config, limits *validation.Overrides, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	defaultInstanceLimits = &cfg.DefaultLimits
	if cfg.ingesterClientFactory == nil {
		cfg.ingesterClientFactory = client.MakeIngesterClient
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, nil, "ingester", logger, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the bucket client")
	}

	i := &Ingester{
		cfg:                          cfg,
		limits:                       limits,
		usersMetadata:                map[string]*userMetricsMetadata{},
		TSDBState:                    newTSDBState(bucketClient, registerer),
		logger:                       logger,
		ingestionRate:                util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval),
		expandedPostingsCacheFactory: cortex_tsdb.NewExpandedPostingsCacheFactory(cfg.BlocksStorageConfig.TSDB.PostingsCache),
		matchersCache:                storecache.NoopMatchersCache,
	}

	if cfg.MatchersCacheMaxItems > 0 {
		r := prometheus.NewRegistry()
		registerer.MustRegister(cortex_tsdb.NewMatchCacheMetrics("cortex_ingester", r, logger))
		i.matchersCache, err = storecache.NewMatchersCache(storecache.WithSize(cfg.MatchersCacheMaxItems), storecache.WithPromRegistry(r))
		if err != nil {
			return nil, err
		}
	}

	i.metrics = newIngesterMetrics(registerer,
		false,
		cfg.ActiveSeriesMetricsEnabled,
		i.getInstanceLimits,
		i.ingestionRate,
		&i.maxInflightPushRequests,
		&i.maxInflightQueryRequests,
		cfg.BlocksStorageConfig.TSDB.PostingsCache.Blocks.Enabled || cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.Enabled)
	i.validateMetrics = validation.NewValidateMetrics(registerer)

	// Replace specific metrics which we can't directly track but we need to read
	// them from the underlying system (ie. TSDB).
	if registerer != nil {
		registerer.Unregister(i.metrics.memSeries)

		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_series",
			Help: "The current number of series in memory.",
		}, i.getMemorySeriesMetric)

		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
			Help: "Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.",
		}, i.getOldestUnshippedBlockMetric)
	}

	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i, "ingester", RingKey, false, cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown, logger, prometheus.WrapRegistererWithPrefix("cortex_", registerer))
	if err != nil {
		return nil, err
	}
	i.subservicesWatcher = services.NewFailureWatcher()
	i.subservicesWatcher.WatchService(i.lifecycler)

	// Init the limter and instantiate the user states which depend on it
	i.limiter = NewLimiter(
		limits,
		i.lifecycler,
		cfg.DistributorShardingStrategy,
		cfg.DistributorShardByAllLabels,
		cfg.LifecyclerConfig.RingConfig.ReplicationFactor,
		cfg.LifecyclerConfig.RingConfig.ZoneAwarenessEnabled,
		cfg.AdminLimitMessage,
	)

	i.TSDBState.shipperIngesterID = i.lifecycler.ID

	// Apply positive jitter only to ensure that the minimum timeout is adhered to.
	i.TSDBState.compactionIdleTimeout = util.DurationWithPositiveJitter(i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout, compactionIdleTimeoutJitter)
	level.Info(i.logger).Log("msg", "TSDB idle compaction timeout set", "timeout", i.TSDBState.compactionIdleTimeout)

	i.BasicService = services.NewBasicService(i.starting, i.updateLoop, i.stopping)
	return i, nil
}

// NewForFlusher constructs a new Ingester to be used by flusher target.
// Compared to the 'New' method:
//   - Always replays the WAL.
//   - Does not start the lifecycler.
//
// this is a special version of ingester used by Flusher. This ingester is not ingesting anything, its only purpose is to react
// on Flush method and flush all opened TSDBs when called.
func NewForFlusher(cfg Config, limits *validation.Overrides, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, nil, "ingester", logger, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the bucket client")
	}

	i := &Ingester{
		cfg:       cfg,
		limits:    limits,
		TSDBState: newTSDBState(bucketClient, registerer),
		logger:    logger,
	}
	i.limiter = NewLimiter(
		limits,
		i.lifecycler,
		cfg.DistributorShardingStrategy,
		cfg.DistributorShardByAllLabels,
		cfg.LifecyclerConfig.RingConfig.ReplicationFactor,
		cfg.LifecyclerConfig.RingConfig.ZoneAwarenessEnabled,
		cfg.AdminLimitMessage,
	)
	i.metrics = newIngesterMetrics(registerer,
		false,
		false,
		i.getInstanceLimits,
		nil,
		&i.maxInflightPushRequests,
		&i.maxInflightQueryRequests,
		cfg.BlocksStorageConfig.TSDB.PostingsCache.Blocks.Enabled || cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.Enabled,
	)

	i.TSDBState.shipperIngesterID = "flusher"

	// This ingester will not start any subservices (lifecycler, compaction, shipping),
	// and will only open TSDBs, wait for Flush to be called, and then close TSDBs again.
	i.BasicService = services.NewIdleService(i.startingV2ForFlusher, i.stoppingV2ForFlusher)
	return i, nil
}

func (i *Ingester) startingV2ForFlusher(ctx context.Context) error {
	if err := i.openExistingTSDB(ctx); err != nil {
		// Try to rollback and close opened TSDBs before halting the ingester.
		i.closeAllTSDB()

		return errors.Wrap(err, "opening existing TSDBs")
	}

	// Don't start any sub-services (lifecycler, compaction, shipper) at all.
	return nil
}

func (i *Ingester) starting(ctx context.Context) error {
	// Important: we want to keep lifecycler running until we ask it to stop, so we need to give it independent context
	if err := i.lifecycler.StartAsync(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start lifecycler")
	}
	if err := i.lifecycler.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to start lifecycler")
	}

	if err := i.openExistingTSDB(ctx); err != nil {
		// Try to rollback and close opened TSDBs before halting the ingester.
		i.closeAllTSDB()

		return errors.Wrap(err, "opening existing TSDBs")
	}

	i.lifecycler.Join()

	// let's start the rest of subservices via manager
	servs := []services.Service(nil)

	compactionService := services.NewBasicService(nil, i.compactionLoop, nil)
	servs = append(servs, compactionService)

	if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		shippingService := services.NewBasicService(nil, i.shipBlocksLoop, nil)
		servs = append(servs, shippingService)
	}

	if i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout > 0 {
		interval := i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval
		if interval == 0 {
			interval = cortex_tsdb.DefaultCloseIdleTSDBInterval
		}
		closeIdleService := services.NewTimerService(interval, nil, i.closeAndDeleteIdleUserTSDBs, nil)
		servs = append(servs, closeIdleService)
	}

	if i.expandedPostingsCacheFactory != nil {
		interval := i.cfg.BlocksStorageConfig.TSDB.ExpandedCachingExpireInterval
		if interval == 0 {
			interval = cortex_tsdb.ExpandedCachingExpireInterval
		}
		servs = append(servs, services.NewTimerService(interval, nil, i.expirePostingsCache, nil))
	}

	var err error
	i.TSDBState.subservices, err = services.NewManager(servs...)
	if err == nil {
		err = services.StartManagerAndAwaitHealthy(ctx, i.TSDBState.subservices)
	}
	return errors.Wrap(err, "failed to start ingester components")
}

func (i *Ingester) stoppingV2ForFlusher(_ error) error {
	if !i.cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown {
		i.closeAllTSDB()
	}
	return nil
}

// runs when ingester is stopping
func (i *Ingester) stopping(_ error) error {
	// This will prevent us accepting any more samples
	i.stopIncomingRequests()
	// It's important to wait until shipper is finished,
	// because the blocks transfer should start only once it's guaranteed
	// there's no shipping on-going.
	if err := services.StopManagerAndAwaitStopped(context.Background(), i.TSDBState.subservices); err != nil {
		level.Warn(i.logger).Log("msg", "failed to stop ingester subservices", "err", err)
	}

	// Next initiate our graceful exit from the ring.
	if err := services.StopAndAwaitTerminated(context.Background(), i.lifecycler); err != nil {
		level.Warn(i.logger).Log("msg", "failed to stop ingester lifecycler", "err", err)
	}

	if !i.cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown {
		i.closeAllTSDB()
	}
	return nil
}

func (i *Ingester) updateLoop(ctx context.Context) error {
	if limits := i.getInstanceLimits(); limits != nil && *limits != (InstanceLimits{}) {
		// This check will not cover enabling instance limits in runtime, but it will do for now.
		logutil.WarnExperimentalUse("ingester instance limits")
	}

	rateUpdateTicker := time.NewTicker(i.cfg.RateUpdatePeriod)
	defer rateUpdateTicker.Stop()

	userTSDBConfigTicker := time.NewTicker(i.cfg.UserTSDBConfigsUpdatePeriod)
	defer userTSDBConfigTicker.Stop()

	ingestionRateTicker := time.NewTicker(instanceIngestionRateTickInterval)
	defer ingestionRateTicker.Stop()

	var activeSeriesTickerChan <-chan time.Time
	if i.cfg.ActiveSeriesMetricsEnabled {
		t := time.NewTicker(i.cfg.ActiveSeriesMetricsUpdatePeriod)
		activeSeriesTickerChan = t.C
		defer t.Stop()
	}

	// Similarly to the above, this is a hardcoded value.
	metadataPurgeTicker := time.NewTicker(metadataPurgePeriod)
	defer metadataPurgeTicker.Stop()

	maxTrackerResetTicker := time.NewTicker(maxInflightRequestResetPeriod)
	defer maxTrackerResetTicker.Stop()

	labelSetMetricsTicker := time.NewTicker(labelSetMetricsTickInterval)
	defer labelSetMetricsTicker.Stop()

	for {
		select {
		case <-metadataPurgeTicker.C:
			i.purgeUserMetricsMetadata()
		case <-ingestionRateTicker.C:
			i.ingestionRate.Tick()
		case <-rateUpdateTicker.C:
			i.stoppedMtx.RLock()
			for _, db := range i.TSDBState.dbs {
				db.ingestedAPISamples.Tick()
				db.ingestedRuleSamples.Tick()
			}
			i.stoppedMtx.RUnlock()

		case <-activeSeriesTickerChan:
			i.updateActiveSeries(ctx)
		case <-maxTrackerResetTicker.C:
			i.maxInflightQueryRequests.Tick()
			i.maxInflightPushRequests.Tick()
		case <-userTSDBConfigTicker.C:
			i.updateUserTSDBConfigs()
		case <-labelSetMetricsTicker.C:
			i.updateLabelSetMetrics()
		case <-ctx.Done():
			return nil
		case err := <-i.subservicesWatcher.Chan():
			return errors.Wrap(err, "ingester subservice failed")
		}
	}
}

func (i *Ingester) updateUserTSDBConfigs() {
	for _, userID := range i.getTSDBUsers() {
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil {
			continue
		}

		cfg := &config.Config{
			StorageConfig: config.StorageConfig{
				ExemplarsConfig: &config.ExemplarsConfig{
					MaxExemplars: i.getMaxExemplars(userID),
				},
				TSDBConfig: &config.TSDBConfig{
					OutOfOrderTimeWindow: time.Duration(i.limits.OutOfOrderTimeWindow(userID)).Milliseconds(),
				},
			},
		}

		// This method currently updates the MaxExemplars and OutOfOrderTimeWindow.
		err = userDB.db.ApplyConfig(cfg)
		if err != nil {
			level.Error(logutil.WithUserID(userID, i.logger)).Log("msg", "failed to update user tsdb configuration.")
		}
	}
}

// getMaxExemplars returns the maxExemplars value set in limits config.
// If limits value is set to zero, it falls back to old configuration
// in block storage config.
func (i *Ingester) getMaxExemplars(userID string) int64 {
	maxExemplarsFromLimits := i.limits.MaxExemplars(userID)

	if maxExemplarsFromLimits == 0 {
		return int64(i.cfg.BlocksStorageConfig.TSDB.MaxExemplars)
	}

	return int64(maxExemplarsFromLimits)
}

func (i *Ingester) updateActiveSeries(ctx context.Context) {
	purgeTime := time.Now().Add(-i.cfg.ActiveSeriesMetricsIdleTimeout)

	for _, userID := range i.getTSDBUsers() {
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil {
			continue
		}

		userDB.activeSeries.Purge(purgeTime)
		i.metrics.activeSeriesPerUser.WithLabelValues(userID).Set(float64(userDB.activeSeries.Active()))
		i.metrics.activeNHSeriesPerUser.WithLabelValues(userID).Set(float64(userDB.activeSeries.ActiveNativeHistogram()))
		if err := userDB.labelSetCounter.UpdateMetric(ctx, userDB, i.metrics); err != nil {
			level.Warn(i.logger).Log("msg", "failed to update per labelSet metrics", "user", userID, "err", err)
		}
	}
}

func (i *Ingester) updateLabelSetMetrics() {
	activeUserSet := make(map[string]map[uint64]struct{})
	for _, userID := range i.getTSDBUsers() {
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil {
			continue
		}

		limits := i.limits.LimitsPerLabelSet(userID)
		activeUserSet[userID] = make(map[uint64]struct{}, len(limits))
		for _, l := range limits {
			activeUserSet[userID][l.Hash] = struct{}{}
		}
	}

	// Update label set metrics in validate metrics.
	i.validateMetrics.UpdateLabelSet(activeUserSet, i.logger)
}

func (i *Ingester) RenewTokenHandler(w http.ResponseWriter, r *http.Request) {
	i.lifecycler.RenewTokens(0.1, r.Context())
	w.WriteHeader(http.StatusNoContent)
}

// ShutdownHandler triggers the following set of operations in order:
//   - Change the state of ring to stop accepting writes.
//   - Flush all the chunks.
func (i *Ingester) ShutdownHandler(w http.ResponseWriter, _ *http.Request) {
	originalFlush := i.lifecycler.FlushOnShutdown()
	// We want to flush the chunks if transfer fails irrespective of original flag.
	i.lifecycler.SetFlushOnShutdown(true)

	// In the case of an HTTP shutdown, we want to unregister no matter what.
	originalUnregister := i.lifecycler.ShouldUnregisterOnShutdown()
	i.lifecycler.SetUnregisterOnShutdown(true)

	_ = services.StopAndAwaitTerminated(context.Background(), i)
	// Set state back to original.
	i.lifecycler.SetFlushOnShutdown(originalFlush)
	i.lifecycler.SetUnregisterOnShutdown(originalUnregister)

	w.WriteHeader(http.StatusNoContent)
}

// check that ingester has finished starting, i.e. it is in Running or Stopping state.
// Why Stopping? Because ingester still runs, even when it is transferring data out in Stopping state.
// Ingester handles this state on its own (via `stopped` flag).
func (i *Ingester) checkRunningOrStopping() error {
	s := i.State()
	if s == services.Running || s == services.Stopping {
		return nil
	}
	return status.Error(codes.Unavailable, s.String())
}

// Using block store, the ingester is only available when it is in a Running state. The ingester is not available
// when stopping to prevent any read or writes to the TSDB after the ingester has closed them.
func (i *Ingester) checkRunning() error {
	s := i.State()
	if s == services.Running {
		return nil
	}
	return status.Error(codes.Unavailable, s.String())
}

// GetRef() is an extra method added to TSDB to let Cortex check before calling Add()
type extendedAppender interface {
	storage.Appender
	storage.GetRef
}

// Push adds metrics to a block
func (i *Ingester) Push(ctx context.Context, req *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}

	span, ctx := opentracing.StartSpanFromContext(ctx, "Ingester.Push")
	defer span.Finish()

	// We will report *this* request in the error too.
	inflight := i.inflightPushRequests.Inc()
	i.maxInflightPushRequests.Track(inflight)
	defer i.inflightPushRequests.Dec()

	gl := i.getInstanceLimits()
	if gl != nil && gl.MaxInflightPushRequests > 0 {
		if inflight > gl.MaxInflightPushRequests {
			return nil, errTooManyInflightPushRequests
		}
	}

	var firstPartialErr error

	// NOTE: because we use `unsafe` in deserialisation, we must not
	// retain anything from `req` past the call to ReuseSlice
	defer cortexpb.ReuseSlice(req.Timeseries)

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	il := i.getInstanceLimits()
	if il != nil && il.MaxIngestionRate > 0 {
		if rate := i.ingestionRate.Rate(); rate >= il.MaxIngestionRate {
			return nil, errMaxSamplesPushRateLimitReached
		}
	}

	db, err := i.getOrCreateTSDB(userID, false)
	if err != nil {
		return nil, wrapWithUser(err, userID)
	}

	// Ensure the ingester shutdown procedure hasn't started
	i.stoppedMtx.RLock()
	if i.stopped {
		i.stoppedMtx.RUnlock()
		return nil, errIngesterStopping
	}
	i.stoppedMtx.RUnlock()

	if err := db.acquireAppendLock(); err != nil {
		return &cortexpb.WriteResponse{}, httpgrpc.Errorf(http.StatusServiceUnavailable, "%s", wrapWithUser(err, userID).Error())
	}
	defer db.releaseAppendLock()

	// Given metadata is a best-effort approach, and we don't halt on errors
	// process it before samples. Otherwise, we risk returning an error before ingestion.
	ingestedMetadata := i.pushMetadata(ctx, userID, req.GetMetadata())

	reasonCounter := newLabelSetReasonCounters()

	// Keep track of some stats which are tracked only if the samples will be
	// successfully committed
	var (
		succeededSamplesCount         = 0
		failedSamplesCount            = 0
		succeededHistogramsCount      = 0
		failedHistogramsCount         = 0
		succeededExemplarsCount       = 0
		failedExemplarsCount          = 0
		startAppend                   = time.Now()
		sampleOutOfBoundsCount        = 0
		sampleOutOfOrderCount         = 0
		sampleTooOldCount             = 0
		newValueForTimestampCount     = 0
		perUserSeriesLimitCount       = 0
		perLabelSetSeriesLimitCount   = 0
		perMetricSeriesLimitCount     = 0
		discardedNativeHistogramCount = 0

		updateFirstPartial = func(errFn func() error) {
			if firstPartialErr == nil {
				firstPartialErr = errFn()
			}
		}

		handleAppendFailure = func(err error, timestampMs int64, lbls []cortexpb.LabelAdapter, copiedLabels labels.Labels, matchedLabelSetLimits []validation.LimitsPerLabelSet) (rollback bool) {
			// Check if the error is a soft error we can proceed on. If so, we keep track
			// of it, so that we can return it back to the distributor, which will return a
			// 400 error to the client. The client (Prometheus) will not retry on 400, and
			// we actually ingested all samples which haven't failed.
			switch cause := errors.Cause(err); {
			case errors.Is(cause, storage.ErrOutOfBounds):
				sampleOutOfBoundsCount++
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, storage.ErrOutOfOrderSample):
				sampleOutOfOrderCount++
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, storage.ErrDuplicateSampleForTimestamp):
				newValueForTimestampCount++
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, storage.ErrTooOldSample):
				sampleTooOldCount++
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, errMaxSeriesPerUserLimitExceeded):
				perUserSeriesLimitCount++
				updateFirstPartial(func() error {
					return makeLimitError(perUserSeriesLimit, i.limiter.FormatError(userID, cause, copiedLabels))
				})

			case errors.Is(cause, errMaxSeriesPerMetricLimitExceeded):
				perMetricSeriesLimitCount++
				updateFirstPartial(func() error {
					return makeMetricLimitError(perMetricSeriesLimit, copiedLabels, i.limiter.FormatError(userID, cause, copiedLabels))
				})

			case errors.As(cause, &errMaxSeriesPerLabelSetLimitExceeded{}):
				perLabelSetSeriesLimitCount++
				// We only track per labelset discarded samples for throttling by labelset limit.
				reasonCounter.increment(matchedLabelSetLimits, perLabelsetSeriesLimit)
				updateFirstPartial(func() error {
					return makeMetricLimitError(perLabelsetSeriesLimit, copiedLabels, i.limiter.FormatError(userID, cause, copiedLabels))
				})

			case errors.Is(cause, histogram.ErrHistogramSpanNegativeOffset):
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, histogram.ErrHistogramSpansBucketsMismatch):
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, histogram.ErrHistogramNegativeBucketCount):
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, histogram.ErrHistogramCountNotBigEnough):
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, histogram.ErrHistogramCountMismatch):
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			case errors.Is(cause, storage.ErrOOONativeHistogramsDisabled):
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(timestampMs), lbls) })

			default:
				rollback = true
			}
			return
		}
	)

	// Walk the samples, appending them to the users database
	app := db.Appender(ctx).(extendedAppender)
	var newSeries []labels.Labels

	for _, ts := range req.Timeseries {
		// The labels must be sorted (in our case, it's guaranteed a write request
		// has sorted labels once hit the ingester).

		// Look up a reference for this series.
		tsLabels := cortexpb.FromLabelAdaptersToLabels(ts.Labels)
		tsLabelsHash := tsLabels.Hash()
		ref, copiedLabels := app.GetRef(tsLabels, tsLabelsHash)

		// To find out if any sample was added to this series, we keep old value.
		oldSucceededSamplesCount := succeededSamplesCount
		// To find out if any histogram was added to this series, we keep old value.
		oldSucceededHistogramsCount := succeededHistogramsCount

		// Copied labels will be empty if ref is 0.
		if ref == 0 {
			// Copy the label set because both TSDB and the active series tracker may retain it.
			copiedLabels = cortexpb.FromLabelAdaptersToLabelsWithCopy(ts.Labels)
		}
		matchedLabelSetLimits := i.limiter.limitsPerLabelSets(userID, copiedLabels)

		for _, s := range ts.Samples {
			var err error

			// If the cached reference exists, we try to use it.
			if ref != 0 {
				if _, err = app.Append(ref, copiedLabels, s.TimestampMs, s.Value); err == nil {
					succeededSamplesCount++
					continue
				}

			} else {
				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, s.TimestampMs, s.Value); err == nil {
					// Keep track of what series needs to be expired on the postings cache
					if db.postingCache != nil {
						newSeries = append(newSeries, copiedLabels)
					}
					succeededSamplesCount++
					continue
				}
			}

			failedSamplesCount++

			if rollback := handleAppendFailure(err, s.TimestampMs, ts.Labels, copiedLabels, matchedLabelSetLimits); !rollback {
				continue
			}
			// The error looks an issue on our side, so we should rollback
			if rollbackErr := app.Rollback(); rollbackErr != nil {
				level.Warn(logutil.WithContext(ctx, i.logger)).Log("msg", "failed to rollback on error", "user", userID, "err", rollbackErr)
			}

			return nil, wrapWithUser(err, userID)
		}

		if i.cfg.BlocksStorageConfig.TSDB.EnableNativeHistograms {
			for _, hp := range ts.Histograms {
				var (
					err error
					h   *histogram.Histogram
					fh  *histogram.FloatHistogram
				)

				if hp.GetCountFloat() > 0 {
					fh = cortexpb.FloatHistogramProtoToFloatHistogram(hp)
				} else {
					h = cortexpb.HistogramProtoToHistogram(hp)
				}

				if ref != 0 {
					if _, err = app.AppendHistogram(ref, copiedLabels, hp.TimestampMs, h, fh); err == nil {
						succeededHistogramsCount++
						continue
					}
				} else {
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = cortexpb.FromLabelAdaptersToLabelsWithCopy(ts.Labels)
					if ref, err = app.AppendHistogram(0, copiedLabels, hp.TimestampMs, h, fh); err == nil {
						// Keep track of what series needs to be expired on the postings cache
						if db.postingCache != nil {
							newSeries = append(newSeries, copiedLabels)
						}
						succeededHistogramsCount++
						continue
					}
				}

				failedHistogramsCount++

				if rollback := handleAppendFailure(err, hp.TimestampMs, ts.Labels, copiedLabels, matchedLabelSetLimits); !rollback {
					continue
				}
				// The error looks an issue on our side, so we should rollback
				if rollbackErr := app.Rollback(); rollbackErr != nil {
					level.Warn(logutil.WithContext(ctx, i.logger)).Log("msg", "failed to rollback on error", "user", userID, "err", rollbackErr)
				}
				return nil, wrapWithUser(err, userID)
			}
		} else {
			discardedNativeHistogramCount += len(ts.Histograms)
		}

		isNHAppended := succeededHistogramsCount > oldSucceededHistogramsCount
		shouldUpdateSeries := (succeededSamplesCount > oldSucceededSamplesCount) || isNHAppended
		if i.cfg.ActiveSeriesMetricsEnabled && shouldUpdateSeries {
			db.activeSeries.UpdateSeries(tsLabels, tsLabelsHash, startAppend, isNHAppended, func(l labels.Labels) labels.Labels {
				// we must already have copied the labels if succeededSamplesCount or succeededHistogramsCount has been incremented.
				return copiedLabels
			})
		}

		maxExemplarsForUser := i.getMaxExemplars(userID)
		if maxExemplarsForUser > 0 {
			// app.AppendExemplar currently doesn't create the series, it must
			// already exist.  If it does not then drop.
			if ref == 0 && len(ts.Exemplars) > 0 {
				updateFirstPartial(func() error {
					return wrappedTSDBIngestExemplarErr(errExemplarRef,
						model.Time(ts.Exemplars[0].TimestampMs), ts.Labels, ts.Exemplars[0].Labels)
				})
				failedExemplarsCount += len(ts.Exemplars)
			} else { // Note that else is explicit, rather than a continue in the above if, in case of additional logic post exemplar processing.
				for _, ex := range ts.Exemplars {
					e := exemplar.Exemplar{
						Value:  ex.Value,
						Ts:     ex.TimestampMs,
						HasTs:  true,
						Labels: cortexpb.FromLabelAdaptersToLabelsWithCopy(ex.Labels),
					}

					if _, err = app.AppendExemplar(ref, nil, e); err == nil {
						succeededExemplarsCount++
						continue
					}

					// Error adding exemplar
					updateFirstPartial(func() error {
						return wrappedTSDBIngestExemplarErr(err, model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
					})
					failedExemplarsCount++
				}
			}
		}
	}

	// At this point all samples have been added to the appender, so we can track the time it took.
	i.TSDBState.appenderAddDuration.Observe(time.Since(startAppend).Seconds())

	startCommit := time.Now()
	if err := app.Commit(); err != nil {
		return nil, wrapWithUser(err, userID)
	}

	// This is a workaround of https://github.com/prometheus/prometheus/pull/15579
	// Calling expire here may result in the series names being expired multiple times,
	// as there may be multiple Push operations concurrently for the same new timeseries.
	// TODO: alanprot remove this when/if the PR is merged
	if db.postingCache != nil {
		for _, s := range newSeries {
			db.postingCache.ExpireSeries(s)
		}
	}

	i.TSDBState.appenderCommitDuration.Observe(time.Since(startCommit).Seconds())

	// If only invalid samples or histograms are pushed, don't change "last update", as TSDB was not modified.
	if succeededSamplesCount > 0 || succeededHistogramsCount > 0 {
		db.setLastUpdate(time.Now())
	}

	// Increment metrics only if the samples have been successfully committed.
	// If the code didn't reach this point, it means that we returned an error
	// which will be converted into an HTTP 5xx and the client should/will retry.
	i.metrics.ingestedSamples.Add(float64(succeededSamplesCount))
	i.metrics.ingestedSamplesFail.Add(float64(failedSamplesCount))
	i.metrics.ingestedHistograms.Add(float64(succeededHistogramsCount))
	i.metrics.ingestedHistogramsFail.Add(float64(failedHistogramsCount))
	i.metrics.ingestedExemplars.Add(float64(succeededExemplarsCount))
	i.metrics.ingestedExemplarsFail.Add(float64(failedExemplarsCount))

	if sampleOutOfBoundsCount > 0 {
		i.validateMetrics.DiscardedSamples.WithLabelValues(sampleOutOfBounds, userID).Add(float64(sampleOutOfBoundsCount))
	}
	if sampleOutOfOrderCount > 0 {
		i.validateMetrics.DiscardedSamples.WithLabelValues(sampleOutOfOrder, userID).Add(float64(sampleOutOfOrderCount))
	}
	if sampleTooOldCount > 0 {
		i.validateMetrics.DiscardedSamples.WithLabelValues(sampleTooOld, userID).Add(float64(sampleTooOldCount))
	}
	if newValueForTimestampCount > 0 {
		i.validateMetrics.DiscardedSamples.WithLabelValues(newValueForTimestamp, userID).Add(float64(newValueForTimestampCount))
	}
	if perUserSeriesLimitCount > 0 {
		i.validateMetrics.DiscardedSamples.WithLabelValues(perUserSeriesLimit, userID).Add(float64(perUserSeriesLimitCount))
	}
	if perMetricSeriesLimitCount > 0 {
		i.validateMetrics.DiscardedSamples.WithLabelValues(perMetricSeriesLimit, userID).Add(float64(perMetricSeriesLimitCount))
	}
	if perLabelSetSeriesLimitCount > 0 {
		i.validateMetrics.DiscardedSamples.WithLabelValues(perLabelsetSeriesLimit, userID).Add(float64(perLabelSetSeriesLimitCount))
	}

	if !i.cfg.BlocksStorageConfig.TSDB.EnableNativeHistograms && discardedNativeHistogramCount > 0 {
		i.validateMetrics.DiscardedSamples.WithLabelValues(nativeHistogramSample, userID).Add(float64(discardedNativeHistogramCount))
	}

	for h, counter := range reasonCounter.counters {
		labelStr := counter.lbls.String()
		i.validateMetrics.LabelSetTracker.Track(userID, h, counter.lbls)
		for reason, count := range counter.reasonCounter {
			i.validateMetrics.DiscardedSamplesPerLabelSet.WithLabelValues(reason, userID, labelStr).Add(float64(count))
		}
	}

	// Distributor counts both samples, metadata and histograms, so for consistency ingester does the same.
	i.ingestionRate.Add(int64(succeededSamplesCount + succeededHistogramsCount + ingestedMetadata))

	switch req.Source {
	case cortexpb.RULE:
		db.ingestedRuleSamples.Add(int64(succeededSamplesCount + succeededHistogramsCount))
	case cortexpb.API:
		fallthrough
	default:
		db.ingestedAPISamples.Add(int64(succeededSamplesCount + succeededHistogramsCount))
	}

	if firstPartialErr != nil {
		code := http.StatusBadRequest
		var ve *validationError
		if errors.As(firstPartialErr, &ve) {
			code = ve.code
		}
		level.Debug(logutil.WithContext(ctx, i.logger)).Log("msg", "partial failures to push", "totalSamples", succeededSamplesCount+failedSamplesCount, "failedSamples", failedSamplesCount, "totalHistograms", succeededHistogramsCount+failedHistogramsCount, "failedHistograms", failedHistogramsCount, "firstPartialErr", firstPartialErr)
		return &cortexpb.WriteResponse{}, httpgrpc.Errorf(code, "%s", wrapWithUser(firstPartialErr, userID).Error())
	}

	return &cortexpb.WriteResponse{}, nil
}

func (u *userTSDB) acquireReadLock() error {
	u.stateMtx.RLock()
	defer u.stateMtx.RUnlock()

	switch u.state {
	case active:
	case activeShipping:
	case forceCompacting:
		// Read are allowed.
	case closing:
		return errors.New("TSDB is closing")
	default:
		return errors.New("TSDB is not active")
	}

	u.readInFlight.Add(1)
	return nil
}

func (u *userTSDB) releaseReadLock() {
	u.readInFlight.Done()
}

func (u *userTSDB) acquireAppendLock() error {
	u.stateMtx.RLock()
	defer u.stateMtx.RUnlock()

	switch u.state {
	case active:
	case activeShipping:
		// Pushes are allowed.
	case forceCompacting:
		return errors.New("forced compaction in progress")
	case closing:
		return errors.New("TSDB is closing")
	default:
		return errors.New("TSDB is not active")
	}

	u.pushesInFlight.Add(1)
	return nil
}

func (u *userTSDB) releaseAppendLock() {
	u.pushesInFlight.Done()
}

// QueryExemplars implements service.IngesterServer
func (i *Ingester) QueryExemplars(ctx context.Context, req *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	from, through, matchers, err := client.FromExemplarQueryRequest(i.matchersCache, req)
	if err != nil {
		return nil, err
	}

	i.metrics.queries.Inc()

	db, err := i.getTSDB(userID)
	if err != nil || db == nil {
		return &client.ExemplarQueryResponse{}, nil
	}

	if err := db.acquireReadLock(); err != nil {
		return &client.ExemplarQueryResponse{}, nil
	}
	defer db.releaseReadLock()

	q, err := db.ExemplarQuerier(ctx)
	if err != nil {
		return nil, err
	}

	// We will report *this* request in the error too.
	c, err := i.trackInflightQueryRequest()
	if err != nil {
		return nil, err
	}

	// It's not required to sort series from a single ingester because series are sorted by the Exemplar Storage before returning from Select.
	res, err := q.Select(from, through, matchers...)
	c()
	if err != nil {
		return nil, err
	}

	numExemplars := 0

	result := &client.ExemplarQueryResponse{}
	for _, es := range res {
		ts := cortexpb.TimeSeries{
			Labels:    cortexpb.FromLabelsToLabelAdapters(es.SeriesLabels),
			Exemplars: cortexpb.FromExemplarsToExemplarProtos(es.Exemplars),
		}

		numExemplars += len(ts.Exemplars)
		result.Timeseries = append(result.Timeseries, ts)
	}

	i.metrics.queriedExemplars.Observe(float64(numExemplars))

	return result, nil
}

// LabelValues returns all label values that are associated with a given label name.
func (i *Ingester) LabelValues(ctx context.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	resp, cleanup, err := i.labelsValuesCommon(ctx, req)
	defer cleanup()
	return resp, err
}

// LabelValuesStream returns all label values that are associated with a given label name.
func (i *Ingester) LabelValuesStream(req *client.LabelValuesRequest, stream client.Ingester_LabelValuesStreamServer) error {
	resp, cleanup, err := i.labelsValuesCommon(stream.Context(), req)
	defer cleanup()

	if err != nil {
		return err
	}

	for i := 0; i < len(resp.LabelValues); i += metadataStreamBatchSize {
		j := i + metadataStreamBatchSize
		if j > len(resp.LabelValues) {
			j = len(resp.LabelValues)
		}
		resp := &client.LabelValuesStreamResponse{
			LabelValues: resp.LabelValues[i:j],
		}
		err := client.SendLabelValuesStream(stream, resp)
		if err != nil {
			return err
		}
	}

	return nil
}

// labelsValuesCommon returns all label values that are associated with a given label name.
// this should be used by LabelValues and LabelValuesStream
// the cleanup function should be called in order to close the querier
func (i *Ingester) labelsValuesCommon(ctx context.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, func(), error) {
	cleanup := func() {}
	if err := i.checkRunning(); err != nil {
		return nil, cleanup, err
	}

	labelName, startTimestampMs, endTimestampMs, limit, matchers, err := client.FromLabelValuesRequest(i.matchersCache, req)
	if err != nil {
		return nil, cleanup, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, cleanup, err
	}

	db, err := i.getTSDB(userID)
	if err != nil || db == nil {
		return &client.LabelValuesResponse{}, cleanup, nil
	}

	if err := db.acquireReadLock(); err != nil {
		return &client.LabelValuesResponse{}, cleanup, nil
	}
	defer db.releaseReadLock()

	mint, maxt, err := metadataQueryRange(startTimestampMs, endTimestampMs, db, i.cfg.QueryIngestersWithin)
	if err != nil {
		return nil, cleanup, err
	}

	q, err := db.Querier(mint, maxt)
	if err != nil {
		return nil, cleanup, err
	}

	cleanup = func() {
		q.Close()
	}

	c, err := i.trackInflightQueryRequest()
	if err != nil {
		return nil, cleanup, err
	}
	defer c()
	vals, _, err := q.LabelValues(ctx, labelName, &storage.LabelHints{Limit: limit}, matchers...)
	if err != nil {
		return nil, cleanup, err
	}

	if limit > 0 && len(vals) > limit {
		vals = vals[:limit]
	}

	return &client.LabelValuesResponse{
		LabelValues: vals,
	}, cleanup, nil
}

// LabelNames return all the label names.
func (i *Ingester) LabelNames(ctx context.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	resp, cleanup, err := i.labelNamesCommon(ctx, req)
	defer cleanup()
	return resp, err
}

// LabelNamesStream return all the label names.
func (i *Ingester) LabelNamesStream(req *client.LabelNamesRequest, stream client.Ingester_LabelNamesStreamServer) error {
	resp, cleanup, err := i.labelNamesCommon(stream.Context(), req)
	defer cleanup()

	if err != nil {
		return err
	}

	for i := 0; i < len(resp.LabelNames); i += metadataStreamBatchSize {
		j := i + metadataStreamBatchSize
		if j > len(resp.LabelNames) {
			j = len(resp.LabelNames)
		}
		resp := &client.LabelNamesStreamResponse{
			LabelNames: resp.LabelNames[i:j],
		}
		err := client.SendLabelNamesStream(stream, resp)
		if err != nil {
			return err
		}
	}

	return nil
}

// labelNamesCommon return all the label names.
// this should be used by LabelNames and LabelNamesStream.
// the cleanup function should be called in order to close the querier
func (i *Ingester) labelNamesCommon(ctx context.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, func(), error) {
	cleanup := func() {}
	if err := i.checkRunning(); err != nil {
		return nil, cleanup, err
	}

	startTimestampMs, endTimestampMs, limit, matchers, err := client.FromLabelNamesRequest(i.matchersCache, req)
	if err != nil {
		return nil, cleanup, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, cleanup, err
	}

	db, err := i.getTSDB(userID)
	if err != nil || db == nil {
		return &client.LabelNamesResponse{}, cleanup, nil
	}

	if err := db.acquireReadLock(); err != nil {
		return &client.LabelNamesResponse{}, cleanup, nil
	}
	defer db.releaseReadLock()

	mint, maxt, err := metadataQueryRange(startTimestampMs, endTimestampMs, db, i.cfg.QueryIngestersWithin)
	if err != nil {
		return nil, cleanup, err
	}

	q, err := db.Querier(mint, maxt)
	if err != nil {
		return nil, cleanup, err
	}

	cleanup = func() {
		q.Close()
	}

	c, err := i.trackInflightQueryRequest()
	if err != nil {
		return nil, cleanup, err
	}
	defer c()
	names, _, err := q.LabelNames(ctx, &storage.LabelHints{Limit: limit}, matchers...)
	if err != nil {
		return nil, cleanup, err
	}

	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}

	return &client.LabelNamesResponse{
		LabelNames: names,
	}, cleanup, nil
}

// MetricsForLabelMatchers returns all the metrics which match a set of matchers.
func (i *Ingester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	result := &client.MetricsForLabelMatchersResponse{}
	cleanup, err := i.metricsForLabelMatchersCommon(ctx, req, func(l labels.Labels) error {
		result.Metric = append(result.Metric, &cortexpb.Metric{
			Labels: cortexpb.FromLabelsToLabelAdapters(l),
		})
		return nil
	})
	defer cleanup()
	return result, err
}

func (i *Ingester) MetricsForLabelMatchersStream(req *client.MetricsForLabelMatchersRequest, stream client.Ingester_MetricsForLabelMatchersStreamServer) error {
	result := &client.MetricsForLabelMatchersStreamResponse{}

	cleanup, err := i.metricsForLabelMatchersCommon(stream.Context(), req, func(l labels.Labels) error {
		result.Metric = append(result.Metric, &cortexpb.Metric{
			Labels: cortexpb.FromLabelsToLabelAdapters(l),
		})

		if len(result.Metric) >= metadataStreamBatchSize {
			err := client.SendMetricsForLabelMatchersStream(stream, result)
			if err != nil {
				return err
			}
			result.Metric = result.Metric[:0]
		}
		return nil
	})
	defer cleanup()
	if err != nil {
		return err
	}

	// Send last batch
	if len(result.Metric) > 0 {
		err := client.SendMetricsForLabelMatchersStream(stream, result)
		if err != nil {
			return err
		}
	}

	return nil
}

// metricsForLabelMatchersCommon returns all the metrics which match a set of matchers.
// this should be used by MetricsForLabelMatchers and MetricsForLabelMatchersStream.
// the cleanup function should be called in order to close the querier
func (i *Ingester) metricsForLabelMatchersCommon(ctx context.Context, req *client.MetricsForLabelMatchersRequest, acc func(labels.Labels) error) (func(), error) {
	cleanup := func() {}
	if err := i.checkRunning(); err != nil {
		return cleanup, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return cleanup, err
	}

	db, err := i.getTSDB(userID)
	if err != nil || db == nil {
		return cleanup, nil
	}

	if err := db.acquireReadLock(); err != nil {
		return cleanup, nil
	}
	defer db.releaseReadLock()

	// Parse the request
	_, _, limit, matchersSet, err := client.FromMetricsForLabelMatchersRequest(i.matchersCache, req)
	if err != nil {
		return cleanup, err
	}

	mint, maxt, err := metadataQueryRange(req.StartTimestampMs, req.EndTimestampMs, db, i.cfg.QueryIngestersWithin)
	if err != nil {
		return cleanup, err
	}

	q, err := db.Querier(mint, maxt)
	if err != nil {
		return cleanup, err
	}

	cleanup = func() {
		q.Close()
	}

	// Run a query for each matchers set and collect all the results.
	var (
		sets      []storage.SeriesSet
		mergedSet storage.SeriesSet
	)

	hints := &storage.SelectHints{
		Start: mint,
		End:   maxt,
		Func:  "series", // There is no series function, this token is used for lookups that don't need samples.
		Limit: limit,
	}
	if len(matchersSet) > 1 {
		for _, matchers := range matchersSet {
			// Interrupt if the context has been canceled.
			if ctx.Err() != nil {
				return cleanup, ctx.Err()
			}

			seriesSet := q.Select(ctx, true, hints, matchers...)
			sets = append(sets, seriesSet)
		}
		mergedSet = storage.NewMergeSeriesSet(sets, limit, storage.ChainedSeriesMerge)
	} else {
		mergedSet = q.Select(ctx, false, hints, matchersSet[0]...)
	}

	cnt := 0
	for mergedSet.Next() {
		cnt++
		// Interrupt if the context has been canceled.
		if cnt%util.CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			return cleanup, ctx.Err()
		}
		if err := acc(mergedSet.At().Labels()); err != nil {
			return cleanup, err
		}

		if limit > 0 && cnt >= limit {
			break
		}
	}

	return cleanup, nil
}

// MetricsMetadata returns all the metric metadata of a user.
func (i *Ingester) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	i.stoppedMtx.RLock()
	if err := i.checkRunningOrStopping(); err != nil {
		i.stoppedMtx.RUnlock()
		return nil, err
	}
	i.stoppedMtx.RUnlock()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	userMetadata := i.getUserMetadata(userID)

	if userMetadata == nil {
		return &client.MetricsMetadataResponse{}, nil
	}

	return &client.MetricsMetadataResponse{Metadata: userMetadata.toClientMetadata(req)}, nil
}

// CheckReady is the readiness handler used to indicate to k8s when the ingesters
// are ready for the addition or removal of another ingester.
func (i *Ingester) CheckReady(ctx context.Context) error {
	if err := i.checkRunningOrStopping(); err != nil {
		return fmt.Errorf("ingester not ready: %v", err)
	}
	return i.lifecycler.CheckReady(ctx)
}

// UserStats returns ingestion statistics for the current user.
func (i *Ingester) UserStats(ctx context.Context, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	db, err := i.getTSDB(userID)
	if err != nil || db == nil {
		return &client.UserStatsResponse{}, nil
	}

	userStat := createUserStats(db, i.cfg.ActiveSeriesMetricsEnabled)

	return &client.UserStatsResponse{
		IngestionRate:     userStat.IngestionRate,
		NumSeries:         userStat.NumSeries,
		ApiIngestionRate:  userStat.APIIngestionRate,
		RuleIngestionRate: userStat.RuleIngestionRate,
		ActiveSeries:      userStat.ActiveSeries,
		LoadedBlocks:      userStat.LoadedBlocks,
	}, nil
}

func (i *Ingester) userStats() []UserIDStats {
	i.stoppedMtx.RLock()
	defer i.stoppedMtx.RUnlock()

	perUserTotals := make(map[string]UserStats)

	users := i.TSDBState.dbs

	response := make([]UserIDStats, 0, len(perUserTotals))
	for id, db := range users {
		response = append(response, UserIDStats{
			UserID:    id,
			UserStats: createUserStats(db, i.cfg.ActiveSeriesMetricsEnabled),
		})
	}

	return response
}

// AllUserStatsHandler shows stats for all users.
func (i *Ingester) AllUserStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats := i.userStats()

	AllUserStatsRender(w, r, stats, 0)
}

// AllUserStats returns ingestion statistics for all users known to this ingester.
func (i *Ingester) AllUserStats(_ context.Context, _ *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}

	userStats := i.userStats()

	response := &client.UsersStatsResponse{
		Stats: make([]*client.UserIDStatsResponse, 0, len(userStats)),
	}
	for _, userStat := range userStats {
		response.Stats = append(response.Stats, &client.UserIDStatsResponse{
			UserId: userStat.UserID,
			Data: &client.UserStatsResponse{
				IngestionRate:     userStat.IngestionRate,
				NumSeries:         userStat.NumSeries,
				ApiIngestionRate:  userStat.APIIngestionRate,
				RuleIngestionRate: userStat.RuleIngestionRate,
				ActiveSeries:      userStat.ActiveSeries,
				LoadedBlocks:      userStat.LoadedBlocks,
			},
		})
	}
	return response, nil
}

func createUserStats(db *userTSDB, activeSeriesMetricsEnabled bool) UserStats {
	apiRate := db.ingestedAPISamples.Rate()
	ruleRate := db.ingestedRuleSamples.Rate()

	var activeSeries uint64
	if activeSeriesMetricsEnabled {
		activeSeries = uint64(db.activeSeries.Active())
	}

	return UserStats{
		IngestionRate:     apiRate + ruleRate,
		APIIngestionRate:  apiRate,
		RuleIngestionRate: ruleRate,
		NumSeries:         db.Head().NumSeries(),
		ActiveSeries:      activeSeries,
		LoadedBlocks:      uint64(len(db.Blocks())),
	}
}

const queryStreamBatchMessageSize = 1 * 1024 * 1024

// QueryStream implements service.IngesterServer
// Streams metrics from a TSDB. This implements the client.IngesterServer interface
func (i *Ingester) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	if err := i.checkRunning(); err != nil {
		return err
	}

	spanlog, ctx := spanlogger.New(stream.Context(), "QueryStream")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	from, through, matchers, err := client.FromQueryRequest(i.matchersCache, req)
	if err != nil {
		return err
	}

	matchers, shardMatcher, err := querysharding.ExtractShardingMatchers(matchers)
	if err != nil {
		return err
	}

	defer shardMatcher.Close()

	i.metrics.queries.Inc()

	db, err := i.getTSDB(userID)
	if err != nil || db == nil {
		return nil
	}

	if err := db.acquireReadLock(); err != nil {
		return nil
	}
	defer db.releaseReadLock()

	numSamples := 0
	numSeries := 0
	totalDataBytes := 0
	numChunks := 0
	numSeries, numSamples, totalDataBytes, numChunks, err = i.queryStreamChunks(ctx, db, int64(from), int64(through), matchers, shardMatcher, stream)

	if err != nil {
		return err
	}

	i.metrics.queriedSeries.Observe(float64(numSeries))
	i.metrics.queriedSamples.Observe(float64(numSamples))
	i.metrics.queriedChunks.Observe(float64(numChunks))
	level.Debug(spanlog).Log("series", numSeries, "samples", numSamples, "data_bytes", totalDataBytes, "chunks", numChunks)
	spanlog.SetTag("series", numSeries)
	spanlog.SetTag("samples", numSamples)
	spanlog.SetTag("data_bytes", totalDataBytes)
	spanlog.SetTag("chunks", numChunks)
	return nil
}

func (i *Ingester) trackInflightQueryRequest() (func(), error) {
	gl := i.getInstanceLimits()
	if gl != nil && gl.MaxInflightQueryRequests > 0 {
		if i.inflightQueryRequests.Load() >= gl.MaxInflightQueryRequests {
			return nil, errTooManyInflightQueryRequests
		}
	}

	i.maxInflightQueryRequests.Track(i.inflightQueryRequests.Inc())
	return func() {
		i.inflightQueryRequests.Dec()
	}, nil
}

// queryStreamChunks streams metrics from a TSDB. This implements the client.IngesterServer interface
func (i *Ingester) queryStreamChunks(ctx context.Context, db *userTSDB, from, through int64, matchers []*labels.Matcher, sm *storepb.ShardMatcher, stream client.Ingester_QueryStreamServer) (numSeries, numSamples, totalBatchSizeBytes, numChunks int, _ error) {
	q, err := db.ChunkQuerier(from, through)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer q.Close()

	c, err := i.trackInflightQueryRequest()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	hints := &storage.SelectHints{
		Start:           from,
		End:             through,
		DisableTrimming: i.cfg.DisableChunkTrimming,
	}
	// It's not required to return sorted series because series are sorted by the Cortex querier.
	ss := q.Select(ctx, false, hints, matchers...)
	c()
	if ss.Err() != nil {
		return 0, 0, 0, 0, ss.Err()
	}

	chunkSeries := getTimeSeriesChunksSlice()
	defer putTimeSeriesChunksSlice(chunkSeries)
	batchSizeBytes := 0
	var it chunks.Iterator
	for ss.Next() {
		series := ss.At()

		if sm.IsSharded() && !sm.MatchesLabels(series.Labels()) {
			continue
		}

		// convert labels to LabelAdapter
		ts := client.TimeSeriesChunk{
			Labels: cortexpb.FromLabelsToLabelAdapters(series.Labels()),
		}

		it := series.Iterator(it)
		for it.Next() {
			// Chunks are ordered by min time.
			meta := it.At()

			// It is not guaranteed that chunk returned by iterator is populated.
			// For now just return error. We could also try to figure out how to read the chunk.
			if meta.Chunk == nil {
				return 0, 0, 0, 0, errors.Errorf("unfilled chunk returned from TSDB chunk querier")
			}

			ch := client.Chunk{
				StartTimestampMs: meta.MinTime,
				EndTimestampMs:   meta.MaxTime,
				Data:             meta.Chunk.Bytes(),
			}

			switch meta.Chunk.Encoding() {
			case chunkenc.EncXOR:
				ch.Encoding = int32(encoding.PrometheusXorChunk)
			case chunkenc.EncHistogram:
				ch.Encoding = int32(encoding.PrometheusHistogramChunk)
			case chunkenc.EncFloatHistogram:
				ch.Encoding = int32(encoding.PrometheusFloatHistogramChunk)
			default:
				return 0, 0, 0, 0, errors.Errorf("unknown chunk encoding from TSDB chunk querier: %v", meta.Chunk.Encoding())
			}

			ts.Chunks = append(ts.Chunks, ch)
			numChunks++
			numSamples += meta.Chunk.NumSamples()
		}
		numSeries++
		tsSize := ts.Size()
		totalBatchSizeBytes += tsSize

		if (batchSizeBytes > 0 && batchSizeBytes+tsSize > queryStreamBatchMessageSize) || len(chunkSeries) >= queryStreamBatchSize {
			// Adding this series to the batch would make it too big,
			// flush the data and add it to new batch instead.
			err = client.SendQueryStream(stream, &client.QueryStreamResponse{
				Chunkseries: chunkSeries,
			})
			if err != nil {
				return 0, 0, 0, 0, err
			}

			batchSizeBytes = 0
			chunkSeries = chunkSeries[:0]
		}

		chunkSeries = append(chunkSeries, ts)
		batchSizeBytes += tsSize
	}

	// Ensure no error occurred while iterating the series set.
	if err := ss.Err(); err != nil {
		return 0, 0, 0, 0, err
	}

	// Final flush any existing metrics
	if batchSizeBytes != 0 {
		err = client.SendQueryStream(stream, &client.QueryStreamResponse{
			Chunkseries: chunkSeries,
		})
		if err != nil {
			return 0, 0, 0, 0, err
		}
	}

	return numSeries, numSamples, totalBatchSizeBytes, numChunks, nil
}

func (i *Ingester) getTSDB(userID string) (*userTSDB, error) {
	i.stoppedMtx.RLock()
	defer i.stoppedMtx.RUnlock()
	db := i.TSDBState.dbs[userID]
	if db == nil {
		return nil, errNoUserDb
	}
	return db, nil
}

// List all users for which we have a TSDB. We do it here in order
// to keep the mutex locked for the shortest time possible.
func (i *Ingester) getTSDBUsers() []string {
	i.stoppedMtx.RLock()
	defer i.stoppedMtx.RUnlock()

	ids := make([]string, 0, len(i.TSDBState.dbs))
	for userID := range i.TSDBState.dbs {
		ids = append(ids, userID)
	}

	return ids
}

func (i *Ingester) getOrCreateTSDB(userID string, force bool) (*userTSDB, error) {
	db, err := i.getTSDB(userID)
	if db != nil {
		if err != nil {
			level.Warn(i.logger).Log("msg", "error getting user DB but userDB is not null", "err", err, "userID", userID)
		}
		return db, nil
	}

	i.stoppedMtx.Lock()
	defer i.stoppedMtx.Unlock()

	// Check again for DB in the event it was created in-between locks
	var ok bool
	db, ok = i.TSDBState.dbs[userID]
	if ok {
		return db, nil
	}

	// We're ready to create the TSDB, however we must be sure that the ingester
	// is in the ACTIVE state, otherwise it may conflict with the transfer in/out.
	// The TSDB is created when the first series is pushed and this shouldn't happen
	// to a non-ACTIVE ingester, however we want to protect from any bug, cause we
	// may have data loss or TSDB WAL corruption if the TSDB is created before/during
	// a transfer in occurs.
	if ingesterState := i.lifecycler.GetState(); !force && ingesterState != ring.ACTIVE {
		return nil, fmt.Errorf(errTSDBCreateIncompatibleState, ingesterState)
	}

	gl := i.getInstanceLimits()
	if gl != nil && gl.MaxInMemoryTenants > 0 {
		if users := int64(len(i.TSDBState.dbs)); users >= gl.MaxInMemoryTenants {
			return nil, errMaxUsersLimitReached
		}
	}

	// Create the database and a shipper for a user
	db, err = i.createTSDB(userID)
	if err != nil {
		return nil, err
	}

	// Add the db to list of user databases
	i.TSDBState.dbs[userID] = db
	i.metrics.memUsers.Inc()

	return db, nil
}

func (i *Ingester) blockChunkQuerierFunc(userId string) tsdb.BlockChunkQuerierFunc {
	return func(b tsdb.BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
		db, err := i.getTSDB(userId)

		var postingCache cortex_tsdb.ExpandedPostingsCache
		if err == nil && db != nil {
			postingCache = db.postingCache
		}

		// Caching expanded postings for queries that are "in the future" may lead to incorrect results being cached.
		// This occurs because the tsdb.PostingsForMatchers function can return invalid data in such scenarios.
		// For more details, see: https://github.com/cortexproject/cortex/issues/6556
		// TODO: alanprot: Consider removing this logic when prometheus is updated as this logic is "fixed" upstream.
		if postingCache == nil || mint > db.Head().MaxTime() {
			return tsdb.NewBlockChunkQuerier(b, mint, maxt)
		}

		return cortex_tsdb.NewCachedBlockChunkQuerier(postingCache, b, mint, maxt)
	}
}

// createTSDB creates a TSDB for a given userID, and returns the created db.
func (i *Ingester) createTSDB(userID string) (*userTSDB, error) {
	tsdbPromReg := prometheus.NewRegistry()
	udir := i.cfg.BlocksStorageConfig.TSDB.BlocksDir(userID)
	userLogger := logutil.WithUserID(userID, i.logger)

	blockRanges := i.cfg.BlocksStorageConfig.TSDB.BlockRanges.ToMilliseconds()

	var postingCache cortex_tsdb.ExpandedPostingsCache
	if i.expandedPostingsCacheFactory != nil {
		postingCache = i.expandedPostingsCacheFactory.NewExpandedPostingsCache(userID, i.metrics.expandedPostingsCacheMetrics)
	}

	userDB := &userTSDB{
		userID:              userID,
		activeSeries:        NewActiveSeries(),
		seriesInMetric:      newMetricCounter(i.limiter, i.cfg.getIgnoreSeriesLimitForMetricNamesMap()),
		labelSetCounter:     newLabelSetCounter(i.limiter),
		ingestedAPISamples:  util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),
		ingestedRuleSamples: util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),

		instanceLimitsFn:             i.getInstanceLimits,
		instanceSeriesCount:          &i.TSDBState.seriesCount,
		interner:                     util.NewLruInterner(i.cfg.LabelsStringInterningEnabled),
		labelsStringInterningEnabled: i.cfg.LabelsStringInterningEnabled,

		blockRetentionPeriod: i.cfg.BlocksStorageConfig.TSDB.Retention.Milliseconds(),
		postingCache:         postingCache,
	}

	enableExemplars := false
	maxExemplarsForUser := i.getMaxExemplars(userID)
	if maxExemplarsForUser > 0 {
		enableExemplars = true
	}
	oooTimeWindow := i.limits.OutOfOrderTimeWindow(userID)

	walCompressType := wlog.CompressionNone
	if i.cfg.BlocksStorageConfig.TSDB.WALCompressionType != "" {
		walCompressType = wlog.CompressionType(i.cfg.BlocksStorageConfig.TSDB.WALCompressionType)
	}

	// Create a new user database
	db, err := tsdb.Open(udir, logutil.GoKitLogToSlog(userLogger), tsdbPromReg, &tsdb.Options{
		RetentionDuration:              i.cfg.BlocksStorageConfig.TSDB.Retention.Milliseconds(),
		MinBlockDuration:               blockRanges[0],
		MaxBlockDuration:               blockRanges[len(blockRanges)-1],
		NoLockfile:                     true,
		StripeSize:                     i.cfg.BlocksStorageConfig.TSDB.StripeSize,
		HeadChunksWriteBufferSize:      i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteBufferSize,
		WALCompression:                 walCompressType,
		WALSegmentSize:                 i.cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes,
		SeriesLifecycleCallback:        userDB,
		BlocksToDelete:                 userDB.blocksToDelete,
		EnableExemplarStorage:          enableExemplars,
		IsolationDisabled:              true,
		MaxExemplars:                   maxExemplarsForUser,
		HeadChunksWriteQueueSize:       i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteQueueSize,
		EnableMemorySnapshotOnShutdown: i.cfg.BlocksStorageConfig.TSDB.MemorySnapshotOnShutdown,
		OutOfOrderTimeWindow:           time.Duration(oooTimeWindow).Milliseconds(),
		OutOfOrderCapMax:               i.cfg.BlocksStorageConfig.TSDB.OutOfOrderCapMax,
		EnableOOONativeHistograms:      i.cfg.BlocksStorageConfig.TSDB.EnableNativeHistograms, // Automatically enabled when EnableNativeHistograms is true.
		EnableOverlappingCompaction:    false,                                                 // Always let compactors handle overlapped blocks, e.g. OOO blocks.
		EnableNativeHistograms:         i.cfg.BlocksStorageConfig.TSDB.EnableNativeHistograms,
		BlockChunkQuerierFunc:          i.blockChunkQuerierFunc(userID),
	}, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open TSDB: %s", udir)
	}
	db.DisableCompactions() // we will compact on our own schedule

	// Run compaction before using this TSDB. If there is data in head that needs to be put into blocks,
	// this will actually create the blocks. If there is no data (empty TSDB), this is a no-op, although
	// local blocks compaction may still take place if configured.
	level.Info(userLogger).Log("msg", "Running compaction after WAL replay")
	err = db.Compact(context.TODO())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to compact TSDB: %s", udir)
	}

	userDB.db = db
	// We set the limiter here because we don't want to limit
	// series during WAL replay.
	userDB.limiter = i.limiter

	if db.Head().NumSeries() > 0 {
		// If there are series in the head, use max time from head. If this time is too old,
		// TSDB will be eligible for flushing and closing sooner, unless more data is pushed to it quickly.
		userDB.setLastUpdate(util.TimeFromMillis(db.Head().MaxTime()))
	} else {
		// If head is empty (eg. new TSDB), don't close it right after.
		userDB.setLastUpdate(time.Now())
	}

	// Thanos shipper requires at least 1 external label to be set. For this reason,
	// we set the tenant ID as external label and we'll filter it out when reading
	// the series from the storage.
	l := labels.Labels{
		{
			Name:  cortex_tsdb.TenantIDExternalLabel,
			Value: userID,
		}, {
			Name:  cortex_tsdb.IngesterIDExternalLabel,
			Value: i.TSDBState.shipperIngesterID,
		},
	}

	// Create a new shipper for this database
	if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		userDB.shipper = shipper.New(
			userLogger,
			tsdbPromReg,
			udir,
			bucket.NewUserBucketClient(userID, i.TSDBState.bucket, i.limits),
			func() labels.Labels { return l },
			metadata.ReceiveSource,
			func() bool {
				return i.cfg.UploadCompactedBlocksEnabled
			},
			true, // Allow out of order uploads. It's fine in Cortex's context.
			metadata.NoneFunc,
			"",
		)
		userDB.shipperMetadataFilePath = filepath.Join(userDB.db.Dir(), filepath.Clean(shipper.DefaultMetaFilename))

		// Initialise the shipper blocks cache.
		if err := userDB.updateCachedShippedBlocks(); err != nil {
			level.Error(userLogger).Log("msg", "failed to update cached shipped blocks after shipper initialisation", "err", err)
		}
	}

	i.TSDBState.tsdbMetrics.setRegistryForUser(userID, tsdbPromReg)
	return userDB, nil
}

func (i *Ingester) closeAllTSDB() {
	i.stoppedMtx.Lock()

	wg := &sync.WaitGroup{}
	wg.Add(len(i.TSDBState.dbs))

	// Concurrently close all users TSDB
	for userID, userDB := range i.TSDBState.dbs {
		userID := userID

		go func(db *userTSDB) {
			defer wg.Done()

			if err := db.Close(); err != nil {
				level.Warn(i.logger).Log("msg", "unable to close TSDB", "err", err, "user", userID)
				return
			}

			// Now that the TSDB has been closed, we should remove it from the
			// set of open ones. This lock acquisition doesn't deadlock with the
			// outer one, because the outer one is released as soon as all go
			// routines are started.
			i.stoppedMtx.Lock()
			delete(i.TSDBState.dbs, userID)
			i.stoppedMtx.Unlock()

			i.metrics.memUsers.Dec()
			i.metrics.activeSeriesPerUser.DeleteLabelValues(userID)
			i.metrics.activeNHSeriesPerUser.DeleteLabelValues(userID)
		}(userDB)
	}

	// Wait until all Close() completed
	i.stoppedMtx.Unlock()
	wg.Wait()
}

// openExistingTSDB walks the user tsdb dir, and opens a tsdb for each user. This may start a WAL replay, so we limit the number of
// concurrently opening TSDB.
func (i *Ingester) openExistingTSDB(ctx context.Context) error {
	level.Info(logutil.WithContext(ctx, i.logger)).Log("msg", "opening existing TSDBs")

	queue := make(chan string)
	group, groupCtx := errgroup.WithContext(ctx)

	// Create a pool of workers which will open existing TSDBs.
	for n := 0; n < i.cfg.BlocksStorageConfig.TSDB.MaxTSDBOpeningConcurrencyOnStartup; n++ {
		group.Go(func() error {
			for userID := range queue {
				startTime := time.Now()

				db, err := i.createTSDB(userID)
				if err != nil {
					level.Error(logutil.WithContext(ctx, i.logger)).Log("msg", "unable to open TSDB", "err", err, "user", userID)
					return errors.Wrapf(err, "unable to open TSDB for user %s", userID)
				}

				// Add the database to the map of user databases
				i.stoppedMtx.Lock()
				i.TSDBState.dbs[userID] = db
				i.stoppedMtx.Unlock()
				i.metrics.memUsers.Inc()

				i.TSDBState.walReplayTime.Observe(time.Since(startTime).Seconds())
			}

			return nil
		})
	}

	// Spawn a goroutine to find all users with a TSDB on the filesystem.
	group.Go(func() error {
		// Close the queue once filesystem walking is done.
		defer close(queue)

		walkErr := filepath.Walk(i.cfg.BlocksStorageConfig.TSDB.Dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				// If the root directory doesn't exist, we're OK (not needed to be created upfront).
				if os.IsNotExist(err) && path == i.cfg.BlocksStorageConfig.TSDB.Dir {
					return filepath.SkipDir
				}

				level.Error(logutil.WithContext(ctx, i.logger)).Log("msg", "an error occurred while iterating the filesystem storing TSDBs", "path", path, "err", err)
				return errors.Wrapf(err, "an error occurred while iterating the filesystem storing TSDBs at %s", path)
			}

			// Skip root dir and all other files
			if path == i.cfg.BlocksStorageConfig.TSDB.Dir || !info.IsDir() {
				return nil
			}

			// Top level directories are assumed to be user TSDBs
			userID := info.Name()
			f, err := os.Open(path)
			if err != nil {
				level.Error(logutil.WithContext(ctx, i.logger)).Log("msg", "unable to open TSDB dir", "err", err, "user", userID, "path", path)
				return errors.Wrapf(err, "unable to open TSDB dir %s for user %s", path, userID)
			}
			defer f.Close()

			// If the dir is empty skip it
			if _, err := f.Readdirnames(1); err != nil {
				if err == io.EOF {
					return filepath.SkipDir
				}

				level.Error(logutil.WithContext(ctx, i.logger)).Log("msg", "unable to read TSDB dir", "err", err, "user", userID, "path", path)
				return errors.Wrapf(err, "unable to read TSDB dir %s for user %s", path, userID)
			}

			// Enqueue the user to be processed.
			select {
			case queue <- userID:
				// Nothing to do.
			case <-groupCtx.Done():
				// Interrupt in case a failure occurred in another goroutine.
				return nil
			}

			// Don't descend into subdirectories.
			return filepath.SkipDir
		})

		return errors.Wrapf(walkErr, "unable to walk directory %s containing existing TSDBs", i.cfg.BlocksStorageConfig.TSDB.Dir)
	})

	// Wait for all workers to complete.
	err := group.Wait()
	if err != nil {
		level.Error(logutil.WithContext(ctx, i.logger)).Log("msg", "error while opening existing TSDBs", "err", err)
		return err
	}

	level.Info(logutil.WithContext(ctx, i.logger)).Log("msg", "successfully opened existing TSDBs")
	return nil
}

// getMemorySeriesMetric returns the total number of in-memory series across all open TSDBs.
func (i *Ingester) getMemorySeriesMetric() float64 {
	if err := i.checkRunning(); err != nil {
		return 0
	}

	i.stoppedMtx.RLock()
	defer i.stoppedMtx.RUnlock()

	count := uint64(0)
	for _, db := range i.TSDBState.dbs {
		count += db.Head().NumSeries()
	}

	return float64(count)
}

// getOldestUnshippedBlockMetric returns the unix timestamp of the oldest unshipped block or
// 0 if all blocks have been shipped.
func (i *Ingester) getOldestUnshippedBlockMetric() float64 {
	i.stoppedMtx.RLock()
	defer i.stoppedMtx.RUnlock()

	oldest := uint64(0)
	for _, db := range i.TSDBState.dbs {
		if ts := db.getOldestUnshippedBlockTime(); oldest == 0 || ts < oldest {
			oldest = ts
		}
	}

	return float64(oldest / 1000)
}

func (i *Ingester) shipBlocksLoop(ctx context.Context) error {
	// We add a slight jitter to make sure that if the head compaction interval and ship interval are set to the same
	// value they don't clash (if they both continuously run at the same exact time, the head compaction may not run
	// because can't successfully change the state).
	shipTicker := time.NewTicker(util.DurationWithJitter(i.cfg.BlocksStorageConfig.TSDB.ShipInterval, 0.01))
	defer shipTicker.Stop()

	for {
		select {
		case <-shipTicker.C:
			i.shipBlocks(ctx, nil)

		case req := <-i.TSDBState.shipTrigger:
			i.shipBlocks(ctx, req.users)
			close(req.callback) // Notify back.

		case <-ctx.Done():
			return nil
		}
	}
}

// shipBlocks runs shipping for all users.
func (i *Ingester) shipBlocks(ctx context.Context, allowed *util.AllowedTenants) {
	// Do not ship blocks if the ingester is PENDING or JOINING. It's
	// particularly important for the JOINING state because there could
	// be a blocks transfer in progress (from another ingester) and if we
	// run the shipper in such state we could end up with race conditions.
	if i.lifecycler != nil {
		if ingesterState := i.lifecycler.GetState(); ingesterState == ring.PENDING || ingesterState == ring.JOINING {
			level.Info(logutil.WithContext(ctx, i.logger)).Log("msg", "TSDB blocks shipping has been skipped because of the current ingester state", "state", ingesterState)
			return
		}
	}

	// Number of concurrent workers is limited in order to avoid to concurrently sync a lot
	// of tenants in a large cluster.
	_ = concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.BlocksStorageConfig.TSDB.ShipConcurrency, func(ctx context.Context, userID string) error {
		if !allowed.IsAllowed(userID) {
			return nil
		}

		// Get the user's DB. If the user doesn't exist, we skip it.
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil || userDB.shipper == nil {
			return nil
		}

		if userDB.deletionMarkFound.Load() {
			return nil
		}

		if time.Since(time.Unix(userDB.lastDeletionMarkCheck.Load(), 0)) > cortex_tsdb.DeletionMarkCheckInterval {
			// Even if check fails with error, we don't want to repeat it too often.
			userDB.lastDeletionMarkCheck.Store(time.Now().Unix())

			deletionMarkExists, err := cortex_tsdb.TenantDeletionMarkExists(ctx, i.TSDBState.bucket, userID)
			if err != nil {
				// If we cannot check for deletion mark, we continue anyway, even though in production shipper will likely fail too.
				// This however simplifies unit tests, where tenant deletion check is enabled by default, but tests don't setup bucket.
				level.Warn(logutil.WithContext(ctx, i.logger)).Log("msg", "failed to check for tenant deletion mark before shipping blocks", "user", userID, "err", err)
			} else if deletionMarkExists {
				userDB.deletionMarkFound.Store(true)

				level.Info(logutil.WithContext(ctx, i.logger)).Log("msg", "tenant deletion mark exists, not shipping blocks", "user", userID)
				return nil
			}
		}

		// Run the shipper's Sync() to upload unshipped blocks. Make sure the TSDB state is active, in order to
		// avoid any race condition with closing idle TSDBs.
		if !userDB.casState(active, activeShipping) {
			level.Info(logutil.WithContext(ctx, i.logger)).Log("msg", "shipper skipped because the TSDB is not active", "user", userID)
			return nil
		}
		defer userDB.casState(activeShipping, active)

		if idxs, err := bucketindex.ReadSyncStatus(ctx, i.TSDBState.bucket, userID, logutil.WithContext(ctx, i.logger)); err == nil {
			// Skip blocks shipping if the bucket index failed to sync due to CMK errors.
			if idxs.Status == bucketindex.CustomerManagedKeyError {
				level.Info(logutil.WithContext(ctx, i.logger)).Log("msg", "skipping shipping blocks due CustomerManagedKeyError", "user", userID)
				return nil
			}
		}

		uploaded, err := userDB.shipper.Sync(ctx)
		if err != nil {
			level.Warn(logutil.WithContext(ctx, i.logger)).Log("msg", "shipper failed to synchronize TSDB blocks with the storage", "user", userID, "uploaded", uploaded, "err", err)
		} else {
			level.Debug(logutil.WithContext(ctx, i.logger)).Log("msg", "shipper successfully synchronized TSDB blocks with storage", "user", userID, "uploaded", uploaded)
		}

		// The shipper meta file could be updated even if the Sync() returned an error,
		// so it's safer to update it each time at least a block has been uploaded.
		// Moreover, the shipper meta file could be updated even if no blocks are uploaded
		// (eg. blocks removed due to retention) but doesn't cause any harm not updating
		// the cached list of blocks in such case, so we're not handling it.
		if uploaded > 0 {
			if err := userDB.updateCachedShippedBlocks(); err != nil {
				level.Error(logutil.WithContext(ctx, i.logger)).Log("msg", "failed to update cached shipped blocks after shipper synchronisation", "user", userID, "err", err)
			}
		}

		return nil
	})
}

func (i *Ingester) compactionLoop(ctx context.Context) error {
	infoFunc := func() (int, int) {
		if i.cfg.LifecyclerConfig.RingConfig.ZoneAwarenessEnabled {
			zones := i.lifecycler.Zones()
			if len(zones) != 0 {
				return slices.Index(zones, i.lifecycler.Zone), len(zones)
			}
		}

		// Lets create the slot based on the hash id
		i := int(client.HashAdd32(client.HashNew32(), i.lifecycler.ID) % 10)
		return i, 10
	}
	ticker := util.NewSlottedTicker(infoFunc, i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval, 1)
	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			i.compactBlocks(ctx, false, nil)

		case req := <-i.TSDBState.forceCompactTrigger:
			i.compactBlocks(ctx, true, req.users)
			close(req.callback) // Notify back.

		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// Compacts all compactable blocks. Force flag will force compaction even if head is not compactable yet.
func (i *Ingester) compactBlocks(ctx context.Context, force bool, allowed *util.AllowedTenants) {
	// Don't compact TSDB blocks while JOINING as there may be ongoing blocks transfers.
	// Compaction loop is not running in LEAVING state, so if we get here in LEAVING state, we're flushing blocks.
	if i.lifecycler != nil {
		if ingesterState := i.lifecycler.GetState(); ingesterState == ring.JOINING {
			level.Info(logutil.WithContext(ctx, i.logger)).Log("msg", "TSDB blocks compaction has been skipped because of the current ingester state", "state", ingesterState)
			return
		}
	}

	_ = concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.BlocksStorageConfig.TSDB.HeadCompactionConcurrency, func(ctx context.Context, userID string) error {
		if !allowed.IsAllowed(userID) {
			return nil
		}

		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil {
			return nil
		}

		// Don't do anything, if there is nothing to compact.
		h := userDB.Head()
		if h.NumSeries() == 0 {
			return nil
		}

		i.TSDBState.compactionsTriggered.Inc()

		reason := ""
		switch {
		case force:
			reason = "forced"
			err = userDB.compactHead(ctx, i.cfg.BlocksStorageConfig.TSDB.BlockRanges[0].Milliseconds())

		case i.TSDBState.compactionIdleTimeout > 0 && userDB.isIdle(time.Now(), i.TSDBState.compactionIdleTimeout):
			reason = "idle"
			level.Info(logutil.WithContext(ctx, i.logger)).Log("msg", "TSDB is idle, forcing compaction", "user", userID)
			err = userDB.compactHead(ctx, i.cfg.BlocksStorageConfig.TSDB.BlockRanges[0].Milliseconds())

		default:
			reason = "regular"
			err = userDB.Compact(ctx)
		}

		if err != nil {
			i.TSDBState.compactionsFailed.Inc()
			level.Warn(logutil.WithContext(ctx, i.logger)).Log("msg", "TSDB blocks compaction for user has failed", "user", userID, "err", err, "compactReason", reason)
		} else {
			level.Debug(logutil.WithContext(ctx, i.logger)).Log("msg", "TSDB blocks compaction completed successfully", "user", userID, "compactReason", reason)
		}

		return nil
	})
}

func (i *Ingester) closeAndDeleteIdleUserTSDBs(ctx context.Context) error {
	for _, userID := range i.getTSDBUsers() {
		if ctx.Err() != nil {
			return nil
		}

		result := i.closeAndDeleteUserTSDBIfIdle(userID)

		i.TSDBState.idleTsdbChecks.WithLabelValues(string(result)).Inc()
	}

	return nil
}

func (i *Ingester) expirePostingsCache(ctx context.Context) error {
	for _, userID := range i.getTSDBUsers() {
		if ctx.Err() != nil {
			return nil
		}
		userDB, err := i.getTSDB(userID)
		if err != nil || userDB == nil || userDB.postingCache == nil {
			continue
		}
		userDB.postingCache.PurgeExpiredItems()
	}

	return nil
}

func (i *Ingester) closeAndDeleteUserTSDBIfIdle(userID string) tsdbCloseCheckResult {
	userDB, err := i.getTSDB(userID)
	if err != nil || userDB == nil || userDB.shipper == nil {
		// We will not delete local data when not using shipping to storage.
		return tsdbShippingDisabled
	}

	if result := userDB.shouldCloseTSDB(i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout); !result.shouldClose() {
		return result
	}

	// This disables pushes and force-compactions. Not allowed to close while shipping is in progress.
	if !userDB.casState(active, closing) {
		return tsdbNotActive
	}

	// If TSDB is fully closed, we will set state to 'closed', which will prevent this deferred closing -> active transition.
	defer userDB.casState(closing, active)

	// Make sure we don't ignore any possible inflight requests.
	userDB.pushesInFlight.Wait()
	userDB.readInFlight.Wait()

	// Verify again, things may have changed during the checks and pushes.
	tenantDeleted := false
	if result := userDB.shouldCloseTSDB(i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout); !result.shouldClose() {
		// This will also change TSDB state back to active (via defer above).
		return result
	} else if result == tsdbTenantMarkedForDeletion {
		tenantDeleted = true
	}

	// At this point there are no more pushes to TSDB, and no possible compaction. Normally TSDB is empty,
	// but if we're closing TSDB because of tenant deletion mark, then it may still contain some series.
	// We need to remove these series from series count.
	i.TSDBState.seriesCount.Sub(int64(userDB.Head().NumSeries()))

	dir := userDB.db.Dir()

	if err := userDB.Close(); err != nil {
		level.Error(i.logger).Log("msg", "failed to close idle TSDB", "user", userID, "err", err)
		return tsdbCloseFailed
	}

	level.Info(i.logger).Log("msg", "closed idle TSDB", "user", userID)

	// This will prevent going back to "active" state in deferred statement.
	userDB.casState(closing, closed)

	// Only remove user from TSDBState when everything is cleaned up
	// This will prevent concurrency problems when cortex are trying to open new TSDB - Ie: New request for a given tenant
	// came in - while closing the tsdb for the same tenant.
	// If this happens now, the request will get reject as the push will not be able to acquire the lock as the tsdb will be
	// in closed state
	defer func() {
		i.stoppedMtx.Lock()
		delete(i.TSDBState.dbs, userID)
		i.stoppedMtx.Unlock()
	}()

	i.metrics.memUsers.Dec()
	i.TSDBState.tsdbMetrics.removeRegistryForUser(userID)

	i.deleteUserMetadata(userID)
	i.metrics.deletePerUserMetrics(userID)

	validation.DeletePerUserValidationMetrics(i.validateMetrics, userID, i.logger)

	// And delete local data.
	if err := os.RemoveAll(dir); err != nil {
		level.Error(i.logger).Log("msg", "failed to delete local TSDB", "user", userID, "err", err)
		return tsdbDataRemovalFailed
	}

	if tenantDeleted {
		level.Info(i.logger).Log("msg", "deleted local TSDB, user marked for deletion", "user", userID, "dir", dir)
		return tsdbTenantMarkedForDeletion
	}

	level.Info(i.logger).Log("msg", "deleted local TSDB, due to being idle", "user", userID, "dir", dir)
	return tsdbIdleClosed
}

// pushMetadata returns number of ingested metadata.
func (i *Ingester) pushMetadata(ctx context.Context, userID string, metadata []*cortexpb.MetricMetadata) int {
	ingestedMetadata := 0
	failedMetadata := 0

	var firstMetadataErr error
	for _, metadata := range metadata {
		err := i.appendMetadata(userID, metadata)
		if err == nil {
			ingestedMetadata++
			continue
		}

		failedMetadata++
		if firstMetadataErr == nil {
			firstMetadataErr = err
		}
	}

	i.metrics.ingestedMetadata.Add(float64(ingestedMetadata))
	i.metrics.ingestedMetadataFail.Add(float64(failedMetadata))

	// If we have any error with regard to metadata we just log and no-op.
	// We consider metadata a best effort approach, errors here should not stop processing.
	if firstMetadataErr != nil {
		logger := logutil.WithContext(ctx, i.logger)
		level.Warn(logger).Log("msg", "failed to ingest some metadata", "err", firstMetadataErr)
	}

	return ingestedMetadata
}

func (i *Ingester) appendMetadata(userID string, m *cortexpb.MetricMetadata) error {
	i.stoppedMtx.RLock()
	if i.stopped {
		i.stoppedMtx.RUnlock()
		return errIngesterStopping
	}
	i.stoppedMtx.RUnlock()

	userMetadata := i.getOrCreateUserMetadata(userID)

	return userMetadata.add(m.GetMetricFamilyName(), m)
}

func (i *Ingester) getOrCreateUserMetadata(userID string) *userMetricsMetadata {
	userMetadata := i.getUserMetadata(userID)
	if userMetadata != nil {
		return userMetadata
	}

	i.usersMetadataMtx.Lock()
	defer i.usersMetadataMtx.Unlock()

	// Ensure it was not created between switching locks.
	userMetadata, ok := i.usersMetadata[userID]
	if !ok {
		userMetadata = newMetadataMap(i.limiter, i.metrics, i.validateMetrics, userID)
		i.usersMetadata[userID] = userMetadata
	}
	return userMetadata
}

func (i *Ingester) getUserMetadata(userID string) *userMetricsMetadata {
	i.usersMetadataMtx.RLock()
	defer i.usersMetadataMtx.RUnlock()
	return i.usersMetadata[userID]
}

func (i *Ingester) deleteUserMetadata(userID string) {
	i.usersMetadataMtx.Lock()
	um := i.usersMetadata[userID]
	delete(i.usersMetadata, userID)
	i.usersMetadataMtx.Unlock()

	if um != nil {
		// We need call purge to update i.metrics.memMetadata correctly (it counts number of metrics with metadata in memory).
		// Passing zero time means purge everything.
		um.purge(time.Time{})
	}
}

func (i *Ingester) getUsersWithMetadata() []string {
	i.usersMetadataMtx.RLock()
	defer i.usersMetadataMtx.RUnlock()

	userIDs := make([]string, 0, len(i.usersMetadata))
	for userID := range i.usersMetadata {
		userIDs = append(userIDs, userID)
	}

	return userIDs
}

func (i *Ingester) purgeUserMetricsMetadata() {
	deadline := time.Now().Add(-i.cfg.MetadataRetainPeriod)

	for _, userID := range i.getUsersWithMetadata() {
		metadata := i.getUserMetadata(userID)
		if metadata == nil {
			continue
		}

		// Remove all metadata that we no longer need to retain.
		metadata.purge(deadline)
	}
}

// This method will flush all data. It is called as part of Lifecycler's shutdown (if flush on shutdown is configured), or from the flusher.
//
// When called as during Lifecycler shutdown, this happens as part of normal Ingester shutdown (see stopping method).
// Samples are not received at this stage. Compaction and Shipping loops have already been stopped as well.
//
// When used from flusher, ingester is constructed in a way that compaction, shipping and receiving of samples is never started.
func (i *Ingester) lifecyclerFlush() {
	level.Info(i.logger).Log("msg", "starting to flush and ship TSDB blocks")

	ctx := context.Background()

	i.compactBlocks(ctx, true, nil)
	if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		i.shipBlocks(ctx, nil)
	}

	level.Info(i.logger).Log("msg", "finished flushing and shipping TSDB blocks")
}

const (
	tenantParam = "tenant"
	waitParam   = "wait"
)

// Blocks version of Flush handler. It force-compacts blocks, and triggers shipping.
func (i *Ingester) flushHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", "failed to parse HTTP request in flush handler", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	tenants := r.Form[tenantParam]

	allowedUsers := util.NewAllowedTenants(tenants, nil)
	run := func() {
		ingCtx := i.BasicService.ServiceContext()
		if ingCtx == nil || ingCtx.Err() != nil {
			level.Info(logutil.WithContext(r.Context(), i.logger)).Log("msg", "flushing TSDB blocks: ingester not running, ignoring flush request")
			return
		}

		compactionCallbackCh := make(chan struct{})

		level.Info(logutil.WithContext(r.Context(), i.logger)).Log("msg", "flushing TSDB blocks: triggering compaction")
		select {
		case i.TSDBState.forceCompactTrigger <- requestWithUsersAndCallback{users: allowedUsers, callback: compactionCallbackCh}:
			// Compacting now.
		case <-ingCtx.Done():
			level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", "failed to compact TSDB blocks, ingester not running anymore")
			return
		}

		// Wait until notified about compaction being finished.
		select {
		case <-compactionCallbackCh:
			level.Info(logutil.WithContext(r.Context(), i.logger)).Log("msg", "finished compacting TSDB blocks")
		case <-ingCtx.Done():
			level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", "failed to compact TSDB blocks, ingester not running anymore")
			return
		}

		if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
			shippingCallbackCh := make(chan struct{}) // must be new channel, as compactionCallbackCh is closed now.

			level.Info(logutil.WithContext(r.Context(), i.logger)).Log("msg", "flushing TSDB blocks: triggering shipping")

			select {
			case i.TSDBState.shipTrigger <- requestWithUsersAndCallback{users: allowedUsers, callback: shippingCallbackCh}:
				// shipping now
			case <-ingCtx.Done():
				level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", "failed to ship TSDB blocks, ingester not running anymore")
				return
			}

			// Wait until shipping finished.
			select {
			case <-shippingCallbackCh:
				level.Info(logutil.WithContext(r.Context(), i.logger)).Log("msg", "shipping of TSDB blocks finished")
			case <-ingCtx.Done():
				level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", "failed to ship TSDB blocks, ingester not running anymore")
				return
			}
		}

		level.Info(logutil.WithContext(r.Context(), i.logger)).Log("msg", "flushing TSDB blocks: finished")
	}

	if len(r.Form[waitParam]) > 0 && r.Form[waitParam][0] == "true" {
		// Run synchronously. This simplifies and speeds up tests.
		run()
	} else {
		go run()
	}

	w.WriteHeader(http.StatusNoContent)
}

// ModeHandler Change mode of ingester.
func (i *Ingester) ModeHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		respMsg := "failed to parse HTTP request in mode handler"
		level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", respMsg, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		// We ignore errors here, because we cannot do anything about them.
		_, _ = w.Write([]byte(respMsg))
		return
	}

	currentState := i.lifecycler.GetState()
	reqMode := strings.ToUpper(r.Form.Get("mode"))
	switch reqMode {
	case "READONLY":
		if currentState != ring.READONLY {
			err = i.lifecycler.ChangeState(r.Context(), ring.READONLY)
			if err != nil {
				respMsg := fmt.Sprintf("failed to change state: %s", err)
				level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", respMsg)
				w.WriteHeader(http.StatusBadRequest)
				// We ignore errors here, because we cannot do anything about them.
				_, _ = w.Write([]byte(respMsg))
				return
			}
		}
	case "ACTIVE":
		if currentState != ring.ACTIVE {
			err = i.lifecycler.ChangeState(r.Context(), ring.ACTIVE)
			if err != nil {
				respMsg := fmt.Sprintf("failed to change state: %s", err)
				level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", respMsg)
				w.WriteHeader(http.StatusBadRequest)
				// We ignore errors here, because we cannot do anything about them.
				_, _ = w.Write([]byte(respMsg))
				return
			}
		}
	default:
		respMsg := fmt.Sprintf("invalid mode input: %s", html.EscapeString(reqMode))
		level.Warn(logutil.WithContext(r.Context(), i.logger)).Log("msg", respMsg)
		w.WriteHeader(http.StatusBadRequest)
		// We ignore errors here, because we cannot do anything about them.
		_, _ = w.Write([]byte(respMsg))
		return
	}

	respMsg := fmt.Sprintf("Ingester mode %s", i.lifecycler.GetState())
	level.Info(logutil.WithContext(r.Context(), i.logger)).Log("msg", respMsg)
	w.WriteHeader(http.StatusOK)
	// We ignore errors here, because we cannot do anything about them.
	_, _ = w.Write([]byte(respMsg))
}

func (i *Ingester) getInstanceLimits() *InstanceLimits {
	// Don't apply any limits while starting. We especially don't want to apply series in memory limit while replaying WAL.
	if i.State() == services.Starting {
		return nil
	}

	if i.cfg.InstanceLimitsFn == nil {
		return defaultInstanceLimits
	}

	l := i.cfg.InstanceLimitsFn()
	if l == nil {
		return defaultInstanceLimits
	}

	return l
}

// stopIncomingRequests is called during the shutdown process.
func (i *Ingester) stopIncomingRequests() {
	i.stoppedMtx.Lock()
	defer i.stoppedMtx.Unlock()
	i.stopped = true
}

// metadataQueryRange returns the best range to query for metadata queries based on the timerange in the ingester.
func metadataQueryRange(queryStart, queryEnd int64, db *userTSDB, queryIngestersWithin time.Duration) (mint, maxt int64, err error) {
	if queryIngestersWithin > 0 {
		// If the feature for querying metadata from store-gateway is enabled,
		// then we don't want to manipulate the mint and maxt.
		return queryStart, queryEnd, nil
	}

	// Ingesters are run with limited retention and we don't support querying the store-gateway for labels yet.
	// This means if someone loads a dashboard that is outside the range of the ingester, and we only return the
	// data for the timerange requested (which will be empty), the dashboards will break. To fix this we should
	// return the "head block" range until we can query the store-gateway.

	// Now the question would be what to do when the query is partially in the ingester range. I would err on the side
	// of caution and query the entire db, as I can't think of a good way to query the head + the overlapping range.
	mint, maxt = queryStart, queryEnd

	lowestTs, err := db.StartTime()
	if err != nil {
		return mint, maxt, err
	}

	// Completely outside.
	if queryEnd < lowestTs {
		mint, maxt = db.Head().MinTime(), db.Head().MaxTime()
	} else if queryStart < lowestTs {
		// Partially inside.
		mint, maxt = 0, math.MaxInt64
	}

	return
}

func wrappedTSDBIngestErr(ingestErr error, timestamp model.Time, labels []cortexpb.LabelAdapter) error {
	if ingestErr == nil {
		return nil
	}

	switch {
	case errors.Is(ingestErr, storage.ErrDuplicateSampleForTimestamp):
		return fmt.Errorf(errTSDBIngestWithTimestamp, ingestErr, cortexpb.FromLabelAdaptersToLabels(labels).String())
	default:
		return fmt.Errorf(errTSDBIngest, ingestErr, timestamp.Time().UTC().Format(time.RFC3339Nano), cortexpb.FromLabelAdaptersToLabels(labels).String())
	}
}

func wrappedTSDBIngestExemplarErr(ingestErr error, timestamp model.Time, seriesLabels, exemplarLabels []cortexpb.LabelAdapter) error {
	if ingestErr == nil {
		return nil
	}

	return fmt.Errorf(errTSDBIngestExemplar, ingestErr, timestamp.Time().UTC().Format(time.RFC3339Nano),
		cortexpb.FromLabelAdaptersToLabels(seriesLabels).String(),
		cortexpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
	)
}

func getTimeSeriesChunksSlice() []client.TimeSeriesChunk {
	if p := tsChunksPool.Get(); p != nil {
		return p
	}

	return make([]client.TimeSeriesChunk, 0, queryStreamBatchSize)
}

func putTimeSeriesChunksSlice(p []client.TimeSeriesChunk) {
	if p != nil {
		tsChunksPool.Put(p[:0])
	}
}

type labelSetReasonCounters struct {
	counters map[uint64]*labelSetReasonCounter
}

type labelSetReasonCounter struct {
	reasonCounter map[string]int
	lbls          labels.Labels
}

func newLabelSetReasonCounters() *labelSetReasonCounters {
	return &labelSetReasonCounters{counters: make(map[uint64]*labelSetReasonCounter)}
}

func (c *labelSetReasonCounters) increment(matchedLabelSetLimits []validation.LimitsPerLabelSet, reason string) {
	for _, l := range matchedLabelSetLimits {
		if rc, exists := c.counters[l.Hash]; exists {
			rc.reasonCounter[reason]++
		} else {
			c.counters[l.Hash] = &labelSetReasonCounter{
				reasonCounter: map[string]int{
					reason: 1,
				},
				lbls: l.LabelSet,
			}
		}
	}
}
