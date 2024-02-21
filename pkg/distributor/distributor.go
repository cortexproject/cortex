package distributor

import (
	"context"
	"flag"
	"fmt"
	io "io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/scrape"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ha"
	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/ring"
	ring_client "github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/extract"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	util_math "github.com/cortexproject/cortex/pkg/util/math"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	emptyPreallocSeries = cortexpb.PreallocTimeseries{}

	supportedShardingStrategies = []string{util.ShardingStrategyDefault, util.ShardingStrategyShuffle}

	// Validation errors.
	errInvalidShardingStrategy = errors.New("invalid sharding strategy")
	errInvalidTenantShardSize  = errors.New("invalid tenant shard size. The value must be greater than or equal to 0")

	// Distributor instance limits errors.
	errTooManyInflightPushRequests    = errors.New("too many inflight push requests in distributor")
	errMaxSamplesPushRateLimitReached = errors.New("distributor's samples push rate limit reached")
)

const (
	// ringKey is the key under which we store the distributors ring in the KVStore.
	ringKey = "distributor"

	typeSamples  = "samples"
	typeMetadata = "metadata"

	instanceIngestionRateTickInterval = time.Second
)

// Distributor is a storage.SampleAppender and a client.Querier which
// forwards appends and queries to individual ingesters.
type Distributor struct {
	services.Service

	cfg           Config
	log           log.Logger
	ingestersRing ring.ReadRing
	ingesterPool  *ring_client.Pool
	limits        *validation.Overrides

	// The global rate limiter requires a distributors ring to count
	// the number of healthy instances
	distributorsLifeCycler *ring.Lifecycler
	distributorsRing       *ring.Ring

	// For handling HA replicas.
	HATracker *ha.HATracker

	// Per-user rate limiter.
	ingestionRateLimiter *limiter.RateLimiter

	// Manager for subservices (HA Tracker, distributor ring and client pool)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	activeUsers *util.ActiveUsersCleanupService

	ingestionRate        *util_math.EwmaRate
	inflightPushRequests atomic.Int64

	// Metrics
	queryDuration                    *instrument.HistogramCollector
	receivedSamples                  *prometheus.CounterVec
	receivedExemplars                *prometheus.CounterVec
	receivedMetadata                 *prometheus.CounterVec
	incomingSamples                  *prometheus.CounterVec
	incomingExemplars                *prometheus.CounterVec
	incomingMetadata                 *prometheus.CounterVec
	nonHASamples                     *prometheus.CounterVec
	dedupedSamples                   *prometheus.CounterVec
	labelsHistogram                  prometheus.Histogram
	ingesterAppends                  *prometheus.CounterVec
	ingesterAppendFailures           *prometheus.CounterVec
	ingesterQueries                  *prometheus.CounterVec
	ingesterQueryFailures            *prometheus.CounterVec
	replicationFactor                prometheus.Gauge
	latestSeenSampleTimestampPerUser *prometheus.GaugeVec
}

// Config contains the configuration required to
// create a Distributor
type Config struct {
	PoolConfig PoolConfig `yaml:"pool"`

	HATrackerConfig HATrackerConfig `yaml:"ha_tracker"`

	MaxRecvMsgSize  int           `yaml:"max_recv_msg_size"`
	RemoteTimeout   time.Duration `yaml:"remote_timeout"`
	ExtraQueryDelay time.Duration `yaml:"extra_queue_delay"`

	ShardingStrategy         string `yaml:"sharding_strategy"`
	ShardByAllLabels         bool   `yaml:"shard_by_all_labels"`
	ExtendWrites             bool   `yaml:"extend_writes"`
	SignWriteRequestsEnabled bool   `yaml:"sign_write_requests"`

	// Distributors ring
	DistributorRing RingConfig `yaml:"ring"`

	// for testing and for extending the ingester by adding calls to the client
	IngesterClientFactory ring_client.PoolFactory `yaml:"-"`

	// when true the distributor does not validate the label name, Cortex doesn't directly use
	// this (and should never use it) but this feature is used by other projects built on top of it
	SkipLabelNameValidation bool `yaml:"-"`

	// This config is dynamically injected because defined in the querier config.
	ShuffleShardingLookbackPeriod time.Duration `yaml:"-"`

	// ZoneResultsQuorumMetadata enables zone results quorum when querying ingester replication set
	// with metadata APIs (labels names, values and series). When zone awareness is enabled, only results
	// from quorum number of zones will be included to reduce data merged and improve performance.
	ZoneResultsQuorumMetadata bool `yaml:"zone_results_quorum_metadata"`

	// Limits for distributor
	InstanceLimits InstanceLimits `yaml:"instance_limits"`
}

type InstanceLimits struct {
	MaxIngestionRate        float64 `yaml:"max_ingestion_rate"`
	MaxInflightPushRequests int     `yaml:"max_inflight_push_requests"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.PoolConfig.RegisterFlags(f)
	cfg.HATrackerConfig.RegisterFlags(f)
	cfg.DistributorRing.RegisterFlags(f)

	f.IntVar(&cfg.MaxRecvMsgSize, "distributor.max-recv-msg-size", 100<<20, "remote_write API max receive message size (bytes).")
	f.DurationVar(&cfg.RemoteTimeout, "distributor.remote-timeout", 2*time.Second, "Timeout for downstream ingesters.")
	f.DurationVar(&cfg.ExtraQueryDelay, "distributor.extra-query-delay", 0, "Time to wait before sending more than the minimum successful query requests.")
	f.BoolVar(&cfg.ShardByAllLabels, "distributor.shard-by-all-labels", false, "Distribute samples based on all labels, as opposed to solely by user and metric name.")
	f.BoolVar(&cfg.SignWriteRequestsEnabled, "distributor.sign-write-requests", false, "EXPERIMENTAL: If enabled, sign the write request between distributors and ingesters.")
	f.StringVar(&cfg.ShardingStrategy, "distributor.sharding-strategy", util.ShardingStrategyDefault, fmt.Sprintf("The sharding strategy to use. Supported values are: %s.", strings.Join(supportedShardingStrategies, ", ")))
	f.BoolVar(&cfg.ExtendWrites, "distributor.extend-writes", true, "Try writing to an additional ingester in the presence of an ingester not in the ACTIVE state. It is useful to disable this along with -ingester.unregister-on-shutdown=false in order to not spread samples to extra ingesters during rolling restarts with consistent naming.")
	f.BoolVar(&cfg.ZoneResultsQuorumMetadata, "distributor.zone-results-quorum-metadata", false, "If zone awareness and this both enabled, when querying metadata APIs (labels names, values and series), only results from quorum number of zones will be included.")

	f.Float64Var(&cfg.InstanceLimits.MaxIngestionRate, "distributor.instance-limits.max-ingestion-rate", 0, "Max ingestion rate (samples/sec) that this distributor will accept. This limit is per-distributor, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. 0 = unlimited.")
	f.IntVar(&cfg.InstanceLimits.MaxInflightPushRequests, "distributor.instance-limits.max-inflight-push-requests", 0, "Max inflight push requests that this distributor can handle. This limit is per-distributor, not per-tenant. Additional requests will be rejected. 0 = unlimited.")
}

// Validate config and returns error on failure
func (cfg *Config) Validate(limits validation.Limits) error {
	if !util.StringsContain(supportedShardingStrategies, cfg.ShardingStrategy) {
		return errInvalidShardingStrategy
	}

	if cfg.ShardingStrategy == util.ShardingStrategyShuffle && limits.IngestionTenantShardSize < 0 {
		return errInvalidTenantShardSize
	}

	haHATrackerConfig := cfg.HATrackerConfig.ToHATrackerConfig()

	return haHATrackerConfig.Validate()
}

const (
	instanceLimitsMetric     = "cortex_distributor_instance_limits"
	instanceLimitsMetricHelp = "Instance limits used by this distributor." // Must be same for all registrations.
	limitLabel               = "limit"
)

// New constructs a new Distributor
func New(cfg Config, clientConfig ingester_client.Config, limits *validation.Overrides, ingestersRing ring.ReadRing, canJoinDistributorsRing bool, reg prometheus.Registerer, log log.Logger) (*Distributor, error) {
	if cfg.IngesterClientFactory == nil {
		cfg.IngesterClientFactory = func(addr string) (ring_client.PoolClient, error) {
			return ingester_client.MakeIngesterClient(addr, clientConfig)
		}
	}

	cfg.PoolConfig.RemoteTimeout = cfg.RemoteTimeout

	haTrackerStatusConfig := ha.HATrackerStatusConfig{
		Title:             "Cortex HA Tracker Status",
		ReplicaGroupLabel: "Cluster",
	}
	haTracker, err := ha.NewHATracker(cfg.HATrackerConfig.ToHATrackerConfig(), limits, haTrackerStatusConfig, prometheus.WrapRegistererWithPrefix("cortex_", reg), "distributor-hatracker", log)
	if err != nil {
		return nil, err
	}

	subservices := []services.Service(nil)
	subservices = append(subservices, haTracker)

	// Create the configured ingestion rate limit strategy (local or global). In case
	// it's an internal dependency and can't join the distributors ring, we skip rate
	// limiting.
	var ingestionRateStrategy limiter.RateLimiterStrategy
	var distributorsLifeCycler *ring.Lifecycler
	var distributorsRing *ring.Ring

	if !canJoinDistributorsRing {
		ingestionRateStrategy = newInfiniteIngestionRateStrategy()
	} else if limits.IngestionRateStrategy() == validation.GlobalIngestionRateStrategy {
		distributorsLifeCycler, err = ring.NewLifecycler(cfg.DistributorRing.ToLifecyclerConfig(), nil, "distributor", ringKey, true, true, log, prometheus.WrapRegistererWithPrefix("cortex_", reg))
		if err != nil {
			return nil, err
		}

		distributorsRing, err = ring.New(cfg.DistributorRing.ToRingConfig(), "distributor", ringKey, log, prometheus.WrapRegistererWithPrefix("cortex_", reg))
		if err != nil {
			return nil, errors.Wrap(err, "failed to initialize distributors' ring client")
		}
		subservices = append(subservices, distributorsLifeCycler, distributorsRing)

		ingestionRateStrategy = newGlobalIngestionRateStrategy(limits, distributorsLifeCycler)
	} else {
		ingestionRateStrategy = newLocalIngestionRateStrategy(limits)
	}

	d := &Distributor{
		cfg:                    cfg,
		log:                    log,
		ingestersRing:          ingestersRing,
		ingesterPool:           NewPool(cfg.PoolConfig, ingestersRing, cfg.IngesterClientFactory, log),
		distributorsLifeCycler: distributorsLifeCycler,
		distributorsRing:       distributorsRing,
		limits:                 limits,
		ingestionRateLimiter:   limiter.NewRateLimiter(ingestionRateStrategy, 10*time.Second),
		HATracker:              haTracker,
		ingestionRate:          util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval),

		queryDuration: instrument.NewHistogramCollector(promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "distributor_query_duration_seconds",
			Help:      "Time spent executing expression and exemplar queries.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		}, []string{"method", "status_code"})),
		receivedSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_samples_total",
			Help:      "The total number of received samples, excluding rejected and deduped samples.",
		}, []string{"user"}),
		receivedExemplars: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_exemplars_total",
			Help:      "The total number of received exemplars, excluding rejected and deduped exemplars.",
		}, []string{"user"}),
		receivedMetadata: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_metadata_total",
			Help:      "The total number of received metadata, excluding rejected.",
		}, []string{"user"}),
		incomingSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_samples_in_total",
			Help:      "The total number of samples that have come in to the distributor, including rejected or deduped samples.",
		}, []string{"user"}),
		incomingExemplars: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_exemplars_in_total",
			Help:      "The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.",
		}, []string{"user"}),
		incomingMetadata: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_metadata_in_total",
			Help:      "The total number of metadata that have come in to the distributor, including rejected.",
		}, []string{"user"}),
		nonHASamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_non_ha_samples_received_total",
			Help:      "The total number of received samples for a user that has HA tracking turned on, but the sample didn't contain both HA labels.",
		}, []string{"user"}),
		dedupedSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_deduped_samples_total",
			Help:      "The total number of deduplicated samples.",
		}, []string{"user", "cluster"}),
		labelsHistogram: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "labels_per_sample",
			Help:      "Number of labels per sample.",
			Buckets:   []float64{5, 10, 15, 20, 25},
		}),
		ingesterAppends: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_ingester_appends_total",
			Help:      "The total number of batch appends sent to ingesters.",
		}, []string{"ingester", "type"}),
		ingesterAppendFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_ingester_append_failures_total",
			Help:      "The total number of failed batch appends sent to ingesters.",
		}, []string{"ingester", "type", "status"}),
		ingesterQueries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_ingester_queries_total",
			Help:      "The total number of queries sent to ingesters.",
		}, []string{"ingester"}),
		ingesterQueryFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_ingester_query_failures_total",
			Help:      "The total number of failed queries sent to ingesters.",
		}, []string{"ingester"}),
		replicationFactor: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "distributor_replication_factor",
			Help:      "The configured replication factor.",
		}),
		latestSeenSampleTimestampPerUser: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_distributor_latest_seen_sample_timestamp_seconds",
			Help: "Unix timestamp of latest received sample per user.",
		}, []string{"user"}),
	}

	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        instanceLimitsMetric,
		Help:        instanceLimitsMetricHelp,
		ConstLabels: map[string]string{limitLabel: "max_inflight_push_requests"},
	}).Set(float64(cfg.InstanceLimits.MaxInflightPushRequests))
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        instanceLimitsMetric,
		Help:        instanceLimitsMetricHelp,
		ConstLabels: map[string]string{limitLabel: "max_ingestion_rate"},
	}).Set(cfg.InstanceLimits.MaxIngestionRate)

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_inflight_push_requests",
		Help: "Current number of inflight push requests in distributor.",
	}, func() float64 {
		return float64(d.inflightPushRequests.Load())
	})
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_ingestion_rate_samples_per_second",
		Help: "Current ingestion rate in samples/sec that distributor is using to limit access.",
	}, func() float64 {
		return d.ingestionRate.Rate()
	})

	d.replicationFactor.Set(float64(ingestersRing.ReplicationFactor()))
	d.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(d.cleanupInactiveUser)

	subservices = append(subservices, d.ingesterPool, d.activeUsers)
	d.subservices, err = services.NewManager(subservices...)
	if err != nil {
		return nil, err
	}
	d.subservicesWatcher = services.NewFailureWatcher()
	d.subservicesWatcher.WatchManager(d.subservices)

	d.Service = services.NewBasicService(d.starting, d.running, d.stopping)
	return d, nil
}

func (d *Distributor) starting(ctx context.Context) error {
	if d.cfg.InstanceLimits != (InstanceLimits{}) {
		util_log.WarnExperimentalUse("distributor instance limits")
	}

	// Only report success if all sub-services start properly
	return services.StartManagerAndAwaitHealthy(ctx, d.subservices)
}

func (d *Distributor) running(ctx context.Context) error {
	ingestionRateTicker := time.NewTicker(instanceIngestionRateTickInterval)
	defer ingestionRateTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ingestionRateTicker.C:
			d.ingestionRate.Tick()

		case err := <-d.subservicesWatcher.Chan():
			return errors.Wrap(err, "distributor subservice failed")
		}
	}
}

func (d *Distributor) cleanupInactiveUser(userID string) {
	d.ingestersRing.CleanupShuffleShardCache(userID)

	d.HATracker.CleanupHATrackerMetricsForUser(userID)

	d.receivedSamples.DeleteLabelValues(userID)
	d.receivedExemplars.DeleteLabelValues(userID)
	d.receivedMetadata.DeleteLabelValues(userID)
	d.incomingSamples.DeleteLabelValues(userID)
	d.incomingExemplars.DeleteLabelValues(userID)
	d.incomingMetadata.DeleteLabelValues(userID)
	d.nonHASamples.DeleteLabelValues(userID)
	d.latestSeenSampleTimestampPerUser.DeleteLabelValues(userID)

	if err := util.DeleteMatchingLabels(d.dedupedSamples, map[string]string{"user": userID}); err != nil {
		level.Warn(d.log).Log("msg", "failed to remove cortex_distributor_deduped_samples_total metric for user", "user", userID, "err", err)
	}

	validation.DeletePerUserValidationMetrics(userID, d.log)
}

// Called after distributor is asked to stop via StopAsync.
func (d *Distributor) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), d.subservices)
}

func (d *Distributor) tokenForLabels(userID string, labels []cortexpb.LabelAdapter) (uint32, error) {
	if d.cfg.ShardByAllLabels {
		return shardByAllLabels(userID, labels), nil
	}

	unsafeMetricName, err := extract.UnsafeMetricNameFromLabelAdapters(labels)
	if err != nil {
		return 0, err
	}
	return shardByMetricName(userID, unsafeMetricName), nil
}

func (d *Distributor) tokenForMetadata(userID string, metricName string) uint32 {
	if d.cfg.ShardByAllLabels {
		return shardByMetricName(userID, metricName)
	}

	return shardByUser(userID)
}

// shardByMetricName returns the token for the given metric. The provided metricName
// is guaranteed to not be retained.
func shardByMetricName(userID string, metricName string) uint32 {
	h := shardByUser(userID)
	h = ingester_client.HashAdd32(h, metricName)
	return h
}

func shardByUser(userID string) uint32 {
	h := ingester_client.HashNew32()
	h = ingester_client.HashAdd32(h, userID)
	return h
}

// This function generates different values for different order of same labels.
func shardByAllLabels(userID string, labels []cortexpb.LabelAdapter) uint32 {
	h := shardByUser(userID)
	for _, label := range labels {
		if len(label.Value) > 0 {
			h = ingester_client.HashAdd32(h, label.Name)
			h = ingester_client.HashAdd32(h, label.Value)
		}
	}
	return h
}

// Remove the label labelname from a slice of LabelPairs if it exists.
func removeLabel(labelName string, labels *[]cortexpb.LabelAdapter) {
	for i := 0; i < len(*labels); i++ {
		pair := (*labels)[i]
		if pair.Name == labelName {
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
			return
		}
	}
}

// Returns a boolean that indicates whether or not we want to remove the replica label going forward,
// and an error that indicates whether we want to accept samples based on the cluster/replica found in ts.
// nil for the error means accept the sample.
func (d *Distributor) checkSample(ctx context.Context, userID, cluster, replica string, limits *validation.Limits) (removeReplicaLabel bool, _ error) {
	// If the sample doesn't have either HA label, accept it.
	// At the moment we want to accept these samples by default.
	if cluster == "" || replica == "" {
		return false, nil
	}

	// If replica label is too long, don't use it. We accept the sample here, but it will fail validation later anyway.
	if len(replica) > limits.MaxLabelValueLength {
		return false, nil
	}

	// At this point we know we have both HA labels, we should lookup
	// the cluster/instance here to see if we want to accept this sample.
	err := d.HATracker.CheckReplica(ctx, userID, cluster, replica, time.Now())
	// checkReplica should only have returned an error if there was a real error talking to Consul, or if the replica labels don't match.
	if err != nil { // Don't accept the sample.
		return false, err
	}
	return true, nil
}

// Validates a single series from a write request. Will remove labels if
// any are configured to be dropped for the user ID.
// Returns the validated series with it's labels/samples, and any error.
// The returned error may retain the series labels.
func (d *Distributor) validateSeries(ts cortexpb.PreallocTimeseries, userID string, skipLabelNameValidation bool, limits *validation.Limits) (cortexpb.PreallocTimeseries, validation.ValidationError) {
	d.labelsHistogram.Observe(float64(len(ts.Labels)))

	if err := validation.ValidateLabels(limits, userID, ts.Labels, skipLabelNameValidation); err != nil {
		return emptyPreallocSeries, err
	}

	var samples []cortexpb.Sample
	if len(ts.Samples) > 0 {
		// Only alloc when data present
		samples = make([]cortexpb.Sample, 0, len(ts.Samples))
		for _, s := range ts.Samples {
			if err := validation.ValidateSample(limits, userID, ts.Labels, s); err != nil {
				return emptyPreallocSeries, err
			}
			samples = append(samples, s)
		}
	}

	var exemplars []cortexpb.Exemplar
	if len(ts.Exemplars) > 0 {
		// Only alloc when data present
		exemplars = make([]cortexpb.Exemplar, 0, len(ts.Exemplars))
		for _, e := range ts.Exemplars {
			if err := validation.ValidateExemplar(userID, ts.Labels, e); err != nil {
				// An exemplar validation error prevents ingesting samples
				// in the same series object. However, because the current Prometheus
				// remote write implementation only populates one or the other,
				// there never will be any.
				return emptyPreallocSeries, err
			}
			exemplars = append(exemplars, e)
		}
	}

	var histograms []cortexpb.Histogram
	if len(ts.Histograms) > 0 {
		// Only alloc when data present
		histograms = make([]cortexpb.Histogram, 0, len(ts.Histograms))
		// TODO(yeya24): we need to have validations for native histograms
		// at some point. Skip validations for now.
		histograms = append(histograms, ts.Histograms...)
	}

	return cortexpb.PreallocTimeseries{
			TimeSeries: &cortexpb.TimeSeries{
				Labels:     ts.Labels,
				Samples:    samples,
				Exemplars:  exemplars,
				Histograms: histograms,
			},
		},
		nil
}

// Push implements client.IngesterServer
func (d *Distributor) Push(ctx context.Context, req *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	span, ctx := opentracing.StartSpanFromContext(ctx, "Distributor.Push")
	defer span.Finish()

	// We will report *this* request in the error too.
	inflight := d.inflightPushRequests.Inc()
	defer d.inflightPushRequests.Dec()

	now := time.Now()
	d.activeUsers.UpdateUserTimestamp(userID, now)

	numSamples := 0
	numExemplars := 0
	for _, ts := range req.Timeseries {
		numSamples += len(ts.Samples) + len(ts.Histograms)
		numExemplars += len(ts.Exemplars)
	}
	// Count the total samples, exemplars in, prior to validation or deduplication, for comparison with other metrics.
	d.incomingSamples.WithLabelValues(userID).Add(float64(numSamples))
	d.incomingExemplars.WithLabelValues(userID).Add(float64(numExemplars))
	// Count the total number of metadata in.
	d.incomingMetadata.WithLabelValues(userID).Add(float64(len(req.Metadata)))

	if d.cfg.InstanceLimits.MaxInflightPushRequests > 0 && inflight > int64(d.cfg.InstanceLimits.MaxInflightPushRequests) {
		return nil, errTooManyInflightPushRequests
	}

	if d.cfg.InstanceLimits.MaxIngestionRate > 0 {
		if rate := d.ingestionRate.Rate(); rate >= d.cfg.InstanceLimits.MaxIngestionRate {
			return nil, errMaxSamplesPushRateLimitReached
		}
	}

	removeReplica := false
	// Cache user limit with overrides so we spend less CPU doing locking. See issue #4904
	limits := d.limits.GetOverridesForUser(userID)

	if limits.AcceptHASamples && len(req.Timeseries) > 0 {
		cluster, replica := findHALabels(limits.HAReplicaLabel, limits.HAClusterLabel, req.Timeseries[0].Labels)
		removeReplica, err = d.checkSample(ctx, userID, cluster, replica, limits)
		if err != nil {
			// Ensure the request slice is reused if the series get deduped.
			cortexpb.ReuseSlice(req.Timeseries)

			if errors.Is(err, ha.ReplicasNotMatchError{}) {
				// These samples have been deduped.
				d.dedupedSamples.WithLabelValues(userID, cluster).Add(float64(numSamples))
				return nil, httpgrpc.Errorf(http.StatusAccepted, err.Error())
			}

			if errors.Is(err, ha.TooManyReplicaGroupsError{}) {
				validation.DiscardedSamples.WithLabelValues(validation.TooManyHAClusters, userID).Add(float64(numSamples))
				return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
			}

			return nil, err
		}
		// If there wasn't an error but removeReplica is false that means we didn't find both HA labels.
		if !removeReplica {
			d.nonHASamples.WithLabelValues(userID).Add(float64(numSamples))
		}
	}

	// A WriteRequest can only contain series or metadata but not both. This might change in the future.
	seriesKeys, validatedTimeseries, validatedSamples, validatedExemplars, firstPartialErr, err := d.prepareSeriesKeys(ctx, req, userID, limits, removeReplica)
	if err != nil {
		return nil, err
	}
	metadataKeys, validatedMetadata, firstPartialErr := d.prepareMetadataKeys(req, limits, userID, firstPartialErr)

	d.receivedSamples.WithLabelValues(userID).Add(float64(validatedSamples))
	d.receivedExemplars.WithLabelValues(userID).Add(float64(validatedExemplars))
	d.receivedMetadata.WithLabelValues(userID).Add(float64(len(validatedMetadata)))

	if len(seriesKeys) == 0 && len(metadataKeys) == 0 {
		// Ensure the request slice is reused if there's no series or metadata passing the validation.
		cortexpb.ReuseSlice(req.Timeseries)

		return &cortexpb.WriteResponse{}, firstPartialErr
	}

	totalN := validatedSamples + validatedExemplars + len(validatedMetadata)
	if !d.ingestionRateLimiter.AllowN(now, userID, totalN) {
		// Ensure the request slice is reused if the request is rate limited.
		cortexpb.ReuseSlice(req.Timeseries)

		validation.DiscardedSamples.WithLabelValues(validation.RateLimited, userID).Add(float64(validatedSamples))
		validation.DiscardedExemplars.WithLabelValues(validation.RateLimited, userID).Add(float64(validatedExemplars))
		validation.DiscardedMetadata.WithLabelValues(validation.RateLimited, userID).Add(float64(len(validatedMetadata)))
		// Return a 429 here to tell the client it is going too fast.
		// Client may discard the data or slow down and re-send.
		// Prometheus v2.26 added a remote-write option 'retry_on_http_429'.
		return nil, httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (%v) exceeded while adding %d samples and %d metadata", d.ingestionRateLimiter.Limit(now, userID), validatedSamples, len(validatedMetadata))
	}

	// totalN included samples and metadata. Ingester follows this pattern when computing its ingestion rate.
	d.ingestionRate.Add(int64(totalN))

	subRing := d.ingestersRing

	// Obtain a subring if required.
	if d.cfg.ShardingStrategy == util.ShardingStrategyShuffle {
		subRing = d.ingestersRing.ShuffleShard(userID, limits.IngestionTenantShardSize)
	}

	keys := append(seriesKeys, metadataKeys...)
	initialMetadataIndex := len(seriesKeys)

	err = d.doBatch(ctx, req, subRing, keys, initialMetadataIndex, validatedMetadata, validatedTimeseries, userID)
	if err != nil {
		return nil, err
	}

	return &cortexpb.WriteResponse{}, firstPartialErr
}

func (d *Distributor) doBatch(ctx context.Context, req *cortexpb.WriteRequest, subRing ring.ReadRing, keys []uint32, initialMetadataIndex int, validatedMetadata []*cortexpb.MetricMetadata, validatedTimeseries []cortexpb.PreallocTimeseries, userID string) error {
	span, _ := opentracing.StartSpanFromContext(ctx, "doBatch")
	defer span.Finish()

	// Use a background context to make sure all ingesters get samples even if we return early
	localCtx, cancel := context.WithTimeout(context.Background(), d.cfg.RemoteTimeout)
	localCtx = user.InjectOrgID(localCtx, userID)
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		localCtx = opentracing.ContextWithSpan(localCtx, sp)
	}
	// Get any HTTP headers that are supposed to be added to logs and add to localCtx for later use
	if headerMap := util_log.HeaderMapFromContext(ctx); headerMap != nil {
		localCtx = util_log.ContextWithHeaderMap(localCtx, headerMap)
	}
	// Get clientIP(s) from Context and add it to localCtx
	source := util.GetSourceIPsFromOutgoingCtx(ctx)
	localCtx = util.AddSourceIPsToOutgoingContext(localCtx, source)

	op := ring.WriteNoExtend
	if d.cfg.ExtendWrites {
		op = ring.Write
	}

	return ring.DoBatch(ctx, op, subRing, keys, func(ingester ring.InstanceDesc, indexes []int) error {
		timeseries := make([]cortexpb.PreallocTimeseries, 0, len(indexes))
		var metadata []*cortexpb.MetricMetadata

		for _, i := range indexes {
			if i >= initialMetadataIndex {
				metadata = append(metadata, validatedMetadata[i-initialMetadataIndex])
			} else {
				timeseries = append(timeseries, validatedTimeseries[i])
			}
		}

		return d.send(localCtx, ingester, timeseries, metadata, req.Source)
	}, func() {
		cortexpb.ReuseSlice(req.Timeseries)
		cancel()
	})
}

func (d *Distributor) prepareMetadataKeys(req *cortexpb.WriteRequest, limits *validation.Limits, userID string, firstPartialErr error) ([]uint32, []*cortexpb.MetricMetadata, error) {
	validatedMetadata := make([]*cortexpb.MetricMetadata, 0, len(req.Metadata))
	metadataKeys := make([]uint32, 0, len(req.Metadata))

	for _, m := range req.Metadata {
		err := validation.ValidateMetadata(limits, userID, m)

		if err != nil {
			if firstPartialErr == nil {
				firstPartialErr = err
			}

			continue
		}

		metadataKeys = append(metadataKeys, d.tokenForMetadata(userID, m.MetricFamilyName))
		validatedMetadata = append(validatedMetadata, m)
	}
	return metadataKeys, validatedMetadata, firstPartialErr
}

func (d *Distributor) prepareSeriesKeys(ctx context.Context, req *cortexpb.WriteRequest, userID string, limits *validation.Limits, removeReplica bool) ([]uint32, []cortexpb.PreallocTimeseries, int, int, error, error) {
	pSpan, _ := opentracing.StartSpanFromContext(ctx, "prepareSeriesKeys")
	defer pSpan.Finish()

	// For each timeseries or samples, we compute a hash to distribute across ingesters;
	// check each sample/metadata and discard if outside limits.
	validatedTimeseries := make([]cortexpb.PreallocTimeseries, 0, len(req.Timeseries))
	seriesKeys := make([]uint32, 0, len(req.Timeseries))
	validatedSamples := 0
	validatedExemplars := 0

	var firstPartialErr error

	latestSampleTimestampMs := int64(0)
	defer func() {
		// Update this metric even in case of errors.
		if latestSampleTimestampMs > 0 {
			d.latestSeenSampleTimestampPerUser.WithLabelValues(userID).Set(float64(latestSampleTimestampMs) / 1000)
		}
	}()

	// For each timeseries, compute a hash to distribute across ingesters;
	// check each sample and discard if outside limits.
	skipLabelNameValidation := d.cfg.SkipLabelNameValidation || req.GetSkipLabelNameValidation()
	for _, ts := range req.Timeseries {
		// Use timestamp of latest sample in the series. If samples for series are not ordered, metric for user may be wrong.
		if len(ts.Samples) > 0 {
			latestSampleTimestampMs = util_math.Max64(latestSampleTimestampMs, ts.Samples[len(ts.Samples)-1].TimestampMs)
		}
		// TODO(yeya24): use timestamp of the latest native histogram in the series as well.

		if mrc := limits.MetricRelabelConfigs; len(mrc) > 0 {
			l, _ := relabel.Process(cortexpb.FromLabelAdaptersToLabels(ts.Labels), mrc...)
			if len(l) == 0 {
				// all labels are gone, samples will be discarded
				validation.DiscardedSamples.WithLabelValues(
					validation.DroppedByRelabelConfiguration,
					userID,
				).Add(float64(len(ts.Samples)))
				continue
			}
			ts.Labels = cortexpb.FromLabelsToLabelAdapters(l)
		}

		// If we found both the cluster and replica labels, we only want to include the cluster label when
		// storing series in Cortex. If we kept the replica label we would end up with another series for the same
		// series we're trying to dedupe when HA tracking moves over to a different replica.
		if removeReplica {
			removeLabel(limits.HAReplicaLabel, &ts.Labels)
		}

		for _, labelName := range limits.DropLabels {
			removeLabel(labelName, &ts.Labels)
		}

		if len(ts.Labels) == 0 {
			validation.DiscardedExemplars.WithLabelValues(
				validation.DroppedByUserConfigurationOverride,
				userID,
			).Add(float64(len(ts.Samples)))

			continue
		}

		// We rely on sorted labels in different places:
		// 1) When computing token for labels, and sharding by all labels. Here different order of labels returns
		// different tokens, which is bad.
		// 2) In validation code, when checking for duplicate label names. As duplicate label names are rejected
		// later in the validation phase, we ignore them here.
		sortLabelsIfNeeded(ts.Labels)

		// Generate the sharding token based on the series labels without the HA replica
		// label and dropped labels (if any)
		key, err := d.tokenForLabels(userID, ts.Labels)
		if err != nil {
			return nil, nil, 0, 0, nil, err
		}
		validatedSeries, validationErr := d.validateSeries(ts, userID, skipLabelNameValidation, limits)

		// Errors in validation are considered non-fatal, as one series in a request may contain
		// invalid data but all the remaining series could be perfectly valid.
		if validationErr != nil && firstPartialErr == nil {
			// The series labels may be retained by validationErr but that's not a problem for this
			// use case because we format it calling Error() and then we discard it.
			firstPartialErr = httpgrpc.Errorf(http.StatusBadRequest, validationErr.Error())
		}

		// validateSeries would have returned an emptyPreallocSeries if there were no valid samples.
		if validatedSeries == emptyPreallocSeries {
			continue
		}

		seriesKeys = append(seriesKeys, key)
		validatedTimeseries = append(validatedTimeseries, validatedSeries)
		// TODO(yeya24): add histogram samples as well when supported.
		validatedSamples += len(ts.Samples)
		validatedExemplars += len(ts.Exemplars)
	}
	return seriesKeys, validatedTimeseries, validatedSamples, validatedExemplars, firstPartialErr, nil
}

func sortLabelsIfNeeded(labels []cortexpb.LabelAdapter) {
	// no need to run sort.Slice, if labels are already sorted, which is most of the time.
	// we can avoid extra memory allocations (mostly interface-related) this way.
	sorted := true
	last := ""
	for _, l := range labels {
		if strings.Compare(last, l.Name) > 0 {
			sorted = false
			break
		}
		last = l.Name
	}

	if sorted {
		return
	}

	sort.Slice(labels, func(i, j int) bool {
		return strings.Compare(labels[i].Name, labels[j].Name) < 0
	})
}

func (d *Distributor) send(ctx context.Context, ingester ring.InstanceDesc, timeseries []cortexpb.PreallocTimeseries, metadata []*cortexpb.MetricMetadata, source cortexpb.WriteRequest_SourceEnum) error {
	h, err := d.ingesterPool.GetClientFor(ingester.Addr)
	if err != nil {
		return err
	}
	c := h.(ingester_client.HealthAndIngesterClient)

	req := cortexpb.PreallocWriteRequestFromPool()
	req.Timeseries = timeseries
	req.Metadata = metadata
	req.Source = source

	_, err = c.PushPreAlloc(ctx, req)

	// We should not reuse the req in case of errors:
	// See: https://github.com/grpc/grpc-go/issues/6355
	if err == nil {
		cortexpb.ReuseWriteRequest(req)
	}

	if len(metadata) > 0 {
		d.ingesterAppends.WithLabelValues(ingester.Addr, typeMetadata).Inc()
		if err != nil {
			d.ingesterAppendFailures.WithLabelValues(ingester.Addr, typeMetadata, getErrorStatus(err)).Inc()
		}
	}
	if len(timeseries) > 0 {
		d.ingesterAppends.WithLabelValues(ingester.Addr, typeSamples).Inc()
		if err != nil {
			d.ingesterAppendFailures.WithLabelValues(ingester.Addr, typeSamples, getErrorStatus(err)).Inc()
		}
	}

	return err
}

func getErrorStatus(err error) string {
	status := "5xx"
	httpResp, ok := httpgrpc.HTTPResponseFromError(err)
	if ok && httpResp.Code/100 == 4 {
		status = "4xx"
	}

	return status
}

// ForReplicationSet runs f, in parallel, for all ingesters in the input replication set.
func (d *Distributor) ForReplicationSet(ctx context.Context, replicationSet ring.ReplicationSet, zoneResultsQuorum bool, f func(context.Context, ingester_client.IngesterClient) (interface{}, error)) ([]interface{}, error) {
	return replicationSet.Do(ctx, d.cfg.ExtraQueryDelay, zoneResultsQuorum, func(ctx context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}

		return f(ctx, client.(ingester_client.IngesterClient))
	})
}

func (d *Distributor) LabelValuesForLabelNameCommon(ctx context.Context, from, to model.Time, labelName model.LabelName, f func(ctx context.Context, rs ring.ReplicationSet, req *ingester_client.LabelValuesRequest) ([]interface{}, error), matchers ...*labels.Matcher) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Distributor.LabelValues", opentracing.Tags{
		"name":  labelName,
		"start": from.Unix(),
		"end":   to.Unix(),
	})
	defer span.Finish()
	replicationSet, err := d.GetIngestersForMetadata(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToLabelValuesRequest(labelName, from, to, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := f(ctx, replicationSet, req)
	if err != nil {
		return nil, err
	}

	span, _ = opentracing.StartSpanFromContext(ctx, "response_merge")
	defer span.Finish()
	valueSet := map[string]struct{}{}
	for _, resp := range resps {
		for _, v := range resp.([]string) {
			valueSet[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}

	// We need the values returned to be sorted.
	sort.Strings(values)
	span.SetTag("result_length", len(values))

	return values, nil
}

// LabelValuesForLabelName returns all the label values that are associated with a given label name.
func (d *Distributor) LabelValuesForLabelName(ctx context.Context, from, to model.Time, labelName model.LabelName, matchers ...*labels.Matcher) ([]string, error) {
	return d.LabelValuesForLabelNameCommon(ctx, from, to, labelName, func(ctx context.Context, rs ring.ReplicationSet, req *ingester_client.LabelValuesRequest) ([]interface{}, error) {
		return d.ForReplicationSet(ctx, rs, d.cfg.ZoneResultsQuorumMetadata, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
			resp, err := client.LabelValues(ctx, req)
			if err != nil {
				return nil, err
			}
			return resp.LabelValues, nil
		})
	}, matchers...)
}

// LabelValuesForLabelNameStream returns all the label values that are associated with a given label name.
func (d *Distributor) LabelValuesForLabelNameStream(ctx context.Context, from, to model.Time, labelName model.LabelName, matchers ...*labels.Matcher) ([]string, error) {
	return d.LabelValuesForLabelNameCommon(ctx, from, to, labelName, func(ctx context.Context, rs ring.ReplicationSet, req *ingester_client.LabelValuesRequest) ([]interface{}, error) {
		return d.ForReplicationSet(ctx, rs, d.cfg.ZoneResultsQuorumMetadata, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
			stream, err := client.LabelValuesStream(ctx, req)
			if err != nil {
				return nil, err
			}
			defer stream.CloseSend() //nolint:errcheck
			allLabelValues := []string{}
			for {
				resp, err := stream.Recv()

				if err == io.EOF {
					break
				} else if err != nil {
					return nil, err
				}
				allLabelValues = append(allLabelValues, resp.LabelValues...)
			}

			return allLabelValues, nil
		})
	}, matchers...)
}

func (d *Distributor) LabelNamesCommon(ctx context.Context, from, to model.Time, f func(ctx context.Context, rs ring.ReplicationSet, req *ingester_client.LabelNamesRequest) ([]interface{}, error)) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Distributor.LabelNames", opentracing.Tags{
		"start": from.Unix(),
		"end":   to.Unix(),
	})
	defer span.Finish()
	replicationSet, err := d.GetIngestersForMetadata(ctx)
	if err != nil {
		return nil, err
	}

	req := &ingester_client.LabelNamesRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
	}
	resps, err := f(ctx, replicationSet, req)
	if err != nil {
		return nil, err
	}

	span, _ = opentracing.StartSpanFromContext(ctx, "response_merge")
	defer span.Finish()
	valueSet := map[string]struct{}{}
	for _, resp := range resps {
		for _, v := range resp.([]string) {
			valueSet[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}

	sort.Strings(values)
	span.SetTag("result_length", len(values))

	return values, nil
}

func (d *Distributor) LabelNamesStream(ctx context.Context, from, to model.Time) ([]string, error) {
	return d.LabelNamesCommon(ctx, from, to, func(ctx context.Context, rs ring.ReplicationSet, req *ingester_client.LabelNamesRequest) ([]interface{}, error) {
		return d.ForReplicationSet(ctx, rs, d.cfg.ZoneResultsQuorumMetadata, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
			stream, err := client.LabelNamesStream(ctx, req)
			if err != nil {
				return nil, err
			}
			defer stream.CloseSend() //nolint:errcheck
			allLabelNames := []string{}
			for {
				resp, err := stream.Recv()

				if err == io.EOF {
					break
				} else if err != nil {
					return nil, err
				}
				allLabelNames = append(allLabelNames, resp.LabelNames...)
			}

			return allLabelNames, nil
		})
	})
}

// LabelNames returns all the label names.
func (d *Distributor) LabelNames(ctx context.Context, from, to model.Time) ([]string, error) {
	return d.LabelNamesCommon(ctx, from, to, func(ctx context.Context, rs ring.ReplicationSet, req *ingester_client.LabelNamesRequest) ([]interface{}, error) {
		return d.ForReplicationSet(ctx, rs, d.cfg.ZoneResultsQuorumMetadata, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
			resp, err := client.LabelNames(ctx, req)
			if err != nil {
				return nil, err
			}
			return resp.LabelNames, nil
		})
	})
}

// MetricsForLabelMatchers gets the metrics that match said matchers
func (d *Distributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return d.metricsForLabelMatchersCommon(ctx, from, through, func(ctx context.Context, rs ring.ReplicationSet, req *ingester_client.MetricsForLabelMatchersRequest, metrics *map[model.Fingerprint]model.Metric, mutex *sync.Mutex, queryLimiter *limiter.QueryLimiter) error {
		_, err := d.ForReplicationSet(ctx, rs, d.cfg.ZoneResultsQuorumMetadata, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
			resp, err := client.MetricsForLabelMatchers(ctx, req)
			if err != nil {
				return nil, err
			}
			if err := queryLimiter.AddDataBytes(resp.Size()); err != nil {
				return nil, validation.LimitError(err.Error())
			}
			s := make([][]cortexpb.LabelAdapter, 0, len(resp.Metric))
			for _, m := range resp.Metric {
				s = append(s, m.Labels)
				m := cortexpb.FromLabelAdaptersToMetric(m.Labels)
				fingerprint := m.Fingerprint()
				mutex.Lock()
				(*metrics)[fingerprint] = m
				mutex.Unlock()
			}
			if err := queryLimiter.AddSeries(s...); err != nil {
				return nil, validation.LimitError(err.Error())
			}
			return nil, nil
		})

		return err
	}, matchers...)
}

func (d *Distributor) MetricsForLabelMatchersStream(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return d.metricsForLabelMatchersCommon(ctx, from, through, func(ctx context.Context, rs ring.ReplicationSet, req *ingester_client.MetricsForLabelMatchersRequest, metrics *map[model.Fingerprint]model.Metric, mutex *sync.Mutex, queryLimiter *limiter.QueryLimiter) error {
		_, err := d.ForReplicationSet(ctx, rs, d.cfg.ZoneResultsQuorumMetadata, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
			stream, err := client.MetricsForLabelMatchersStream(ctx, req)
			if err != nil {
				return nil, err
			}
			defer stream.CloseSend() //nolint:errcheck
			for {
				resp, err := stream.Recv()
				if err := queryLimiter.AddDataBytes(resp.Size()); err != nil {
					return nil, validation.LimitError(err.Error())
				}

				if err == io.EOF {
					break
				} else if err != nil {
					return nil, err
				}
				s := make([][]cortexpb.LabelAdapter, 0, len(resp.Metric))
				for _, metric := range resp.Metric {
					m := cortexpb.FromLabelAdaptersToMetricWithCopy(metric.Labels)
					s = append(s, metric.Labels)
					fingerprint := m.Fingerprint()
					mutex.Lock()
					(*metrics)[fingerprint] = m
					mutex.Unlock()
				}
				if err := queryLimiter.AddSeries(s...); err != nil {
					return nil, validation.LimitError(err.Error())
				}
			}

			return nil, nil
		})

		return err
	}, matchers...)
}

func (d *Distributor) metricsForLabelMatchersCommon(ctx context.Context, from, through model.Time, f func(context.Context, ring.ReplicationSet, *ingester_client.MetricsForLabelMatchersRequest, *map[model.Fingerprint]model.Metric, *sync.Mutex, *limiter.QueryLimiter) error, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	replicationSet, err := d.GetIngestersForMetadata(ctx)
	queryLimiter := limiter.QueryLimiterFromContextWithFallback(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToMetricsForLabelMatchersRequest(from, through, matchers)
	if err != nil {
		return nil, err
	}
	mutex := sync.Mutex{}
	metrics := map[model.Fingerprint]model.Metric{}

	err = f(ctx, replicationSet, req, &metrics, &mutex, queryLimiter)

	if err != nil {
		return nil, err
	}

	mutex.Lock()
	result := make([]metric.Metric, 0, len(metrics))
	for _, m := range metrics {
		result = append(result, metric.Metric{
			Metric: m,
		})
	}
	mutex.Unlock()
	return result, nil
}

// MetricsMetadata returns all metric metadata of a user.
func (d *Distributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	replicationSet, err := d.GetIngestersForMetadata(ctx)
	if err != nil {
		return nil, err
	}

	req := &ingester_client.MetricsMetadataRequest{}
	// TODO(gotjosh): We only need to look in all the ingesters if shardByAllLabels is enabled.
	resps, err := d.ForReplicationSet(ctx, replicationSet, d.cfg.ZoneResultsQuorumMetadata, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		return client.MetricsMetadata(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	result := []scrape.MetricMetadata{}
	dedupTracker := map[cortexpb.MetricMetadata]struct{}{}
	for _, resp := range resps {
		r := resp.(*ingester_client.MetricsMetadataResponse)
		for _, m := range r.Metadata {
			// Given we look across all ingesters - dedup the metadata.
			_, ok := dedupTracker[*m]
			if ok {
				continue
			}
			dedupTracker[*m] = struct{}{}

			result = append(result, scrape.MetricMetadata{
				Metric: m.MetricFamilyName,
				Help:   m.Help,
				Unit:   m.Unit,
				Type:   cortexpb.MetricMetadataMetricTypeToMetricType(m.GetType()),
			})
		}
	}

	return result, nil
}

// UserStats returns statistics about the current user.
func (d *Distributor) UserStats(ctx context.Context) (*UserStats, error) {
	replicationSet, err := d.GetIngestersForMetadata(ctx)
	if err != nil {
		return nil, err
	}

	// Make sure we get a successful response from all of them.
	replicationSet.MaxErrors = 0

	req := &ingester_client.UserStatsRequest{}
	resps, err := d.ForReplicationSet(ctx, replicationSet, false, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		return client.UserStats(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	totalStats := &UserStats{}
	for _, resp := range resps {
		r := resp.(*ingester_client.UserStatsResponse)
		totalStats.IngestionRate += r.IngestionRate
		totalStats.APIIngestionRate += r.ApiIngestionRate
		totalStats.RuleIngestionRate += r.RuleIngestionRate
		totalStats.NumSeries += r.NumSeries
		totalStats.ActiveSeries += r.ActiveSeries
	}

	factor := d.ingestersRing.ReplicationFactor()
	totalStats.IngestionRate /= float64(factor)
	totalStats.NumSeries /= uint64(factor)
	totalStats.ActiveSeries /= uint64(factor)

	return totalStats, nil
}

// UserIDStats models ingestion statistics for one user, including the user ID
type UserIDStats struct {
	UserID string `json:"userID"`
	UserStats
}

// AllUserStats returns statistics about all users.
// Note it does not divide by the ReplicationFactor like UserStats()
func (d *Distributor) AllUserStats(ctx context.Context) ([]UserIDStats, error) {
	// Add up by user, across all responses from ingesters
	perUserTotals := make(map[string]UserStats)

	req := &ingester_client.UserStatsRequest{}
	ctx = user.InjectOrgID(ctx, "1") // fake: ingester insists on having an org ID
	// Not using d.ForReplicationSet(), so we can fail after first error.
	replicationSet, err := d.ingestersRing.GetAllHealthy(ring.Read)
	if err != nil {
		return nil, err
	}
	for _, ingester := range replicationSet.Instances {
		client, err := d.ingesterPool.GetClientFor(ingester.Addr)
		if err != nil {
			return nil, err
		}
		resp, err := client.(ingester_client.IngesterClient).AllUserStats(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, u := range resp.Stats {
			s := perUserTotals[u.UserId]
			s.IngestionRate += u.Data.IngestionRate
			s.APIIngestionRate += u.Data.ApiIngestionRate
			s.RuleIngestionRate += u.Data.RuleIngestionRate
			s.NumSeries += u.Data.NumSeries
			s.ActiveSeries += u.Data.ActiveSeries
			perUserTotals[u.UserId] = s
		}
	}

	// Turn aggregated map into a slice for return
	response := make([]UserIDStats, 0, len(perUserTotals))
	for id, stats := range perUserTotals {
		response = append(response, UserIDStats{
			UserID: id,
			UserStats: UserStats{
				IngestionRate:     stats.IngestionRate,
				APIIngestionRate:  stats.APIIngestionRate,
				RuleIngestionRate: stats.RuleIngestionRate,
				NumSeries:         stats.NumSeries,
				ActiveSeries:      stats.ActiveSeries,
			},
		})
	}

	return response, nil
}

func (d *Distributor) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if d.distributorsRing != nil {
		d.distributorsRing.ServeHTTP(w, req)
	} else {
		var ringNotEnabledPage = `
			<!DOCTYPE html>
			<html>
				<head>
					<meta charset="UTF-8">
					<title>Cortex Distributor Status</title>
				</head>
				<body>
					<h1>Cortex Distributor Status</h1>
					<p>Distributor is not running with global limits enabled</p>
				</body>
			</html>`
		util.WriteHTMLResponse(w, ringNotEnabledPage)
	}
}

func findHALabels(replicaLabel, clusterLabel string, labels []cortexpb.LabelAdapter) (string, string) {
	var cluster, replica string
	var pair cortexpb.LabelAdapter

	for _, pair = range labels {
		if pair.Name == replicaLabel {
			replica = pair.Value
		}
		if pair.Name == clusterLabel {
			// cluster label is unmarshalled into yoloString, which retains original remote write request body in memory.
			// Hence, we clone the yoloString to allow the request body to be garbage collected.
			cluster = util.StringsClone(pair.Value)
		}
	}

	return cluster, replica
}
