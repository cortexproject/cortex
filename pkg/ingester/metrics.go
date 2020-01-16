package ingester

import (
	"sync"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type ingesterMetrics struct {
	flushQueueLength      prometheus.Gauge
	ingestedSamples       prometheus.Counter
	ingestedSamplesFail   prometheus.Counter
	queries               prometheus.Counter
	queriedSamples        prometheus.Histogram
	queriedSeries         prometheus.Histogram
	queriedChunks         prometheus.Histogram
	memSeries             prometheus.Gauge
	memUsers              prometheus.Gauge
	memSeriesCreatedTotal *prometheus.CounterVec
	memSeriesRemovedTotal *prometheus.CounterVec
}

func newIngesterMetrics(r prometheus.Registerer) *ingesterMetrics {
	m := &ingesterMetrics{
		flushQueueLength: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_flush_queue_length",
			Help: "The total number of series pending in the flush queue.",
		}),
		ingestedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_total",
			Help: "The total number of samples ingested.",
		}),
		ingestedSamplesFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_failures_total",
			Help: "The total number of samples that errored on ingestion.",
		}),
		queries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_queries_total",
			Help: "The total number of queries the ingester has handled.",
		}),
		queriedSamples: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_ingester_queried_samples",
			Help: "The total number of samples returned from queries.",
			// Could easily return 10m samples per query - 10*(8^(8-1)) = 20.9m.
			Buckets: prometheus.ExponentialBuckets(10, 8, 8),
		}),
		queriedSeries: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_ingester_queried_series",
			Help: "The total number of series returned from queries.",
			// A reasonable upper bound is around 100k - 10*(8^(6-1)) = 327k.
			Buckets: prometheus.ExponentialBuckets(10, 8, 6),
		}),
		queriedChunks: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_ingester_queried_chunks",
			Help: "The total number of chunks returned from queries.",
			// A small number of chunks per series - 10*(8^(7-1)) = 2.6m.
			Buckets: prometheus.ExponentialBuckets(10, 8, 7),
		}),
		memSeries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_series",
			Help: "The current number of series in memory.",
		}),
		memUsers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_users",
			Help: "The current number of users in memory.",
		}),
		memSeriesCreatedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_memory_series_created_total",
			Help: "The total number of series that were created per user.",
		}, []string{"user"}),
		memSeriesRemovedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_memory_series_removed_total",
			Help: "The total number of series that were removed per user.",
		}, []string{"user"}),
	}

	if r != nil {
		r.MustRegister(
			m.flushQueueLength,
			m.ingestedSamples,
			m.ingestedSamplesFail,
			m.queries,
			m.queriedSamples,
			m.queriedSeries,
			m.queriedChunks,
			m.memSeries,
			m.memUsers,
			m.memSeriesCreatedTotal,
			m.memSeriesRemovedTotal,
		)
	}

	return m
}

// TSDB shipper metrics. We aggregate metrics from individual TSDB shippers into
// a single set of counters, which are exposed as Cortex metrics.
type shipperMetrics struct {
	dirSyncs        *prometheus.Desc // sum(thanos_shipper_dir_syncs_total)
	dirSyncFailures *prometheus.Desc // sum(thanos_shipper_dir_sync_failures_total)
	uploads         *prometheus.Desc // sum(thanos_shipper_uploads_total)
	uploadFailures  *prometheus.Desc // sum(thanos_shipper_upload_failures_total)

	regsMu sync.RWMutex                    // custom mutex for shipper registry, to avoid blocking main user state mutex on collection
	regs   map[string]*prometheus.Registry // One prometheus registry (used by shipper) per tenant
}

func newShipperMetrics(r prometheus.Registerer) *shipperMetrics {
	m := &shipperMetrics{
		regs: make(map[string]*prometheus.Registry),

		dirSyncs: prometheus.NewDesc(
			"cortex_ingester_shipper_dir_syncs_total",
			"TSDB: Total dir sync attempts",
			nil, nil),
		dirSyncFailures: prometheus.NewDesc(
			"cortex_ingester_shipper_dir_sync_failures_total",
			"TSDB: Total number of failed dir syncs",
			nil, nil),
		uploads: prometheus.NewDesc(
			"cortex_ingester_shipper_uploads_total",
			"TSDB: Total object upload attempts",
			nil, nil),
		uploadFailures: prometheus.NewDesc(
			"cortex_ingester_shipper_upload_failures_total",
			"TSDB: Total number of failed object uploads",
			nil, nil),
	}

	if r != nil {
		r.MustRegister(m)
	}
	return m
}

func (sm *shipperMetrics) Describe(out chan<- *prometheus.Desc) {
	out <- sm.dirSyncs
	out <- sm.dirSyncFailures
	out <- sm.uploads
	out <- sm.uploadFailures
}

func (sm *shipperMetrics) Collect(out chan<- prometheus.Metric) {
	gathered := make(map[string][]*dto.MetricFamily)

	regs := sm.shipperRegistries()
	for userID, r := range regs {
		m, err := r.Gather()
		if err != nil {
			level.Warn(util.Logger).Log("msg", "failed to gather metrics from TSDB shipper", "user", userID, "err", err)
			continue
		}

		addToGatheredMap(gathered, m)
	}

	// OK, we have it all. Let's build results.
	out <- prometheus.MustNewConstMetric(sm.dirSyncs, prometheus.CounterValue, sumCounters(gathered["thanos_shipper_dir_syncs_total"]))
	out <- prometheus.MustNewConstMetric(sm.dirSyncFailures, prometheus.CounterValue, sumCounters(gathered["thanos_shipper_dir_sync_failures_total"]))
	out <- prometheus.MustNewConstMetric(sm.uploads, prometheus.CounterValue, sumCounters(gathered["thanos_shipper_uploads_total"]))
	out <- prometheus.MustNewConstMetric(sm.uploadFailures, prometheus.CounterValue, sumCounters(gathered["thanos_shipper_upload_failures_total"]))
}

func (sm *shipperMetrics) shipperRegistries() []*prometheus.Registry {
	sm.regsMu.RLock()
	defer sm.regsMu.RUnlock()

	regs := make([]*prometheus.Registry, 0, len(sm.regs))
	for _, r := range sm.regs {
		regs = append(regs, r)
	}
	return regs
}

func (sm *shipperMetrics) newRegistryForUser(userID string) prometheus.Registerer {
	reg := prometheus.NewRegistry()
	sm.regsMu.Lock()
	sm.regs[userID] = reg
	sm.regsMu.Unlock()
	return reg
}

func sumCounters(mfs []*dto.MetricFamily) float64 {
	result := float64(0)
	for _, mf := range mfs {
		if mf.Type == nil || *mf.Type != dto.MetricType_COUNTER {
			continue
		}

		for _, m := range mf.Metric {
			if m == nil || m.Counter == nil || m.Counter.Value == nil {
				continue
			}

			result += *m.Counter.Value
		}
	}
	return result
}

func addToGatheredMap(all map[string][]*dto.MetricFamily, mfs []*dto.MetricFamily) {
	for _, m := range mfs {
		if m.Name == nil {
			continue
		}
		all[*m.Name] = append(all[*m.Name], m)
	}
}
