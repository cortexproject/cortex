package compactor

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSyncerMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	sm := newSyncerMetrics(reg)
	sm.gatherThanosSyncerMetrics(generateTestData(12345))
	sm.gatherThanosSyncerMetrics(generateTestData(76543))
	sm.gatherThanosSyncerMetrics(generateTestData(22222))
	// total base = 111110

	err := testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP cortex_compactor_meta_sync_consistency_delay_seconds TSDB Syncer: Configured consistency delay in seconds.
			# TYPE cortex_compactor_meta_sync_consistency_delay_seconds gauge
			cortex_compactor_meta_sync_consistency_delay_seconds 300

			# HELP cortex_compactor_meta_syncs_total TSDB Syncer: Total blocks metadata synchronization attempts.
			# TYPE cortex_compactor_meta_syncs_total counter
			cortex_compactor_meta_syncs_total 111110

			# HELP cortex_compactor_meta_sync_failures_total TSDB Syncer: Total blocks metadata synchronization failures.
			# TYPE cortex_compactor_meta_sync_failures_total counter
			cortex_compactor_meta_sync_failures_total 222220

			# HELP cortex_compactor_meta_sync_duration_seconds TSDB Syncer: Duration of the blocks metadata synchronization in seconds.
			# TYPE cortex_compactor_meta_sync_duration_seconds histogram
			# Observed values: 3.7035, 22.9629, 6.6666 (seconds)
			cortex_compactor_meta_sync_duration_seconds_bucket{le="0.01"} 0
			cortex_compactor_meta_sync_duration_seconds_bucket{le="0.1"} 0
			cortex_compactor_meta_sync_duration_seconds_bucket{le="0.3"} 0
			cortex_compactor_meta_sync_duration_seconds_bucket{le="0.6"} 0
			cortex_compactor_meta_sync_duration_seconds_bucket{le="1"} 0
			cortex_compactor_meta_sync_duration_seconds_bucket{le="3"} 0
			cortex_compactor_meta_sync_duration_seconds_bucket{le="6"} 1
			cortex_compactor_meta_sync_duration_seconds_bucket{le="9"} 2
			cortex_compactor_meta_sync_duration_seconds_bucket{le="20"} 2
			cortex_compactor_meta_sync_duration_seconds_bucket{le="30"} 3
			cortex_compactor_meta_sync_duration_seconds_bucket{le="60"} 3
			cortex_compactor_meta_sync_duration_seconds_bucket{le="90"} 3
			cortex_compactor_meta_sync_duration_seconds_bucket{le="120"} 3
			cortex_compactor_meta_sync_duration_seconds_bucket{le="240"} 3
			cortex_compactor_meta_sync_duration_seconds_bucket{le="360"} 3
			cortex_compactor_meta_sync_duration_seconds_bucket{le="720"} 3
			cortex_compactor_meta_sync_duration_seconds_bucket{le="+Inf"} 3
			# rounding error
			cortex_compactor_meta_sync_duration_seconds_sum 33.333000000000006
			cortex_compactor_meta_sync_duration_seconds_count 3

			# HELP cortex_compactor_garbage_collected_blocks_total TSDB Syncer: Total number of deleted blocks by compactor.
			# TYPE cortex_compactor_garbage_collected_blocks_total counter
			cortex_compactor_garbage_collected_blocks_total 444440

			# HELP cortex_compactor_garbage_collection_total TSDB Syncer: Total number of garbage collection operations.
			# TYPE cortex_compactor_garbage_collection_total counter
			cortex_compactor_garbage_collection_total 555550

			# HELP cortex_compactor_garbage_collection_failures_total TSDB Syncer: Total number of failed garbage collection operations.
			# TYPE cortex_compactor_garbage_collection_failures_total counter
			cortex_compactor_garbage_collection_failures_total 666660

			# HELP cortex_compactor_garbage_collection_duration_seconds TSDB Syncer: Time it took to perform garbage collection iteration.
			# TYPE cortex_compactor_garbage_collection_duration_seconds histogram
			# Observed values: 8.6415, 53.5801, 15.5554
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.01"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.1"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.3"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.6"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="1"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="3"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="6"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="9"} 1
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="20"} 2
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="30"} 2
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="60"} 3
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="90"} 3
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="120"} 3
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="240"} 3
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="360"} 3
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="720"} 3
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="+Inf"} 3
			cortex_compactor_garbage_collection_duration_seconds_sum 77.777
			cortex_compactor_garbage_collection_duration_seconds_count 3

			# HELP cortex_compactor_group_compactions_total TSDB Syncer: Total number of group compaction attempts that resulted in a new block.
			# TYPE cortex_compactor_group_compactions_total counter
			# Sum across all groups
			cortex_compactor_group_compactions_total 2999970

			# HELP cortex_compactor_group_compaction_runs_started_total TSDB Syncer: Total number of group compaction attempts.
			# TYPE cortex_compactor_group_compaction_runs_started_total counter
			# Sum across all groups
			cortex_compactor_group_compaction_runs_started_total 3999960

			# HELP cortex_compactor_group_compaction_runs_completed_total TSDB Syncer: Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.
			# TYPE cortex_compactor_group_compaction_runs_completed_total counter
			# Sum across all groups
			cortex_compactor_group_compaction_runs_completed_total 4999950

			# HELP cortex_compactor_group_compactions_failures_total TSDB Syncer: Total number of failed group compactions.
			# TYPE cortex_compactor_group_compactions_failures_total counter
			cortex_compactor_group_compactions_failures_total 5999940

			# HELP cortex_compactor_group_vertical_compactions_total TSDB Syncer: Total number of group compaction attempts that resulted in a new block based on overlapping blocks.
			# TYPE cortex_compactor_group_vertical_compactions_total counter
			cortex_compactor_group_vertical_compactions_total 6999930
	`))
	require.NoError(t, err)
}

func generateTestData(base float64) *prometheus.Registry {
	r := prometheus.NewRegistry()
	m := newTestSyncerMetrics(r)
	m.metaSync.Add(1 * base)
	m.metaSyncFailures.Add(2 * base)
	m.metaSyncDuration.Observe(3 * base / 10000)
	m.metaSyncConsistencyDelay.Set(300)
	m.garbageCollectedBlocks.Add(4 * base)
	m.garbageCollections.Add(5 * base)
	m.garbageCollectionFailures.Add(6 * base)
	m.garbageCollectionDuration.Observe(7 * base / 10000)
	m.compactions.WithLabelValues("aaa").Add(8 * base)
	m.compactions.WithLabelValues("bbb").Add(9 * base)
	m.compactions.WithLabelValues("ccc").Add(10 * base)
	m.compactionRunsStarted.WithLabelValues("aaa").Add(11 * base)
	m.compactionRunsStarted.WithLabelValues("bbb").Add(12 * base)
	m.compactionRunsStarted.WithLabelValues("ccc").Add(13 * base)
	m.compactionRunsCompleted.WithLabelValues("aaa").Add(14 * base)
	m.compactionRunsCompleted.WithLabelValues("bbb").Add(15 * base)
	m.compactionRunsCompleted.WithLabelValues("ccc").Add(16 * base)
	m.compactionFailures.WithLabelValues("aaa").Add(17 * base)
	m.compactionFailures.WithLabelValues("bbb").Add(18 * base)
	m.compactionFailures.WithLabelValues("ccc").Add(19 * base)
	m.verticalCompactions.WithLabelValues("aaa").Add(20 * base)
	m.verticalCompactions.WithLabelValues("bbb").Add(21 * base)
	m.verticalCompactions.WithLabelValues("ccc").Add(22 * base)
	return r
}

// directly copied from Thanos (and renamed syncerMetrics to testSyncerMetrics to avoid conflict)
type testSyncerMetrics struct {
	metaSync                  prometheus.Counter
	metaSyncFailures          prometheus.Counter
	metaSyncDuration          prometheus.Histogram
	metaSyncConsistencyDelay  prometheus.Gauge
	garbageCollectedBlocks    prometheus.Counter
	garbageCollections        prometheus.Counter
	garbageCollectionFailures prometheus.Counter
	garbageCollectionDuration prometheus.Histogram
	compactions               *prometheus.CounterVec
	compactionRunsStarted     *prometheus.CounterVec
	compactionRunsCompleted   *prometheus.CounterVec
	compactionFailures        *prometheus.CounterVec
	verticalCompactions       *prometheus.CounterVec
}

func newTestSyncerMetrics(reg prometheus.Registerer) *testSyncerMetrics {
	var m testSyncerMetrics

	m.metaSync = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blocks_meta_syncs_total",
		Help: "Total blocks metadata synchronization attempts.",
	})
	m.metaSyncFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blocks_meta_sync_failures_total",
		Help: "Total blocks metadata synchronization failures.",
	})
	m.metaSyncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "blocks_meta_sync_duration_seconds",
		Help:    "Duration of the blocks metadata synchronization in seconds.",
		Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720},
	})
	m.metaSyncConsistencyDelay = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "consistency_delay_seconds",
		Help: "Configured consistency delay in seconds.",
	})

	m.garbageCollectedBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collected_blocks_total",
		Help: "Total number of deleted blocks by compactor.",
	})
	m.garbageCollections = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_total",
		Help: "Total number of garbage collection operations.",
	})
	m.garbageCollectionFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_failures_total",
		Help: "Total number of failed garbage collection operations.",
	})
	m.garbageCollectionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_compact_garbage_collection_duration_seconds",
		Help:    "Time it took to perform garbage collection iteration.",
		Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720},
	})

	m.compactions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compactions_total",
		Help: "Total number of group compaction attempts that resulted in a new block.",
	}, []string{"group"})
	m.compactionRunsStarted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compaction_runs_started_total",
		Help: "Total number of group compaction attempts.",
	}, []string{"group"})
	m.compactionRunsCompleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compaction_runs_completed_total",
		Help: "Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.",
	}, []string{"group"})
	m.compactionFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compactions_failures_total",
		Help: "Total number of failed group compactions.",
	}, []string{"group"})
	m.verticalCompactions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_vertical_compactions_total",
		Help: "Total number of group compaction attempts that resulted in a new block based on overlapping blocks.",
	}, []string{"group"})

	if reg != nil {
		reg.MustRegister(
			m.metaSync,
			m.metaSyncFailures,
			m.metaSyncDuration,
			m.metaSyncConsistencyDelay,
			m.garbageCollectedBlocks,
			m.garbageCollections,
			m.garbageCollectionFailures,
			m.garbageCollectionDuration,
			m.compactions,
			m.compactionRunsStarted,
			m.compactionRunsCompleted,
			m.compactionFailures,
			m.verticalCompactions,
		)
	}
	return &m
}
