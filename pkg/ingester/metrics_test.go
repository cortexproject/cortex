package ingester

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTSDBMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	tsdbMetrics := newTSDBMetrics(mainReg)

	tsdbMetrics.setRegistryForUser("user1", populateTSDBMetrics(12345))
	tsdbMetrics.setRegistryForUser("user2", populateTSDBMetrics(85787))
	tsdbMetrics.setRegistryForUser("user3", populateTSDBMetrics(999))

	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
			# HELP cortex_ingester_shipper_dir_syncs_total Total number of TSDB dir syncs
			# TYPE cortex_ingester_shipper_dir_syncs_total counter
			# 12345 + 85787 + 999
			cortex_ingester_shipper_dir_syncs_total 99131

			# HELP cortex_ingester_shipper_dir_sync_failures_total Total number of failed TSDB dir syncs
			# TYPE cortex_ingester_shipper_dir_sync_failures_total counter
			# 2*(12345 + 85787 + 999)
			cortex_ingester_shipper_dir_sync_failures_total 198262

			# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
			# TYPE cortex_ingester_shipper_uploads_total counter
			# 3*(12345 + 85787 + 999)
			cortex_ingester_shipper_uploads_total 297393

			# HELP cortex_ingester_shipper_upload_failures_total Total number of TSDB block upload failures
			# TYPE cortex_ingester_shipper_upload_failures_total counter
			# 4*(12345 + 85787 + 999)
			cortex_ingester_shipper_upload_failures_total 396524

			# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
			# TYPE cortex_ingester_tsdb_compactions_total counter
			cortex_ingester_tsdb_compactions_total 693917

			# HELP cortex_ingester_tsdb_compaction_duration_seconds Duration of TSDB compaction runs.
			# TYPE cortex_ingester_tsdb_compaction_duration_seconds histogram
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="1"} 0
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="2"} 0
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="4"} 0
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="8"} 0
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="16"} 3
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="32"} 3
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="64"} 3
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="128"} 3
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="256"} 3
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="512"} 3
			cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="+Inf"} 3
			cortex_ingester_tsdb_compaction_duration_seconds_sum 27
			cortex_ingester_tsdb_compaction_duration_seconds_count 3

			# HELP cortex_ingester_tsdb_wal_fsync_duration_seconds Duration of TSDB WAL fsync.
			# TYPE cortex_ingester_tsdb_wal_fsync_duration_seconds summary
			cortex_ingester_tsdb_wal_fsync_duration_seconds{quantile="0.5"} 30
			cortex_ingester_tsdb_wal_fsync_duration_seconds{quantile="0.9"} 30
			cortex_ingester_tsdb_wal_fsync_duration_seconds{quantile="0.99"} 30
			cortex_ingester_tsdb_wal_fsync_duration_seconds_sum 30
			cortex_ingester_tsdb_wal_fsync_duration_seconds_count 3

			# HELP cortex_ingester_tsdb_wal_page_flushes_total Total number of TSDB WAL page flushes.
			# TYPE cortex_ingester_tsdb_wal_page_flushes_total counter
			cortex_ingester_tsdb_wal_page_flushes_total 1090441

			# HELP cortex_ingester_tsdb_wal_completed_pages_total Total number of TSDB WAL completed pages.
			# TYPE cortex_ingester_tsdb_wal_completed_pages_total counter
			cortex_ingester_tsdb_wal_completed_pages_total 1189572

			# HELP cortex_ingester_tsdb_wal_truncations_failed_total Total number of TSDB WAL truncations that failed.
			# TYPE cortex_ingester_tsdb_wal_truncations_failed_total counter
			cortex_ingester_tsdb_wal_truncations_failed_total 1288703

			# HELP cortex_ingester_tsdb_wal_truncations_total Total number of TSDB  WAL truncations attempted.
			# TYPE cortex_ingester_tsdb_wal_truncations_total counter
			cortex_ingester_tsdb_wal_truncations_total 1387834

			# HELP cortex_ingester_tsdb_wal_writes_failed_total Total number of TSDB WAL writes that failed.
			# TYPE cortex_ingester_tsdb_wal_writes_failed_total counter
			cortex_ingester_tsdb_wal_writes_failed_total 1486965

			# HELP cortex_ingester_tsdb_checkpoint_deletions_failed_total Total number of TSDB checkpoint deletions that failed.
			# TYPE cortex_ingester_tsdb_checkpoint_deletions_failed_total counter
			cortex_ingester_tsdb_checkpoint_deletions_failed_total 1586096

			# HELP cortex_ingester_tsdb_checkpoint_deletions_total Total number of TSDB checkpoint deletions attempted.
			# TYPE cortex_ingester_tsdb_checkpoint_deletions_total counter
			cortex_ingester_tsdb_checkpoint_deletions_total 1685227

			# HELP cortex_ingester_tsdb_checkpoint_creations_failed_total Total number of TSDB checkpoint creations that failed.
			# TYPE cortex_ingester_tsdb_checkpoint_creations_failed_total counter
			cortex_ingester_tsdb_checkpoint_creations_failed_total 1784358

			# HELP cortex_ingester_tsdb_checkpoint_creations_total Total number of TSDB checkpoint creations attempted.
			# TYPE cortex_ingester_tsdb_checkpoint_creations_total counter
			cortex_ingester_tsdb_checkpoint_creations_total 1883489

			# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
			# TYPE cortex_ingester_memory_series_created_total counter
			# 5 * (12345, 85787 and 999 respectively)
			cortex_ingester_memory_series_created_total{user="user1"} 61725
			cortex_ingester_memory_series_created_total{user="user2"} 428935
			cortex_ingester_memory_series_created_total{user="user3"} 4995

			# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
			# TYPE cortex_ingester_memory_series_removed_total counter
			# 6 * (12345, 85787 and 999 respectively)
			cortex_ingester_memory_series_removed_total{user="user1"} 74070
			cortex_ingester_memory_series_removed_total{user="user2"} 514722
			cortex_ingester_memory_series_removed_total{user="user3"} 5994

			# HELP cortex_ingester_tsdb_head_active_appenders Number of currently active TSDB appender transactions.
			# TYPE cortex_ingester_tsdb_head_active_appenders gauge
			cortex_ingester_tsdb_head_active_appenders 1982620

			# HELP cortex_ingester_tsdb_head_series_not_found_total Total number of TSDB requests for series that were not found.
			# TYPE cortex_ingester_tsdb_head_series_not_found_total counter
			cortex_ingester_tsdb_head_series_not_found_total 2081751

			# HELP cortex_ingester_tsdb_head_chunks Total number of chunks in the TSDB head block.
			# TYPE cortex_ingester_tsdb_head_chunks gauge
			cortex_ingester_tsdb_head_chunks 2180882

			# HELP cortex_ingester_tsdb_head_chunks_created_total Total number of series created in the TSDB head.
			# TYPE cortex_ingester_tsdb_head_chunks_created_total counter
			cortex_ingester_tsdb_head_chunks_created_total{user="user1"} 283935
			cortex_ingester_tsdb_head_chunks_created_total{user="user2"} 1973101
			cortex_ingester_tsdb_head_chunks_created_total{user="user3"} 22977

			# HELP cortex_ingester_tsdb_head_chunks_removed_total Total number of series removed in the TSDB head.
			# TYPE cortex_ingester_tsdb_head_chunks_removed_total counter
			cortex_ingester_tsdb_head_chunks_removed_total{user="user1"} 296280
			cortex_ingester_tsdb_head_chunks_removed_total{user="user2"} 2058888
			cortex_ingester_tsdb_head_chunks_removed_total{user="user3"} 23976

			# HELP cortex_ingester_tsdb_wal_truncate_duration_seconds Duration of TSDB WAL truncation.
			# TYPE cortex_ingester_tsdb_wal_truncate_duration_seconds summary
			cortex_ingester_tsdb_wal_truncate_duration_seconds_sum 75
			cortex_ingester_tsdb_wal_truncate_duration_seconds_count 3

			# HELP cortex_ingester_tsdb_mmap_chunk_corruptions_total Total number of memory-mapped TSDB chunk corruptions.
			# TYPE cortex_ingester_tsdb_mmap_chunk_corruptions_total counter
			cortex_ingester_tsdb_mmap_chunk_corruptions_total 2577406
	`))
	require.NoError(t, err)
}

func populateTSDBMetrics(base float64) *prometheus.Registry {
	r := prometheus.NewRegistry()

	// Thanos shipper.
	dirSyncs := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_syncs_total",
		Help: "Total number of dir syncs",
	})
	dirSyncs.Add(1 * base)

	dirSyncFailures := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_sync_failures_total",
		Help: "Total number of failed dir syncs",
	})
	dirSyncFailures.Add(2 * base)

	uploads := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_uploads_total",
		Help: "Total number of uploaded blocks",
	})
	uploads.Add(3 * base)

	uploadFailures := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_upload_failures_total",
		Help: "Total number of block upload failures",
	})
	uploadFailures.Add(4 * base)

	// TSDB Head
	seriesCreated := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_created_total",
	})
	seriesCreated.Add(5 * base)

	seriesRemoved := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_removed_total",
	})
	seriesRemoved.Add(6 * base)

	ran := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	ran.Add(7 * base)

	duration := promauto.With(r).NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_duration_seconds",
		Help:    "Duration of compaction runs",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	})
	duration.Observe(9)

	fsyncDuration := promauto.With(r).NewSummary(prometheus.SummaryOpts{
		Name:       "prometheus_tsdb_wal_fsync_duration_seconds",
		Help:       "Duration of WAL fsync.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	fsyncDuration.Observe(10)

	pageFlushes := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_page_flushes_total",
		Help: "Total number of page flushes.",
	})
	pageFlushes.Add(11 * base)

	pageCompletions := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_completed_pages_total",
		Help: "Total number of completed pages.",
	})
	pageCompletions.Add(12 * base)

	truncateFail := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_truncations_failed_total",
		Help: "Total number of WAL truncations that failed.",
	})
	truncateFail.Add(13 * base)

	truncateTotal := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_truncations_total",
		Help: "Total number of WAL truncations attempted.",
	})
	truncateTotal.Add(14 * base)

	writesFailed := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_writes_failed_total",
		Help: "Total number of WAL writes that failed.",
	})
	writesFailed.Add(15 * base)

	checkpointDeleteFail := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_deletions_failed_total",
		Help: "Total number of checkpoint deletions that failed.",
	})
	checkpointDeleteFail.Add(16 * base)

	checkpointDeleteTotal := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_deletions_total",
		Help: "Total number of checkpoint deletions attempted.",
	})
	checkpointDeleteTotal.Add(17 * base)

	checkpointCreationFail := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_creations_failed_total",
		Help: "Total number of checkpoint creations that failed.",
	})
	checkpointCreationFail.Add(18 * base)

	checkpointCreationTotal := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_creations_total",
		Help: "Total number of checkpoint creations attempted.",
	})
	checkpointCreationTotal.Add(19 * base)

	activeAppenders := promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_active_appenders",
		Help: "Number of currently active appender transactions",
	})
	activeAppenders.Set(20 * base)

	seriesNotFound := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_not_found_total",
		Help: "Total number of requests for series that were not found.",
	})
	seriesNotFound.Add(21 * base)

	chunks := promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_chunks",
		Help: "Total number of chunks in the head block.",
	})
	chunks.Set(22 * base)

	chunksCreated := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_chunks_created_total",
		Help: "Total number of chunks created in the head",
	})
	chunksCreated.Add(23 * base)

	chunksRemoved := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_chunks_removed_total",
		Help: "Total number of chunks removed in the head",
	})
	chunksRemoved.Add(24 * base)

	walTruncateDuration := promauto.With(r).NewSummary(prometheus.SummaryOpts{
		Name: "prometheus_tsdb_wal_truncate_duration_seconds",
		Help: "Duration of WAL truncation.",
	})
	walTruncateDuration.Observe(25)

	mmapChunkCorruptionTotal := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_mmap_chunk_corruptions_total",
		Help: "Total number of memory-mapped chunk corruptions.",
	})
	mmapChunkCorruptionTotal.Add(26 * base)

	return r
}
