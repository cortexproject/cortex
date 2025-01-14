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
	cm := newCompactorMetricsWithLabels(reg, commonLabels, commonLabels)

	generateTestData(cm, 1234)
	generateTestData(cm, 7654)
	generateTestData(cm, 2222)

	err := testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP blocks_meta_cortex_compactor_meta_base_syncs_total Total blocks metadata synchronization attempts by base Fetcher.
			# TYPE blocks_meta_cortex_compactor_meta_base_syncs_total counter
			blocks_meta_cortex_compactor_meta_base_syncs_total 11110
			# HELP blocks_meta_cortex_compactor_meta_modified Number of blocks whose metadata changed
			# TYPE blocks_meta_cortex_compactor_meta_modified gauge
			blocks_meta_cortex_compactor_meta_modified{modified="replica-label-removed"} 0
			# HELP blocks_meta_cortex_compactor_meta_sync_duration_seconds Duration of the blocks metadata synchronization in seconds.
			# TYPE blocks_meta_cortex_compactor_meta_sync_duration_seconds histogram
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_bucket{le="0.01"} 0
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_bucket{le="1"} 2
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_bucket{le="10"} 3
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_bucket{le="100"} 3
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_bucket{le="300"} 3
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_bucket{le="600"} 3
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_bucket{le="1000"} 3
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_bucket{le="+Inf"} 3
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_sum 4.444
			blocks_meta_cortex_compactor_meta_sync_duration_seconds_count 3
			# HELP blocks_meta_cortex_compactor_meta_sync_failures_total Total blocks metadata synchronization failures.
			# TYPE blocks_meta_cortex_compactor_meta_sync_failures_total counter
			blocks_meta_cortex_compactor_meta_sync_failures_total 33330
			# HELP blocks_meta_cortex_compactor_meta_synced Number of block metadata synced
			# TYPE blocks_meta_cortex_compactor_meta_synced gauge
			blocks_meta_cortex_compactor_meta_synced{state="corrupted-meta-json"} 0
			blocks_meta_cortex_compactor_meta_synced{state="duplicate"} 0
			blocks_meta_cortex_compactor_meta_synced{state="failed"} 0
			blocks_meta_cortex_compactor_meta_synced{state="label-excluded"} 0
			blocks_meta_cortex_compactor_meta_synced{state="loaded"} 0
			blocks_meta_cortex_compactor_meta_synced{state="marked-for-deletion"} 0
			blocks_meta_cortex_compactor_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_cortex_compactor_meta_synced{state="no-meta-json"} 0
			blocks_meta_cortex_compactor_meta_synced{state="time-excluded"} 0
			blocks_meta_cortex_compactor_meta_synced{state="too-fresh"} 0
			# HELP blocks_meta_cortex_compactor_meta_syncs_total Total blocks metadata synchronization attempts.
			# TYPE blocks_meta_cortex_compactor_meta_syncs_total counter
			blocks_meta_cortex_compactor_meta_syncs_total 22220
			# HELP cortex_compact_group_compaction_planned_total Total number of compaction planned.
			# TYPE cortex_compact_group_compaction_planned_total counter
			cortex_compact_group_compaction_planned_total{user="aaa"} 211090
			cortex_compact_group_compaction_planned_total{user="bbb"} 222200
			cortex_compact_group_compaction_planned_total{user="ccc"} 233310
			# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
			# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
			cortex_compactor_blocks_marked_for_deletion_total{reason="compaction",user="aaa"} 144430
			cortex_compactor_blocks_marked_for_deletion_total{reason="compaction",user="bbb"} 155540
			cortex_compactor_blocks_marked_for_deletion_total{reason="compaction",user="ccc"} 166650
			# HELP cortex_compactor_garbage_collected_blocks_total Total number of blocks marked for deletion by compactor.
			# TYPE cortex_compactor_garbage_collected_blocks_total counter
			cortex_compactor_garbage_collected_blocks_total 99990
			# HELP cortex_compactor_garbage_collection_duration_seconds Time it took to perform garbage collection iteration.
			# TYPE cortex_compactor_garbage_collection_duration_seconds histogram
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.005"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.01"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.025"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.05"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.1"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.25"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="0.5"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="1"} 0
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="2.5"} 1
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="5"} 2
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="10"} 3
			cortex_compactor_garbage_collection_duration_seconds_bucket{le="+Inf"} 3
			cortex_compactor_garbage_collection_duration_seconds_sum 13.331999999999999
			cortex_compactor_garbage_collection_duration_seconds_count 3
			# HELP cortex_compactor_garbage_collection_failures_total Total number of failed garbage collection operations.
			# TYPE cortex_compactor_garbage_collection_failures_total counter
			cortex_compactor_garbage_collection_failures_total 122210
			# HELP cortex_compactor_garbage_collection_total Total number of garbage collection operations.
			# TYPE cortex_compactor_garbage_collection_total counter
			cortex_compactor_garbage_collection_total 111100
			# HELP cortex_compactor_group_compaction_runs_completed_total Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.
			# TYPE cortex_compactor_group_compaction_runs_completed_total counter
			cortex_compactor_group_compaction_runs_completed_total{user="aaa"} 277750
			cortex_compactor_group_compaction_runs_completed_total{user="bbb"} 288860
			cortex_compactor_group_compaction_runs_completed_total{user="ccc"} 299970
			# HELP cortex_compactor_group_compaction_runs_started_total Total number of group compaction attempts.
			# TYPE cortex_compactor_group_compaction_runs_started_total counter
			cortex_compactor_group_compaction_runs_started_total{user="aaa"} 244420
			cortex_compactor_group_compaction_runs_started_total{user="bbb"} 255530
			cortex_compactor_group_compaction_runs_started_total{user="ccc"} 266640
			# HELP cortex_compactor_group_compactions_failures_total Total number of failed group compactions.
			# TYPE cortex_compactor_group_compactions_failures_total counter
			cortex_compactor_group_compactions_failures_total{user="aaa"} 311080
			cortex_compactor_group_compactions_failures_total{user="bbb"} 322190
			cortex_compactor_group_compactions_failures_total{user="ccc"} 333300
			# HELP cortex_compactor_group_compactions_total Total number of group compaction attempts that resulted in a new block.
			# TYPE cortex_compactor_group_compactions_total counter
			cortex_compactor_group_compactions_total{user="aaa"} 177760
			cortex_compactor_group_compactions_total{user="bbb"} 188870
			cortex_compactor_group_compactions_total{user="ccc"} 199980
			# HELP cortex_compactor_group_vertical_compactions_total Total number of group compaction attempts that resulted in a new block based on overlapping blocks.
			# TYPE cortex_compactor_group_vertical_compactions_total counter
			cortex_compactor_group_vertical_compactions_total{user="aaa"} 344410
			cortex_compactor_group_vertical_compactions_total{user="bbb"} 355520
			cortex_compactor_group_vertical_compactions_total{user="ccc"} 366630
			# HELP cortex_compactor_remaining_planned_compactions Total number of plans that remain to be compacted. Only available with shuffle-sharding strategy
			# TYPE cortex_compactor_remaining_planned_compactions gauge
			cortex_compactor_remaining_planned_compactions{user="aaa"} 377740
			cortex_compactor_remaining_planned_compactions{user="bbb"} 388850
			cortex_compactor_remaining_planned_compactions{user="ccc"} 399960
			# HELP cortex_compactor_compaction_error_total Total number of errors from compactions.
			# TYPE cortex_compactor_compaction_error_total counter
			cortex_compactor_compaction_error_total{type="halt",user="aaa"} 444400
			cortex_compactor_compaction_error_total{type="halt",user="bbb"} 455510
			cortex_compactor_compaction_error_total{type="halt",user="ccc"} 466620
			cortex_compactor_compaction_error_total{type="retriable",user="aaa"} 411070
			cortex_compactor_compaction_error_total{type="retriable",user="bbb"} 422180
			cortex_compactor_compaction_error_total{type="retriable",user="ccc"} 433290
			cortex_compactor_compaction_error_total{type="unauthorized",user="aaa"} 477730
			cortex_compactor_compaction_error_total{type="unauthorized",user="bbb"} 488840
			cortex_compactor_compaction_error_total{type="unauthorized",user="ccc"} 499950
			# HELP cortex_compactor_group_partition_count Number of partitions for each compaction group.
			# TYPE cortex_compactor_group_partition_count gauge
			cortex_compactor_group_partition_count{user="aaa"} 511060
			cortex_compactor_group_partition_count{user="bbb"} 522170
			cortex_compactor_group_partition_count{user="ccc"} 533280
			# HELP cortex_compactor_group_compactions_not_planned_total Total number of group compaction not planned due to error.
			# TYPE cortex_compactor_group_compactions_not_planned_total counter
			cortex_compactor_group_compactions_not_planned_total{user="aaa"} 544390
			cortex_compactor_group_compactions_not_planned_total{user="bbb"} 555500
			cortex_compactor_group_compactions_not_planned_total{user="ccc"} 566610
	`))
	require.NoError(t, err)

}

func generateTestData(cm *compactorMetrics, base float64) {
	cm.baseFetcherSyncs.WithLabelValues().Add(1 * base)
	cm.metaFetcherSyncs.WithLabelValues().Add(2 * base)
	cm.metaFetcherSyncFailures.WithLabelValues().Add(3 * base)
	cm.metaFetcherSyncDuration.WithLabelValues().Observe(4 * base / 10000)
	cm.metaFetcherSynced.WithLabelValues("loaded").Add(5 * base)
	cm.metaFetcherSynced.WithLabelValues("no-meta-json").Add(6 * base)
	cm.metaFetcherSynced.WithLabelValues("failed").Add(7 * base)
	cm.metaFetcherModified.WithLabelValues("replica-label-removed").Add(8 * base)

	cm.syncerGarbageCollectedBlocks.WithLabelValues().Add(9 * base)
	cm.syncerGarbageCollections.WithLabelValues().Add(10 * base)
	cm.syncerGarbageCollectionFailures.WithLabelValues().Add(11 * base)
	cm.syncerGarbageCollectionDuration.WithLabelValues().Observe(12 * base / 10000)
	cm.syncerBlocksMarkedForDeletion.WithLabelValues("aaa", "compaction").Add(13 * base)
	cm.syncerBlocksMarkedForDeletion.WithLabelValues("bbb", "compaction").Add(14 * base)
	cm.syncerBlocksMarkedForDeletion.WithLabelValues("ccc", "compaction").Add(15 * base)

	cm.compactions.WithLabelValues("aaa").Add(16 * base)
	cm.compactions.WithLabelValues("bbb").Add(17 * base)
	cm.compactions.WithLabelValues("ccc").Add(18 * base)
	cm.compactionPlanned.WithLabelValues("aaa").Add(19 * base)
	cm.compactionPlanned.WithLabelValues("bbb").Add(20 * base)
	cm.compactionPlanned.WithLabelValues("ccc").Add(21 * base)
	cm.compactionRunsStarted.WithLabelValues("aaa").Add(22 * base)
	cm.compactionRunsStarted.WithLabelValues("bbb").Add(23 * base)
	cm.compactionRunsStarted.WithLabelValues("ccc").Add(24 * base)
	cm.compactionRunsCompleted.WithLabelValues("aaa").Add(25 * base)
	cm.compactionRunsCompleted.WithLabelValues("bbb").Add(26 * base)
	cm.compactionRunsCompleted.WithLabelValues("ccc").Add(27 * base)
	cm.compactionFailures.WithLabelValues("aaa").Add(28 * base)
	cm.compactionFailures.WithLabelValues("bbb").Add(29 * base)
	cm.compactionFailures.WithLabelValues("ccc").Add(30 * base)
	cm.verticalCompactions.WithLabelValues("aaa").Add(31 * base)
	cm.verticalCompactions.WithLabelValues("bbb").Add(32 * base)
	cm.verticalCompactions.WithLabelValues("ccc").Add(33 * base)
	cm.remainingPlannedCompactions.WithLabelValues("aaa").Add(34 * base)
	cm.remainingPlannedCompactions.WithLabelValues("bbb").Add(35 * base)
	cm.remainingPlannedCompactions.WithLabelValues("ccc").Add(36 * base)
	cm.compactionErrorsCount.WithLabelValues("aaa", retriableError).Add(37 * base)
	cm.compactionErrorsCount.WithLabelValues("bbb", retriableError).Add(38 * base)
	cm.compactionErrorsCount.WithLabelValues("ccc", retriableError).Add(39 * base)
	cm.compactionErrorsCount.WithLabelValues("aaa", haltError).Add(40 * base)
	cm.compactionErrorsCount.WithLabelValues("bbb", haltError).Add(41 * base)
	cm.compactionErrorsCount.WithLabelValues("ccc", haltError).Add(42 * base)
	cm.compactionErrorsCount.WithLabelValues("aaa", unauthorizedError).Add(43 * base)
	cm.compactionErrorsCount.WithLabelValues("bbb", unauthorizedError).Add(44 * base)
	cm.compactionErrorsCount.WithLabelValues("ccc", unauthorizedError).Add(45 * base)
	cm.partitionCount.WithLabelValues("aaa").Add(46 * base)
	cm.partitionCount.WithLabelValues("bbb").Add(47 * base)
	cm.partitionCount.WithLabelValues("ccc").Add(48 * base)
	cm.compactionsNotPlanned.WithLabelValues("aaa").Add(49 * base)
	cm.compactionsNotPlanned.WithLabelValues("bbb").Add(50 * base)
	cm.compactionsNotPlanned.WithLabelValues("ccc").Add(51 * base)
}
