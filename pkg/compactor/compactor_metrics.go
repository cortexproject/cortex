package compactor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/extprom"
)

type compactorMetrics struct {
	reg              prometheus.Registerer
	commonLabels     []string
	compactionLabels []string

	// block.BaseFetcherMetrics
	baseFetcherSyncs *prometheus.CounterVec

	// block.FetcherMetrics
	metaFetcherSyncs        *prometheus.CounterVec
	metaFetcherSyncFailures *prometheus.CounterVec
	metaFetcherSyncDuration *prometheus.HistogramVec
	metaFetcherSynced       *extprom.TxGaugeVec
	metaFetcherModified     *extprom.TxGaugeVec

	// compact.SyncerMetrics
	syncerGarbageCollectedBlocks    *prometheus.CounterVec
	syncerGarbageCollections        *prometheus.CounterVec
	syncerGarbageCollectionFailures *prometheus.CounterVec
	syncerGarbageCollectionDuration *prometheus.HistogramVec
	syncerBlocksMarkedForDeletion   *prometheus.CounterVec

	compactions                 *prometheus.CounterVec
	compactionPlanned           *prometheus.CounterVec
	compactionRunsStarted       *prometheus.CounterVec
	compactionRunsCompleted     *prometheus.CounterVec
	compactionFailures          *prometheus.CounterVec
	verticalCompactions         *prometheus.CounterVec
	remainingPlannedCompactions *prometheus.GaugeVec
	compactionErrorsCount       *prometheus.CounterVec
	partitionCount              *prometheus.GaugeVec
	compactionsNotPlanned       *prometheus.CounterVec
	compactionDuration          *prometheus.GaugeVec
}

const (
	userLabelName                 = "user"
	timeRangeLabelName            = "time_range_milliseconds"
	reasonLabelName               = "reason"
	compactionErrorTypesLabelName = "type"

	retriableError    = "retriable"
	haltError         = "halt"
	unauthorizedError = "unauthorized"
)

var (
	commonLabels     = []string{userLabelName}
	compactionLabels = []string{timeRangeLabelName}
)

func newDefaultCompactorMetrics(reg prometheus.Registerer) *compactorMetrics {
	return newCompactorMetricsWithLabels(reg, commonLabels, []string{"resolution"})
}

func newCompactorMetrics(reg prometheus.Registerer) *compactorMetrics {
	return newCompactorMetricsWithLabels(reg, commonLabels, append(commonLabels, compactionLabels...))
}

func newCompactorMetricsWithLabels(reg prometheus.Registerer, commonLabels []string, compactionLabels []string) *compactorMetrics {
	var m compactorMetrics
	m.reg = reg
	m.commonLabels = commonLabels
	m.compactionLabels = compactionLabels

	// Copied from Thanos, pkg/block/fetcher.go
	m.baseFetcherSyncs = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: block.FetcherSubSys,
		Name:      "cortex_compactor_meta_base_syncs_total",
		Help:      "Total blocks metadata synchronization attempts by base Fetcher.",
	}, nil)

	// Copied from Thanos, pkg/block/fetcher.go
	m.metaFetcherSyncs = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: block.FetcherSubSys,
		Name:      "cortex_compactor_meta_syncs_total",
		Help:      "Total blocks metadata synchronization attempts.",
	}, nil)
	m.metaFetcherSyncFailures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Subsystem: block.FetcherSubSys,
		Name:      "cortex_compactor_meta_sync_failures_total",
		Help:      "Total blocks metadata synchronization failures.",
	}, nil)
	m.metaFetcherSyncDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: block.FetcherSubSys,
		Name:      "cortex_compactor_meta_sync_duration_seconds",
		Help:      "Duration of the blocks metadata synchronization in seconds.",
		Buckets:   []float64{0.01, 1, 10, 100, 300, 600, 1000},
	}, nil)
	m.metaFetcherSynced = extprom.NewTxGaugeVec(
		reg,
		prometheus.GaugeOpts{
			Subsystem: block.FetcherSubSys,
			Name:      "cortex_compactor_meta_synced",
			Help:      "Number of block metadata synced",
		},
		[]string{"state"},
		block.DefaultSyncedStateLabelValues()...,
	)
	m.metaFetcherModified = extprom.NewTxGaugeVec(
		reg,
		prometheus.GaugeOpts{
			Subsystem: block.FetcherSubSys,
			Name:      "cortex_compactor_meta_modified",
			Help:      "Number of blocks whose metadata changed",
		},
		[]string{"modified"},
		block.DefaultModifiedLabelValues()...,
	)

	// Copied from Thanos, pkg/compact/compact.go.
	m.syncerGarbageCollectedBlocks = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_garbage_collected_blocks_total",
		Help: "Total number of blocks marked for deletion by compactor.",
	}, nil)
	m.syncerGarbageCollections = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_garbage_collection_total",
		Help: "Total number of garbage collection operations.",
	}, nil)
	m.syncerGarbageCollectionFailures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_garbage_collection_failures_total",
		Help: "Total number of failed garbage collection operations.",
	}, nil)
	m.syncerGarbageCollectionDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "cortex_compactor_garbage_collection_duration_seconds",
		Help: "Time it took to perform garbage collection iteration.",
	}, nil)
	m.syncerBlocksMarkedForDeletion = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: blocksMarkedForDeletionName,
		Help: blocksMarkedForDeletionHelp,
	}, append(commonLabels, reasonLabelName))

	m.compactions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_group_compactions_total",
		Help: "Total number of group compaction attempts that resulted in a new block.",
	}, compactionLabels)
	m.compactionPlanned = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compact_group_compaction_planned_total",
		Help: "Total number of compaction planned.",
	}, compactionLabels)
	m.compactionRunsStarted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_group_compaction_runs_started_total",
		Help: "Total number of group compaction attempts.",
	}, compactionLabels)
	m.compactionRunsCompleted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_group_compaction_runs_completed_total",
		Help: "Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.",
	}, compactionLabels)
	m.compactionFailures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_group_compactions_failures_total",
		Help: "Total number of failed group compactions.",
	}, compactionLabels)
	m.verticalCompactions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_group_vertical_compactions_total",
		Help: "Total number of group compaction attempts that resulted in a new block based on overlapping blocks.",
	}, compactionLabels)
	m.remainingPlannedCompactions = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_compactor_remaining_planned_compactions",
		Help: "Total number of plans that remain to be compacted. Only available with shuffle-sharding strategy",
	}, commonLabels)
	m.compactionErrorsCount = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_compaction_error_total",
		Help: "Total number of errors from compactions.",
	}, append(commonLabels, compactionErrorTypesLabelName))
	m.partitionCount = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_compactor_group_partition_count",
		Help: "Number of partitions for each compaction group.",
	}, compactionLabels)
	m.compactionsNotPlanned = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_compactor_group_compactions_not_planned_total",
		Help: "Total number of group compaction not planned due to error.",
	}, compactionLabels)
	m.compactionDuration = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_compact_group_compaction_duration_seconds",
		Help: "Duration of completed compactions in seconds",
	}, compactionLabels)

	return &m
}

func (m *compactorMetrics) getBaseFetcherMetrics() *block.BaseFetcherMetrics {
	var baseFetcherMetrics block.BaseFetcherMetrics
	baseFetcherMetrics.Syncs = m.baseFetcherSyncs.WithLabelValues()
	return &baseFetcherMetrics
}

func (m *compactorMetrics) getMetaFetcherMetrics() *block.FetcherMetrics {
	var fetcherMetrics block.FetcherMetrics
	fetcherMetrics.Syncs = m.metaFetcherSyncs.WithLabelValues()
	fetcherMetrics.SyncFailures = m.metaFetcherSyncFailures.WithLabelValues()
	fetcherMetrics.SyncDuration = m.metaFetcherSyncDuration.WithLabelValues()
	fetcherMetrics.Synced = m.metaFetcherSynced
	fetcherMetrics.Modified = m.metaFetcherModified
	return &fetcherMetrics
}

func (m *compactorMetrics) getSyncerMetrics(userID string) *compact.SyncerMetrics {
	var syncerMetrics compact.SyncerMetrics
	labelValues := m.getCommonLabelValues(userID)
	syncerMetrics.GarbageCollectedBlocks = m.syncerGarbageCollectedBlocks.WithLabelValues()
	syncerMetrics.GarbageCollections = m.syncerGarbageCollections.WithLabelValues()
	syncerMetrics.GarbageCollectionFailures = m.syncerGarbageCollectionFailures.WithLabelValues()
	syncerMetrics.GarbageCollectionDuration = m.syncerGarbageCollectionDuration.WithLabelValues()
	syncerMetrics.BlocksMarkedForDeletion = m.syncerBlocksMarkedForDeletion.WithLabelValues(append(labelValues, "compaction")...)
	return &syncerMetrics
}

func (m *compactorMetrics) getCommonLabelValues(userID string) []string {
	var labelValues []string
	if len(m.commonLabels) > 0 {
		labelValues = append(labelValues, userID)
	}
	return labelValues
}

func (m *compactorMetrics) initMetricWithCompactionLabelValues(labelValue ...string) {
	if len(m.compactionLabels) != len(commonLabels)+len(compactionLabels) {
		return
	}

	m.compactions.WithLabelValues(labelValue...)
	m.compactionPlanned.WithLabelValues(labelValue...)
	m.compactionRunsStarted.WithLabelValues(labelValue...)
	m.compactionRunsCompleted.WithLabelValues(labelValue...)
	m.compactionFailures.WithLabelValues(labelValue...)
	m.verticalCompactions.WithLabelValues(labelValue...)
	m.partitionCount.WithLabelValues(labelValue...)
	m.compactionsNotPlanned.WithLabelValues(labelValue...)
	m.compactionDuration.WithLabelValues(labelValue...)
}

func (m *compactorMetrics) deleteMetricsForDeletedTenant(userID string) {
	m.syncerBlocksMarkedForDeletion.DeleteLabelValues(userID)
	m.compactions.DeleteLabelValues(userID)
	m.compactionPlanned.DeleteLabelValues(userID)
	m.compactionRunsStarted.DeleteLabelValues(userID)
	m.compactionRunsCompleted.DeleteLabelValues(userID)
	m.compactionFailures.DeleteLabelValues(userID)
	m.verticalCompactions.DeleteLabelValues(userID)
	m.partitionCount.DeleteLabelValues(userID)
	m.compactionsNotPlanned.DeleteLabelValues(userID)
	m.compactionDuration.DeleteLabelValues(userID)
}
