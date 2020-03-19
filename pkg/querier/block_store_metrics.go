package querier

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
)

// This struct aggregates metrics exported by Thanos Bucket Store
// and re-exports those aggregates as Cortex metrics.
type tsdbBucketStoreMetrics struct {
	// Maps userID -> registry
	regsMu sync.Mutex
	regs   map[string]*prometheus.Registry

	// exported metrics, gathered from Thanos BucketStore
	blockLoads               *prometheus.Desc
	blockLoadFailures        *prometheus.Desc
	blockDrops               *prometheus.Desc
	blockDropFailures        *prometheus.Desc
	blocksLoaded             *prometheus.Desc
	seriesDataTouched        *prometheus.Desc
	seriesDataFetched        *prometheus.Desc
	seriesDataSizeTouched    *prometheus.Desc
	seriesDataSizeFetched    *prometheus.Desc
	seriesBlocksQueried      *prometheus.Desc
	seriesGetAllDuration     *prometheus.Desc
	seriesMergeDuration      *prometheus.Desc
	seriesRefetches          *prometheus.Desc
	resultSeriesCount        *prometheus.Desc
	metaSyncs                *prometheus.Desc
	metaSyncFailures         *prometheus.Desc
	metaSyncDuration         *prometheus.Desc
	metaSyncConsistencyDelay *prometheus.Desc

	// Ignored:
	// blocks_meta_synced
}

func newTSDBBucketStoreMetrics() *tsdbBucketStoreMetrics {
	return &tsdbBucketStoreMetrics{
		regs: map[string]*prometheus.Registry{},

		blockLoads: prometheus.NewDesc(
			"cortex_querier_bucket_store_block_loads_total",
			"TSDB: Total number of remote block loading attempts.",
			nil, nil),
		blockLoadFailures: prometheus.NewDesc(
			"cortex_querier_bucket_store_block_load_failures_total",
			"TSDB: Total number of failed remote block loading attempts.",
			nil, nil),
		blockDrops: prometheus.NewDesc(
			"cortex_querier_bucket_store_block_drops_total",
			"TSDB: Total number of local blocks that were dropped.",
			nil, nil),
		blockDropFailures: prometheus.NewDesc(
			"cortex_querier_bucket_store_block_drop_failures_total",
			"TSDB: Total number of local blocks that failed to be dropped.",
			nil, nil),
		blocksLoaded: prometheus.NewDesc(
			"cortex_querier_bucket_store_blocks_loaded",
			"TSDB: Number of currently loaded blocks.",
			nil, nil),
		seriesDataTouched: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_data_touched",
			"TSDB: How many items of a data type in a block were touched for a single series request.",
			[]string{"data_type"}, nil),
		seriesDataFetched: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_data_fetched",
			"TSDB: How many items of a data type in a block were fetched for a single series request.",
			[]string{"data_type"}, nil),
		seriesDataSizeTouched: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_data_size_touched_bytes",
			"TSDB: Size of all items of a data type in a block were touched for a single series request.",
			[]string{"data_type"}, nil),
		seriesDataSizeFetched: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_data_size_fetched_bytes",
			"TSDB: Size of all items of a data type in a block were fetched for a single series request.",
			[]string{"data_type"}, nil),
		seriesBlocksQueried: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_blocks_queried",
			"TSDB: Number of blocks in a bucket store that were touched to satisfy a query.",
			nil, nil),

		seriesGetAllDuration: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_get_all_duration_seconds",
			"TSDB: Time it takes until all per-block prepares and preloads for a query are finished.",
			nil, nil),
		seriesMergeDuration: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_merge_duration_seconds",
			"TSDB: Time it takes to merge sub-results from all queried blocks into a single result.",
			nil, nil),
		seriesRefetches: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_refetches_total",
			"TSDB: Total number of cases where the built-in max series size was not enough to fetch series from index, resulting in refetch.",
			nil, nil),
		resultSeriesCount: prometheus.NewDesc(
			"cortex_querier_bucket_store_series_result_series",
			"TSDB: Number of series observed in the final result of a query.",
			nil, nil),
		metaSyncs: prometheus.NewDesc(
			"cortex_querier_bucket_store_blocks_meta_syncs_total",
			"TSDB: Total blocks metadata synchronization attempts",
			nil, nil),
		metaSyncFailures: prometheus.NewDesc(
			"cortex_querier_bucket_store_blocks_meta_sync_failures_total",
			"TSDB: Total blocks metadata synchronization failures",
			nil, nil),
		metaSyncDuration: prometheus.NewDesc(
			"cortex_querier_bucket_store_blocks_meta_sync_duration_seconds",
			"TSDB: Duration of the blocks metadata synchronization in seconds",
			nil, nil),
		metaSyncConsistencyDelay: prometheus.NewDesc(
			"cortex_querier_bucket_store_blocks_meta_sync_consistency_delay_seconds",
			"TSDB: Configured consistency delay in seconds.",
			nil, nil),
	}
}

func (m *tsdbBucketStoreMetrics) addUserRegistry(user string, reg *prometheus.Registry) {
	m.regsMu.Lock()
	m.regs[user] = reg
	m.regsMu.Unlock()
}

func (m *tsdbBucketStoreMetrics) registries() map[string]*prometheus.Registry {
	regs := map[string]*prometheus.Registry{}

	m.regsMu.Lock()
	defer m.regsMu.Unlock()
	for uid, r := range m.regs {
		regs[uid] = r
	}

	return regs
}

func (m *tsdbBucketStoreMetrics) Describe(out chan<- *prometheus.Desc) {
	out <- m.blockLoads
	out <- m.blockLoadFailures
	out <- m.blockDrops
	out <- m.blockDropFailures
	out <- m.blocksLoaded
	out <- m.seriesDataTouched
	out <- m.seriesDataFetched
	out <- m.seriesDataSizeTouched
	out <- m.seriesDataSizeFetched
	out <- m.seriesBlocksQueried
	out <- m.seriesGetAllDuration
	out <- m.seriesMergeDuration
	out <- m.seriesRefetches
	out <- m.resultSeriesCount

	out <- m.metaSyncs
	out <- m.metaSyncFailures
	out <- m.metaSyncDuration
	out <- m.metaSyncConsistencyDelay
}

func (m *tsdbBucketStoreMetrics) Collect(out chan<- prometheus.Metric) {
	data := util.BuildMetricFamiliesPerUserFromUserRegistries(m.registries())

	data.SendSumOfCounters(out, m.blockLoads, "thanos_bucket_store_block_loads_total")
	data.SendSumOfCounters(out, m.blockLoadFailures, "thanos_bucket_store_block_load_failures_total")
	data.SendSumOfCounters(out, m.blockDrops, "thanos_bucket_store_block_drops_total")
	data.SendSumOfCounters(out, m.blockDropFailures, "thanos_bucket_store_block_drop_failures_total")

	data.SendSumOfGauges(out, m.blocksLoaded, "thanos_bucket_store_blocks_loaded")

	data.SendSumOfSummariesWithLabels(out, m.seriesDataTouched, "thanos_bucket_store_series_data_touched", "data_type")
	data.SendSumOfSummariesWithLabels(out, m.seriesDataFetched, "thanos_bucket_store_series_data_fetched", "data_type")
	data.SendSumOfSummariesWithLabels(out, m.seriesDataSizeTouched, "thanos_bucket_store_series_data_size_touched_bytes", "data_type")
	data.SendSumOfSummariesWithLabels(out, m.seriesDataSizeFetched, "thanos_bucket_store_series_data_size_fetched_bytes", "data_type")
	data.SendSumOfSummariesWithLabels(out, m.seriesBlocksQueried, "thanos_bucket_store_series_blocks_queried")

	data.SendSumOfHistograms(out, m.seriesGetAllDuration, "thanos_bucket_store_series_get_all_duration_seconds")
	data.SendSumOfHistograms(out, m.seriesMergeDuration, "thanos_bucket_store_series_merge_duration_seconds")
	data.SendSumOfCounters(out, m.seriesRefetches, "thanos_bucket_store_series_refetches_total")
	data.SendSumOfSummaries(out, m.resultSeriesCount, "thanos_bucket_store_series_result_series")

	data.SendSumOfCounters(out, m.metaSyncs, "blocks_meta_syncs_total")
	data.SendSumOfCounters(out, m.metaSyncFailures, "blocks_meta_sync_failures_total")
	data.SendSumOfHistograms(out, m.metaSyncDuration, "blocks_meta_sync_duration_seconds")
	data.SendMaxOfGauges(out, m.metaSyncConsistencyDelay, "consistency_delay_seconds")
}
