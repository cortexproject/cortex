package querier

import (
	"sync"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
)

// This struct aggregates metrics exported by Thanos Bucket Store
// and re-exports those aggregates as Cortex metrics.
type tsdbBucketStoreMetrics struct {
	// Maps userID -> registry
	regsMu sync.Mutex
	regs   map[string]*prometheus.Registry

	// exported metrics, gathered from Thanos BucketStore
	blockLoads            *prometheus.Desc
	blockLoadFailures     *prometheus.Desc
	blockDrops            *prometheus.Desc
	blockDropFailures     *prometheus.Desc
	blocksLoaded          *prometheus.Desc
	seriesDataTouched     *prometheus.Desc
	seriesDataFetched     *prometheus.Desc
	seriesDataSizeTouched *prometheus.Desc
	seriesDataSizeFetched *prometheus.Desc
	seriesBlocksQueried   *prometheus.Desc
	seriesGetAllDuration  *prometheus.Desc
	seriesMergeDuration   *prometheus.Desc
	resultSeriesCount     *prometheus.Desc
	metaSyncs             *prometheus.Desc
	metaSyncFailures      *prometheus.Desc
	metaSyncDuration      *prometheus.Desc

	// Ignored:
	// blocks_meta_synced

	// Metrics gathered from Thanos storecache.InMemoryIndexCache
	cacheItemsEvicted          *prometheus.Desc
	cacheItemsAdded            *prometheus.Desc
	cacheRequests              *prometheus.Desc
	cacheItemsOverflow         *prometheus.Desc
	cacheHits                  *prometheus.Desc
	cacheItemsCurrentCount     *prometheus.Desc
	cacheItemsCurrentSize      *prometheus.Desc
	cacheItemsTotalCurrentSize *prometheus.Desc

	// Ignored:
	// thanos_store_index_cache_max_size_bytes
	// thanos_store_index_cache_max_item_size_bytes
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

		// Cache
		cacheItemsEvicted: prometheus.NewDesc(
			"cortex_querier_blocks_index_cache_items_evicted_total",
			"TSDB: Total number of items that were evicted from the index cache.",
			[]string{"item_type"}, nil),
		cacheItemsAdded: prometheus.NewDesc(
			"cortex_querier_blocks_index_cache_items_added_total",
			"TSDB: Total number of items that were added to the index cache.",
			[]string{"item_type"}, nil),
		cacheRequests: prometheus.NewDesc(
			"cortex_querier_blocks_index_cache_requests_total",
			"TSDB: Total number of requests to the cache.",
			[]string{"item_type"}, nil),
		cacheItemsOverflow: prometheus.NewDesc(
			"cortex_querier_blocks_index_cache_items_overflowed_total",
			"TSDB: Total number of items that could not be added to the cache due to being too big.",
			[]string{"item_type"}, nil),
		cacheHits: prometheus.NewDesc(
			"cortex_querier_blocks_index_cache_hits_total",
			"TSDB: Total number of requests to the cache that were a hit.",
			[]string{"item_type"}, nil),
		cacheItemsCurrentCount: prometheus.NewDesc(
			"cortex_querier_blocks_index_cache_items",
			"TSDB: Current number of items in the index cache.",
			[]string{"item_type"}, nil),
		cacheItemsCurrentSize: prometheus.NewDesc(
			"cortex_querier_blocks_index_cache_items_size_bytes",
			"TSDB: Current byte size of items in the index cache.",
			[]string{"item_type"}, nil),
		cacheItemsTotalCurrentSize: prometheus.NewDesc(
			"cortex_querier_blocks_index_cache_total_size_bytes",
			"TSDB: Current byte size of items (both value and key) in the index cache.",
			[]string{"item_type"}, nil),
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
	out <- m.resultSeriesCount

	out <- m.metaSyncs
	out <- m.metaSyncFailures
	out <- m.metaSyncDuration

	out <- m.cacheItemsEvicted
	out <- m.cacheItemsAdded
	out <- m.cacheRequests
	out <- m.cacheItemsOverflow
	out <- m.cacheHits
	out <- m.cacheItemsCurrentCount
	out <- m.cacheItemsCurrentSize
	out <- m.cacheItemsTotalCurrentSize
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
	data.SendSumOfSummaries(out, m.resultSeriesCount, "thanos_bucket_store_series_result_series")

	data.SendSumOfCounters(out, m.metaSyncs, "blocks_meta_syncs_total")
	data.SendSumOfCounters(out, m.metaSyncFailures, "blocks_meta_sync_failures_total")
	data.SendSumOfHistograms(out, m.metaSyncDuration, "blocks_meta_sync_duration_seconds")

	data.SendSumOfCountersWithLabels(out, m.cacheItemsEvicted, "thanos_store_index_cache_items_evicted_total", "item_type")
	data.SendSumOfCountersWithLabels(out, m.cacheItemsAdded, "thanos_store_index_cache_items_added_total", "item_type")
	data.SendSumOfCountersWithLabels(out, m.cacheRequests, "thanos_store_index_cache_requests_total", "item_type")
	data.SendSumOfCountersWithLabels(out, m.cacheItemsOverflow, "thanos_store_index_cache_items_overflowed_total", "item_type")
	data.SendSumOfCountersWithLabels(out, m.cacheHits, "thanos_store_index_cache_hits_total", "item_type")

	data.SendSumOfGaugesWithLabels(out, m.cacheItemsCurrentCount, "thanos_store_index_cache_items", "item_type")
	data.SendSumOfGaugesWithLabels(out, m.cacheItemsCurrentSize, "thanos_store_index_cache_items_size_bytes", "item_type")
	data.SendSumOfGaugesWithLabels(out, m.cacheItemsTotalCurrentSize, "thanos_store_index_cache_total_size_bytes", "item_type")
}
