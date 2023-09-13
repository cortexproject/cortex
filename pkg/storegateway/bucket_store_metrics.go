package storegateway

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
)

// BucketStoreMetrics aggregates metrics exported by Thanos Bucket Store
// and re-exports those aggregates as Cortex metrics.
type BucketStoreMetrics struct {
	regs *util.UserRegistries

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
	seriesRefetches       *prometheus.Desc
	chunkRefetches        *prometheus.Desc
	resultSeriesCount     *prometheus.Desc
	queriesDropped        *prometheus.Desc
	chunkSizeBytes        *prometheus.Desc
	postingsSizeBytes     *prometheus.Desc
	emptyPostingCount     *prometheus.Desc

	cachedPostingsCompressions           *prometheus.Desc
	cachedPostingsCompressionErrors      *prometheus.Desc
	cachedPostingsCompressionTimeSeconds *prometheus.Desc
	cachedPostingsOriginalSizeBytes      *prometheus.Desc
	cachedPostingsCompressedSizeBytes    *prometheus.Desc

	seriesFetchDuration   *prometheus.Desc
	postingsFetchDuration *prometheus.Desc
	chunkFetchDuration    *prometheus.Desc

	lazyExpandedPostingsCount                     *prometheus.Desc
	lazyExpandedPostingSizeBytes                  *prometheus.Desc
	lazyExpandedPostingSeriesOverfetchedSizeBytes *prometheus.Desc

	indexHeaderLazyLoadCount         *prometheus.Desc
	indexHeaderLazyLoadFailedCount   *prometheus.Desc
	indexHeaderLazyUnloadCount       *prometheus.Desc
	indexHeaderLazyUnloadFailedCount *prometheus.Desc
	indexHeaderLazyLoadDuration      *prometheus.Desc
}

func NewBucketStoreMetrics() *BucketStoreMetrics {
	return &BucketStoreMetrics{
		regs: util.NewUserRegistries(),

		blockLoads: prometheus.NewDesc(
			"cortex_bucket_store_block_loads_total",
			"Total number of remote block loading attempts.",
			nil, nil),
		blockLoadFailures: prometheus.NewDesc(
			"cortex_bucket_store_block_load_failures_total",
			"Total number of failed remote block loading attempts.",
			nil, nil),
		blockDrops: prometheus.NewDesc(
			"cortex_bucket_store_block_drops_total",
			"Total number of local blocks that were dropped.",
			nil, nil),
		blockDropFailures: prometheus.NewDesc(
			"cortex_bucket_store_block_drop_failures_total",
			"Total number of local blocks that failed to be dropped.",
			nil, nil),
		blocksLoaded: prometheus.NewDesc(
			"cortex_bucket_store_blocks_loaded",
			"Number of currently loaded blocks.",
			[]string{"user"}, nil),
		seriesDataTouched: prometheus.NewDesc(
			"cortex_bucket_store_series_data_touched",
			"How many items of a data type in a block were touched for a single series request.",
			[]string{"data_type"}, nil),
		seriesDataFetched: prometheus.NewDesc(
			"cortex_bucket_store_series_data_fetched",
			"How many items of a data type in a block were fetched for a single series request.",
			[]string{"data_type"}, nil),
		seriesDataSizeTouched: prometheus.NewDesc(
			"cortex_bucket_store_series_data_size_touched_bytes",
			"Size of all items of a data type in a block were touched for a single series request.",
			[]string{"data_type"}, nil),
		seriesDataSizeFetched: prometheus.NewDesc(
			"cortex_bucket_store_series_data_size_fetched_bytes",
			"Size of all items of a data type in a block were fetched for a single series request.",
			[]string{"data_type"}, nil),
		seriesBlocksQueried: prometheus.NewDesc(
			"cortex_bucket_store_series_blocks_queried",
			"Number of blocks in a bucket store that were touched to satisfy a query.",
			nil, nil),

		seriesGetAllDuration: prometheus.NewDesc(
			"cortex_bucket_store_series_get_all_duration_seconds",
			"Time it takes until all per-block prepares and preloads for a query are finished.",
			nil, nil),
		seriesMergeDuration: prometheus.NewDesc(
			"cortex_bucket_store_series_merge_duration_seconds",
			"Time it takes to merge sub-results from all queried blocks into a single result.",
			nil, nil),
		seriesRefetches: prometheus.NewDesc(
			"cortex_bucket_store_series_refetches_total",
			"Total number of cases where the built-in max series size was not enough to fetch series from index, resulting in refetch.",
			nil, nil),
		chunkRefetches: prometheus.NewDesc(
			"cortex_bucket_store_chunk_refetches_total",
			"Total number of cases where configured estimated chunk bytes was not enough was to fetch chunks from object store, resulting in refetch.",
			nil, nil),
		resultSeriesCount: prometheus.NewDesc(
			"cortex_bucket_store_series_result_series",
			"Number of series observed in the final result of a query.",
			nil, nil),
		queriesDropped: prometheus.NewDesc(
			"cortex_bucket_store_queries_dropped_total",
			"Number of queries that were dropped due to the max chunks per query limit.",
			nil, nil),
		chunkSizeBytes: prometheus.NewDesc(
			"cortex_bucket_store_sent_chunk_size_bytes",
			"Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.",
			nil, nil),
		postingsSizeBytes: prometheus.NewDesc(
			"cortex_bucket_store_postings_size_bytes",
			"Size in bytes of the postings for a single series call.",
			nil, nil),
		emptyPostingCount: prometheus.NewDesc(
			"cortex_bucket_store_empty_postings_total",
			"Total number of empty postings when fetching block series.",
			nil, nil),

		cachedPostingsCompressions: prometheus.NewDesc(
			"cortex_bucket_store_cached_postings_compressions_total",
			"Number of postings compressions and decompressions when storing to index cache.",
			[]string{"op"}, nil),
		cachedPostingsCompressionErrors: prometheus.NewDesc(
			"cortex_bucket_store_cached_postings_compression_errors_total",
			"Number of postings compression and decompression errors.",
			[]string{"op"}, nil),
		cachedPostingsCompressionTimeSeconds: prometheus.NewDesc(
			"cortex_bucket_store_cached_postings_compression_time_seconds_total",
			"Time spent compressing and decompressing postings when storing to / reading from postings cache.",
			[]string{"op"}, nil),
		cachedPostingsOriginalSizeBytes: prometheus.NewDesc(
			"cortex_bucket_store_cached_postings_original_size_bytes_total",
			"Original size of postings stored into cache.",
			nil, nil),
		cachedPostingsCompressedSizeBytes: prometheus.NewDesc(
			"cortex_bucket_store_cached_postings_compressed_size_bytes_total",
			"Compressed size of postings stored into cache.",
			nil, nil),

		seriesFetchDuration: prometheus.NewDesc(
			"cortex_bucket_store_series_fetch_duration_seconds",
			"Time it takes to fetch series to respond a request sent to store-gateway. It includes both the time to fetch it from cache and from storage in case of cache misses.",
			nil, nil),
		postingsFetchDuration: prometheus.NewDesc(
			"cortex_bucket_store_postings_fetch_duration_seconds",
			"Time it takes to fetch postings to respond a request sent to store-gateway. It includes both the time to fetch it from cache and from storage in case of cache misses.",
			nil, nil),
		chunkFetchDuration: prometheus.NewDesc(
			"cortex_bucket_store_chunks_fetch_duration_seconds",
			"The total time spent fetching chunks within a single request a store gateway.",
			nil, nil),

		indexHeaderLazyLoadCount: prometheus.NewDesc(
			"cortex_bucket_store_indexheader_lazy_load_total",
			"Total number of index-header lazy load operations.",
			nil, nil),
		indexHeaderLazyLoadFailedCount: prometheus.NewDesc(
			"cortex_bucket_store_indexheader_lazy_load_failed_total",
			"Total number of failed index-header lazy load operations.",
			nil, nil),
		indexHeaderLazyUnloadCount: prometheus.NewDesc(
			"cortex_bucket_store_indexheader_lazy_unload_total",
			"Total number of index-header lazy unload operations.",
			nil, nil),
		indexHeaderLazyUnloadFailedCount: prometheus.NewDesc(
			"cortex_bucket_store_indexheader_lazy_unload_failed_total",
			"Total number of failed index-header lazy unload operations.",
			nil, nil),
		indexHeaderLazyLoadDuration: prometheus.NewDesc(
			"cortex_bucket_store_indexheader_lazy_load_duration_seconds",
			"Duration of the index-header lazy loading in seconds.",
			nil, nil),

		lazyExpandedPostingsCount: prometheus.NewDesc(
			"cortex_bucket_store_lazy_expanded_postings_total",
			"Total number of lazy expanded postings when fetching block series.",
			nil, nil),
		lazyExpandedPostingSizeBytes: prometheus.NewDesc(
			"cortex_bucket_store_lazy_expanded_posting_size_bytes_total",
			"Total number of lazy posting group size in bytes.",
			nil, nil),
		lazyExpandedPostingSeriesOverfetchedSizeBytes: prometheus.NewDesc(
			"cortex_bucket_store_lazy_expanded_posting_series_overfetched_size_bytes_total",
			"Total number of series size in bytes overfetched due to posting lazy expansion.",
			nil, nil),
	}
}

func (m *BucketStoreMetrics) AddUserRegistry(user string, reg *prometheus.Registry) {
	m.regs.AddUserRegistry(user, reg)
}

func (m *BucketStoreMetrics) RemoveUserRegistry(user string) {
	m.regs.RemoveUserRegistry(user, false)
}

func (m *BucketStoreMetrics) Describe(out chan<- *prometheus.Desc) {
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
	out <- m.chunkRefetches
	out <- m.resultSeriesCount
	out <- m.queriesDropped
	out <- m.chunkSizeBytes
	out <- m.postingsSizeBytes
	out <- m.emptyPostingCount

	out <- m.cachedPostingsCompressions
	out <- m.cachedPostingsCompressionErrors
	out <- m.cachedPostingsCompressionTimeSeconds
	out <- m.cachedPostingsOriginalSizeBytes
	out <- m.cachedPostingsCompressedSizeBytes

	out <- m.seriesFetchDuration
	out <- m.postingsFetchDuration
	out <- m.chunkFetchDuration

	out <- m.indexHeaderLazyLoadCount
	out <- m.indexHeaderLazyLoadFailedCount
	out <- m.indexHeaderLazyUnloadCount
	out <- m.indexHeaderLazyUnloadFailedCount
	out <- m.indexHeaderLazyLoadDuration

	out <- m.lazyExpandedPostingsCount
	out <- m.lazyExpandedPostingSizeBytes
	out <- m.lazyExpandedPostingSeriesOverfetchedSizeBytes
}

func (m *BucketStoreMetrics) Collect(out chan<- prometheus.Metric) {
	data := m.regs.BuildMetricFamiliesPerUser()

	data.SendSumOfCounters(out, m.blockLoads, "thanos_bucket_store_block_loads_total")
	data.SendSumOfCounters(out, m.blockLoadFailures, "thanos_bucket_store_block_load_failures_total")
	data.SendSumOfCounters(out, m.blockDrops, "thanos_bucket_store_block_drops_total")
	data.SendSumOfCounters(out, m.blockDropFailures, "thanos_bucket_store_block_drop_failures_total")

	data.SendSumOfGaugesPerUser(out, m.blocksLoaded, "thanos_bucket_store_blocks_loaded")

	data.SendSumOfHistogramsWithLabels(out, m.seriesDataTouched, "thanos_bucket_store_series_data_touched", "data_type")
	data.SendSumOfHistogramsWithLabels(out, m.seriesDataFetched, "thanos_bucket_store_series_data_fetched", "data_type")
	data.SendSumOfHistogramsWithLabels(out, m.seriesDataSizeTouched, "thanos_bucket_store_series_data_size_touched_bytes", "data_type")
	data.SendSumOfHistogramsWithLabels(out, m.seriesDataSizeFetched, "thanos_bucket_store_series_data_size_fetched_bytes", "data_type")
	data.SendSumOfHistogramsWithLabels(out, m.seriesBlocksQueried, "thanos_bucket_store_series_blocks_queried")

	data.SendSumOfHistograms(out, m.seriesGetAllDuration, "thanos_bucket_store_series_get_all_duration_seconds")
	data.SendSumOfHistograms(out, m.seriesMergeDuration, "thanos_bucket_store_series_merge_duration_seconds")
	data.SendSumOfCounters(out, m.seriesRefetches, "thanos_bucket_store_series_refetches_total")
	data.SendSumOfCounters(out, m.chunkRefetches, "thanos_bucket_store_chunk_refetches_total")
	data.SendSumOfHistograms(out, m.resultSeriesCount, "thanos_bucket_store_series_result_series")
	data.SendSumOfCounters(out, m.queriesDropped, "thanos_bucket_store_queries_dropped_total")
	data.SendSumOfHistograms(out, m.chunkSizeBytes, "thanos_bucket_store_sent_chunk_size_bytes")
	data.SendSumOfHistograms(out, m.postingsSizeBytes, "thanos_bucket_store_postings_size_bytes")
	data.SendSumOfCounters(out, m.emptyPostingCount, "thanos_bucket_store_empty_postings_total")

	data.SendSumOfCountersWithLabels(out, m.cachedPostingsCompressions, "thanos_bucket_store_cached_postings_compressions_total", "op")
	data.SendSumOfCountersWithLabels(out, m.cachedPostingsCompressionErrors, "thanos_bucket_store_cached_postings_compression_errors_total", "op")
	data.SendSumOfCountersWithLabels(out, m.cachedPostingsCompressionTimeSeconds, "thanos_bucket_store_cached_postings_compression_time_seconds_total", "op")
	data.SendSumOfCountersWithLabels(out, m.cachedPostingsOriginalSizeBytes, "thanos_bucket_store_cached_postings_original_size_bytes_total")
	data.SendSumOfCountersWithLabels(out, m.cachedPostingsCompressedSizeBytes, "thanos_bucket_store_cached_postings_compressed_size_bytes_total")

	data.SendSumOfHistograms(out, m.seriesFetchDuration, "thanos_bucket_store_series_fetch_duration_seconds")
	data.SendSumOfHistograms(out, m.postingsFetchDuration, "thanos_bucket_store_postings_fetch_duration_seconds")
	data.SendSumOfHistograms(out, m.chunkFetchDuration, "thanos_bucket_store_chunks_fetch_duration_seconds")

	data.SendSumOfCounters(out, m.indexHeaderLazyLoadCount, "thanos_bucket_store_indexheader_lazy_load_total")
	data.SendSumOfCounters(out, m.indexHeaderLazyLoadFailedCount, "thanos_bucket_store_indexheader_lazy_load_failed_total")
	data.SendSumOfCounters(out, m.indexHeaderLazyUnloadCount, "thanos_bucket_store_indexheader_lazy_unload_total")
	data.SendSumOfCounters(out, m.indexHeaderLazyUnloadFailedCount, "thanos_bucket_store_indexheader_lazy_unload_failed_total")
	data.SendSumOfHistograms(out, m.indexHeaderLazyLoadDuration, "thanos_bucket_store_indexheader_lazy_load_duration_seconds")

	data.SendSumOfCounters(out, m.lazyExpandedPostingsCount, "thanos_bucket_store_lazy_expanded_postings_total")
	data.SendSumOfCounters(out, m.lazyExpandedPostingSizeBytes, "thanos_bucket_store_lazy_expanded_posting_size_bytes_total")
	data.SendSumOfCounters(out, m.lazyExpandedPostingSeriesOverfetchedSizeBytes, "thanos_bucket_store_lazy_expanded_posting_series_overfetched_size_bytes_total")
}
