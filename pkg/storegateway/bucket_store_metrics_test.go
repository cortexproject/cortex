package storegateway

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestBucketStoreMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	tsdbMetrics := NewBucketStoreMetrics()
	mainReg.MustRegister(tsdbMetrics)

	tsdbMetrics.AddUserRegistry("user1", populateMockedBucketStoreMetrics(5328))
	tsdbMetrics.AddUserRegistry("user2", populateMockedBucketStoreMetrics(6908))
	tsdbMetrics.AddUserRegistry("user3", populateMockedBucketStoreMetrics(10283))

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded 22519

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total 45038

			# HELP cortex_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_bucket_store_block_load_failures_total counter
			cortex_bucket_store_block_load_failures_total 67557

			# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
			# TYPE cortex_bucket_store_block_drops_total counter
			cortex_bucket_store_block_drops_total 90076

			# HELP cortex_bucket_store_block_drop_failures_total Total number of local blocks that failed to be dropped.
			# TYPE cortex_bucket_store_block_drop_failures_total counter
			cortex_bucket_store_block_drop_failures_total 112595

			# HELP cortex_bucket_store_series_blocks_queried Number of blocks in a bucket store that were touched to satisfy a query.
			# TYPE cortex_bucket_store_series_blocks_queried summary
			cortex_bucket_store_series_blocks_queried_sum 1.283583e+06
			cortex_bucket_store_series_blocks_queried_count 9

			# HELP cortex_bucket_store_series_data_fetched How many items of a data type in a block were fetched for a single series request.
			# TYPE cortex_bucket_store_series_data_fetched summary
			cortex_bucket_store_series_data_fetched_sum{data_type="fetched-a"} 202671
			cortex_bucket_store_series_data_fetched_count{data_type="fetched-a"} 3
			cortex_bucket_store_series_data_fetched_sum{data_type="fetched-b"} 225190
			cortex_bucket_store_series_data_fetched_count{data_type="fetched-b"} 3
			cortex_bucket_store_series_data_fetched_sum{data_type="fetched-c"} 247709
			cortex_bucket_store_series_data_fetched_count{data_type="fetched-c"} 3

			# HELP cortex_bucket_store_series_data_size_fetched_bytes Size of all items of a data type in a block were fetched for a single series request.
			# TYPE cortex_bucket_store_series_data_size_fetched_bytes summary
			cortex_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-a"} 337785
			cortex_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-a"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-b"} 360304
			cortex_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-b"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-c"} 382823
			cortex_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-c"} 3

			# HELP cortex_bucket_store_series_data_size_touched_bytes Size of all items of a data type in a block were touched for a single series request.
			# TYPE cortex_bucket_store_series_data_size_touched_bytes summary
			cortex_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-a"} 270228
			cortex_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-a"} 3
			cortex_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-b"} 292747
			cortex_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-b"} 3
			cortex_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-c"} 315266
			cortex_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-c"} 3

			# HELP cortex_bucket_store_series_data_touched How many items of a data type in a block were touched for a single series request.
			# TYPE cortex_bucket_store_series_data_touched summary
			cortex_bucket_store_series_data_touched_sum{data_type="touched-a"} 135114
			cortex_bucket_store_series_data_touched_count{data_type="touched-a"} 3
			cortex_bucket_store_series_data_touched_sum{data_type="touched-b"} 157633
			cortex_bucket_store_series_data_touched_count{data_type="touched-b"} 3
			cortex_bucket_store_series_data_touched_sum{data_type="touched-c"} 180152
			cortex_bucket_store_series_data_touched_count{data_type="touched-c"} 3

			# HELP cortex_bucket_store_series_get_all_duration_seconds Time it takes until all per-block prepares and preloads for a query are finished.
			# TYPE cortex_bucket_store_series_get_all_duration_seconds histogram
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="0.001"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="0.01"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="0.1"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="0.3"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="0.6"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="1"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="3"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="6"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="9"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="20"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="30"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="60"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="90"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="120"} 0
			cortex_bucket_store_series_get_all_duration_seconds_bucket{le="+Inf"} 9
			cortex_bucket_store_series_get_all_duration_seconds_sum 1.486254e+06
			cortex_bucket_store_series_get_all_duration_seconds_count 9

			# HELP cortex_bucket_store_series_merge_duration_seconds Time it takes to merge sub-results from all queried blocks into a single result.
			# TYPE cortex_bucket_store_series_merge_duration_seconds histogram
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="0.001"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="0.01"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="0.1"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="0.3"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="0.6"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="1"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="3"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="6"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="9"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="20"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="30"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="60"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="90"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="120"} 0
			cortex_bucket_store_series_merge_duration_seconds_bucket{le="+Inf"} 9
			cortex_bucket_store_series_merge_duration_seconds_sum 1.688925e+06
			cortex_bucket_store_series_merge_duration_seconds_count 9

			# HELP cortex_bucket_store_series_refetches_total Total number of cases where the built-in max series size was not enough to fetch series from index, resulting in refetch.
			# TYPE cortex_bucket_store_series_refetches_total counter
			cortex_bucket_store_series_refetches_total 743127

			# HELP cortex_bucket_store_series_result_series Number of series observed in the final result of a query.
			# TYPE cortex_bucket_store_series_result_series summary
			cortex_bucket_store_series_result_series_sum 1.238545e+06
			cortex_bucket_store_series_result_series_count 6

			# HELP cortex_bucket_store_cached_postings_compressions_total Number of postings compressions and decompressions when storing to index cache.
			# TYPE cortex_bucket_store_cached_postings_compressions_total counter
			cortex_bucket_store_cached_postings_compressions_total{op="encode"} 1125950
			cortex_bucket_store_cached_postings_compressions_total{op="decode"} 1148469

			# HELP cortex_bucket_store_cached_postings_compression_errors_total Number of postings compression and decompression errors.
			# TYPE cortex_bucket_store_cached_postings_compression_errors_total counter
			cortex_bucket_store_cached_postings_compression_errors_total{op="encode"} 1170988
			cortex_bucket_store_cached_postings_compression_errors_total{op="decode"} 1193507

			# HELP cortex_bucket_store_cached_postings_compression_time_seconds Time spent compressing and decompressing postings when storing to / reading from postings cache.
			# TYPE cortex_bucket_store_cached_postings_compression_time_seconds counter
			cortex_bucket_store_cached_postings_compression_time_seconds{op="encode"} 1216026
			cortex_bucket_store_cached_postings_compression_time_seconds{op="decode"} 1238545

			# HELP cortex_bucket_store_cached_postings_original_size_bytes_total Original size of postings stored into cache.
			# TYPE cortex_bucket_store_cached_postings_original_size_bytes_total counter
			cortex_bucket_store_cached_postings_original_size_bytes_total 1261064

			# HELP cortex_bucket_store_cached_postings_compressed_size_bytes_total Compressed size of postings stored into cache.
			# TYPE cortex_bucket_store_cached_postings_compressed_size_bytes_total counter
			cortex_bucket_store_cached_postings_compressed_size_bytes_total 1283583
`))
	require.NoError(t, err)
}

func BenchmarkMetricsCollections10(b *testing.B) {
	benchmarkMetricsCollection(b, 10)
}

func BenchmarkMetricsCollections100(b *testing.B) {
	benchmarkMetricsCollection(b, 100)
}

func BenchmarkMetricsCollections1000(b *testing.B) {
	benchmarkMetricsCollection(b, 1000)
}

func BenchmarkMetricsCollections10000(b *testing.B) {
	benchmarkMetricsCollection(b, 10000)
}

func benchmarkMetricsCollection(b *testing.B, users int) {
	mainReg := prometheus.NewRegistry()

	tsdbMetrics := NewBucketStoreMetrics()
	mainReg.MustRegister(tsdbMetrics)

	base := 123456.0
	for i := 0; i < users; i++ {
		tsdbMetrics.AddUserRegistry(fmt.Sprintf("user-%d", i), populateMockedBucketStoreMetrics(base*float64(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mainReg.Gather()
	}
}

func populateMockedBucketStoreMetrics(base float64) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	m := newMockedBucketStoreMetrics(reg)

	m.blocksLoaded.Add(1 * base)
	m.blockLoads.Add(2 * base)
	m.blockLoadFailures.Add(3 * base)
	m.blockDrops.Add(4 * base)
	m.blockDropFailures.Add(5 * base)
	m.seriesDataTouched.WithLabelValues("touched-a").Observe(6 * base)
	m.seriesDataTouched.WithLabelValues("touched-b").Observe(7 * base)
	m.seriesDataTouched.WithLabelValues("touched-c").Observe(8 * base)

	m.seriesDataFetched.WithLabelValues("fetched-a").Observe(9 * base)
	m.seriesDataFetched.WithLabelValues("fetched-b").Observe(10 * base)
	m.seriesDataFetched.WithLabelValues("fetched-c").Observe(11 * base)

	m.seriesDataSizeTouched.WithLabelValues("size-touched-a").Observe(12 * base)
	m.seriesDataSizeTouched.WithLabelValues("size-touched-b").Observe(13 * base)
	m.seriesDataSizeTouched.WithLabelValues("size-touched-c").Observe(14 * base)

	m.seriesDataSizeFetched.WithLabelValues("size-fetched-a").Observe(15 * base)
	m.seriesDataSizeFetched.WithLabelValues("size-fetched-b").Observe(16 * base)
	m.seriesDataSizeFetched.WithLabelValues("size-fetched-c").Observe(17 * base)

	m.seriesBlocksQueried.Observe(18 * base)
	m.seriesBlocksQueried.Observe(19 * base)
	m.seriesBlocksQueried.Observe(20 * base)

	m.seriesGetAllDuration.Observe(21 * base)
	m.seriesGetAllDuration.Observe(22 * base)
	m.seriesGetAllDuration.Observe(23 * base)

	m.seriesMergeDuration.Observe(24 * base)
	m.seriesMergeDuration.Observe(25 * base)
	m.seriesMergeDuration.Observe(26 * base)

	m.resultSeriesCount.Observe(27 * base)
	m.resultSeriesCount.Observe(28 * base)

	m.chunkSizeBytes.Observe(29 * base)
	m.chunkSizeBytes.Observe(30 * base)

	m.queriesDropped.Add(31 * base)

	m.seriesRefetches.Add(33 * base)

	m.cachedPostingsCompressions.WithLabelValues("encode").Add(50 * base)
	m.cachedPostingsCompressions.WithLabelValues("decode").Add(51 * base)

	m.cachedPostingsCompressionErrors.WithLabelValues("encode").Add(52 * base)
	m.cachedPostingsCompressionErrors.WithLabelValues("decode").Add(53 * base)

	m.cachedPostingsCompressionTimeSeconds.WithLabelValues("encode").Add(54 * base)
	m.cachedPostingsCompressionTimeSeconds.WithLabelValues("decode").Add(55 * base)

	m.cachedPostingsOriginalSizeBytes.Add(56 * base)
	m.cachedPostingsCompressedSizeBytes.Add(57 * base)

	return reg
}

// copied from Thanos, pkg/store/bucket.go
type mockedBucketStoreMetrics struct {
	blocksLoaded          prometheus.Gauge
	blockLoads            prometheus.Counter
	blockLoadFailures     prometheus.Counter
	blockDrops            prometheus.Counter
	blockDropFailures     prometheus.Counter
	seriesDataTouched     *prometheus.SummaryVec
	seriesDataFetched     *prometheus.SummaryVec
	seriesDataSizeTouched *prometheus.SummaryVec
	seriesDataSizeFetched *prometheus.SummaryVec
	seriesBlocksQueried   prometheus.Summary
	seriesGetAllDuration  prometheus.Histogram
	seriesMergeDuration   prometheus.Histogram
	seriesRefetches       prometheus.Counter
	resultSeriesCount     prometheus.Summary
	chunkSizeBytes        prometheus.Histogram
	queriesDropped        prometheus.Counter

	cachedPostingsCompressions           *prometheus.CounterVec
	cachedPostingsCompressionErrors      *prometheus.CounterVec
	cachedPostingsCompressionTimeSeconds *prometheus.CounterVec
	cachedPostingsOriginalSizeBytes      prometheus.Counter
	cachedPostingsCompressedSizeBytes    prometheus.Counter
}

func newMockedBucketStoreMetrics(reg prometheus.Registerer) *mockedBucketStoreMetrics {
	var m mockedBucketStoreMetrics

	m.blockLoads = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_loads_total",
		Help: "Total number of remote block loading attempts.",
	})
	m.blockLoadFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_load_failures_total",
		Help: "Total number of failed remote block loading attempts.",
	})
	m.blockDrops = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drops_total",
		Help: "Total number of local blocks that were dropped.",
	})
	m.blockDropFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drop_failures_total",
		Help: "Total number of local blocks that failed to be dropped.",
	})
	m.blocksLoaded = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_blocks_loaded",
		Help: "Number of currently loaded blocks.",
	})

	m.seriesDataTouched = promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_touched",
		Help: "How many items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataFetched = promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_fetched",
		Help: "How many items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesDataSizeTouched = promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_touched_bytes",
		Help: "Size of all items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataSizeFetched = promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_fetched_bytes",
		Help: "Size of all items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesBlocksQueried = promauto.With(reg).NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_blocks_queried",
		Help: "Number of blocks in a bucket store that were touched to satisfy a query.",
	})
	m.seriesGetAllDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_get_all_duration_seconds",
		Help:    "Time it takes until all per-block prepares and preloads for a query are finished.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.seriesMergeDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_merge_duration_seconds",
		Help:    "Time it takes to merge sub-results from all queried blocks into a single result.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.resultSeriesCount = promauto.With(reg).NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_result_series",
		Help: "Number of series observed in the final result of a query.",
	})

	m.chunkSizeBytes = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_sent_chunk_size_bytes",
		Help: "Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	})

	m.queriesDropped = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_queries_dropped_total",
		Help: "Number of queries that were dropped due to the sample limit.",
	})
	m.seriesRefetches = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_series_refetches_total",
		Help: fmt.Sprintf("Total number of cases where %v bytes was not enough was to fetch series from index, resulting in refetch.", 64*1024),
	})

	m.cachedPostingsCompressions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_compressions_total",
		Help: "Number of postings compressions before storing to index cache.",
	}, []string{"op"})
	m.cachedPostingsCompressionErrors = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_compression_errors_total",
		Help: "Number of postings compression errors.",
	}, []string{"op"})
	m.cachedPostingsCompressionTimeSeconds = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_compression_time_seconds",
		Help: "Time spent compressing postings before storing them into postings cache.",
	}, []string{"op"})
	m.cachedPostingsOriginalSizeBytes = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_original_size_bytes_total",
		Help: "Original size of postings stored into cache.",
	})
	m.cachedPostingsCompressedSizeBytes = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_compressed_size_bytes_total",
		Help: "Compressed size of postings stored into cache.",
	})

	return &m
}
