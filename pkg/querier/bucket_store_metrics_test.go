package querier

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTSDBBucketStoreMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	tsdbMetrics := newTSDBBucketStoreMetrics()
	mainReg.MustRegister(tsdbMetrics)

	tsdbMetrics.addUserRegistry("user1", populateTSDBBucketStoreMetrics(5328))
	tsdbMetrics.addUserRegistry("user2", populateTSDBBucketStoreMetrics(6908))
	tsdbMetrics.addUserRegistry("user3", populateTSDBBucketStoreMetrics(10283))

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
			# HELP cortex_querier_bucket_store_blocks_loaded TSDB: Number of currently loaded blocks.
			# TYPE cortex_querier_bucket_store_blocks_loaded gauge
			cortex_querier_bucket_store_blocks_loaded 22519

			# HELP cortex_querier_bucket_store_block_loads_total TSDB: Total number of remote block loading attempts.
			# TYPE cortex_querier_bucket_store_block_loads_total counter
			cortex_querier_bucket_store_block_loads_total 45038

			# HELP cortex_querier_bucket_store_block_load_failures_total TSDB: Total number of failed remote block loading attempts.
			# TYPE cortex_querier_bucket_store_block_load_failures_total counter
			cortex_querier_bucket_store_block_load_failures_total 67557

			# HELP cortex_querier_bucket_store_block_drops_total TSDB: Total number of local blocks that were dropped.
			# TYPE cortex_querier_bucket_store_block_drops_total counter
			cortex_querier_bucket_store_block_drops_total 90076

			# HELP cortex_querier_bucket_store_block_drop_failures_total TSDB: Total number of local blocks that failed to be dropped.
			# TYPE cortex_querier_bucket_store_block_drop_failures_total counter
			cortex_querier_bucket_store_block_drop_failures_total 112595

			# HELP cortex_querier_bucket_store_series_blocks_queried TSDB: Number of blocks in a bucket store that were touched to satisfy a query.
			# TYPE cortex_querier_bucket_store_series_blocks_queried summary
			cortex_querier_bucket_store_series_blocks_queried_sum 1.283583e+06
			cortex_querier_bucket_store_series_blocks_queried_count 9

			# HELP cortex_querier_bucket_store_series_data_fetched TSDB: How many items of a data type in a block were fetched for a single series request.
			# TYPE cortex_querier_bucket_store_series_data_fetched summary
			cortex_querier_bucket_store_series_data_fetched_sum{data_type="fetched-a"} 202671
			cortex_querier_bucket_store_series_data_fetched_count{data_type="fetched-a"} 3
			cortex_querier_bucket_store_series_data_fetched_sum{data_type="fetched-b"} 225190
			cortex_querier_bucket_store_series_data_fetched_count{data_type="fetched-b"} 3
			cortex_querier_bucket_store_series_data_fetched_sum{data_type="fetched-c"} 247709
			cortex_querier_bucket_store_series_data_fetched_count{data_type="fetched-c"} 3

			# HELP cortex_querier_bucket_store_series_data_size_fetched_bytes TSDB: Size of all items of a data type in a block were fetched for a single series request.
			# TYPE cortex_querier_bucket_store_series_data_size_fetched_bytes summary
			cortex_querier_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-a"} 337785
			cortex_querier_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-a"} 3
			cortex_querier_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-b"} 360304
			cortex_querier_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-b"} 3
			cortex_querier_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-c"} 382823
			cortex_querier_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-c"} 3

			# HELP cortex_querier_bucket_store_series_data_size_touched_bytes TSDB: Size of all items of a data type in a block were touched for a single series request.
			# TYPE cortex_querier_bucket_store_series_data_size_touched_bytes summary
			cortex_querier_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-a"} 270228
			cortex_querier_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-a"} 3
			cortex_querier_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-b"} 292747
			cortex_querier_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-b"} 3
			cortex_querier_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-c"} 315266
			cortex_querier_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-c"} 3

			# HELP cortex_querier_bucket_store_series_data_touched TSDB: How many items of a data type in a block were touched for a single series request.
			# TYPE cortex_querier_bucket_store_series_data_touched summary
			cortex_querier_bucket_store_series_data_touched_sum{data_type="touched-a"} 135114
			cortex_querier_bucket_store_series_data_touched_count{data_type="touched-a"} 3
			cortex_querier_bucket_store_series_data_touched_sum{data_type="touched-b"} 157633
			cortex_querier_bucket_store_series_data_touched_count{data_type="touched-b"} 3
			cortex_querier_bucket_store_series_data_touched_sum{data_type="touched-c"} 180152
			cortex_querier_bucket_store_series_data_touched_count{data_type="touched-c"} 3

			# HELP cortex_querier_bucket_store_series_get_all_duration_seconds TSDB: Time it takes until all per-block prepares and preloads for a query are finished.
			# TYPE cortex_querier_bucket_store_series_get_all_duration_seconds histogram
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="0.001"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="0.01"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="0.1"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="0.3"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="0.6"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="1"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="3"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="6"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="9"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="20"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="30"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="60"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="90"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="120"} 0
			cortex_querier_bucket_store_series_get_all_duration_seconds_bucket{le="+Inf"} 9
			cortex_querier_bucket_store_series_get_all_duration_seconds_sum 1.486254e+06
			cortex_querier_bucket_store_series_get_all_duration_seconds_count 9

			# HELP cortex_querier_bucket_store_series_merge_duration_seconds TSDB: Time it takes to merge sub-results from all queried blocks into a single result.
			# TYPE cortex_querier_bucket_store_series_merge_duration_seconds histogram
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="0.001"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="0.01"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="0.1"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="0.3"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="0.6"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="1"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="3"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="6"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="9"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="20"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="30"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="60"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="90"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="120"} 0
			cortex_querier_bucket_store_series_merge_duration_seconds_bucket{le="+Inf"} 9
			cortex_querier_bucket_store_series_merge_duration_seconds_sum 1.688925e+06
			cortex_querier_bucket_store_series_merge_duration_seconds_count 9

			# HELP cortex_querier_bucket_store_blocks_meta_sync_duration_seconds TSDB: Duration of the blocks metadata synchronization in seconds
			# TYPE cortex_querier_bucket_store_blocks_meta_sync_duration_seconds histogram
			cortex_querier_bucket_store_blocks_meta_sync_duration_seconds_bucket{le="+Inf"} 0
			cortex_querier_bucket_store_blocks_meta_sync_duration_seconds_sum 0
			cortex_querier_bucket_store_blocks_meta_sync_duration_seconds_count 0

			# HELP cortex_querier_bucket_store_blocks_meta_sync_failures_total TSDB: Total blocks metadata synchronization failures
			# TYPE cortex_querier_bucket_store_blocks_meta_sync_failures_total counter
			cortex_querier_bucket_store_blocks_meta_sync_failures_total 0

			# HELP cortex_querier_bucket_store_blocks_meta_syncs_total TSDB: Total blocks metadata synchronization attempts
			# TYPE cortex_querier_bucket_store_blocks_meta_syncs_total counter
			cortex_querier_bucket_store_blocks_meta_syncs_total 0

			# HELP cortex_querier_bucket_store_series_refetches_total TSDB: Total number of cases where the built-in max series size was not enough to fetch series from index, resulting in refetch.
			# TYPE cortex_querier_bucket_store_series_refetches_total counter
			cortex_querier_bucket_store_series_refetches_total 743127

			# HELP cortex_querier_bucket_store_series_result_series TSDB: Number of series observed in the final result of a query.
			# TYPE cortex_querier_bucket_store_series_result_series summary
			cortex_querier_bucket_store_series_result_series_sum 1.238545e+06
			cortex_querier_bucket_store_series_result_series_count 6

			# HELP cortex_querier_bucket_store_blocks_meta_sync_consistency_delay_seconds TSDB: Configured consistency delay in seconds.
			# TYPE cortex_querier_bucket_store_blocks_meta_sync_consistency_delay_seconds gauge
			cortex_querier_bucket_store_blocks_meta_sync_consistency_delay_seconds 300
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

	tsdbMetrics := newTSDBBucketStoreMetrics()
	mainReg.MustRegister(tsdbMetrics)

	base := 123456.0
	for i := 0; i < users; i++ {
		tsdbMetrics.addUserRegistry(fmt.Sprintf("user-%d", i), populateTSDBBucketStoreMetrics(base*float64(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mainReg.Gather()
	}
}

func populateTSDBBucketStoreMetrics(base float64) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	m := newBucketStoreMetrics(reg)

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
	m.queriesLimit.Add(32 * base)

	m.seriesRefetches.Add(33 * base)

	m.metaSyncConsistencyDelay.Set(300)

	return reg
}

// copied from Thanos, pkg/store/bucket.go
type bucketStoreMetrics struct {
	blocksLoaded             prometheus.Gauge
	blockLoads               prometheus.Counter
	blockLoadFailures        prometheus.Counter
	blockDrops               prometheus.Counter
	blockDropFailures        prometheus.Counter
	seriesDataTouched        *prometheus.SummaryVec
	seriesDataFetched        *prometheus.SummaryVec
	seriesDataSizeTouched    *prometheus.SummaryVec
	seriesDataSizeFetched    *prometheus.SummaryVec
	seriesBlocksQueried      prometheus.Summary
	seriesGetAllDuration     prometheus.Histogram
	seriesMergeDuration      prometheus.Histogram
	seriesRefetches          prometheus.Counter
	resultSeriesCount        prometheus.Summary
	chunkSizeBytes           prometheus.Histogram
	queriesDropped           prometheus.Counter
	queriesLimit             prometheus.Gauge
	metaSyncConsistencyDelay prometheus.Gauge
}

func newBucketStoreMetrics(reg prometheus.Registerer) *bucketStoreMetrics {
	var m bucketStoreMetrics

	m.blockLoads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_loads_total",
		Help: "Total number of remote block loading attempts.",
	})
	m.blockLoadFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_load_failures_total",
		Help: "Total number of failed remote block loading attempts.",
	})
	m.blockDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drops_total",
		Help: "Total number of local blocks that were dropped.",
	})
	m.blockDropFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drop_failures_total",
		Help: "Total number of local blocks that failed to be dropped.",
	})
	m.blocksLoaded = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_blocks_loaded",
		Help: "Number of currently loaded blocks.",
	})

	m.seriesDataTouched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_touched",
		Help: "How many items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataFetched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_fetched",
		Help: "How many items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesDataSizeTouched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_touched_bytes",
		Help: "Size of all items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataSizeFetched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_fetched_bytes",
		Help: "Size of all items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesBlocksQueried = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_blocks_queried",
		Help: "Number of blocks in a bucket store that were touched to satisfy a query.",
	})
	m.seriesGetAllDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_get_all_duration_seconds",
		Help:    "Time it takes until all per-block prepares and preloads for a query are finished.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.seriesMergeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_merge_duration_seconds",
		Help:    "Time it takes to merge sub-results from all queried blocks into a single result.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.seriesRefetches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_series_refetches_total",
		Help: "Total number of cases where the built-in max series size was not enough to fetch series from index, resulting in refetch.",
	})
	m.resultSeriesCount = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_result_series",
		Help: "Number of series observed in the final result of a query.",
	})

	m.chunkSizeBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_sent_chunk_size_bytes",
		Help: "Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	})

	m.queriesDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_queries_dropped_total",
		Help: "Number of queries that were dropped due to the sample limit.",
	})
	m.queriesLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_queries_concurrent_max",
		Help: "Number of maximum concurrent queries.",
	})

	m.metaSyncConsistencyDelay = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "consistency_delay_seconds",
		Help: "Configured consistency delay in seconds.",
	})

	if reg != nil {
		reg.MustRegister(
			m.blockLoads,
			m.blockLoadFailures,
			m.blockDrops,
			m.blockDropFailures,
			m.blocksLoaded,
			m.seriesDataTouched,
			m.seriesDataFetched,
			m.seriesDataSizeTouched,
			m.seriesDataSizeFetched,
			m.seriesBlocksQueried,
			m.seriesGetAllDuration,
			m.seriesMergeDuration,
			m.seriesRefetches,
			m.resultSeriesCount,
			m.chunkSizeBytes,
			m.queriesDropped,
			m.queriesLimit,
			m.metaSyncConsistencyDelay,
		)
	}
	return &m
}
