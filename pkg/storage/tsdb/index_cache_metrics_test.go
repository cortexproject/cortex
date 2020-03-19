package tsdb

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	cacheTypePostings string = "Postings"
	cacheTypeSeries   string = "Series"

	cacheOpSet      string = "set"
	cacheOpGetMulti string = "getmulti"
)

func TestInMemoryIndexCacheMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()
	cacheMetrics := NewInMemoryIndexCacheMetrics(populateInMemoryIndexCacheMetrics(5328))
	mainReg.MustRegister(cacheMetrics)

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
			# HELP cortex_querier_blocks_index_cache_items_evicted_total Total number of items that were evicted from the index cache.
			# TYPE cortex_querier_blocks_index_cache_items_evicted_total counter
			cortex_querier_blocks_index_cache_items_evicted_total{item_type="Postings"} 5328
			cortex_querier_blocks_index_cache_items_evicted_total{item_type="Series"} 10656

			# HELP cortex_querier_blocks_index_cache_requests_total Total number of requests to the cache.
			# TYPE cortex_querier_blocks_index_cache_requests_total counter
			cortex_querier_blocks_index_cache_requests_total{item_type="Postings"} 15984
			cortex_querier_blocks_index_cache_requests_total{item_type="Series"} 21312

			# HELP cortex_querier_blocks_index_cache_hits_total Total number of requests to the cache that were a hit.
			# TYPE cortex_querier_blocks_index_cache_hits_total counter
			cortex_querier_blocks_index_cache_hits_total{item_type="Postings"} 26640
			cortex_querier_blocks_index_cache_hits_total{item_type="Series"} 31968

			# HELP cortex_querier_blocks_index_cache_items_added_total Total number of items that were added to the index cache.
			# TYPE cortex_querier_blocks_index_cache_items_added_total counter
			cortex_querier_blocks_index_cache_items_added_total{item_type="Postings"} 37296
			cortex_querier_blocks_index_cache_items_added_total{item_type="Series"} 42624

			# HELP cortex_querier_blocks_index_cache_items Current number of items in the index cache.
			# TYPE cortex_querier_blocks_index_cache_items gauge
			cortex_querier_blocks_index_cache_items{item_type="Postings"} 47952
			cortex_querier_blocks_index_cache_items{item_type="Series"} 53280

			# HELP cortex_querier_blocks_index_cache_items_size_bytes Current byte size of items in the index cache.
			# TYPE cortex_querier_blocks_index_cache_items_size_bytes gauge
			cortex_querier_blocks_index_cache_items_size_bytes{item_type="Postings"} 58608
			cortex_querier_blocks_index_cache_items_size_bytes{item_type="Series"} 63936

			# HELP cortex_querier_blocks_index_cache_total_size_bytes Current byte size of items (both value and key) in the index cache.
			# TYPE cortex_querier_blocks_index_cache_total_size_bytes gauge
			cortex_querier_blocks_index_cache_total_size_bytes{item_type="Postings"} 69264
			cortex_querier_blocks_index_cache_total_size_bytes{item_type="Series"} 74592

			# HELP cortex_querier_blocks_index_cache_items_overflowed_total Total number of items that could not be added to the cache due to being too big.
			# TYPE cortex_querier_blocks_index_cache_items_overflowed_total counter
			cortex_querier_blocks_index_cache_items_overflowed_total{item_type="Postings"} 79920
			cortex_querier_blocks_index_cache_items_overflowed_total{item_type="Series"} 85248
`))
	require.NoError(t, err)
}

// Copied from Thanos, pkg/store/cache/inmemory.go, InMemoryIndexCache struct
type inMemoryIndexStoreCacheMetrics struct {
	evicted          *prometheus.CounterVec
	requests         *prometheus.CounterVec
	hits             *prometheus.CounterVec
	added            *prometheus.CounterVec
	current          *prometheus.GaugeVec
	currentSize      *prometheus.GaugeVec
	totalCurrentSize *prometheus.GaugeVec
	overflow         *prometheus.CounterVec
}

func newInMemoryIndexStoreCacheMetrics(reg prometheus.Registerer) *inMemoryIndexStoreCacheMetrics {
	c := inMemoryIndexStoreCacheMetrics{}
	c.evicted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_evicted_total",
		Help: "Total number of items that were evicted from the index cache.",
	}, []string{"item_type"})
	c.evicted.WithLabelValues(cacheTypePostings)
	c.evicted.WithLabelValues(cacheTypeSeries)

	c.added = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_added_total",
		Help: "Total number of items that were added to the index cache.",
	}, []string{"item_type"})
	c.added.WithLabelValues(cacheTypePostings)
	c.added.WithLabelValues(cacheTypeSeries)

	c.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of requests to the cache.",
	}, []string{"item_type"})
	c.requests.WithLabelValues(cacheTypePostings)
	c.requests.WithLabelValues(cacheTypeSeries)

	c.overflow = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_overflowed_total",
		Help: "Total number of items that could not be added to the cache due to being too big.",
	}, []string{"item_type"})
	c.overflow.WithLabelValues(cacheTypePostings)
	c.overflow.WithLabelValues(cacheTypeSeries)

	c.hits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of requests to the cache that were a hit.",
	}, []string{"item_type"})
	c.hits.WithLabelValues(cacheTypePostings)
	c.hits.WithLabelValues(cacheTypeSeries)

	c.current = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items",
		Help: "Current number of items in the index cache.",
	}, []string{"item_type"})
	c.current.WithLabelValues(cacheTypePostings)
	c.current.WithLabelValues(cacheTypeSeries)

	c.currentSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items_size_bytes",
		Help: "Current byte size of items in the index cache.",
	}, []string{"item_type"})
	c.currentSize.WithLabelValues(cacheTypePostings)
	c.currentSize.WithLabelValues(cacheTypeSeries)

	c.totalCurrentSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_total_size_bytes",
		Help: "Current byte size of items (both value and key) in the index cache.",
	}, []string{"item_type"})
	c.totalCurrentSize.WithLabelValues(cacheTypePostings)
	c.totalCurrentSize.WithLabelValues(cacheTypeSeries)

	if reg != nil {
		reg.MustRegister(c.requests, c.hits, c.added, c.evicted, c.current, c.currentSize, c.totalCurrentSize, c.overflow)
	}

	return &c
}

func populateInMemoryIndexCacheMetrics(base float64) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	c := newInMemoryIndexStoreCacheMetrics(reg)

	c.evicted.WithLabelValues(cacheTypePostings).Add(base * 1)
	c.evicted.WithLabelValues(cacheTypeSeries).Add(base * 2)
	c.requests.WithLabelValues(cacheTypePostings).Add(base * 3)
	c.requests.WithLabelValues(cacheTypeSeries).Add(base * 4)
	c.hits.WithLabelValues(cacheTypePostings).Add(base * 5)
	c.hits.WithLabelValues(cacheTypeSeries).Add(base * 6)
	c.added.WithLabelValues(cacheTypePostings).Add(base * 7)
	c.added.WithLabelValues(cacheTypeSeries).Add(base * 8)
	c.current.WithLabelValues(cacheTypePostings).Set(base * 9)
	c.current.WithLabelValues(cacheTypeSeries).Set(base * 10)
	c.currentSize.WithLabelValues(cacheTypePostings).Set(base * 11)
	c.currentSize.WithLabelValues(cacheTypeSeries).Set(base * 12)
	c.totalCurrentSize.WithLabelValues(cacheTypePostings).Set(base * 13)
	c.totalCurrentSize.WithLabelValues(cacheTypeSeries).Set(base * 14)
	c.overflow.WithLabelValues(cacheTypePostings).Add(base * 15)
	c.overflow.WithLabelValues(cacheTypeSeries).Add(base * 16)

	return reg
}

func TestMemcachedIndexCacheMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()
	cacheMetrics := NewMemcachedIndexCacheMetrics(populateMemcachedIndexCacheMetrics(1))
	mainReg.MustRegister(cacheMetrics)

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
			# HELP cortex_querier_blocks_index_cache_requests_total Total number of requests to the cache.
			# TYPE cortex_querier_blocks_index_cache_requests_total counter
			cortex_querier_blocks_index_cache_requests_total{item_type="Postings"} 1
			cortex_querier_blocks_index_cache_requests_total{item_type="Series"} 2

			# HELP cortex_querier_blocks_index_cache_hits_total Total number of requests to the cache that were a hit.
			# TYPE cortex_querier_blocks_index_cache_hits_total counter
			cortex_querier_blocks_index_cache_hits_total{item_type="Postings"} 3
			cortex_querier_blocks_index_cache_hits_total{item_type="Series"} 4

			# HELP cortex_querier_blocks_index_cache_memcached_operations_total Total number of operations against memcached.
			# TYPE cortex_querier_blocks_index_cache_memcached_operations_total counter
			cortex_querier_blocks_index_cache_memcached_operations_total{operation="set"} 5
			cortex_querier_blocks_index_cache_memcached_operations_total{operation="getmulti"} 6

			# HELP cortex_querier_blocks_index_cache_memcached_operation_failures_total Total number of operations against memcached that failed.
			# TYPE cortex_querier_blocks_index_cache_memcached_operation_failures_total counter
			cortex_querier_blocks_index_cache_memcached_operation_failures_total{operation="set"} 7
			cortex_querier_blocks_index_cache_memcached_operation_failures_total{operation="getmulti"} 8

			# HELP cortex_querier_blocks_index_cache_memcached_operation_duration_seconds Duration of operations against memcached.
			# TYPE cortex_querier_blocks_index_cache_memcached_operation_duration_seconds histogram
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="0.001"} 0
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="0.005"} 0
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="0.01"} 0
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="0.025"} 0
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="0.05"} 0
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="0.1"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="0.2"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="0.5"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="1"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="set",le="+Inf"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_sum{operation="set"} 0.1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_count{operation="set"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="0.001"} 0
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="0.005"} 0
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="0.01"} 0
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="0.025"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="0.05"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="0.1"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="0.2"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="0.5"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="1"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_bucket{operation="getmulti",le="+Inf"} 1
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_sum{operation="getmulti"} 0.025
			cortex_querier_blocks_index_cache_memcached_operation_duration_seconds_count{operation="getmulti"} 1
`))
	require.NoError(t, err)
}

type memcachedIndexStoreCacheMetrics struct {
	requests   *prometheus.CounterVec
	hits       *prometheus.CounterVec
	operations *prometheus.CounterVec
	failures   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
}

func newMemcachedIndexStoreCacheMetrics(reg prometheus.Registerer) *memcachedIndexStoreCacheMetrics {
	c := memcachedIndexStoreCacheMetrics{}

	c.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of requests to the cache.",
	}, []string{"item_type"})
	c.requests.WithLabelValues(cacheTypePostings)
	c.requests.WithLabelValues(cacheTypeSeries)

	c.hits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of requests to the cache that were a hit.",
	}, []string{"item_type"})
	c.hits.WithLabelValues(cacheTypePostings)
	c.hits.WithLabelValues(cacheTypeSeries)

	c.operations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_memcached_operations_total",
		Help: "Total number of operations against memcached.",
	}, []string{"operation"})

	c.failures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_memcached_operation_failures_total",
		Help: "Total number of operations against memcached that failed.",
	}, []string{"operation"})

	c.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_memcached_operation_duration_seconds",
		Help:    "Duration of operations against memcached.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1},
	}, []string{"operation"})

	if reg != nil {
		reg.MustRegister(c.requests, c.hits, c.operations, c.failures, c.duration)
	}

	return &c
}

func populateMemcachedIndexCacheMetrics(base float64) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	c := newMemcachedIndexStoreCacheMetrics(reg)

	c.requests.WithLabelValues(cacheTypePostings).Add(base * 1)
	c.requests.WithLabelValues(cacheTypeSeries).Add(base * 2)
	c.hits.WithLabelValues(cacheTypePostings).Add(base * 3)
	c.hits.WithLabelValues(cacheTypeSeries).Add(base * 4)

	c.operations.WithLabelValues(cacheOpSet).Add(base * 5)
	c.operations.WithLabelValues(cacheOpGetMulti).Add(base * 6)
	c.failures.WithLabelValues(cacheOpSet).Add(base * 7)
	c.failures.WithLabelValues(cacheOpGetMulti).Add(base * 8)
	c.duration.WithLabelValues(cacheOpSet).Observe(0.1)
	c.duration.WithLabelValues(cacheOpGetMulti).Observe(0.025)

	return reg
}
