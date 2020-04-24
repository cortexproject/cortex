package e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvaluatePromQLWithTextMetrics(t *testing.T) {
	metrics := `
			# HELP blocks_index_cache_items_evicted_total Total number of items that were evicted from the index cache.
			# TYPE blocks_index_cache_items_evicted_total counter
			blocks_index_cache_items_evicted_total{item_type="Postings"} 5328
			blocks_index_cache_items_evicted_total{item_type="Series"} 10656

			# HELP blocks_index_cache_requests_total Total number of requests to the cache.
			# TYPE blocks_index_cache_requests_total counter
			blocks_index_cache_requests_total{item_type="Postings"} 15984
			blocks_index_cache_requests_total{item_type="Series"} 21312

			# HELP blocks_index_cache_hits_total Total number of requests to the cache that were a hit.
			# TYPE blocks_index_cache_hits_total counter
			blocks_index_cache_hits_total{item_type="Postings"} 26640
			blocks_index_cache_hits_total{item_type="Series"} 31968

			# HELP blocks_index_cache_items_added_total Total number of items that were added to the index cache.
			# TYPE blocks_index_cache_items_added_total counter
			blocks_index_cache_items_added_total{item_type="Postings"} 37296
			blocks_index_cache_items_added_total{item_type="Series"} 42624

			# HELP blocks_index_cache_items Current number of items in the index cache.
			# TYPE blocks_index_cache_items gauge
			blocks_index_cache_items{item_type="Postings"} 47952
			blocks_index_cache_items{item_type="Series"} 53280

			# HELP bucket_store_series_get_all_duration_seconds Time it takes until all per-block prepares and preloads for a query are finished.
			# TYPE bucket_store_series_get_all_duration_seconds histogram
			bucket_store_series_get_all_duration_seconds_bucket{le="0.001"} 0
			bucket_store_series_get_all_duration_seconds_bucket{le="0.01"} 0
			bucket_store_series_get_all_duration_seconds_bucket{le="0.1"} 0
			bucket_store_series_get_all_duration_seconds_bucket{le="0.3"} 0
			bucket_store_series_get_all_duration_seconds_bucket{le="0.6"} 0
			bucket_store_series_get_all_duration_seconds_bucket{le="1"} 0
			bucket_store_series_get_all_duration_seconds_bucket{le="+Inf"} 9
			bucket_store_series_get_all_duration_seconds_sum 1.486254e+06
			bucket_store_series_get_all_duration_seconds_count 9
`

	for _, tc := range []struct {
		expr string
		val  float64
		err  error
	}{
		{
			expr: "sum(blocks_index_cache_items_evicted_total)",
			val:  15984,
		},
		{
			expr: "sum({__name__=~'blocks_index_cache_items_evicted_.*'})",
			val:  15984,
		},
		{
			expr: "blocks_index_cache_requests_total{item_type='Series'}",
			val:  21312,
		},
		{
			expr: "blocks_index_cache_requests_total{item_type!='Series'}",
			val:  15984,
		},
		{
			expr: "count(blocks_index_cache_hits_total)",
			val:  2,
		},
		{
			expr: "blocks_index_cache_items > 50000",
			val:  53280, // Returns blocks_index_cache_items{item_type="Series"} 53280.
		},
		{
			expr: "blocks_index_cache_items{item_type='Series'} >bool 50000",
			val:  1,
		},
		{
			expr: "bucket_store_series_get_all_duration_seconds_bucket > 0",
			val:  9,
		},
		{
			expr: "bucket_store_series_get_all_duration_seconds_bucket",
			err:  fmt.Errorf(errTooManyVectorElements, 7),
		},
		{
			expr: "blocks_index_cache_items[1m]",
			err:  errNotScalarOrVector,
		},
		{
			expr: "unknown_metric",
			val:  0,
		},
		{
			expr: "increase(blocks_index_cache_items[1m])",
			val:  0,
		},
		{
			expr: "rate(blocks_index_cache_items[1h])",
			val:  0,
		},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			val, err := evaluatePromQLWithTextMetrics(tc.expr, metrics)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.val, val)
		})
	}
}
