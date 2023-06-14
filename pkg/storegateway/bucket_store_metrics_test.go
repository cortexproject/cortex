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
        	cortex_bucket_store_blocks_loaded{user="user1"} 5328
        	cortex_bucket_store_blocks_loaded{user="user2"} 6908
        	cortex_bucket_store_blocks_loaded{user="user3"} 10283

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
			# TYPE cortex_bucket_store_series_blocks_queried histogram
			cortex_bucket_store_series_blocks_queried_bucket{le="1"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="2"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="4"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="8"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="16"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="32"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="64"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="128"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="256"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="512"} 0
			cortex_bucket_store_series_blocks_queried_bucket{le="+Inf"} 9
			cortex_bucket_store_series_blocks_queried_sum 1.283583e+06
			cortex_bucket_store_series_blocks_queried_count 9

			# HELP cortex_bucket_store_series_data_fetched How many items of a data type in a block were fetched for a single series request.
			# TYPE cortex_bucket_store_series_data_fetched histogram
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="200"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="400"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="800"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="1600"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="3200"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="6400"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="12800"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="25600"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="51200"} 1
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="102400"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="204800"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="409600"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="819200"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="1.6384e+06"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="3.2768e+06"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-a",le="+Inf"} 3
			cortex_bucket_store_series_data_fetched_sum{data_type="fetched-a"} 202671
			cortex_bucket_store_series_data_fetched_count{data_type="fetched-a"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="200"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="400"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="800"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="1600"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="3200"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="6400"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="12800"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="25600"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="51200"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="102400"} 2
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="204800"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="409600"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="819200"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="1.6384e+06"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="3.2768e+06"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-b",le="+Inf"} 3
			cortex_bucket_store_series_data_fetched_sum{data_type="fetched-b"} 225190
			cortex_bucket_store_series_data_fetched_count{data_type="fetched-b"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="200"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="400"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="800"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="1600"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="3200"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="6400"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="12800"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="25600"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="51200"} 0
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="102400"} 2
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="204800"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="409600"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="819200"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="1.6384e+06"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="3.2768e+06"} 3
			cortex_bucket_store_series_data_fetched_bucket{data_type="fetched-c",le="+Inf"} 3
			cortex_bucket_store_series_data_fetched_sum{data_type="fetched-c"} 247709
			cortex_bucket_store_series_data_fetched_count{data_type="fetched-c"} 3

			# HELP cortex_bucket_store_series_data_size_fetched_bytes Size of all items of a data type in a block were fetched for a single series request.
			# TYPE cortex_bucket_store_series_data_size_fetched_bytes histogram
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="1024"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="2048"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="4096"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="8192"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="16384"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="32768"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="65536"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="131072"} 2
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="262144"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="524288"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="1.048576e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="2.097152e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="4.194304e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="8.388608e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="1.6777216e+07"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-a",le="+Inf"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-a"} 337785
			cortex_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-a"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="1024"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="2048"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="4096"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="8192"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="16384"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="32768"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="65536"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="131072"} 2
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="262144"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="524288"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="1.048576e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="2.097152e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="4.194304e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="8.388608e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="1.6777216e+07"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-b",le="+Inf"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-b"} 360304
			cortex_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-b"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="1024"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="2048"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="4096"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="8192"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="16384"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="32768"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="65536"} 0
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="131072"} 2
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="262144"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="524288"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="1.048576e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="2.097152e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="4.194304e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="8.388608e+06"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="1.6777216e+07"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_bucket{data_type="size-fetched-c",le="+Inf"} 3
			cortex_bucket_store_series_data_size_fetched_bytes_sum{data_type="size-fetched-c"} 382823
			cortex_bucket_store_series_data_size_fetched_bytes_count{data_type="size-fetched-c"} 3

			# HELP cortex_bucket_store_series_data_size_touched_bytes Size of all items of a data type in a block were touched for a single series request.
			# TYPE cortex_bucket_store_series_data_size_touched_bytes histogram
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="1024"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="2048"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="4096"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="8192"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="16384"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="32768"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="65536"} 1
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="131072"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="262144"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="524288"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="1.048576e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="2.097152e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="4.194304e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="8.388608e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="1.6777216e+07"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-a",le="+Inf"} 3
			cortex_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-a"} 270228
			cortex_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-a"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="1024"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="2048"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="4096"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="8192"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="16384"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="32768"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="65536"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="131072"} 2
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="262144"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="524288"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="1.048576e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="2.097152e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="4.194304e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="8.388608e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="1.6777216e+07"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-b",le="+Inf"} 3
			cortex_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-b"} 292747
			cortex_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-b"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="1024"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="2048"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="4096"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="8192"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="16384"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="32768"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="65536"} 0
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="131072"} 2
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="262144"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="524288"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="1.048576e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="2.097152e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="4.194304e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="8.388608e+06"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="1.6777216e+07"} 3
			cortex_bucket_store_series_data_size_touched_bytes_bucket{data_type="size-touched-c",le="+Inf"} 3
			cortex_bucket_store_series_data_size_touched_bytes_sum{data_type="size-touched-c"} 315266
			cortex_bucket_store_series_data_size_touched_bytes_count{data_type="size-touched-c"} 3

			# HELP cortex_bucket_store_series_data_touched How many items of a data type in a block were touched for a single series request.
			# TYPE cortex_bucket_store_series_data_touched histogram
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="200"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="400"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="800"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="1600"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="3200"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="6400"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="12800"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="25600"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="51200"} 2
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="102400"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="204800"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="409600"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="819200"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="1.6384e+06"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="3.2768e+06"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-a",le="+Inf"} 3
			cortex_bucket_store_series_data_touched_sum{data_type="touched-a"} 135114
			cortex_bucket_store_series_data_touched_count{data_type="touched-a"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="200"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="400"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="800"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="1600"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="3200"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="6400"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="12800"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="25600"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="51200"} 2
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="102400"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="204800"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="409600"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="819200"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="1.6384e+06"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="3.2768e+06"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-b",le="+Inf"} 3
			cortex_bucket_store_series_data_touched_sum{data_type="touched-b"} 157633
			cortex_bucket_store_series_data_touched_count{data_type="touched-b"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="200"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="400"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="800"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="1600"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="3200"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="6400"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="12800"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="25600"} 0
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="51200"} 1
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="102400"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="204800"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="409600"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="819200"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="1.6384e+06"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="3.2768e+06"} 3
			cortex_bucket_store_series_data_touched_bucket{data_type="touched-c",le="+Inf"} 3
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
			# TYPE cortex_bucket_store_series_result_series histogram
			cortex_bucket_store_series_result_series_bucket{le="1"} 0
			cortex_bucket_store_series_result_series_bucket{le="2"} 0
			cortex_bucket_store_series_result_series_bucket{le="4"} 0
			cortex_bucket_store_series_result_series_bucket{le="8"} 0
			cortex_bucket_store_series_result_series_bucket{le="16"} 0
			cortex_bucket_store_series_result_series_bucket{le="32"} 0
			cortex_bucket_store_series_result_series_bucket{le="64"} 0
			cortex_bucket_store_series_result_series_bucket{le="128"} 0
			cortex_bucket_store_series_result_series_bucket{le="256"} 0
			cortex_bucket_store_series_result_series_bucket{le="512"} 0
			cortex_bucket_store_series_result_series_bucket{le="1024"} 0
			cortex_bucket_store_series_result_series_bucket{le="2048"} 0
			cortex_bucket_store_series_result_series_bucket{le="4096"} 0
			cortex_bucket_store_series_result_series_bucket{le="8192"} 0
			cortex_bucket_store_series_result_series_bucket{le="16384"} 0
			cortex_bucket_store_series_result_series_bucket{le="+Inf"} 6
			cortex_bucket_store_series_result_series_sum 1.238545e+06
			cortex_bucket_store_series_result_series_count 6

			# HELP cortex_bucket_store_queries_dropped_total Number of queries that were dropped due to the max chunks per query limit.
			# TYPE cortex_bucket_store_queries_dropped_total counter
			cortex_bucket_store_queries_dropped_total 698089
        	# HELP cortex_bucket_store_sent_chunk_size_bytes Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.
        	# TYPE cortex_bucket_store_sent_chunk_size_bytes histogram
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="32"} 0
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="256"} 0
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="512"} 0
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="1024"} 0
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="32768"} 0
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="262144"} 7
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="524288"} 9
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="1.048576e+06"} 9
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="3.3554432e+07"} 9
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="2.68435456e+08"} 9
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="5.36870912e+08"} 9
        	cortex_bucket_store_sent_chunk_size_bytes_bucket{le="+Inf"} 9
        	cortex_bucket_store_sent_chunk_size_bytes_sum 1.57633e+06
        	cortex_bucket_store_sent_chunk_size_bytes_count 9
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

			# HELP cortex_bucket_store_cached_series_fetch_duration_seconds Time it takes to fetch series to respond a request sent to store-gateway. It includes both the time to fetch it from cache and from storage in case of cache misses.
			# TYPE cortex_bucket_store_cached_series_fetch_duration_seconds histogram
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="0.001"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="0.01"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="0.1"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="0.3"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="0.6"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="1"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="3"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="6"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="9"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="20"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="30"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="60"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="90"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="120"} 0
			cortex_bucket_store_cached_series_fetch_duration_seconds_bucket{le="+Inf"} 3
			cortex_bucket_store_cached_series_fetch_duration_seconds_sum 1.306102e+06
			cortex_bucket_store_cached_series_fetch_duration_seconds_count 3
        	# HELP cortex_bucket_store_empty_postings_total Total number of empty postings when fetching block series.
            # TYPE cortex_bucket_store_empty_postings_total counter
        	cortex_bucket_store_empty_postings_total 112595
			# HELP cortex_bucket_store_cached_postings_fetch_duration_seconds Time it takes to fetch postings to respond a request sent to store-gateway. It includes both the time to fetch it from cache and from storage in case of cache misses.
			# TYPE cortex_bucket_store_cached_postings_fetch_duration_seconds histogram
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="0.001"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="0.01"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="0.1"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="0.3"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="0.6"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="1"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="3"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="6"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="9"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="20"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="30"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="60"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="90"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="120"} 0
			cortex_bucket_store_cached_postings_fetch_duration_seconds_bucket{le="+Inf"} 3
			cortex_bucket_store_cached_postings_fetch_duration_seconds_sum 1.328621e+06
			cortex_bucket_store_cached_postings_fetch_duration_seconds_count 3

			# HELP cortex_bucket_store_indexheader_lazy_load_duration_seconds Duration of the index-header lazy loading in seconds.
			# TYPE cortex_bucket_store_indexheader_lazy_load_duration_seconds histogram
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.01"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.02"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.05"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.1"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.2"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.5"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="1"} 3
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="2"} 3
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="5"} 3
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="+Inf"} 3
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_sum 1.9500000000000002
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_count 3

			# HELP cortex_bucket_store_indexheader_lazy_load_failed_total Total number of failed index-header lazy load operations.
			# TYPE cortex_bucket_store_indexheader_lazy_load_failed_total counter
			cortex_bucket_store_indexheader_lazy_load_failed_total 1.373659e+06

			# HELP cortex_bucket_store_indexheader_lazy_load_total Total number of index-header lazy load operations.
			# TYPE cortex_bucket_store_indexheader_lazy_load_total counter
			cortex_bucket_store_indexheader_lazy_load_total 1.35114e+06

			# HELP cortex_bucket_store_indexheader_lazy_unload_failed_total Total number of failed index-header lazy unload operations.
			# TYPE cortex_bucket_store_indexheader_lazy_unload_failed_total counter
			cortex_bucket_store_indexheader_lazy_unload_failed_total 1.418697e+06

			# HELP cortex_bucket_store_indexheader_lazy_unload_total Total number of index-header lazy unload operations.
			# TYPE cortex_bucket_store_indexheader_lazy_unload_total counter
			cortex_bucket_store_indexheader_lazy_unload_total 1.396178e+06
        	# HELP cortex_bucket_store_postings_size_bytes Size in bytes of the postings for a single series call.
        	# TYPE cortex_bucket_store_postings_size_bytes histogram
        	cortex_bucket_store_postings_size_bytes_bucket{le="32"} 0
        	cortex_bucket_store_postings_size_bytes_bucket{le="256"} 0
        	cortex_bucket_store_postings_size_bytes_bucket{le="512"} 0
        	cortex_bucket_store_postings_size_bytes_bucket{le="1024"} 0
        	cortex_bucket_store_postings_size_bytes_bucket{le="32768"} 0
        	cortex_bucket_store_postings_size_bytes_bucket{le="262144"} 3
        	cortex_bucket_store_postings_size_bytes_bucket{le="524288"} 3
        	cortex_bucket_store_postings_size_bytes_bucket{le="1.048576e+06"} 3
        	cortex_bucket_store_postings_size_bytes_bucket{le="3.3554432e+07"} 3
        	cortex_bucket_store_postings_size_bytes_bucket{le="2.68435456e+08"} 3
        	cortex_bucket_store_postings_size_bytes_bucket{le="5.36870912e+08"} 3
        	cortex_bucket_store_postings_size_bytes_bucket{le="+Inf"} 3
        	cortex_bucket_store_postings_size_bytes_sum 225190
        	cortex_bucket_store_postings_size_bytes_count 3
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

	m.queriesDropped.WithLabelValues("chunks").Add(31 * base)
	m.queriesDropped.WithLabelValues("series").Add(0)

	m.postingsSizeBytes.Observe(10 * base)
	m.chunkSizeBytes.Observe(11 * base)

	m.seriesRefetches.Add(33 * base)

	m.cachedPostingsCompressions.WithLabelValues("encode").Add(50 * base)
	m.cachedPostingsCompressions.WithLabelValues("decode").Add(51 * base)

	m.cachedPostingsCompressionErrors.WithLabelValues("encode").Add(52 * base)
	m.cachedPostingsCompressionErrors.WithLabelValues("decode").Add(53 * base)

	m.cachedPostingsCompressionTimeSeconds.WithLabelValues("encode").Add(54 * base)
	m.cachedPostingsCompressionTimeSeconds.WithLabelValues("decode").Add(55 * base)

	m.cachedPostingsOriginalSizeBytes.Add(56 * base)
	m.cachedPostingsCompressedSizeBytes.Add(57 * base)

	m.seriesFetchDuration.Observe(58 * base)
	m.postingsFetchDuration.Observe(59 * base)

	m.indexHeaderLazyLoadCount.Add(60 * base)
	m.indexHeaderLazyLoadFailedCount.Add(61 * base)
	m.indexHeaderLazyUnloadCount.Add(62 * base)
	m.indexHeaderLazyUnloadFailedCount.Add(63 * base)
	m.indexHeaderLazyLoadDuration.Observe(0.65)

	m.emptyPostingCount.Add(5 * base)

	return reg
}

// copied from Thanos, pkg/store/bucket.go
type mockedBucketStoreMetrics struct {
	blocksLoaded          prometheus.Gauge
	blockLoads            prometheus.Counter
	blockLoadFailures     prometheus.Counter
	blockDrops            prometheus.Counter
	blockDropFailures     prometheus.Counter
	seriesDataTouched     *prometheus.HistogramVec
	seriesDataFetched     *prometheus.HistogramVec
	seriesDataSizeTouched *prometheus.HistogramVec
	seriesDataSizeFetched *prometheus.HistogramVec
	seriesBlocksQueried   prometheus.Histogram
	seriesGetAllDuration  prometheus.Histogram
	seriesMergeDuration   prometheus.Histogram
	seriesRefetches       prometheus.Counter
	resultSeriesCount     prometheus.Histogram
	chunkSizeBytes        prometheus.Histogram
	postingsSizeBytes     prometheus.Histogram
	queriesDropped        *prometheus.CounterVec
	emptyPostingCount     prometheus.Counter

	cachedPostingsCompressions           *prometheus.CounterVec
	cachedPostingsCompressionErrors      *prometheus.CounterVec
	cachedPostingsCompressionTimeSeconds *prometheus.CounterVec
	cachedPostingsOriginalSizeBytes      prometheus.Counter
	cachedPostingsCompressedSizeBytes    prometheus.Counter

	seriesFetchDuration   prometheus.Histogram
	postingsFetchDuration prometheus.Histogram

	indexHeaderLazyLoadCount         prometheus.Counter
	indexHeaderLazyLoadFailedCount   prometheus.Counter
	indexHeaderLazyUnloadCount       prometheus.Counter
	indexHeaderLazyUnloadFailedCount prometheus.Counter
	indexHeaderLazyLoadDuration      prometheus.Histogram
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

	m.seriesDataTouched = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_data_touched",
		Help:    "How many items of a data type in a block were touched for a single series request.",
		Buckets: prometheus.ExponentialBuckets(200, 2, 15),
	}, []string{"data_type"})
	m.seriesDataFetched = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_data_fetched",
		Help:    "How many items of a data type in a block were fetched for a single series request.",
		Buckets: prometheus.ExponentialBuckets(200, 2, 15),
	}, []string{"data_type"})

	m.seriesDataSizeTouched = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_data_size_touched_bytes",
		Help:    "Size of all items of a data type in a block were touched for a single series request.",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 15),
	}, []string{"data_type"})
	m.seriesDataSizeFetched = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_data_size_fetched_bytes",
		Help:    "Size of all items of a data type in a block were fetched for a single series request.",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 15),
	}, []string{"data_type"})

	m.seriesBlocksQueried = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_blocks_queried",
		Help:    "Number of blocks in a bucket store that were touched to satisfy a query.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
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
	m.resultSeriesCount = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_result_series",
		Help:    "Number of series observed in the final result of a query.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 15),
	})

	m.chunkSizeBytes = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_sent_chunk_size_bytes",
		Help: "Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	})

	m.postingsSizeBytes = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_postings_size_bytes",
		Help: "Size in bytes of the postings for a single series call.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	})

	m.queriesDropped = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_queries_dropped_total",
		Help: "Number of queries that were dropped due to the limit.",
	}, []string{"reason"})
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
		Name: "thanos_bucket_store_cached_postings_compression_time_seconds_total",
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

	m.seriesFetchDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_cached_series_fetch_duration_seconds",
		Help:    "Time it takes to fetch series from a bucket to respond a query. It also includes the time it takes to cache fetch and store operations.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.postingsFetchDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_cached_postings_fetch_duration_seconds",
		Help:    "Time it takes to fetch postings from a bucket to respond a query. It also includes the time it takes to cache fetch and store operations.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})

	m.indexHeaderLazyLoadCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_indexheader_lazy_load_total",
		Help: "Total number of index-header lazy load operations.",
	})
	m.indexHeaderLazyLoadFailedCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_indexheader_lazy_load_failed_total",
		Help: "Total number of failed index-header lazy load operations.",
	})
	m.indexHeaderLazyUnloadCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_indexheader_lazy_unload_total",
		Help: "Total number of index-header lazy unload operations.",
	})
	m.indexHeaderLazyUnloadFailedCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_indexheader_lazy_unload_failed_total",
		Help: "Total number of failed index-header lazy unload operations.",
	})
	m.indexHeaderLazyLoadDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_indexheader_lazy_load_duration_seconds",
		Help:    "Duration of the index-header lazy loading in seconds.",
		Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5},
	})

	m.emptyPostingCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_empty_postings_total",
		Help: "Total number of empty postings when fetching block series.",
	})

	return &m
}
