// +build requires_docker

package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestQuerierWithBlocksStorage(t *testing.T) {
	tests := map[string]struct {
		flags map[string]string
	}{
		"querier running with ingester gRPC streaming disabled and inmemory index cache": {
			flags: mergeFlags(BlocksStorageFlags, map[string]string{
				"-querier.ingester-streaming":                         "false",
				"-experimental.tsdb.bucket-store.index-cache.backend": "inmemory",
			}),
		},
		"querier running with ingester gRPC streaming enabled and inmemory index cache": {
			flags: mergeFlags(BlocksStorageFlags, map[string]string{
				"-querier.ingester-streaming":                         "true",
				"-experimental.tsdb.bucket-store.index-cache.backend": "inmemory",
			}),
		},
		"querier running with memcached index cache": {
			flags: mergeFlags(BlocksStorageFlags, map[string]string{
				// The address will be inject during the test execution because it's dynamic.
				"-experimental.tsdb.bucket-store.index-cache.backend": "memcached",
			}),
		},
	}

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			const blockRangePeriod = 5 * time.Second

			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Configure the blocks storage to frequently compact TSDB head
			// and ship blocks to the storage.
			flags := mergeFlags(testCfg.flags, map[string]string{
				"-experimental.tsdb.block-ranges-period":        blockRangePeriod.String(),
				"-experimental.tsdb.ship-interval":              "1s",
				"-experimental.tsdb.bucket-store.sync-interval": "1s",
				"-experimental.tsdb.retention-period":           ((blockRangePeriod * 2) - 1).String(),
			})

			// Detect the index cache backend from flags.
			indexCacheBackend := tsdb.IndexCacheBackendDefault
			if flags["-experimental.tsdb.bucket-store.index-cache.backend"] != "" {
				indexCacheBackend = flags["-experimental.tsdb.bucket-store.index-cache.backend"]
			}

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-experimental.tsdb.s3.bucket-name"])
			memcached := e2ecache.NewMemcached()
			require.NoError(t, s.StartAndWaitReady(consul, minio, memcached))

			// Add the memcached address to the flags.
			flags["-experimental.tsdb.bucket-store.index-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)

			// Start Cortex components.
			distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
			ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, "")
			querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

			// Wait until both the distributor and querier have updated the ring.
			require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

			c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
			require.NoError(t, err)

			// Push some series to Cortex.
			series1Timestamp := time.Now()
			series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
			series1, expectedVector1 := generateSeries("series_1", series1Timestamp)
			series2, expectedVector2 := generateSeries("series_2", series2Timestamp)

			res, err := c.Push(series1)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			res, err = c.Push(series2)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			// Wait until the TSDB head is compacted and shipped to the storage.
			// The shipped block contains the 1st series, while the 2ns series in in the head.
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))

			// Push another series to further compact another block and delete the first block
			// due to expired retention.
			series3Timestamp := series2Timestamp.Add(blockRangePeriod * 2)
			series3, expectedVector3 := generateSeries("series_3", series3Timestamp)

			res, err = c.Push(series3)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(3), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_removed_total"))

			// Wait until the querier has synched the new uploaded blocks.
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2), "cortex_querier_bucket_store_blocks_loaded"))

			// Query back the series (1 only in the storage, 1 only in the ingesters, 1 on both).
			result, err := c.Query("series_1", series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))

			result, err = c.Query("series_2", series2Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector2, result.(model.Vector))

			result, err = c.Query("series_3", series3Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector3, result.(model.Vector))

			// Check the in-memory index cache metrics (in the querier).
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(7), "cortex_querier_blocks_index_cache_requests_total"))
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(0), "cortex_querier_blocks_index_cache_hits_total")) // no cache hit cause the cache was empty

			if indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*2), "cortex_querier_blocks_index_cache_items"))             // 2 series both for postings and series cache
				require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*2), "cortex_querier_blocks_index_cache_items_added_total")) // 2 series both for postings and series cache
			} else if indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, querier.WaitSumMetrics(e2e.Equals(11), "cortex_querier_blocks_index_cache_memcached_operations_total")) // 7 gets + 4 sets
			}

			// Query back again the 1st series from storage. This time it should use the index cache.
			result, err = c.Query("series_1", series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))

			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(7+2), "cortex_querier_blocks_index_cache_requests_total"))
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2), "cortex_querier_blocks_index_cache_hits_total")) // this time has used the index cache

			if indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*2), "cortex_querier_blocks_index_cache_items"))             // as before
				require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*2), "cortex_querier_blocks_index_cache_items_added_total")) // as before
			} else if indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, querier.WaitSumMetrics(e2e.Equals(11+2), "cortex_querier_blocks_index_cache_memcached_operations_total")) // as before + 2 gets
			}

			// Ensure no service-specific metrics prefix is used by the wrong service.
			assertServiceMetricsPrefixes(t, Distributor, distributor)
			assertServiceMetricsPrefixes(t, Ingester, ingester)
			assertServiceMetricsPrefixes(t, Querier, querier)
		})
	}
}

func TestQuerierWithBlocksStorageOnMissingBlocksFromStorage(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags, map[string]string{
		"-experimental.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-experimental.tsdb.ship-interval":       "1s",
		"-experimental.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-experimental.tsdb.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push some series to Cortex.
	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	series1Timestamp := time.Now()
	series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series1, expectedVector1 := generateSeries("series_1", series1Timestamp)
	series2, _ := generateSeries("series_2", series2Timestamp)

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is compacted and shipped to the storage.
	// The shipped block contains the 1st series, while the 2ns series in in the head.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))

	// Start the querier and configure it to not frequently sync blocks.
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-experimental.tsdb.bucket-store.sync-interval": "1m",
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Query back the series.
	c, err = e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	result, err := c.Query("series_1", series1Timestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector1, result.(model.Vector))

	// Delete all blocks from the storage.
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-experimental.tsdb.s3.bucket-name"])
	require.NoError(t, err)
	require.NoError(t, storage.DeleteBlocks("user-1"))

	// Query back again the series. Now we do expect a 500 error because the blocks are
	// missing from the storage.
	_, err = c.Query("series_1", series1Timestamp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestQuerierWithChunksStorage(t *testing.T) {
	const numUsers = 10
	const numQueriesPerUser = 10

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))
	flags := mergeFlags(ChunksStorageFlags, map[string]string{})

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, dynamo))

	tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorageFlags, "")
	require.NoError(t, s.StartAndWaitReady(tableManager))

	// Wait until the first table-manager sync has completed, so that we're
	// sure the tables have been created.
	require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Cortex.
	now := time.Now()
	expectedVectors := make([]model.Vector, numUsers)

	for u := 0; u < numUsers; u++ {
		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", fmt.Sprintf("user-%d", u))
		require.NoError(t, err)

		var series []prompb.TimeSeries
		series, expectedVectors[u] = generateSeries("series_1", now)

		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// Add 2 memcache instances to test for: https://github.com/cortexproject/cortex/issues/2302
	// Note these are not running but added to trigger the behaviour.
	querierFlags := mergeFlags(flags, map[string]string{
		"-store.index-cache-read.memcached.addresses":  "dns+memcached0:11211",
		"-store.index-cache-write.memcached.addresses": "dns+memcached1:11211",
	})

	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), querierFlags, "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Query the series for each user in parallel.
	wg := sync.WaitGroup{}
	wg.Add(numUsers * numQueriesPerUser)

	for u := 0; u < numUsers; u++ {
		userID := u

		c, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", fmt.Sprintf("user-%d", userID))
		require.NoError(t, err)

		for q := 0; q < numQueriesPerUser; q++ {
			go func() {
				defer wg.Done()

				result, err := c.Query("series_1", now)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVectors[userID], result.(model.Vector))
			}()
		}
	}

	wg.Wait()

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
	assertServiceMetricsPrefixes(t, TableManager, tableManager)
}
