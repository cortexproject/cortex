// +build requires_docker

package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestQuerierWithBlocksStorageRunningInMicroservicesMode(t *testing.T) {
	tests := map[string]struct {
		blocksShardingEnabled    bool
		ingesterStreamingEnabled bool
		indexCacheBackend        string
	}{
		"blocks sharding enabled, ingester gRPC streaming disabled, inmemory index cache": {
			blocksShardingEnabled:    true,
			ingesterStreamingEnabled: false,
			indexCacheBackend:        tsdb.IndexCacheBackendInMemory,
		},
		"blocks sharding enabled, ingester gRPC streaming enabled, inmemory index cache": {
			blocksShardingEnabled:    true,
			ingesterStreamingEnabled: true,
			indexCacheBackend:        tsdb.IndexCacheBackendInMemory,
		},
		"blocks sharding disabled, ingester gRPC streaming disabled, memcached index cache": {
			blocksShardingEnabled:    false,
			ingesterStreamingEnabled: false,
			// Memcached index cache is required to avoid flaky tests when the blocks sharding is disabled
			// because two different requests may hit two different store-gateways, so if the cache is not
			// shared there's no guarantee we'll have a cache hit.
			indexCacheBackend: tsdb.IndexCacheBackendMemcached,
		},
		"blocks sharding enabled, ingester gRPC streaming enabled, memcached index cache": {
			blocksShardingEnabled:    true,
			ingesterStreamingEnabled: true,
			indexCacheBackend:        tsdb.IndexCacheBackendMemcached,
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
			flags := mergeFlags(BlocksStorageFlags, map[string]string{
				"-experimental.blocks-storage.tsdb.block-ranges-period":         blockRangePeriod.String(),
				"-experimental.blocks-storage.tsdb.ship-interval":               "1s",
				"-experimental.blocks-storage.bucket-store.sync-interval":       "1s",
				"-experimental.blocks-storage.tsdb.retention-period":            ((blockRangePeriod * 2) - 1).String(),
				"-experimental.blocks-storage.bucket-store.index-cache.backend": testCfg.indexCacheBackend,
				"-experimental.store-gateway.sharding-enabled":                  strconv.FormatBool(testCfg.blocksShardingEnabled),
				"-querier.ingester-streaming":                                   strconv.FormatBool(testCfg.ingesterStreamingEnabled),
			})

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-experimental.blocks-storage.s3.bucket-name"])
			memcached := e2ecache.NewMemcached()
			require.NoError(t, s.StartAndWaitReady(consul, minio, memcached))

			// Add the memcached address to the flags.
			flags["-experimental.blocks-storage.bucket-store.index-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)

			// Start Cortex components.
			distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
			ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, "")
			storeGateway1 := e2ecortex.NewStoreGateway("store-gateway-1", consul.NetworkHTTPEndpoint(), flags, "")
			storeGateway2 := e2ecortex.NewStoreGateway("store-gateway-2", consul.NetworkHTTPEndpoint(), flags, "")
			storeGateways := e2ecortex.NewCompositeCortexService(storeGateway1, storeGateway2)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway1, storeGateway2))

			// Start the querier with configuring store-gateway addresses if sharding is disabled.
			if !testCfg.blocksShardingEnabled {
				flags = mergeFlags(flags, map[string]string{
					"-experimental.querier.store-gateway-addresses": strings.Join([]string{storeGateway1.NetworkGRPCEndpoint(), storeGateway2.NetworkGRPCEndpoint()}, ","),
				})
			}
			querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(querier))

			// Wait until both the distributor and querier have updated the ring. The querier will also watch
			// the store-gateway ring if blocks sharding is enabled.
			require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
			if testCfg.blocksShardingEnabled {
				require.NoError(t, querier.WaitSumMetrics(e2e.Equals(float64(512+(512*storeGateways.NumInstances()))), "cortex_ring_tokens_total"))
			} else {
				require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
			}

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

			// Wait until the querier has discovered the uploaded blocks.
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2), "cortex_blocks_meta_synced"))

			// Wait until the store-gateway has synched the new uploaded blocks. When sharding is enabled
			// we don't known which store-gateway instance will synch the blocks, so we need to wait on
			// metrics extracted from all instances.
			if testCfg.blocksShardingEnabled {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2), "cortex_bucket_store_blocks_loaded"))
			} else {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(2*storeGateways.NumInstances())), "cortex_bucket_store_blocks_loaded"))
			}

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

			// Check the in-memory index cache metrics (in the store-gateway).
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(7), "thanos_store_index_cache_requests_total"))
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(0), "thanos_store_index_cache_hits_total")) // no cache hit cause the cache was empty

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2*2), "thanos_store_index_cache_items"))             // 2 series both for postings and series cache
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2*2), "thanos_store_index_cache_items_added_total")) // 2 series both for postings and series cache
			} else if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(11), "thanos_memcached_operations_total")) // 7 gets + 4 sets
			}

			// Query back again the 1st series from storage. This time it should use the index cache.
			result, err = c.Query("series_1", series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))

			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(7+2), "thanos_store_index_cache_requests_total"))
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2), "thanos_store_index_cache_hits_total")) // this time has used the index cache

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2*2), "thanos_store_index_cache_items"))             // as before
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2*2), "thanos_store_index_cache_items_added_total")) // as before
			} else if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(11+2), "thanos_memcached_operations_total")) // as before + 2 gets
			}

			// Ensure no service-specific metrics prefix is used by the wrong service.
			assertServiceMetricsPrefixes(t, Distributor, distributor)
			assertServiceMetricsPrefixes(t, Ingester, ingester)
			assertServiceMetricsPrefixes(t, Querier, querier)
			assertServiceMetricsPrefixes(t, StoreGateway, storeGateway1)
			assertServiceMetricsPrefixes(t, StoreGateway, storeGateway2)
		})
	}
}

func TestQuerierWithBlocksStorageRunningInSingleBinaryMode(t *testing.T) {
	tests := map[string]struct {
		blocksShardingEnabled    bool
		ingesterStreamingEnabled bool
		indexCacheBackend        string
	}{
		"blocks sharding enabled, ingester gRPC streaming disabled, inmemory index cache": {
			blocksShardingEnabled:    true,
			ingesterStreamingEnabled: false,
			indexCacheBackend:        tsdb.IndexCacheBackendInMemory,
		},
		"blocks sharding enabled, ingester gRPC streaming enabled, inmemory index cache": {
			blocksShardingEnabled:    true,
			ingesterStreamingEnabled: true,
			indexCacheBackend:        tsdb.IndexCacheBackendInMemory,
		},
		"blocks sharding disabled, ingester gRPC streaming disabled, memcached index cache": {
			blocksShardingEnabled:    false,
			ingesterStreamingEnabled: false,
			// Memcached index cache is required to avoid flaky tests when the blocks sharding is disabled
			// because two different requests may hit two different store-gateways, so if the cache is not
			// shared there's no guarantee we'll have a cache hit.
			indexCacheBackend: tsdb.IndexCacheBackendMemcached,
		},
		"blocks sharding enabled, ingester gRPC streaming enabled, memcached index cache": {
			blocksShardingEnabled:    true,
			ingesterStreamingEnabled: true,
			indexCacheBackend:        tsdb.IndexCacheBackendMemcached,
		},
	}

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			const blockRangePeriod = 5 * time.Second

			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, bucketName)
			memcached := e2ecache.NewMemcached()
			require.NoError(t, s.StartAndWaitReady(consul, minio, memcached))

			// Setting the replication factor equal to the number of Cortex replicas
			// make sure each replica creates the same blocks, so the total number of
			// blocks is stable and easy to assert on.
			const seriesReplicationFactor = 2

			// Configure the blocks storage to frequently compact TSDB head
			// and ship blocks to the storage.
			flags := mergeFlags(BlocksStorageFlags, map[string]string{
				"-experimental.blocks-storage.tsdb.block-ranges-period":                     blockRangePeriod.String(),
				"-experimental.blocks-storage.tsdb.ship-interval":                           "1s",
				"-experimental.blocks-storage.bucket-store.sync-interval":                   "1s",
				"-experimental.blocks-storage.tsdb.retention-period":                        ((blockRangePeriod * 2) - 1).String(),
				"-experimental.blocks-storage.bucket-store.index-cache.backend":             testCfg.indexCacheBackend,
				"-experimental.blocks-storage.bucket-store.index-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
				"-querier.ingester-streaming":                                               strconv.FormatBool(testCfg.ingesterStreamingEnabled),
				// Ingester.
				"-ring.store":      "consul",
				"-consul.hostname": consul.NetworkHTTPEndpoint(),
				// Distributor.
				"-distributor.replication-factor": strconv.FormatInt(seriesReplicationFactor, 10),
				// Store-gateway.
				"-experimental.store-gateway.sharding-enabled":              strconv.FormatBool(testCfg.blocksShardingEnabled),
				"-experimental.store-gateway.sharding-ring.store":           "consul",
				"-experimental.store-gateway.sharding-ring.consul.hostname": consul.NetworkHTTPEndpoint(),
				"-experimental.store-gateway.replication-factor":            "1",
			})

			// Start Cortex replicas.
			cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags, "")
			cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags, "")
			cluster := e2ecortex.NewCompositeCortexService(cortex1, cortex2)
			require.NoError(t, s.StartAndWaitReady(cortex1, cortex2))

			// Wait until Cortex replicas have updated the ring state.
			for _, replica := range cluster.Instances() {
				numTokensPerInstance := 512 // Ingesters ring.
				if testCfg.blocksShardingEnabled {
					numTokensPerInstance += 512 * 2 // Store-gateway ring (read both by the querier and store-gateway).
				}

				require.NoError(t, replica.WaitSumMetrics(e2e.Equals(float64(numTokensPerInstance*cluster.NumInstances())), "cortex_ring_tokens_total"))
			}

			c, err := e2ecortex.NewClient(cortex1.HTTPEndpoint(), cortex2.HTTPEndpoint(), "", "", "user-1")
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
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series_removed_total"))

			// Push another series to further compact another block and delete the first block
			// due to expired retention.
			series3Timestamp := series2Timestamp.Add(blockRangePeriod * 2)
			series3, expectedVector3 := generateSeries("series_3", series3Timestamp)

			res, err = c.Push(series3)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(3*cluster.NumInstances())), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_memory_series_removed_total"))

			// Wait until the querier has discovered the uploaded blocks (discovered both by the querier and store-gateway).
			require.NoError(t, cluster.WaitSumMetricsWithOptions(e2e.Equals(float64(2*cluster.NumInstances()*2)), []string{"cortex_blocks_meta_synced"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "component", "querier"))))

			// Wait until the store-gateway has synched the new uploaded blocks.
			const shippedBlocks = 2

			if testCfg.blocksShardingEnabled {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(shippedBlocks*seriesReplicationFactor)), "cortex_bucket_store_blocks_loaded"))
			} else {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(shippedBlocks*seriesReplicationFactor*cluster.NumInstances())), "cortex_bucket_store_blocks_loaded"))
			}

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

			// Check the in-memory index cache metrics (in the store-gateway).
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(7*seriesReplicationFactor)), "thanos_store_index_cache_requests_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(0), "thanos_store_index_cache_hits_total")) // no cache hit cause the cache was empty

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*2*seriesReplicationFactor)), "thanos_store_index_cache_items"))             // 2 series both for postings and series cache
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*2*seriesReplicationFactor)), "thanos_store_index_cache_items_added_total")) // 2 series both for postings and series cache
			} else if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(11*seriesReplicationFactor)), "thanos_memcached_operations_total")) // 7 gets + 4 sets
			}

			// Query back again the 1st series from storage. This time it should use the index cache.
			result, err = c.Query("series_1", series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))

			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((7+2)*seriesReplicationFactor)), "thanos_store_index_cache_requests_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*seriesReplicationFactor)), "thanos_store_index_cache_hits_total")) // this time has used the index cache

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*2*seriesReplicationFactor)), "thanos_store_index_cache_items"))             // as before
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*2*seriesReplicationFactor)), "thanos_store_index_cache_items_added_total")) // as before
			} else if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((11+2)*seriesReplicationFactor)), "thanos_memcached_operations_total")) // as before + 2 gets
			}
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
		"-experimental.blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-experimental.blocks-storage.tsdb.ship-interval":       "1s",
		"-experimental.blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-experimental.blocks-storage.s3.bucket-name"])
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

	// Start the querier and store-gateway, and configure them to not frequently sync blocks.
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-experimental.blocks-storage.bucket-store.sync-interval": "1m",
	}), "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-experimental.blocks-storage.bucket-store.sync-interval": "1m",
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

	// Wait until the querier and store-gateway have updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Query back the series.
	c, err = e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	result, err := c.Query("series_1", series1Timestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector1, result.(model.Vector))

	// Delete all blocks from the storage.
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-experimental.blocks-storage.s3.bucket-name"])
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

		if userID == 0 { // No need to repeat this test for each user.
			res, body, err := c.QueryRaw("{instance=~\"hello.*\"}")
			require.NoError(t, err)
			require.Equal(t, 422, res.StatusCode)
			require.Contains(t, string(body), "query must contain metric name")
		}

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
