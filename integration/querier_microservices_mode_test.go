//go:build integration_querier_microservices_mode

package integration

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
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
		bucketStorageType      string
		blocksShardingStrategy string // Empty means sharding is disabled.
		tenantShardSize        int
		indexCacheBackend      string
		chunkCacheBackend      string
		parquetLabelsCache     string
		bucketIndexEnabled     bool
	}{
		// tsdb bucket storage
		"[TSDB] blocks sharding disabled, memcached index cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "",
			indexCacheBackend:      tsdb.IndexCacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"[TSDB] blocks sharding disabled, multilevel index cache (inmemory, memcached)": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "",
			indexCacheBackend:      fmt.Sprintf("%v,%v", tsdb.IndexCacheBackendInMemory, tsdb.IndexCacheBackendMemcached),
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"[TSDB] blocks sharding disabled, redis index cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
		},
		"[TSDB] blocks sharding disabled, multilevel index cache (inmemory, redis)": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "",
			indexCacheBackend:      fmt.Sprintf("%v,%v", tsdb.IndexCacheBackendInMemory, tsdb.IndexCacheBackendRedis),
			chunkCacheBackend:      tsdb.CacheBackendRedis,
		},
		"[TSDB] blocks default sharding, inmemory index cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendInMemory,
		},
		"[TSDB] blocks default sharding, memcached index cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"[TSDB] blocks shuffle sharding, memcached index cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"[TSDB] blocks default sharding, inmemory index cache, bucket index enabled": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[TSDB] blocks shuffle sharding, memcached index cache, bucket index enabled": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[TSDB] blocks default sharding, redis index cache, bucket index enabled": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
			bucketIndexEnabled:     true,
		},
		"[TSDB] blocks shuffle sharding, redis index cache, bucket index enabled": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
			bucketIndexEnabled:     true,
		},
		"[TSDB] blocks sharding disabled, in-memory chunk cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[TSDB] blocks default sharding, in-memory chunk cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[TSDB] blocks shuffle sharding, in-memory chunk cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[TSDB] block sharding disabled, multi-level chunk cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		"[TSDB] block default sharding, multi-level chunk cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		"[TSDB] block shuffle sharding, multi-level chunk cache": {
			bucketStorageType:      "tsdb",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		//parquet bucket storage
		"[Parquet] blocks sharding disabled, memcached parquet label cache, memcached chunks cache": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "",
			parquetLabelsCache:     tsdb.CacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"[Parquet] blocks sharding disabled, multilevel parquet label cache (inmemory, memcached)": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "",
			parquetLabelsCache:     fmt.Sprintf("%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached),
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"[Parquet] blocks sharding disabled, redis parquet label cache, redis chunks cache": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
		},
		"[Parquet] blocks sharding disabled, multilevel parquet label cache cache (inmemory, redis), redis chunks cache": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "",
			parquetLabelsCache:     fmt.Sprintf("%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendRedis),
			chunkCacheBackend:      tsdb.CacheBackendRedis,
		},
		"[Parquet] blocks default sharding, inmemory parquet label cache": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendInMemory,
		},
		"[Parquet] blocks default sharding, memcached parquet label cache, memcached chunks cache": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"[Parquet] blocks shuffle sharding, memcached parquet label cache, memcached chunks cache": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"[Parquet] blocks default sharding, inmemory parquet label cache, bucket index enabled": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[Parquet] blocks shuffle sharding, memcached parquet label cache, bucket index enabled": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[Parquet] blocks default sharding, redis parquet label cache, redis chunks cache, bucket index enabled": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
			bucketIndexEnabled:     true,
		},
		"[Parquet] blocks shuffle sharding, redis parquet label cache, redis chunks cache, bucket index enabled": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
			bucketIndexEnabled:     true,
		},
		"[Parquet] blocks sharding disabled, redis parquet label cache, in-memory chunks cache, bucket index enabled": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[Parquet] blocks default sharding, redis parquet label cache, in-memory chunk cache": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[Parquet] blocks shuffle sharding, redis parquet label cache, in-memory chunk cache, bucket index enabled": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"[Parquet] block sharding disabled, redis parquet label cache, multi-level chunk cache (in-memory, memcached, redis), bucket index enabled": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		"[Parquet] block default sharding, redis parquet label cache, multi-level chunk cache (in-memory, memcached, redis), bucket index enabled": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		"[Parquet] block shuffle sharding, redis parquet label cache, multi-level chunk cache ((in-memory, memcached, redis), bucket index enabled)": {
			bucketStorageType:      "parquet",
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
	}

	for testName, testCfg := range tests {
		for _, thanosEngine := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s,thanosEngine=%t", testName, thanosEngine), func(t *testing.T) {
				const blockRangePeriod = 5 * time.Second

				s, err := e2e.NewScenario(networkName)
				require.NoError(t, err)
				defer s.Close()

				numberOfCacheBackends := len(strings.Split(testCfg.indexCacheBackend, ","))

				// Configure the blocks storage to frequently compact TSDB head
				// and ship blocks to the storage.
				flags := mergeFlags(BlocksStorageFlags(), map[string]string{
					"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
					"-blocks-storage.tsdb.ship-interval":                "1s",
					"-blocks-storage.bucket-store.sync-interval":        "1s",
					"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
					"-blocks-storage.bucket-store.chunks-cache.backend": testCfg.chunkCacheBackend,
					"-store-gateway.sharding-enabled":                   strconv.FormatBool(testCfg.blocksShardingStrategy != ""),
					"-store-gateway.sharding-strategy":                  testCfg.blocksShardingStrategy,
					"-store-gateway.tenant-shard-size":                  fmt.Sprintf("%d", testCfg.tenantShardSize),
					"-querier.thanos-engine":                            strconv.FormatBool(thanosEngine),
					"-blocks-storage.bucket-store.bucket-index.enabled": strconv.FormatBool(testCfg.bucketIndexEnabled),
					"-blocks-storage.bucket-store.bucket-store-type":    testCfg.bucketStorageType,
				})

				// Start dependencies.
				consul := e2edb.NewConsul()
				minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
				memcached := e2ecache.NewMemcached()
				redis := e2ecache.NewRedis()
				require.NoError(t, s.StartAndWaitReady(consul, minio, memcached, redis))
				switch testCfg.bucketStorageType {
				case "tsdb":
					flags["-blocks-storage.bucket-store.index-cache.backend"] = testCfg.indexCacheBackend
				case "parquet":
					flags["-parquet-converter.enabled"] = "true"
					flags["-parquet-converter.conversion-interval"] = "1s"
					flags["-parquet-converter.ring.consul.hostname"] = consul.NetworkHTTPEndpoint()
					flags["-compactor.block-ranges"] = "1ms,12h" // to convert all blocks to parquet blocks
					flags["-blocks-storage.bucket-store.parquet-labels-cache.backend"] = testCfg.parquetLabelsCache
				}

				// Add the cache address to the flags.
				if strings.Contains(testCfg.indexCacheBackend, tsdb.IndexCacheBackendMemcached) {
					flags["-blocks-storage.bucket-store.index-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
				}
				if strings.Contains(testCfg.indexCacheBackend, tsdb.IndexCacheBackendRedis) {
					flags["-blocks-storage.bucket-store.index-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
				}
				if strings.Contains(testCfg.chunkCacheBackend, tsdb.CacheBackendMemcached) {
					flags["-blocks-storage.bucket-store.chunks-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
				}
				if strings.Contains(testCfg.chunkCacheBackend, tsdb.CacheBackendRedis) {
					flags["-blocks-storage.bucket-store.chunks-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
				}
				if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendMemcached) {
					flags["-blocks-storage.bucket-store.parquet-labels-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
				}
				if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendRedis) {
					flags["-blocks-storage.bucket-store.parquet-labels-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
				}

				// Start Cortex components.
				distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
				ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
				storeGateway1 := e2ecortex.NewStoreGateway("store-gateway-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
				storeGateway2 := e2ecortex.NewStoreGateway("store-gateway-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
				storeGateways := e2ecortex.NewCompositeCortexService(storeGateway1, storeGateway2)
				require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway1, storeGateway2))

				var parquetConverter *e2ecortex.CortexService
				if testCfg.bucketStorageType == "parquet" {
					// start parquet converter
					parquetConverter = e2ecortex.NewParquetConverter("parquet-converter", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
					require.NoError(t, s.StartAndWaitReady(parquetConverter))
				}

				// Start the querier with configuring store-gateway addresses if sharding is disabled.
				if testCfg.blocksShardingStrategy == "" {
					flags = mergeFlags(flags, map[string]string{
						"-querier.store-gateway-addresses": strings.Join([]string{storeGateway1.NetworkGRPCEndpoint(), storeGateway2.NetworkGRPCEndpoint()}, ","),
					})
				}
				querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
				require.NoError(t, s.StartAndWaitReady(querier))

				// Wait until both the distributor and querier have updated the ring. The querier will also watch
				// the store-gateway ring if blocks sharding is enabled.
				require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
				if testCfg.blocksShardingStrategy != "" {
					require.NoError(t, querier.WaitSumMetrics(e2e.Equals(float64(512+(512*storeGateways.NumInstances()))), "cortex_ring_tokens_total"))
				} else {
					require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
				}

				c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
				require.NoError(t, err)

				// Push some series to Cortex.
				series1Timestamp := time.Now()
				series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
				series1, expectedVector1 := generateSeries("series_1", series1Timestamp, prompb.Label{Name: "series_1", Value: "series_1"})
				series2, expectedVector2 := generateSeries("series_2", series2Timestamp, prompb.Label{Name: "series_2", Value: "series_2"})

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
				series3, expectedVector3 := generateSeries("series_3", series3Timestamp, prompb.Label{Name: "series_3", Value: "series_3"})

				res, err = c.Push(series3)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)

				require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_shipper_uploads_total"))
				require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))
				require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(3), "cortex_ingester_memory_series_created_total"))
				require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_removed_total"))

				if testCfg.bucketIndexEnabled {
					// Start the compactor to have the bucket index created before querying.
					compactor := e2ecortex.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags, "")
					require.NoError(t, s.StartAndWaitReady(compactor))
				}

				switch testCfg.bucketStorageType {
				case "tsdb":
					// Wait until the store-gateway has synched the new uploaded blocks. When sharding is enabled
					// we don't known which store-gateway instance will synch the blocks, so we need to wait on
					// metrics extracted from all instances.
					if testCfg.blocksShardingStrategy != "" {
						// If shuffle sharding is enabled and we have tenant shard size set to 1,
						// then the metric only appears in one store gateway instance.
						require.NoError(t, storeGateways.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_bucket_store_blocks_loaded"}, e2e.SkipMissingMetrics))
					} else {
						require.NoError(t, storeGateways.WaitSumMetricsWithOptions(e2e.Equals(float64(2*storeGateways.NumInstances())), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))
					}

					// Check how many tenants have been discovered and synced by store-gateways.
					require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1*storeGateways.NumInstances())), "cortex_bucket_stores_tenants_discovered"))
					if testCfg.blocksShardingStrategy == "shuffle-sharding" {
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1)), "cortex_bucket_stores_tenants_synced"))
					} else {
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1*storeGateways.NumInstances())), "cortex_bucket_stores_tenants_synced"))
					}

					if !testCfg.bucketIndexEnabled {
						// Wait until the querier has discovered the uploaded blocks.
						require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics))
					}
				case "parquet":
					// Wait until the parquet-converter convert blocks
					require.NoError(t, parquetConverter.WaitSumMetricsWithOptions(e2e.Equals(float64(2)), []string{"cortex_parquet_converter_blocks_converted_total"}, e2e.WaitMissingMetrics))
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

				if testCfg.bucketStorageType == "tsdb" {
					// Check the in-memory index cache metrics (in the store-gateway).
					require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64((5+5+2)*numberOfCacheBackends)), "thanos_store_index_cache_requests_total"))
					require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(0), "thanos_store_index_cache_hits_total")) // no cache hit cause the cache was empty
				}

				// Query back again the 1st series from storage. This time it should use the index cache.
				result, err = c.Query("series_1", series1Timestamp)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVector1, result.(model.Vector))

				switch testCfg.bucketStorageType {
				case "tsdb":
					if numberOfCacheBackends > 1 {
						// 6 requests for Expanded Postings, 5 for Postings and 3 for Series.
						require.NoError(t, storeGateways.WaitSumMetricsWithOptions(e2e.Equals(float64(6+5+3)), []string{"thanos_store_index_cache_requests_total"}, e2e.WithLabelMatchers(
							labels.MustNewMatcher(labels.MatchEqual, "level", "L0"),
						)))
						// In case of L0 cache hits, store gateway might send fewer requests. Should be within range 12 ~ 14.
						require.NoError(t, storeGateways.WaitSumMetricsWithOptions(e2e.EqualsAmong(float64(12), float64(14)), []string{"thanos_store_index_cache_requests_total"}, e2e.WithLabelMatchers(
							labels.MustNewMatcher(labels.MatchEqual, "level", "L1"),
						)))
						l1IndexCacheRequests, err := storeGateways.SumMetrics([]string{"thanos_store_index_cache_requests_total"}, e2e.WithLabelMatchers(
							labels.MustNewMatcher(labels.MatchEqual, "level", "L1"),
						))
						require.NoError(t, err)
						l0IndexCacheHits, err := storeGateways.SumMetrics([]string{"thanos_store_index_cache_hits_total"}, e2e.WithLabelMatchers(
							labels.MustNewMatcher(labels.MatchEqual, "level", "L0"),
						))
						require.NoError(t, err)
						// Make sure l1 cache requests + l0 cache hits is 14.
						require.Equal(t, float64(14), l1IndexCacheRequests[0]+l0IndexCacheHits[0])
					} else {
						// 6 requests for Expanded Postings, 5 for Postings and 3 for Series.
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(6+5+3)), "thanos_store_index_cache_requests_total"))
					}
					require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2), "thanos_store_index_cache_hits_total")) // this time has used the index cache
				case "parquet":
					if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendInMemory) {
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_cache_inmemory_requests_total"))
					}
					if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendMemcached) {
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_memcached_operations_total"))
					}
					if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendRedis) {
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_cache_redis_requests_total"))
					}

					if strings.Contains(testCfg.chunkCacheBackend, tsdb.CacheBackendInMemory) {
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_cache_inmemory_requests_total"))
					}
					if strings.Contains(testCfg.chunkCacheBackend, tsdb.CacheBackendMemcached) {
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_memcached_operations_total"))
					}
					if strings.Contains(testCfg.chunkCacheBackend, tsdb.CacheBackendRedis) {
						require.NoError(t, storeGateways.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_cache_redis_requests_total"))
					}

					// ensure parquet shard cache works
					require.NoError(t, storeGateways.WaitSumMetricsWithOptions(e2e.Greater(float64(0)), []string{"cortex_parquet_cache_hits_total"}, e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "component", "store-gateway"),
						labels.MustNewMatcher(labels.MatchEqual, "name", "parquet-shards")),
						e2e.SkipMissingMetrics), // one store gateway may not receive queries
					)
					require.NoError(t, storeGateways.WaitSumMetricsWithOptions(e2e.Greater(float64(0)), []string{"cortex_parquet_cache_item_count"}, e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "component", "store-gateway"),
						labels.MustNewMatcher(labels.MatchEqual, "name", "parquet-shards")),
						e2e.SkipMissingMetrics), // one store gateway may not receive queries
					)
					require.NoError(t, storeGateways.WaitSumMetricsWithOptions(e2e.Greater(float64(0)), []string{"cortex_parquet_cache_misses_total"}, e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "component", "store-gateway"),
						labels.MustNewMatcher(labels.MatchEqual, "name", "parquet-shards")),
						e2e.SkipMissingMetrics), // one store gateway may not receive queries
					)
				}

				// Query metadata.
				testMetadataQueriesWithBlocksStorage(t, c, series1[0], series2[0], series3[0], blockRangePeriod)

				// Ensure no service-specific metrics prefix is used by the wrong service.
				assertServiceMetricsPrefixes(t, Distributor, distributor)
				assertServiceMetricsPrefixes(t, Ingester, ingester)
				assertServiceMetricsPrefixes(t, Querier, querier)
				assertServiceMetricsPrefixes(t, StoreGateway, storeGateway1)
				assertServiceMetricsPrefixes(t, StoreGateway, storeGateway2)
			})
		}
	}
}
