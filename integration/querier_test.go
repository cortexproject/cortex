//go:build integration_querier

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/execution/parse"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/api"
	"github.com/cortexproject/cortex/pkg/util/backoff"
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
					"-querier.query-store-for-labels-enabled":           "true",
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

				if testCfg.bucketStorageType == "tedb" {
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

func TestQuerierWithBlocksStorageRunningInSingleBinaryMode(t *testing.T) {
	tests := map[string]struct {
		bucketStorageType     string
		blocksShardingEnabled bool
		indexCacheBackend     string
		parquetLabelsCache    string
		bucketIndexEnabled    bool
	}{
		// tsdb bucket storage
		"[TSDB] blocks sharding enabled, inmemory index cache": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendInMemory,
		},
		"[TSDB] blocks sharding disabled, memcached index cache": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: false,
			indexCacheBackend:     tsdb.IndexCacheBackendMemcached,
		},
		"[TSDB] blocks sharding enabled, memcached index cache": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendMemcached,
		},
		"[TSDB] blocks sharding enabled, memcached index cache, bucket index enabled": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendMemcached,
			bucketIndexEnabled:    true,
		},
		"[TSDB] blocks sharding disabled,redis index cache": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: false,
			indexCacheBackend:     tsdb.IndexCacheBackendRedis,
		},
		"[TSDB] blocks sharding enabled, redis index cache": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendRedis,
		},
		"[TSDB] blocks sharding enabled, redis index cache, bucket index enabled": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendRedis,
			bucketIndexEnabled:    true,
		},
		// parquet bucket storage
		"[Parquet] blocks sharding enabled, inmemory parquet labels cache": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: true,
			parquetLabelsCache:    tsdb.CacheBackendInMemory,
		},
		"[Parquet] blocks sharding disabled, memcached parquet labels cache": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: false,
			parquetLabelsCache:    tsdb.CacheBackendMemcached,
		},
		"[Parquet] blocks sharding enabled, memcached parquet labels cache": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: true,
			parquetLabelsCache:    tsdb.CacheBackendMemcached,
		},
		"[Parquet] blocks sharding enabled, memcached parquet labels cache, bucket index enabled": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: true,
			parquetLabelsCache:    tsdb.CacheBackendMemcached,
			bucketIndexEnabled:    true,
		},
		"[Parquet] blocks sharding disabled, redis parquet labels cache": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: false,
			parquetLabelsCache:    tsdb.CacheBackendRedis,
		},
		"[Parquet] blocks sharding enabled, redis parquet labels cache": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: true,
			parquetLabelsCache:    tsdb.CacheBackendRedis,
		},
		"[Parquet] blocks sharding enabled, redis parquet labels cache, bucket index enabled": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: true,
			parquetLabelsCache:    tsdb.CacheBackendRedis,
			bucketIndexEnabled:    true,
		},
	}

	for testName, testCfg := range tests {
		for _, thanosEngine := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s,thanosEngine=%t", testName, thanosEngine), func(t *testing.T) {
				// Skip Thanos engine tests on non-amd64 architectures due to timing-sensitive
				// cache request count assertions that vary across architectures.
				if runtime.GOARCH != "amd64" && thanosEngine {
					t.Skip("Skipping test: Thanos engine tests only run on amd64 due to timing-sensitive assertions")
				}

				const blockRangePeriod = 5 * time.Second

				s, err := e2e.NewScenario(networkName)
				require.NoError(t, err)
				defer s.Close()

				// Start dependencies.
				consul := e2edb.NewConsul()
				minio := e2edb.NewMinio(9000, bucketName)
				memcached := e2ecache.NewMemcached()
				redis := e2ecache.NewRedis()
				require.NoError(t, s.StartAndWaitReady(consul, minio, memcached, redis))

				// Setting the replication factor equal to the number of Cortex replicas
				// make sure each replica creates the same blocks, so the total number of
				// blocks is stable and easy to assert on.
				const seriesReplicationFactor = 2

				// Configure the blocks storage to frequently compact TSDB head
				// and ship blocks to the storage.
				flags := mergeFlags(
					BlocksStorageFlags(),
					AlertmanagerLocalFlags(),
					map[string]string{
						"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
						"-blocks-storage.tsdb.ship-interval":                "1s",
						"-blocks-storage.bucket-store.sync-interval":        "1s",
						"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
						"-blocks-storage.bucket-store.bucket-index.enabled": strconv.FormatBool(testCfg.bucketIndexEnabled),
						"-blocks-storage.bucket-store.bucket-store-type":    testCfg.bucketStorageType,
						"-querier.query-store-for-labels-enabled":           "true",
						"-querier.thanos-engine":                            strconv.FormatBool(thanosEngine),
						"-querier.enable-x-functions":                       strconv.FormatBool(thanosEngine),
						// Ingester.
						"-ring.store":      "consul",
						"-consul.hostname": consul.NetworkHTTPEndpoint(),
						// Distributor.
						"-distributor.replication-factor": strconv.FormatInt(seriesReplicationFactor, 10),
						// Store-gateway.
						"-store-gateway.sharding-enabled":                 strconv.FormatBool(testCfg.blocksShardingEnabled),
						"-store-gateway.sharding-ring.store":              "consul",
						"-store-gateway.sharding-ring.consul.hostname":    consul.NetworkHTTPEndpoint(),
						"-store-gateway.sharding-ring.replication-factor": "1",
						// alert manager
						"-alertmanager.web.external-url": "http://localhost/alertmanager",
					},
				)
				require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(cortexAlertmanagerUserConfigYaml)))
				switch testCfg.bucketStorageType {
				case "tsdb":
					flags["-blocks-storage.bucket-store.index-cache.backend"] = testCfg.indexCacheBackend
				case "parquet":
					flags["-parquet-converter.enabled"] = "true"
					flags["-parquet-converter.conversion-interval"] = "1s"
					flags["-parquet-converter.ring.consul.hostname"] = consul.NetworkHTTPEndpoint()
					flags["-blocks-storage.bucket-store.parquet-labels-cache.backend"] = testCfg.parquetLabelsCache
					flags["-compactor.block-ranges"] = "1ms,12h"
				}

				// Add the index cache address to the flags.
				switch testCfg.indexCacheBackend {
				case tsdb.IndexCacheBackendMemcached:
					flags["-blocks-storage.bucket-store.index-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
				case tsdb.IndexCacheBackendRedis:
					flags["-blocks-storage.bucket-store.index-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
				}

				// Add the parquet label cache address
				if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendMemcached) {
					flags["-blocks-storage.bucket-store.parquet-labels-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
				}
				if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendRedis) {
					flags["-blocks-storage.bucket-store.parquet-labels-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
				}

				// Start Cortex replicas.
				cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags, "")
				cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags, "")
				cluster := e2ecortex.NewCompositeCortexService(cortex1, cortex2)
				require.NoError(t, s.StartAndWaitReady(cortex1, cortex2))

				var parquetConverter *e2ecortex.CortexService
				if testCfg.bucketStorageType == "parquet" {
					parquetConverter = e2ecortex.NewParquetConverter("parquet-converter", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
					require.NoError(t, s.StartAndWaitReady(parquetConverter))
				}

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
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_shipper_uploads_total"))
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series"))
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_memory_series_created_total"))
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series_removed_total"))

				// Push another series to further compact another block and delete the first block
				// due to expired retention.
				series3Timestamp := series2Timestamp.Add(blockRangePeriod * 2)
				series3, expectedVector3 := generateSeries("series_3", series3Timestamp, prompb.Label{Name: "series_3", Value: "series_3"})

				res, err = c.Push(series3)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)

				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_shipper_uploads_total"))
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series"))
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(3*cluster.NumInstances())), "cortex_ingester_memory_series_created_total"))
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_memory_series_removed_total"))

				if testCfg.bucketIndexEnabled {
					// Start the compactor to have the bucket index created before querying. We need to run the compactor
					// as a separate service because it's currently not part of the single binary.
					compactor := e2ecortex.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags, "")
					require.NoError(t, s.StartAndWaitReady(compactor))
				}

				switch testCfg.bucketStorageType {
				case "tsdb":
					// Wait until the store-gateway has synched the new uploaded blocks. The number of blocks loaded
					// may be greater than expected if the compactor is running (there may have been compacted).
					const shippedBlocks = 2
					if testCfg.blocksShardingEnabled {
						require.NoError(t, cluster.WaitSumMetrics(e2e.GreaterOrEqual(float64(shippedBlocks*seriesReplicationFactor)), "cortex_bucket_store_blocks_loaded"))
					} else {
						require.NoError(t, cluster.WaitSumMetrics(e2e.GreaterOrEqual(float64(shippedBlocks*seriesReplicationFactor*cluster.NumInstances())), "cortex_bucket_store_blocks_loaded"))
					}

					if !testCfg.bucketIndexEnabled {
						// Wait until the querier has discovered the uploaded blocks (discovered both by the querier and store-gateway).
						require.NoError(t, cluster.WaitSumMetricsWithOptions(e2e.Equals(float64(2*cluster.NumInstances()*2)), []string{"cortex_blocks_meta_synced"}, e2e.WithLabelMatchers(
							labels.MustNewMatcher(labels.MatchEqual, "component", "querier"))))
					}
				case "parquet":
					// Wait until the parquet-converter convert blocks
					require.NoError(t, parquetConverter.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_parquet_converter_blocks_converted_total"))
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
					require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((5+5+2)*seriesReplicationFactor)), "thanos_store_index_cache_requests_total")) // 5 for expanded postings and postings, 2 for series
					require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(0), "thanos_store_index_cache_hits_total"))                                            // no cache hit cause the cache was empty
				}

				if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
					require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(21*seriesReplicationFactor)), "thanos_memcached_operations_total")) // 14 gets + 7 sets
				}

				// Query back again the 1st series from storage. This time it should use the index cache.
				result, err = c.Query("series_1", series1Timestamp)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVector1, result.(model.Vector))

				switch testCfg.bucketStorageType {
				case "tsdb":
					require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((12+2)*seriesReplicationFactor)), "thanos_store_index_cache_requests_total"))
					require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*seriesReplicationFactor)), "thanos_store_index_cache_hits_total")) // this time has used the index cache
				case "parquet":
					switch testCfg.parquetLabelsCache {
					case tsdb.CacheBackendInMemory:
						require.NoError(t, cluster.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_cache_inmemory_requests_total"))
					case tsdb.CacheBackendMemcached:
						require.NoError(t, cluster.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_memcached_operations_total"))
					case tsdb.CacheBackendRedis:
						require.NoError(t, cluster.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_cache_redis_requests_total"))
					}
				}

				if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
					require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((21+2)*seriesReplicationFactor)), "thanos_memcached_operations_total")) // as before + 2 gets
				}

				// Query metadata.
				testMetadataQueriesWithBlocksStorage(t, c, series1[0], series2[0], series3[0], blockRangePeriod)
			})
		}
	}
}

func testMetadataQueriesWithBlocksStorage(
	t *testing.T,
	c *e2ecortex.Client,
	lastSeriesInStorage prompb.TimeSeries,
	lastSeriesInIngesterBlocks prompb.TimeSeries,
	firstSeriesInIngesterHead prompb.TimeSeries,
	blockRangePeriod time.Duration,
) {
	var (
		lastSeriesInIngesterBlocksName = getMetricName(lastSeriesInIngesterBlocks.Labels)
		firstSeriesInIngesterHeadName  = getMetricName(firstSeriesInIngesterHead.Labels)
		lastSeriesInStorageName        = getMetricName(lastSeriesInStorage.Labels)

		lastSeriesInStorageTs        = util.TimeFromMillis(lastSeriesInStorage.Samples[0].Timestamp)
		lastSeriesInIngesterBlocksTs = util.TimeFromMillis(lastSeriesInIngesterBlocks.Samples[0].Timestamp)
		firstSeriesInIngesterHeadTs  = util.TimeFromMillis(firstSeriesInIngesterHead.Samples[0].Timestamp)
	)

	type seriesTest struct {
		lookup string
		ok     bool
		resp   []prompb.Label
	}
	type labelValuesTest struct {
		label   string
		matches []string
		resp    []string
	}

	testCases := map[string]struct {
		from time.Time
		to   time.Time

		seriesTests []seriesTest

		labelValuesTests []labelValuesTest

		labelNames []string
	}{
		"query metadata entirely inside the head range": {
			from: firstSeriesInIngesterHeadTs,
			to:   firstSeriesInIngesterHeadTs.Add(blockRangePeriod),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     true,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     false,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     false,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					label: labels.MetricName,
					resp:  []string{firstSeriesInIngesterHeadName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{firstSeriesInIngesterHeadName},
					matches: []string{firstSeriesInIngesterHeadName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{},
					matches: []string{lastSeriesInStorageName},
				},
			},
			labelNames: []string{labels.MetricName, firstSeriesInIngesterHeadName},
		},
		"query metadata entirely inside the ingester range but outside the head range": {
			from: lastSeriesInIngesterBlocksTs,
			to:   lastSeriesInIngesterBlocksTs.Add(blockRangePeriod / 2),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     false,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     true,
					resp:   lastSeriesInIngesterBlocks.Labels,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     false,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					label: labels.MetricName,
					resp:  []string{lastSeriesInIngesterBlocksName},
				},

				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInIngesterBlocksName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{},
					matches: []string{firstSeriesInIngesterHeadName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInIngesterBlocksName},
		},
		"query metadata partially inside the ingester range": {
			from: lastSeriesInStorageTs.Add(-blockRangePeriod),
			to:   firstSeriesInIngesterHeadTs.Add(blockRangePeriod),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     true,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     true,
					resp:   lastSeriesInIngesterBlocks.Labels,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     true,
					resp:   lastSeriesInStorage.Labels,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					label: labels.MetricName,
					resp:  []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName, firstSeriesInIngesterHeadName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInStorageName},
					matches: []string{lastSeriesInStorageName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInIngesterBlocksName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInStorageName, lastSeriesInIngesterBlocksName, firstSeriesInIngesterHeadName},
		},
		"query metadata entirely outside the ingester range should return the head data as well": {
			from: lastSeriesInStorageTs.Add(-2 * blockRangePeriod),
			to:   lastSeriesInStorageTs,
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     true,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     false,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     true,
					resp:   lastSeriesInStorage.Labels,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					label: labels.MetricName,
					resp:  []string{lastSeriesInStorageName, firstSeriesInIngesterHeadName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInStorageName},
					matches: []string{lastSeriesInStorageName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{firstSeriesInIngesterHeadName},
					matches: []string{firstSeriesInIngesterHeadName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInStorageName, firstSeriesInIngesterHeadName},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, st := range tc.seriesTests {
				seriesRes, err := c.Series([]string{st.lookup}, tc.from, tc.to)
				require.NoError(t, err)
				if st.ok {
					require.Equal(t, 1, len(seriesRes))
					require.Equal(t, model.LabelSet(prompbLabelsToModelMetric(st.resp)), seriesRes[0])
				} else {
					require.Equal(t, 0, len(seriesRes))
				}
			}

			for _, lvt := range tc.labelValuesTests {
				labelsRes, err := c.LabelValues(lvt.label, tc.from, tc.to, lvt.matches)
				require.NoError(t, err)
				exp := model.LabelValues{}
				for _, val := range lvt.resp {
					exp = append(exp, model.LabelValue(val))
				}
				require.Equal(t, exp, labelsRes)
			}

			labelNames, err := c.LabelNames(tc.from, tc.to)
			require.NoError(t, err)
			require.Equal(t, tc.labelNames, labelNames)
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
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":       "1s",
		"-blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
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

	// Start the querier and store-gateway, and configure them to frequently sync blocks fast enough to trigger consistency check.
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "5s",
	}), "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "5s",
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

	// Wait until the querier and store-gateway have updated the ring, and wait until the blocks are old enough for consistency check
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(4), []string{"cortex_querier_blocks_scan_duration_seconds"}, e2e.WithMetricCount))

	// Query back the series.
	c, err = e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	result, err := c.Query("series_1", series1Timestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector1, result.(model.Vector))

	// Delete all blocks from the storage.
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	require.NoError(t, storage.DeleteBlocks("user-1"))

	// Query back again the series. Now we do expect a 500 error because the blocks are
	// missing from the storage.
	_, err = c.Query("series_1", series1Timestamp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestQuerierWithBlocksStorageLimits(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":       "1s",
		"-blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push some series to Cortex.
	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	seriesTimestamp := time.Now()
	series2Timestamp := seriesTimestamp.Add(blockRangePeriod * 2)
	series1, _ := generateSeries("series_1", seriesTimestamp, prompb.Label{Name: "job", Value: "test"})
	series2, _ := generateSeries("series_2", seriesTimestamp, prompb.Label{Name: "job", Value: "test"})
	// Make sure series in ingester has a different job label.
	series3, _ := generateSeries("series_3", series2Timestamp, prompb.Label{Name: "job", Value: "prometheus"})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c.Push(series3)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is compacted and shipped to the storage.
	// The shipped block contains the 1st and 2nd series, while the 3rd series is in the head.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(3), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_removed_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))

	// Start the querier and store-gateway, and configure them to frequently sync blocks fast enough to trigger consistency check.
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "5s",
		"-querier.max-fetched-series-per-query":      "1",
	}), "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.query-store-for-labels-enabled":    "true",
		"-blocks-storage.bucket-store.sync-interval": "5s",
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

	// Wait until the querier and store-gateway have updated the ring, and wait until the blocks are old enough for consistency check
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(4), []string{"cortex_querier_blocks_scan_duration_seconds"}, e2e.WithMetricCount))

	// Query back the series.
	c, err = e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// We expect all queries hitting 422 exceeded series limit on store gateway.
	resp, body, err := c.QueryRangeRaw(`{job="test"}`, seriesTimestamp.Add(-time.Second), seriesTimestamp, time.Second, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), "exceeded series limit")

	resp, body, err = c.SeriesRaw([]string{`{job="test"}`}, seriesTimestamp.Add(-time.Second), seriesTimestamp, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), "exceeded series limit")

	resp, body, err = c.LabelNamesRaw([]string{`{job="test"}`}, seriesTimestamp.Add(-time.Second), seriesTimestamp, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), "exceeded series limit")

	resp, body, err = c.LabelValuesRaw("job", []string{`{job="test"}`}, seriesTimestamp.Add(-time.Second), seriesTimestamp, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), "exceeded series limit")
}

func TestQuerierWithStoreGatewayDataBytesLimits(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":       "1s",
		"-blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push some series to Cortex.
	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	seriesTimestamp := time.Now()
	series2Timestamp := seriesTimestamp.Add(blockRangePeriod * 2)
	series1, _ := generateSeries("series_1", seriesTimestamp, prompb.Label{Name: "job", Value: "test"})
	series2, _ := generateSeries("series_2", series2Timestamp, prompb.Label{Name: "job", Value: "test"})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is compacted and shipped to the storage.
	// The shipped block contains the 1st series, while the 2ns series is in the head.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))

	// Start the querier and store-gateway, and configure them to frequently sync blocks fast enough to trigger consistency check.
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval":      "5s",
		"-store-gateway.max-downloaded-bytes-per-request": "1", // Use a small limit to make sure it hits the limit.
	}), "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "5s",
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

	// Wait until the querier and store-gateway have updated the ring, and wait until the blocks are old enough for consistency check
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(4), []string{"cortex_querier_blocks_scan_duration_seconds"}, e2e.WithMetricCount))

	// Query back the series.
	c, err = e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// We expect all queries hitting 422 exceeded series limit
	resp, body, err := c.QueryRaw(`{job="test"}`, series2Timestamp, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), "exceeded bytes limit")
}

func TestQueryLimitsWithBlocksStorageRunningInMicroServices(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period":   blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":         "1s",
		"-blocks-storage.bucket-store.sync-interval": "1s",
		"-blocks-storage.tsdb.retention-period":      ((blockRangePeriod * 2) - 1).String(),
		"-querier.query-store-for-labels-enabled":    "true",
		"-querier.max-fetched-series-per-query":      "3",
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(consul, minio, memcached))

	// Add the memcached address to the flags.
	flags["-blocks-storage.bucket-store.index-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)

	// Start Cortex components.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway))

	// Start the querier with configuring store-gateway addresses if sharding is disabled.
	flags = mergeFlags(flags, map[string]string{
		"-querier.store-gateway-addresses": strings.Join([]string{storeGateway.NetworkGRPCEndpoint()}, ","),
	})

	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(querier))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Cortex.
	series1Timestamp := time.Now()
	series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series3Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series4Timestamp := series1Timestamp.Add(blockRangePeriod * 3)

	series1, _ := generateSeries("series_1", series1Timestamp, prompb.Label{Name: "series_1", Value: "series_1"})
	series2, _ := generateSeries("series_2", series2Timestamp, prompb.Label{Name: "series_2", Value: "series_2"})
	series3, _ := generateSeries("series_3", series3Timestamp, prompb.Label{Name: "series_3", Value: "series_3"})
	series4, _ := generateSeries("series_4", series4Timestamp, prompb.Label{Name: "series_4", Value: "series_4"})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	result, err := c.QueryRange("{__name__=~\"series_.+\"}", series1Timestamp, series2Timestamp.Add(1*time.Hour), blockRangePeriod)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, result.Type())

	res, err = c.Push(series3)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c.Push(series4)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	_, err = c.QueryRange("{__name__=~\"series_.+\"}", series1Timestamp, series4Timestamp.Add(1*time.Hour), blockRangePeriod)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max number of series limit")
}

func TestHashCollisionHandling(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := BlocksStorageFlags()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Cortex.
	now := time.Now()

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-0")
	require.NoError(t, err)

	var series []prompb.TimeSeries
	var expectedVector model.Vector
	// Generate two series which collide on fingerprints and fast fingerprints.
	tsMillis := e2e.TimeToMilliseconds(now)
	metric1 := []prompb.Label{
		{Name: "A", Value: "K6sjsNNczPl"},
		{Name: labels.MetricName, Value: "fingerprint_collision"},
	}
	metric2 := []prompb.Label{
		{Name: "A", Value: "cswpLMIZpwt"},
		{Name: labels.MetricName, Value: "fingerprint_collision"},
	}

	series = append(series, prompb.TimeSeries{
		Labels: metric1,
		Samples: []prompb.Sample{
			{Value: float64(0), Timestamp: tsMillis},
		},
	})
	expectedVector = append(expectedVector, &model.Sample{
		Metric:    prompbLabelsToModelMetric(metric1),
		Value:     model.SampleValue(float64(0)),
		Timestamp: model.Time(tsMillis),
	})
	series = append(series, prompb.TimeSeries{
		Labels: metric2,
		Samples: []prompb.Sample{
			{Value: float64(1), Timestamp: tsMillis},
		},
	})
	expectedVector = append(expectedVector, &model.Sample{
		Metric:    prompbLabelsToModelMetric(metric2),
		Value:     model.SampleValue(float64(1)),
		Timestamp: model.Time(tsMillis),
	})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(storeGateway))
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))

	// Query the series.
	c, err = e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-0")
	require.NoError(t, err)

	result, err := c.Query("fingerprint_collision", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	require.Equal(t, expectedVector, result.(model.Vector))
}

func getMetricName(lbls []prompb.Label) string {
	for _, lbl := range lbls {
		if lbl.Name == labels.MetricName {
			return lbl.Value
		}
	}

	panic(fmt.Sprintf("series %v has no metric name", lbls))
}

func prompbLabelsToModelMetric(pbLabels []prompb.Label) model.Metric {
	metric := model.Metric{}

	for _, l := range pbLabels {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}

	return metric
}

func TestQuerierMaxSamplesLimit(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":       "1s",
		"-blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
		"-querier.max-samples":                     "1",
	})

	// Start dependencies.
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", "", flags, "")
	require.NoError(t, s.Start(queryFrontend))

	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the distributor and querier has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Cortex.
	series1Timestamp := time.Now()
	series1, _ := generateSeries("series_1", series1Timestamp, prompb.Label{Name: "job", Value: "test"})
	series2, _ := generateSeries("series_2", series1Timestamp, prompb.Label{Name: "job", Value: "test"})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	retries := backoff.New(context.Background(), backoff.Config{
		MinBackoff: 5 * time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 5,
	})

	var body []byte
	for retries.Ongoing() {
		// We expect request to hit max samples limit.
		res, body, err = c.QueryRaw(`sum({job="test"})`, series1Timestamp, map[string]string{})
		if err == nil {
			break
		}
		retries.Wait()
	}
	require.NoError(t, err)
	require.Equal(t, 422, res.StatusCode)
	var response api.Response
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)
	// Make sure that the returned response is in Prometheus error format.
	require.Equal(t, response, api.Response{
		Status:    "error",
		ErrorType: v1.ErrExec,
		Error:     "query processing would load too many samples into memory in query execution",
	})
}

func TestQuerierEngineConfigs(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":       "1s",
		"-blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
		"-querier.thanos-engine":                   "true",
		"-querier.enable-x-functions":              "true",
		"-querier.optimizers":                      "all",
	})

	// Start dependencies.
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", "", flags, "")
	require.NoError(t, s.Start(queryFrontend))

	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the distributor and querier has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Cortex.
	series1Timestamp := time.Now()
	series1, _ := generateSeries("series_1", series1Timestamp, prompb.Label{Name: "job", Value: "test"})
	series2, _ := generateSeries("series_2", series1Timestamp, prompb.Label{Name: "job", Value: "test"})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	for xFunc := range parse.XFunctions {
		result, err := c.Query(fmt.Sprintf(`%s(series_1{job="test"}[1m])`, xFunc), series1Timestamp)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
	}

}

func TestQuerierDistributedExecution(t *testing.T) {
	// e2e test setup
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// initialize the flags
	flags := mergeFlags(
		BlocksStorageFlags(),
		map[string]string{
			"-blocks-storage.tsdb.block-ranges-period": (5 * time.Second).String(),
			"-blocks-storage.tsdb.ship-interval":       "1s",
			"-blocks-storage.tsdb.retention-period":    ((5 * time.Second * 2) - 1).String(),
			"-querier.thanos-engine":                   "true",
			// enable distributed execution (logical plan execution)
			"-querier.distributed-exec-enabled": "true",
		},
	)

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// start services
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	queryScheduler := e2ecortex.NewQueryScheduler("query-scheduler", flags, "")
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(queryScheduler, distributor, ingester, storeGateway))
	flags = mergeFlags(flags, map[string]string{
		"-querier.store-gateway-addresses": strings.Join([]string{storeGateway.NetworkGRPCEndpoint()}, ","),
	})

	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", mergeFlags(flags, map[string]string{
		"-frontend.scheduler-address": queryScheduler.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.Start(queryFrontend))

	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.scheduler-address": queryScheduler.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// wait until the distributor and querier has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	series1Timestamp := time.Now()
	series2Timestamp := series1Timestamp.Add(time.Minute * 1)
	series1, expectedVector1 := generateSeries("series_1", series1Timestamp, prompb.Label{Name: "series_1", Value: "series_1"})
	series2, expectedVector2 := generateSeries("series_2", series2Timestamp, prompb.Label{Name: "series_2", Value: "series_2"})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// main tests
	// - make sure queries are still executable with distributed execution enabled
	var val model.Value
	val, err = c.Query("series_1", series1Timestamp)
	require.NoError(t, err)
	require.Equal(t, expectedVector1, val.(model.Vector))

	val, err = c.Query("series_2", series2Timestamp)
	require.NoError(t, err)
	require.Equal(t, expectedVector2, val.(model.Vector))
}
