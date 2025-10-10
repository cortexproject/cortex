//go:build integration_querier
// +build integration_querier

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/log"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
)

func TestParquetGatewayWithBlocksStorageRunningInMicroservicesMode(t *testing.T) {
	tests := map[string]struct {
		blocksShardingStrategy string // Empty means sharding is disabled.
		tenantShardSize        int
		parquetLabelsCache     string
		chunkCacheBackend      string
		bucketIndexEnabled     bool
	}{
		"blocks sharding disabled, memcached parquet label cache, memcached chunks cache": {
			blocksShardingStrategy: "",
			parquetLabelsCache:     tsdb.CacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"blocks sharding disabled, multilevel parquet label cache (inmemory, memcached)": {
			blocksShardingStrategy: "",
			parquetLabelsCache:     fmt.Sprintf("%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached),
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"blocks sharding disabled, redis parquet label cache, redis chunks cache": {
			blocksShardingStrategy: "",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
		},
		"blocks sharding disabled, multilevel parquet label cache cache (inmemory, redis), redis chunks cache": {
			blocksShardingStrategy: "",
			parquetLabelsCache:     fmt.Sprintf("%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendRedis),
			chunkCacheBackend:      tsdb.CacheBackendRedis,
		},
		"blocks default sharding, inmemory parquet label cache": {
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendInMemory,
		},
		"blocks default sharding, memcached parquet label cache, memcached chunks cache": {
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"blocks shuffle sharding, memcached parquet label cache, memcached chunks cache": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"blocks default sharding, inmemory parquet label cache, bucket index enabled": {
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"blocks shuffle sharding, memcached parquet label cache, bucket index enabled": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"blocks default sharding, redis parquet label cache, redis chunks cache, bucket index enabled": {
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
			bucketIndexEnabled:     true,
		},
		"blocks shuffle sharding, redis parquet label cache, redis chunks cache, bucket index enabled": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
			bucketIndexEnabled:     true,
		},
		"blocks sharding disabled, redis parquet label cache, in-memory chunks cache, bucket index enabled": {
			blocksShardingStrategy: "",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"blocks default sharding, redis parquet label cache, in-memory chunk cache": {
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"blocks shuffle sharding, redis parquet label cache, in-memory chunk cache, bucket index enabled": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"block sharding disabled, redis parquet label cache, multi-level chunk cache (in-memory, memcached, redis), bucket index enabled": {
			blocksShardingStrategy: "",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		"block default sharding, redis parquet label cache, multi-level chunk cache (in-memory, memcached, redis), bucket index enabled": {
			blocksShardingStrategy: "default",
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		"block shuffle sharding, redis parquet label cache, multi-level chunk cache ((in-memory, memcached, redis), bucket index enabled)": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			parquetLabelsCache:     tsdb.CacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
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
			memcached := e2ecache.NewMemcached()
			redis := e2ecache.NewRedis()
			require.NoError(t, s.StartAndWaitReady(consul, memcached, redis))

			// Configure the blocks storage to frequently compact TSDB head
			// and ship blocks to the storage.
			flags := mergeFlags(BlocksStorageFlags(), map[string]string{
				"-blocks-storage.tsdb.block-ranges-period":                  blockRangePeriod.String(),
				"-blocks-storage.tsdb.ship-interval":                        "1s",
				"-blocks-storage.bucket-store.sync-interval":                "1s",
				"-blocks-storage.tsdb.retention-period":                     ((blockRangePeriod * 2) - 1).String(),
				"-blocks-storage.bucket-store.parquet-labels-cache.backend": testCfg.parquetLabelsCache,
				"-blocks-storage.bucket-store.chunks-cache.backend":         testCfg.chunkCacheBackend,
				"-store-gateway.sharding-enabled":                           strconv.FormatBool(testCfg.blocksShardingStrategy != ""),
				"-store-gateway.sharding-strategy":                          testCfg.blocksShardingStrategy,
				"-store-gateway.tenant-shard-size":                          fmt.Sprintf("%d", testCfg.tenantShardSize),
				"-querier.query-store-for-labels-enabled":                   "true",
				// Enable parquet converter
				"-blocks-storage.bucket-store.bucket-index.enabled": strconv.FormatBool(testCfg.bucketIndexEnabled),
				"-blocks-storage.bucket-store.bucket-store-type":    "parquet",
				"-parquet-converter.enabled":                        "true",
				"-parquet-converter.conversion-interval":            "1s",
				"-parquet-converter.ring.consul.hostname":           consul.NetworkHTTPEndpoint(),
				// compactor
				"-compactor.cleanup-interval": "1s",
				"-compactor.block-ranges":     "1ms,12h", // to convert all blocks to parquet blocks
			})

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			// Add the cache address to the flags.
			if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendMemcached) {
				flags["-blocks-storage.bucket-store.parquet-labels-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
			}
			if strings.Contains(testCfg.parquetLabelsCache, tsdb.CacheBackendRedis) {
				flags["-blocks-storage.bucket-store.parquet-labels-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
			}
			if strings.Contains(testCfg.chunkCacheBackend, tsdb.CacheBackendMemcached) {
				flags["-blocks-storage.bucket-store.chunks-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
			}
			if strings.Contains(testCfg.chunkCacheBackend, tsdb.CacheBackendRedis) {
				flags["-blocks-storage.bucket-store.chunks-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
			}

			// Start Cortex components.
			distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			parquetConverter := e2ecortex.NewParquetConverter("parquet-converter", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			storeGateway1 := e2ecortex.NewStoreGateway("store-gateway-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			storeGateway2 := e2ecortex.NewStoreGateway("store-gateway-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			storeGateways := e2ecortex.NewCompositeCortexService(storeGateway1, storeGateway2)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester, parquetConverter, storeGateway1, storeGateway2))

			// Start the querier with configuring store-gateway addresses if sharding is disabled.
			if testCfg.blocksShardingStrategy == "" {
				flags = mergeFlags(flags, map[string]string{
					"-querier.store-gateway-addresses": strings.Join([]string{storeGateway1.NetworkGRPCEndpoint(), storeGateway2.NetworkGRPCEndpoint()}, ","),
				})
			}
			querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(querier))

			if testCfg.bucketIndexEnabled {
				// Start the compactor to have the bucket index created before querying.
				compactor := e2ecortex.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags, "")
				require.NoError(t, s.StartAndWaitReady(compactor))
			}

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

			// Prepare test data similar to parquet_querier_test.go
			ctx := context.Background()
			rnd := rand.New(rand.NewSource(time.Now().Unix()))
			dir := filepath.Join(s.SharedDir(), "data")
			numSeries := 10
			numSamples := 60
			lbls := make([]labels.Labels, 0, numSeries*2)
			scrapeInterval := time.Minute
			statusCodes := []string{"200", "400", "404", "500", "502"}
			now := time.Now()
			start := now.Add(-time.Hour * 24)
			end := now.Add(-time.Hour)

			for i := 0; i < numSeries; i++ {
				lbls = append(lbls, labels.FromStrings(labels.MetricName, "test_series_a", "job", "test", "series", strconv.Itoa(i%3), "status_code", statusCodes[i%5]))
				lbls = append(lbls, labels.FromStrings(labels.MetricName, "test_series_b", "job", "test", "series", strconv.Itoa((i+1)%3), "status_code", statusCodes[(i+1)%5]))
			}

			// Create a block with test data
			id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
			require.NoError(t, err)

			// Upload the block to storage
			storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, err)
			bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)

			err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
			require.NoError(t, err)

			// Push some series to Cortex for real-time data
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
			// The shipped block contains the 1st series, while the 2nd series is in the head.
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
				cortex_testutil.Poll(t, 30*time.Second, true, func() interface{} {
					foundBucketIndex := false

					err := bkt.Iter(context.Background(), "", func(name string) error {
						if name == "bucket-index.json.gz" {
							foundBucketIndex = true
						}
						return nil
					}, objstore.WithRecursiveIter())
					require.NoError(t, err)
					return foundBucketIndex
				})
			}

			// Wait until we convert the blocks to parquet
			cortex_testutil.Poll(t, 30*time.Second, true, func() interface{} {
				found := false

				err := bkt.Iter(context.Background(), "", func(name string) error {
					if name == fmt.Sprintf("parquet-markers/%v-parquet-converter-mark.json", id.String()) {
						found = true
					}

					return nil
				}, objstore.WithRecursiveIter())
				require.NoError(t, err)
				return found
			})

			// Check how many tenants have been discovered and synced by store-gateways.
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1*storeGateways.NumInstances())), "cortex_bucket_stores_tenants_discovered"))
			if testCfg.blocksShardingStrategy == "shuffle-sharding" {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1)), "cortex_bucket_stores_tenants_synced"))
			} else {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1*storeGateways.NumInstances())), "cortex_bucket_stores_tenants_synced"))
			}

			// Wait until the parquet-converter convert blocks
			require.NoError(t, parquetConverter.WaitSumMetricsWithOptions(e2e.Equals(float64(3)), []string{"cortex_parquet_converter_blocks_converted_total"}, e2e.WaitMissingMetrics))

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

			// Query the pre-uploaded test data
			result, err = c.QueryRange("test_series_a", start, end, scrapeInterval)
			require.NoError(t, err)
			require.Equal(t, model.ValMatrix, result.Type())
			// Should have some results from the pre-uploaded data
			assert.Greater(t, len(result.(model.Matrix)), 0)

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

func TestParquetGatewayWithBlocksStorageRunningInSingleBinaryMode(t *testing.T) {
	tests := map[string]struct {
		blocksShardingEnabled bool
		chunkCacheBackend     string
		bucketIndexEnabled    bool
	}{
		"blocks sharding enabled, inmemory chunks cache": {
			blocksShardingEnabled: true,
			chunkCacheBackend:     tsdb.CacheBackendInMemory,
		},
		"blocks sharding disabled, memcached chunks cache": {
			blocksShardingEnabled: false,
			chunkCacheBackend:     tsdb.CacheBackendMemcached,
		},
		"blocks sharding enabled, memcached chunks cache": {
			blocksShardingEnabled: true,
			chunkCacheBackend:     tsdb.CacheBackendMemcached,
		},
		"blocks sharding enabled, memcached chunk cache, bucket index enabled": {
			blocksShardingEnabled: true,
			chunkCacheBackend:     tsdb.CacheBackendMemcached,
			bucketIndexEnabled:    true,
		},
		"blocks sharding disabled, redis chunks cache": {
			blocksShardingEnabled: false,
			chunkCacheBackend:     tsdb.CacheBackendRedis,
		},
		"blocks sharding enabled, redis chunks cache": {
			blocksShardingEnabled: true,
			chunkCacheBackend:     tsdb.CacheBackendRedis,
		},
		"blocks sharding enabled, redis chunks cache, bucket index enabled": {
			blocksShardingEnabled: true,
			chunkCacheBackend:     tsdb.CacheBackendRedis,
			bucketIndexEnabled:    true,
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
					"-target": "all,parquet-converter",
					"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
					"-blocks-storage.tsdb.ship-interval":                "1s",
					"-blocks-storage.bucket-store.sync-interval":        "1s",
					"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
					"-blocks-storage.bucket-store.chunks-cache.backend": testCfg.chunkCacheBackend,
					"-blocks-storage.bucket-store.bucket-index.enabled": strconv.FormatBool(testCfg.bucketIndexEnabled),
					"-blocks-storage.bucket-store.bucket-store-type":    "parquet",
					"-querier.query-store-for-labels-enabled":           "true",
					// Enable parquet converter
					"-parquet-converter.enabled":              "true",
					"-parquet-converter.conversion-interval":  "1s",
					"-parquet-converter.ring.consul.hostname": consul.NetworkHTTPEndpoint(),
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
					// compactor
					"-compactor.cleanup-interval": "1s",
					"-compactor.block-ranges":     "1ms,12h", // to convert all blocks to parquet blocks
				},
			)
			require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(cortexAlertmanagerUserConfigYaml)))

			// Add the cache address to the flags.
			switch testCfg.chunkCacheBackend {
			case tsdb.CacheBackendMemcached:
				flags["-blocks-storage.bucket-store.chunks-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
			case tsdb.CacheBackendRedis:
				flags["-blocks-storage.bucket-store.chunks-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
			}

			// Start Cortex replicas.
			cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags, "")
			cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags, "")
			cluster := e2ecortex.NewCompositeCortexService(cortex1, cortex2)
			require.NoError(t, s.StartAndWaitReady(cortex1, cortex2))

			// Wait until Cortex replicas have updated the ring state.
			for _, replica := range cluster.Instances() {
				numTokensPerInstance := 512      // Ingesters ring.
				parquetConverterRingToken := 512 // Parquet converter ring.
				if testCfg.blocksShardingEnabled {
					numTokensPerInstance += 512 * 2 // Store-gateway ring (read both by the querier and store-gateway).
				}
				require.NoError(t, replica.WaitSumMetrics(e2e.Equals(float64((parquetConverterRingToken+numTokensPerInstance)*cluster.NumInstances())), "cortex_ring_tokens_total"))
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
			// The shipped block contains the 1st series, while the 2nd series is in the head.
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

			// Wait until the parquet-converter convert blocks
			time.Sleep(time.Second * 5)
			require.NoError(t, cluster.WaitSumMetricsWithOptions(e2e.Equals(float64(2*cluster.NumInstances())), []string{"cortex_parquet_converter_blocks_converted_total"}, e2e.WaitMissingMetrics))

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

			// Query back again the 1st series from storage.
			result, err = c.Query("series_1", series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))

			switch testCfg.chunkCacheBackend {
			case tsdb.CacheBackendInMemory:
				require.NoError(t, cluster.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_cache_inmemory_requests_total"))
			case tsdb.CacheBackendMemcached:
				require.NoError(t, cluster.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_memcached_operations_total"))
			case tsdb.CacheBackendRedis:
				require.NoError(t, cluster.WaitSumMetrics(e2e.Greater(float64(0)), "thanos_cache_redis_requests_total"))
			}

			// Query metadata.
			testMetadataQueriesWithBlocksStorage(t, c, series1[0], series2[0], series3[0], blockRangePeriod)
		})
	}
}
