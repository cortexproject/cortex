//go:build integration_parquet_gateway
// +build integration_parquet_gateway

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
		indexCacheBackend      string
		chunkCacheBackend      string
		bucketIndexEnabled     bool
	}{
		"blocks sharding disabled, memcached index cache": {
			blocksShardingStrategy: "",
			indexCacheBackend:      tsdb.IndexCacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"blocks sharding disabled, multilevel index cache (inmemory, memcached)": {
			blocksShardingStrategy: "",
			indexCacheBackend:      fmt.Sprintf("%v,%v", tsdb.IndexCacheBackendInMemory, tsdb.IndexCacheBackendMemcached),
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"blocks sharding disabled, redis index cache": {
			blocksShardingStrategy: "",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
		},
		"blocks sharding disabled, multilevel index cache (inmemory, redis)": {
			blocksShardingStrategy: "",
			indexCacheBackend:      fmt.Sprintf("%v,%v", tsdb.IndexCacheBackendInMemory, tsdb.IndexCacheBackendRedis),
			chunkCacheBackend:      tsdb.CacheBackendRedis,
		},
		"blocks default sharding, inmemory index cache": {
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendInMemory,
		},
		"blocks default sharding, memcached index cache": {
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"blocks shuffle sharding, memcached index cache": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendMemcached,
			chunkCacheBackend:      tsdb.CacheBackendMemcached,
		},
		"blocks default sharding, inmemory index cache, bucket index enabled": {
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"blocks shuffle sharding, memcached index cache, bucket index enabled": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"blocks default sharding, redis index cache, bucket index enabled": {
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
			bucketIndexEnabled:     true,
		},
		"blocks shuffle sharding, redis index cache, bucket index enabled": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendRedis,
			bucketIndexEnabled:     true,
		},
		"blocks sharding disabled, in-memory chunk cache": {
			blocksShardingStrategy: "",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"blocks default sharding, in-memory chunk cache": {
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"blocks shuffle sharding, in-memory chunk cache": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      tsdb.CacheBackendInMemory,
			bucketIndexEnabled:     true,
		},
		"block sharding disabled, multi-level chunk cache": {
			blocksShardingStrategy: "",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		"block default sharding, multi-level chunk cache": {
			blocksShardingStrategy: "default",
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
			chunkCacheBackend:      fmt.Sprintf("%v,%v,%v", tsdb.CacheBackendInMemory, tsdb.CacheBackendMemcached, tsdb.CacheBackendRedis),
			bucketIndexEnabled:     true,
		},
		"block shuffle sharding, multi-level chunk cache": {
			blocksShardingStrategy: "shuffle-sharding",
			tenantShardSize:        1,
			indexCacheBackend:      tsdb.IndexCacheBackendRedis,
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

			numberOfCacheBackends := len(strings.Split(testCfg.indexCacheBackend, ","))

			// Configure the blocks storage to frequently compact TSDB head
			// and ship blocks to the storage.
			flags := mergeFlags(BlocksStorageFlags(), map[string]string{
				"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
				"-blocks-storage.tsdb.ship-interval":                "1s",
				"-blocks-storage.bucket-store.sync-interval":        "1s",
				"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
				"-blocks-storage.bucket-store.index-cache.backend":  testCfg.indexCacheBackend,
				"-blocks-storage.bucket-store.chunks-cache.backend": testCfg.chunkCacheBackend,
				"-store-gateway.sharding-enabled":                   strconv.FormatBool(testCfg.blocksShardingStrategy != ""),
				"-store-gateway.sharding-strategy":                  testCfg.blocksShardingStrategy,
				"-store-gateway.tenant-shard-size":                  fmt.Sprintf("%d", testCfg.tenantShardSize),
				"-querier.query-store-for-labels-enabled":           "true",
				"-blocks-storage.bucket-store.bucket-index.enabled": strconv.FormatBool(testCfg.bucketIndexEnabled),
				"-blocks-storage.bucket-store.bucket-store-type":    "parquet",
				// Enable parquet converter
				"-parquet-converter.enabled":              "true",
				"-parquet-converter.conversion-interval":  "1s",
				"-parquet-converter.ring.consul.hostname": "consul:8500",
			})

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			memcached := e2ecache.NewMemcached()
			redis := e2ecache.NewRedis()
			require.NoError(t, s.StartAndWaitReady(consul, minio, memcached, redis))

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

			// Start Cortex components.
			distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			storeGateway1 := e2ecortex.NewStoreGateway("store-gateway-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			storeGateway2 := e2ecortex.NewStoreGateway("store-gateway-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			storeGateways := e2ecortex.NewCompositeCortexService(storeGateway1, storeGateway2)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway1, storeGateway2))

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

			// Wait until we convert the blocks to parquet
			cortex_testutil.Poll(t, 30*time.Second, true, func() interface{} {
				found := false
				foundBucketIndex := false

				err := bkt.Iter(context.Background(), "", func(name string) error {
					if name == fmt.Sprintf("parquet-markers/%v-parquet-converter-mark.json", id.String()) {
						found = true
					}
					if name == "bucket-index.json.gz" {
						foundBucketIndex = true
					}
					return nil
				}, objstore.WithRecursiveIter())
				require.NoError(t, err)
				return found && foundBucketIndex
			})

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
				// Start the compactor to have the bucket index created before querying.
				compactor := e2ecortex.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags, "")
				require.NoError(t, s.StartAndWaitReady(compactor))
			} else {
				// Wait until the querier has discovered the uploaded blocks.
				require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics))
			}

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
			result, err = c.Query("test_series_a", now.Add(-time.Hour))
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			// Should have some results from the pre-uploaded data
			assert.Greater(t, len(result.(model.Vector)), 0)

			// Check the in-memory index cache metrics (in the store-gateway).
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64((5+5+2)*numberOfCacheBackends)), "thanos_store_index_cache_requests_total"))
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(0), "thanos_store_index_cache_hits_total")) // no cache hit cause the cache was empty

			// Query back again the 1st series from storage. This time it should use the index cache.
			result, err = c.Query("series_1", series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))

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

			// Query metadata.
			testMetadataQueriesWithBlocksStorage(t, c, series1[0], series2[0], series3[0], blockRangePeriod)

			// Ensure no service-specific metrics prefix is used by the wrong service.
			assertServiceMetricsPrefixes(t, Distributor, distributor)
			assertServiceMetricsPrefixes(t, Ingester, ingester)
			assertServiceMetricsPrefixes(t, Querier, querier)
			assertServiceMetricsPrefixes(t, StoreGateway, storeGateway1)
			assertServiceMetricsPrefixes(t, StoreGateway, storeGateway2)

			// Verify that parquet bucket stores are being used
			require.NoError(t, storeGateways.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_parquet_bucket_stores_cache_hits_total"}, e2e.SkipMissingMetrics))
		})
	}
}

func TestParquetGatewayWithBlocksStorageRunningInSingleBinaryMode(t *testing.T) {
	tests := map[string]struct {
		blocksShardingEnabled bool
		indexCacheBackend     string
		bucketIndexEnabled    bool
	}{
		"blocks sharding enabled, inmemory index cache": {
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendInMemory,
		},
		"blocks sharding disabled, memcached index cache": {
			blocksShardingEnabled: false,
			indexCacheBackend:     tsdb.IndexCacheBackendMemcached,
		},
		"blocks sharding enabled, memcached index cache": {
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendMemcached,
		},
		"blocks sharding enabled, memcached index cache, bucket index enabled": {
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendMemcached,
			bucketIndexEnabled:    true,
		},
		"blocks sharding disabled,redis index cache": {
			blocksShardingEnabled: false,
			indexCacheBackend:     tsdb.IndexCacheBackendRedis,
		},
		"blocks sharding enabled, redis index cache": {
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendRedis,
		},
		"blocks sharding enabled, redis index cache, bucket index enabled": {
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendRedis,
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
					"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
					"-blocks-storage.tsdb.ship-interval":                "1s",
					"-blocks-storage.bucket-store.sync-interval":        "1s",
					"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
					"-blocks-storage.bucket-store.index-cache.backend":  testCfg.indexCacheBackend,
					"-blocks-storage.bucket-store.bucket-index.enabled": strconv.FormatBool(testCfg.bucketIndexEnabled),
					"-blocks-storage.bucket-store.bucket-store-type":    "parquet",
					"-querier.query-store-for-labels-enabled":           "true",
					// Enable parquet converter
					"-parquet-converter.enabled":              "true",
					"-parquet-converter.conversion-interval":  "1s",
					"-parquet-converter.ring.consul.hostname": "consul:8500",
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

			// Add the cache address to the flags.
			switch testCfg.indexCacheBackend {
			case tsdb.IndexCacheBackendMemcached:
				flags["-blocks-storage.bucket-store.index-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)
			case tsdb.IndexCacheBackendRedis:
				flags["-blocks-storage.bucket-store.index-cache.redis.addresses"] = redis.NetworkEndpoint(e2ecache.RedisPort)
			}

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

			if testCfg.bucketIndexEnabled {
				// Start the compactor to have the bucket index created before querying. We need to run the compactor
				// as a separate service because it's currently not part of the single binary.
				compactor := e2ecortex.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags, "")
				require.NoError(t, s.StartAndWaitReady(compactor))
			} else {
				// Wait until the querier has discovered the uploaded blocks (discovered both by the querier and store-gateway).
				require.NoError(t, cluster.WaitSumMetricsWithOptions(e2e.Equals(float64(2*cluster.NumInstances()*2)), []string{"cortex_blocks_meta_synced"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "component", "querier"))))
			}

			// Wait until the store-gateway has synched the new uploaded blocks. The number of blocks loaded
			// may be greater than expected if the compactor is running (there may have been compacted).
			const shippedBlocks = 2
			if testCfg.blocksShardingEnabled {
				require.NoError(t, cluster.WaitSumMetrics(e2e.GreaterOrEqual(float64(shippedBlocks*seriesReplicationFactor)), "cortex_bucket_store_blocks_loaded"))
			} else {
				require.NoError(t, cluster.WaitSumMetrics(e2e.GreaterOrEqual(float64(shippedBlocks*seriesReplicationFactor*cluster.NumInstances())), "cortex_bucket_store_blocks_loaded"))
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
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((5+5+2)*seriesReplicationFactor)), "thanos_store_index_cache_requests_total")) // 5 for expanded postings and postings, 2 for series
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(0), "thanos_store_index_cache_hits_total"))                                            // no cache hit cause the cache was empty

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(21*seriesReplicationFactor)), "thanos_memcached_operations_total")) // 14 gets + 7 sets
			}

			// Query back again the 1st series from storage. This time it should use the index cache.
			result, err = c.Query("series_1", series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))

			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((12+2)*seriesReplicationFactor)), "thanos_store_index_cache_requests_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*seriesReplicationFactor)), "thanos_store_index_cache_hits_total")) // this time has used the index cache

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((21+2)*seriesReplicationFactor)), "thanos_memcached_operations_total")) // as before + 2 gets
			}

			// Query metadata.
			testMetadataQueriesWithBlocksStorage(t, c, series1[0], series2[0], series3[0], blockRangePeriod)

			// Verify that parquet bucket stores are being used
			require.NoError(t, cluster.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_parquet_bucket_stores_cache_hits_total"}, e2e.SkipMissingMetrics))
		})
	}
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
