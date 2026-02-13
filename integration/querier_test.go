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
	"github.com/cortexproject/cortex/pkg/util/api"
	"github.com/cortexproject/cortex/pkg/util/backoff"
)

func TestQuerierWithBlocksStorageRunningInSingleBinaryMode(t *testing.T) {
	tests := map[string]struct {
		bucketStorageType     string
		blocksShardingEnabled bool
		indexCacheBackend     string
		parquetLabelsCache    string
	}{
		// tsdb bucket storage
		"[TSDB] blocks sharding enabled, memcached index cache, bucket index enabled": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendMemcached,
		},
		"[TSDB] blocks sharding enabled, redis index cache, bucket index enabled": {
			bucketStorageType:     "tsdb",
			blocksShardingEnabled: true,
			indexCacheBackend:     tsdb.IndexCacheBackendRedis,
		},
		// parquet bucket storage
		"[Parquet] blocks sharding enabled, memcached parquet labels cache, bucket index enabled": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: true,
			parquetLabelsCache:    tsdb.CacheBackendMemcached,
		},
		"[Parquet] blocks sharding enabled, redis parquet labels cache, bucket index enabled": {
			bucketStorageType:     "parquet",
			blocksShardingEnabled: true,
			parquetLabelsCache:    tsdb.CacheBackendRedis,
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
						"-blocks-storage.tsdb.block-ranges-period":       blockRangePeriod.String(),
						"-blocks-storage.tsdb.ship-interval":             "1s",
						"-blocks-storage.bucket-store.sync-interval":     "1s",
						"-blocks-storage.tsdb.retention-period":          ((blockRangePeriod * 2) - 1).String(),
						"-blocks-storage.bucket-store.bucket-store-type": testCfg.bucketStorageType,
						"-querier.thanos-engine":                         strconv.FormatBool(thanosEngine),
						"-querier.enable-x-functions":                    strconv.FormatBool(thanosEngine),
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
						"-compactor.sharding-enabled":                 "true",
						"-compactor.ring.store":                       "consul",
						"-compactor.ring.consul.hostname":             consul.NetworkHTTPEndpoint(),
						"-compactor.ring.wait-stability-min-duration": "0",
						"-compactor.ring.wait-stability-max-duration": "0",
						"-compactor.cleanup-interval":                 "1s",
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
					numTokensPerInstance := 512 + 512 // Ingesters ring + compactor ring.
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
		// TODO: run a compactor here instead of disabling the bucket-index
		"-blocks-storage.bucket-store.bucket-index.enabled": "false",
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
		// TODO: run a compactor here instead of disabling the bucket-index
		"-blocks-storage.bucket-store.bucket-index.enabled": "false",
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
		// TODO: run a compactor here instead of disabling the bucket-index
		"-blocks-storage.bucket-store.bucket-index.enabled": "false",
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
		"-querier.max-fetched-series-per-query":      "3",
		// TODO: run a compactor here instead of disabling the bucket-index
		"-blocks-storage.bucket-store.bucket-index.enabled": "false",
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
