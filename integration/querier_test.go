// +build integration

package main

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestQuerierWithBlocksStorage(t *testing.T) {
	tests := map[string]struct {
		flags map[string]string
	}{
		"querier running with ingester gRPC streaming disabled": {
			flags: mergeFlags(BlocksStorageFlags, map[string]string{
				"-querier.ingester-streaming": "false",
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

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-experimental.tsdb.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start Cortex components.
			distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
			ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, "")
			querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

			// Wait until both the distributor and querier have updated the ring.
			require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

			c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "user-1")
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
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))

			// Push another series to further compact another block and delete the first block
			// due to expired retention.
			series3Timestamp := series2Timestamp.Add(blockRangePeriod * 2)
			series3, expectedVector3 := generateSeries("series_3", series3Timestamp)

			res, err = c.Push(series3)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(3), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_removed_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))

			// Wait until the querier has synched the new uploaded blocks.
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2), "cortex_querier_bucket_store_blocks_loaded"))

			// Query back the series (1 only in the storage, 1 only in the ingesters, 1 on both).
			// TODO: apparently Thanos has a bug which cause a block to not be considered if the
			//       query timetamp matches the block max timestamp
			series1Timestamp = series1Timestamp.Add(time.Duration(time.Millisecond))
			expectedVector1[0].Timestamp = model.Time(e2e.TimeToMilliseconds(series1Timestamp))

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
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*2), "cortex_querier_blocks_index_cache_items"))             // 2 series both for postings and series cache
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*2), "cortex_querier_blocks_index_cache_items_added_total")) // 2 series both for postings and series cache
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(0), "cortex_querier_blocks_index_cache_hits_total"))          // no cache hit cause the cache was empty

			// Query back again the 1st series from storage. This time it should use the index cache.
			result, err = c.Query("series_1", series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))

			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*2), "cortex_querier_blocks_index_cache_items"))             // as before
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*2), "cortex_querier_blocks_index_cache_items_added_total")) // as before
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2), "cortex_querier_blocks_index_cache_hits_total"))          // this time has used the index cache
		})
	}
}
