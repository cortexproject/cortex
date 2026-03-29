//go:build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

type cardinalityAPIResponse struct {
	Status string `json:"status"`
	Data   struct {
		NumSeries               uint64 `json:"numSeries"`
		Approximated            bool   `json:"approximated"`
		SeriesCountByMetricName []struct {
			Name  string `json:"name"`
			Value uint64 `json:"value"`
		} `json:"seriesCountByMetricName"`
		LabelValueCountByLabelName []struct {
			Name  string `json:"name"`
			Value uint64 `json:"value"`
		} `json:"labelValueCountByLabelName"`
		SeriesCountByLabelValuePair []struct {
			Name  string `json:"name"`
			Value uint64 `json:"value"`
		} `json:"seriesCountByLabelValuePair"`
	} `json:"data"`
}

func TestCardinalityAPI(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Configure the blocks storage to frequently compact TSDB head and ship blocks to storage.
	flags := mergeFlags(BlocksStorageFlags(), AlertmanagerLocalFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":                "1s",
		"-blocks-storage.bucket-store.sync-interval":        "1s",
		"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
		"-blocks-storage.bucket-store.bucket-index.enabled": "false",
		"-querier.cardinality-api-enabled":                  "true",
		"-alertmanager.web.external-url":                    "http://localhost/alertmanager",
		// Use inmemory ring to avoid needing Consul.
		"-ring.store":                          "inmemory",
		"-compactor.ring.store":                "inmemory",
		"-store-gateway.sharding-ring.store":   "inmemory",
		"-store-gateway.sharding-enabled":      "true",
		"-store-gateway.sharding-ring.replication-factor": "1",
	})

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	cortex := e2ecortex.NewSingleBinary("cortex-1", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push multiple series with different metric names and labels.
	now := time.Now()
	series1, _ := generateSeries("test_metric_1", now, prompb.Label{Name: "job", Value: "api"})
	series2, _ := generateSeries("test_metric_2", now, prompb.Label{Name: "job", Value: "worker"})
	series3, _ := generateSeries("test_metric_3", now, prompb.Label{Name: "job", Value: "api"}, prompb.Label{Name: "instance", Value: "host1"})

	for _, s := range [][]prompb.TimeSeries{series1, series2, series3} {
		res, err := c.Push(s)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// --- Test 1: Head path ---
	t.Run("head path returns cardinality data", func(t *testing.T) {
		resp, body, err := c.CardinalityRaw("head", 10, time.Time{}, time.Time{})
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode, "body: %s", string(body))

		var result cardinalityAPIResponse
		require.NoError(t, json.Unmarshal(body, &result))

		assert.Equal(t, "success", result.Status)
		assert.GreaterOrEqual(t, result.Data.NumSeries, uint64(3))
		assert.NotEmpty(t, result.Data.SeriesCountByMetricName, "seriesCountByMetricName should not be empty")
		assert.NotEmpty(t, result.Data.LabelValueCountByLabelName, "labelValueCountByLabelName should not be empty")
		assert.NotEmpty(t, result.Data.SeriesCountByLabelValuePair, "seriesCountByLabelValuePair should not be empty")
	})

	// --- Test 2: Default source (should be head) ---
	t.Run("default source is head", func(t *testing.T) {
		resp, body, err := c.CardinalityRaw("", 10, time.Time{}, time.Time{})
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode, "body: %s", string(body))

		var result cardinalityAPIResponse
		require.NoError(t, json.Unmarshal(body, &result))
		assert.Equal(t, "success", result.Status)
		assert.GreaterOrEqual(t, result.Data.NumSeries, uint64(3))
	})

	// --- Test 3: Parameter validation ---
	t.Run("invalid source returns 400", func(t *testing.T) {
		resp, _, err := c.CardinalityRaw("invalid", 0, time.Time{}, time.Time{})
		require.NoError(t, err)
		assert.Equal(t, 400, resp.StatusCode)
	})

	// --- Test 4: Blocks path ---
	// Push series at timestamps spanning two block ranges to trigger head compaction and shipping.
	t.Run("blocks path returns cardinality data", func(t *testing.T) {
		// Push a series at a timestamp in a different block range to trigger compaction of the first block.
		series4, _ := generateSeries("test_metric_4", now.Add(blockRangePeriod*2),
			prompb.Label{Name: "job", Value: "scheduler"})
		res, err := c.Push(series4)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		// Wait until at least one block is shipped from the ingester.
		require.NoError(t, cortex.WaitSumMetricsWithOptions(
			e2e.Greater(0),
			[]string{"cortex_ingester_shipper_uploads_total"},
			e2e.WaitMissingMetrics,
		))

		// Wait until the store gateway has loaded the shipped blocks.
		require.NoError(t, cortex.WaitSumMetricsWithOptions(
			e2e.Greater(0),
			[]string{"cortex_bucket_store_blocks_loaded"},
			e2e.WaitMissingMetrics,
		))

		// Query the blocks path with a wide time range.
		start := now.Add(-1 * time.Hour)
		end := now.Add(1 * time.Hour)
		resp, body, err := c.CardinalityRaw("blocks", 10, start, end)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode, "body: %s", string(body))

		var result cardinalityAPIResponse
		require.NoError(t, json.Unmarshal(body, &result))

		assert.Equal(t, "success", result.Status)
		// Blocks path should have data from shipped blocks.
		assert.NotEmpty(t, result.Data.LabelValueCountByLabelName,
			fmt.Sprintf("labelValueCountByLabelName should not be empty, full response: %s", string(body)))
	})

	// --- Test 5: Blocks path requires start/end ---
	t.Run("blocks path without start/end returns 400", func(t *testing.T) {
		resp, _, err := c.CardinalityRaw("blocks", 0, time.Time{}, time.Time{})
		require.NoError(t, err)
		assert.Equal(t, 400, resp.StatusCode)
	})
}
