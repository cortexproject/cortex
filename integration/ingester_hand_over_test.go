package main

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/framework"
)

func TestIngesterHandOverWithBlocksStorage(t *testing.T) {
	runIngesterHandOverTest(t, BlocksStorage, func(t *testing.T, s *framework.Scenario) {
		// Start dependencies
		require.NoError(t, s.StartMinio())
		require.NoError(t, s.StartConsul())
		require.NoError(t, s.WaitReady("consul", "minio"))

		// Start Cortex components
		require.NoError(t, s.StartIngester("ingester-1", BlocksStorage, ""))
		require.NoError(t, s.StartQuerier("querier", BlocksStorage, ""))
		require.NoError(t, s.StartDistributor("distributor", BlocksStorage, ""))
		require.NoError(t, s.WaitReady("distributor", "querier", "ingester-1"))
	})
}

func TestIngesterHandOverWithChunksStorage(t *testing.T) {
	runIngesterHandOverTest(t, ChunksStorage, func(t *testing.T, s *framework.Scenario) {
		// Start dependencies
		require.NoError(t, s.StartDynamoDB())
		require.NoError(t, s.StartConsul())
		require.NoError(t, s.WaitReady("consul", "dynamodb"))

		// Start Cortex components
		require.NoError(t, s.StartTableManager("table-manager", ChunksStorage, ""))
		require.NoError(t, s.StartIngester("ingester-1", ChunksStorage, ""))
		require.NoError(t, s.StartQuerier("querier", ChunksStorage, ""))
		require.NoError(t, s.StartDistributor("distributor", ChunksStorage, ""))
		require.NoError(t, s.WaitReady("distributor", "querier", "ingester-1", "table-manager"))

		// Wait until the first table-manager sync has completed, so that we're
		// sure the tables have been created
		require.NoError(t, s.Service("table-manager").WaitMetric(80, "cortex_dynamo_sync_tables_seconds", 1))
	})
}

func runIngesterHandOverTest(t *testing.T, flags map[string]string, setup func(t *testing.T, s *framework.Scenario)) {
	s, err := framework.NewScenario()
	require.NoError(t, err)
	defer s.Shutdown()

	// Setup
	setup(t, s)

	// Wait until both the distributor and querier have updated the ring
	require.NoError(t, s.Service("distributor").WaitMetric(80, "cortex_ring_tokens_total", 512))
	require.NoError(t, s.Service("querier").WaitMetric(80, "cortex_ring_tokens_total", 512))

	c, err := framework.NewClient(s.Endpoint("distributor", 80), s.Endpoint("querier", 80), "user-1")
	require.NoError(t, err)

	// Push some series to Cortex
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series
	result, err := c.Query("series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	// Start ingester-2
	require.NoError(t, s.StartIngester("ingester-2", mergeFlags(flags, map[string]string{
		"-ingester.join-after": "10s",
	}), ""))

	// Stop ingester-1. This function will return once the ingester-1 is successfully
	// stopped, which means the transfer to ingester-2 is completed.
	require.NoError(t, s.StopService("ingester-1"))

	// Query the series again
	result, err = c.Query("series_1", now)
	require.NoError(t, err)
	assert.Equal(t, expectedVector, result.(model.Vector))
}
