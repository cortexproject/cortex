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

func TestQueryFrontendWithBlocksStorage(t *testing.T) {
	runQueryFrontendTest(t, BlocksStorage, func(t *testing.T, s *e2e.Scenario) {
		minio := e2edb.NewMinio(9000, BlocksStorage["-experimental.tsdb.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))
	})
}

func TestQueryFrontendWithChunksStorage(t *testing.T) {
	runQueryFrontendTest(t, ChunksStorage, func(t *testing.T, s *e2e.Scenario) {
		dynamo := e2edb.NewDynamoDB()
		require.NoError(t, s.StartAndWaitReady(dynamo))

		require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

		tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorage, "")
		require.NoError(t, s.StartAndWaitReady(tableManager))

		// Wait until the first table-manager sync has completed, so that we're
		// sure the tables have been created.
		require.NoError(t, tableManager.WaitSumMetric("cortex_dynamo_sync_tables_seconds", 1))
	})
}

func runQueryFrontendTest(t *testing.T, flags map[string]string, setup func(t *testing.T, s *e2e.Scenario)) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	setup(t, s)

	// Start Cortex components.
	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", flags, "")
	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(networkName), flags, "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(networkName), mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkEndpoint(networkName, e2ecortex.GRPCPort),
	}), "")
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(networkName), flags, "")
	require.NoError(t, s.StartAndWaitReady(queryFrontend, distributor, querier, ingester))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetric("cortex_ring_tokens_total", 512))
	require.NoError(t, querier.WaitSumMetric("cortex_ring_tokens_total", 512))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	// Push some series to Cortex.
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series.
	result, err := c.Query("series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))
}
