// +build requires_docker

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

func TestIngesterHandOverWithBlocksStorage(t *testing.T) {
	runIngesterHandOverTest(t, BlocksStorageFlags, func(t *testing.T, s *e2e.Scenario) {
		minio := e2edb.NewMinio(9000, BlocksStorageFlags["-experimental.tsdb.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))
	})
}

func TestIngesterHandOverWithChunksStorage(t *testing.T) {
	runIngesterHandOverTest(t, ChunksStorageFlags, func(t *testing.T, s *e2e.Scenario) {
		dynamo := e2edb.NewDynamoDB()
		require.NoError(t, s.StartAndWaitReady(dynamo))

		require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

		tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorageFlags, "")
		require.NoError(t, s.StartAndWaitReady(tableManager))

		// Wait until the first table-manager sync has completed, so that we're
		// sure the tables have been created.
		require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))
	})
}

func runIngesterHandOverTest(t *testing.T, flags map[string]string, setup func(t *testing.T, s *e2e.Scenario)) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	setup(t, s)

	// Start Cortex components.
	ingester1 := e2ecortex.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags, "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, "")
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, querier, ingester1))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
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

	// Ensure 1st ingester metrics are tracked correctly.
	if flags["-store.engine"] != blocksStorageEngine {
		require.NoError(t, ingester1.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_chunks_created_total"))
	}

	// Start ingester-2.
	ingester2 := e2ecortex.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-ingester.join-after": "10s",
	}), "")
	require.NoError(t, s.Start(ingester2))

	// Stop ingester-1. This function will return once the ingester-1 is successfully
	// stopped, which means the transfer to ingester-2 is completed.
	require.NoError(t, s.Stop(ingester1))

	// Query the series again.
	result, err = c.Query("series_1", now)
	require.NoError(t, err)
	assert.Equal(t, expectedVector, result.(model.Vector))

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester2)
	assertServiceMetricsPrefixes(t, Querier, querier)
}
