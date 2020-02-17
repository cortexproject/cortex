package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
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
	runIngesterHandOverTest(t, BlocksStorage, func(t *testing.T, s *e2e.Scenario) {
		minio := e2edb.NewMinio(9000, BlocksStorage["-experimental.tsdb.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))
	})
}

func TestIngesterHandOverWithChunksStorage(t *testing.T) {
	runIngesterHandOverTest(t, ChunksStorage, func(t *testing.T, s *e2e.Scenario) {
		dynamo := e2edb.NewDynamoDB()
		require.NoError(t, s.StartAndWaitReady(dynamo))

		require.NoError(t, ioutil.WriteFile(
			filepath.Join(s.SharedDir(), cortexSchemaConfigFile),
			[]byte(cortexSchemaConfigYaml),
			os.ModePerm),
		)
		tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorage, "")
		require.NoError(t, s.StartAndWaitReady(tableManager))

		// Wait until the first table-manager sync has completed, so that we're
		// sure the tables have been created.
		require.NoError(t, tableManager.WaitSumMetric("cortex_dynamo_sync_tables_seconds", 1))
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
	ingester1 := e2ecortex.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(networkName), flags, "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(networkName), flags, "")
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(networkName), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, querier, ingester1))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetric("cortex_ring_tokens_total", 512))
	require.NoError(t, querier.WaitSumMetric("cortex_ring_tokens_total", 512))

	c, err := e2ecortex.NewClient(distributor.Endpoint(80), querier.Endpoint(80), "user-1")
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

	// Start ingester-2.
	ingester2 := e2ecortex.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(networkName), mergeFlags(flags, map[string]string{
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
}
