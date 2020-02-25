package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
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
		require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_dynamo_sync_tables_seconds"))
	})
}

func runQueryFrontendTest(t *testing.T, flags map[string]string, setup func(t *testing.T, s *e2e.Scenario)) {
	const numUsers = 10
	const numQueriesPerUser = 10

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	setup(t, s)

	// Start Cortex components.
	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", flags, "")
	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, "")
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(queryFrontend, distributor, ingester))

	// Start the querier after the query-frontend otherwise we're not
	// able to get the query-frontend network endpoint.
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkEndpoint(e2ecortex.GRPCPort),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Cortex.
	now := time.Now()
	expectedVectors := make([]model.Vector, numUsers)

	for u := 0; u < numUsers; u++ {
		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", fmt.Sprintf("user-%d", u))
		require.NoError(t, err)

		var series []prompb.TimeSeries
		series, expectedVectors[u] = generateSeries("series_1", now)

		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// Query the series for each user in parallel.
	wg := sync.WaitGroup{}
	wg.Add(numUsers * numQueriesPerUser)

	for u := 0; u < numUsers; u++ {
		userID := u

		c, err := e2ecortex.NewClient("", queryFrontend.HTTPEndpoint(), "", fmt.Sprintf("user-%d", userID))
		require.NoError(t, err)

		for q := 0; q < numQueriesPerUser; q++ {
			go func() {
				defer wg.Done()

				result, err := c.Query("series_1", now)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVectors[userID], result.(model.Vector))
			}()
		}
	}

	wg.Wait()
}
