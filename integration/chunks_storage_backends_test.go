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

func TestChunksStorageAllIndexBackends(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	bigtable := e2edb.NewBigtable()
	cassandra := e2edb.NewCassandra()

	stores := []string{"aws-dynamo", "bigtable", "cassandra"}
	perStoreDuration := 14 * 24 * time.Hour

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(cassandra, dynamo, bigtable, consul))

	// lets build config for each type of Index Store.
	now := time.Now()
	oldestStoreStartTime := now.Add(time.Duration(-len(stores)) * perStoreDuration)

	storeConfigs := make([]storeConfig, len(stores))
	for i, store := range stores {
		storeConfigs[i] = storeConfig{From: oldestStoreStartTime.Add(time.Duration(i) * perStoreDuration).Format("2006-01-02"), IndexStore: store}
	}

	storageFlags := mergeFlags(ChunksStorageFlags, map[string]string{
		"-cassandra.addresses": cassandra.NetworkHTTPEndpoint(),
		"-cassandra.keyspace":  "tests", // keyspace gets created on startup if it does not exist
	})

	// bigtable client needs to set an environment variable when connecting to an emulator
	bigtableFlag := map[string]string{"BIGTABLE_EMULATOR_HOST": bigtable.NetworkHTTPEndpoint()}

	// here we are starting and stopping table manager for each index store
	// this is a workaround to make table manager create tables for each config since it considers only latest schema config while creating tables
	for i := range storeConfigs {
		require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(buildSchemaConfigWith(storeConfigs[i:i+1]))))

		tableManager := e2ecortex.NewTableManager("table-manager", mergeFlags(storageFlags, map[string]string{
			"-table-manager.retention-period": "2520h", // setting retention high enough
		}), "")
		tableManager.HTTPService.SetEnvVars(bigtableFlag)
		require.NoError(t, s.StartAndWaitReady(tableManager))

		// Wait until the first table-manager sync has completed, so that we're
		// sure the tables have been created.
		require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))
		require.NoError(t, s.Stop(tableManager))
	}

	// Start rest of the Cortex components.
	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(buildSchemaConfigWith(storeConfigs))))

	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), mergeFlags(storageFlags, map[string]string{
		"-ingester.retain-period": "0s", // we want to make ingester not retain any chunks in memory after they are flushed so that queries get data only from the store
	}), "")
	ingester.HTTPService.SetEnvVars(bigtableFlag)

	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), storageFlags, "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), storageFlags, "")
	querier.HTTPService.SetEnvVars(bigtableFlag)

	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push and Query some series from Cortex for each day starting from oldest start time from configs until now so that we test all the Index Stores
	for ts := oldestStoreStartTime; ts.Before(now); ts = ts.Add(24 * time.Hour) {
		series, expectedVector := generateSeries("series_1", ts)

		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		// lets make ingester flush the chunks immediately to the store
		res, err = e2e.GetRequest("http://" + ingester.HTTPEndpoint() + "/flush")
		require.NoError(t, err)
		require.Equal(t, 204, res.StatusCode)

		// lets wait till ingester has no chunks in memory
		require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_memory_chunks"))

		// Query back the series.
		result, err := client.Query("series_1", ts)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
}
