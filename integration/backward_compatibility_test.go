// +build requires_docker

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

var (
	// If you change the image tag, remember to update it in the preloading done
	// by CircleCI too (see .circleci/config.yml).
	previousVersionImages = []string{
		// 0.6.0 used 204 status code for querier and ingester
		// distributor didn't have /ready page, and we used check on the /ring page instead
		"quay.io/cortexproject/cortex:v0.6.0",

		// 0.7.0 used 204 status code for all components
		"quay.io/cortexproject/cortex:v0.7.0",
	}
)

func TestBackwardCompatibilityWithChunksStorage(t *testing.T) {
	for _, previousImage := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			runBackwardCompatibilityTestWithChunksStorage(t, previousImage)
		})
	}
}

func runBackwardCompatibilityTestWithChunksStorage(t *testing.T, previousImage string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(dynamo, consul))

	flagsForOldImage := mergeFlags(ChunksStorageFlags, map[string]string{
		"-schema-config-file": "",
		"-config-yaml":        ChunksStorageFlags["-schema-config-file"],
	})

	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

	// Start Cortex table-manager (running on current version since the backward compatibility
	// test is about testing a rolling update of other services).
	tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorageFlags, "")
	require.NoError(t, s.StartAndWaitReady(tableManager))

	// Wait until the first table-manager sync has completed, so that we're
	// sure the tables have been created.
	require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))

	// Start other Cortex components (ingester running on previous version).
	ingester1 := e2ecortex.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flagsForOldImage, previousImage)
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flagsForOldImage, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push some series to Cortex.
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	ingester2 := e2ecortex.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), mergeFlags(ChunksStorageFlags, map[string]string{
		"-ingester.join-after": "10s",
	}), "")
	// Start ingester-2 on new version, to ensure the transfer is backward compatible.
	require.NoError(t, s.Start(ingester2))

	// Stop ingester-1. This function will return once the ingester-1 is successfully
	// stopped, which means the transfer to ingester-2 is completed.
	require.NoError(t, s.Stop(ingester1))

	// Query the new ingester both with the old and the new querier.
	for _, image := range []string{previousImage, ""} {
		var querier *e2ecortex.CortexService

		if image == previousImage {
			querier = e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flagsForOldImage, image)
		} else {
			querier = e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), ChunksStorageFlags, image)
		}

		require.NoError(t, s.StartAndWaitReady(querier))

		// Wait until the querier has updated the ring.
		require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

		// Query the series
		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
		require.NoError(t, err)

		result, err := c.Query("series_1", now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))

		// Stop the querier, so that the test on the next image will work.
		require.NoError(t, s.Stop(querier))
	}
}
