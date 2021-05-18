// +build requires_docker

package integration

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
	// by GitHub Actions too (see .github/workflows/test-build-deploy.yml).
	previousVersionImages = map[string]func(map[string]string) map[string]string{
		"quay.io/cortexproject/cortex:v1.0.0": preCortex14Flags,
		"quay.io/cortexproject/cortex:v1.1.0": preCortex14Flags,
		"quay.io/cortexproject/cortex:v1.2.0": preCortex14Flags,
		"quay.io/cortexproject/cortex:v1.3.0": preCortex14Flags,
		"quay.io/cortexproject/cortex:v1.4.0": preCortex16Flags,
		"quay.io/cortexproject/cortex:v1.5.0": preCortex16Flags,
		"quay.io/cortexproject/cortex:v1.6.0": nil,
		"quay.io/cortexproject/cortex:v1.7.0": nil,
		"quay.io/cortexproject/cortex:v1.8.0": nil,
		"quay.io/cortexproject/cortex:v1.9.0": nil,
	}
)

func preCortex14Flags(flags map[string]string) map[string]string {
	return e2e.MergeFlagsWithoutRemovingEmpty(flags, map[string]string{
		// Blocks storage CLI flags removed the "experimental" prefix in 1.4.
		"-store-gateway.sharding-enabled":                 "",
		"-store-gateway.sharding-ring.store":              "",
		"-store-gateway.sharding-ring.consul.hostname":    "",
		"-store-gateway.sharding-ring.replication-factor": "",
		// Query-scheduler has been introduced in 1.6.0
		"-frontend.scheduler-dns-lookup-period": "",
	})
}

func preCortex16Flags(flags map[string]string) map[string]string {
	return e2e.MergeFlagsWithoutRemovingEmpty(flags, map[string]string{
		// Query-scheduler has been introduced in 1.6.0
		"-frontend.scheduler-dns-lookup-period": "",
	})
}

func TestBackwardCompatibilityWithChunksStorage(t *testing.T) {
	for previousImage, flagsFn := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			flags := ChunksStorageFlags()
			if flagsFn != nil {
				flags = flagsFn(flags)
			}

			runBackwardCompatibilityTestWithChunksStorage(t, previousImage, flags)
		})
	}
}

func TestNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T) {
	for previousImage, flagsFn := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			flags := ChunksStorageFlags()
			if flagsFn != nil {
				flags = flagsFn(flags)
			}

			runNewDistributorsCanPushToOldIngestersWithReplication(t, previousImage, flags)
		})
	}
}

func runBackwardCompatibilityTestWithChunksStorage(t *testing.T, previousImage string, flagsForOldImage map[string]string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(dynamo, consul))

	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

	// Start Cortex table-manager (running on current version since the backward compatibility
	// test is about testing a rolling update of other services).
	tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorageFlags(), "")
	require.NoError(t, s.StartAndWaitReady(tableManager))

	// Wait until the first table-manager sync has completed, so that we're
	// sure the tables have been created.
	require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))

	// Start other Cortex components (ingester running on previous version).
	ingester1 := e2ecortex.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flagsForOldImage, previousImage)
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), ChunksStorageFlags(), "")
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

	ingester2 := e2ecortex.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), mergeFlags(ChunksStorageFlags(), map[string]string{
		"-ingester.join-after": "10s",
	}), "")
	// Start ingester-2 on new version, to ensure the transfer is backward compatible.
	require.NoError(t, s.Start(ingester2))

	// Stop ingester-1. This function will return once the ingester-1 is successfully
	// stopped, which means the transfer to ingester-2 is completed.
	require.NoError(t, s.Stop(ingester1))

	checkQueries(t, consul,
		expectedVector,
		previousImage,
		flagsForOldImage,
		ChunksStorageFlags(),
		now,
		s,
		1,
	)
}

// Check for issues like https://github.com/cortexproject/cortex/issues/2356
func runNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T, previousImage string, flagsForPreviousImage map[string]string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(dynamo, consul))

	flagsForNewImage := mergeFlags(ChunksStorageFlags(), map[string]string{
		"-distributor.replication-factor": "3",
	})

	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

	// Start Cortex table-manager (running on current version since the backward compatibility
	// test is about testing a rolling update of other services).
	tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorageFlags(), "")
	require.NoError(t, s.StartAndWaitReady(tableManager))

	// Wait until the first table-manager sync has completed, so that we're
	// sure the tables have been created.
	require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))

	// Start other Cortex components (ingester running on previous version).
	ingester1 := e2ecortex.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	ingester2 := e2ecortex.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	ingester3 := e2ecortex.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flagsForNewImage, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(1536), "cortex_ring_tokens_total"))

	// Push some series to Cortex.
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	checkQueries(t, consul,
		expectedVector,
		previousImage,
		flagsForPreviousImage,
		flagsForNewImage,
		now,
		s,
		3,
	)
}

func checkQueries(
	t *testing.T,
	consul *e2e.HTTPService,
	expectedVector model.Vector,
	previousImage string,
	flagsForOldImage, flagsForNewImage map[string]string,
	now time.Time,
	s *e2e.Scenario,
	numIngesters int,
) {
	cases := map[string]struct {
		queryFrontendImage string
		queryFrontendFlags map[string]string
		querierImage       string
		querierFlags       map[string]string
	}{
		"old query-frontend, new querier": {
			queryFrontendImage: previousImage,
			queryFrontendFlags: flagsForOldImage,
			querierImage:       "",
			querierFlags:       flagsForNewImage,
		},
		"new query-frontend, old querier": {
			queryFrontendImage: "",
			queryFrontendFlags: flagsForNewImage,
			querierImage:       previousImage,
			querierFlags:       flagsForOldImage,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// Start query-frontend.
			queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", c.queryFrontendFlags, c.queryFrontendImage)
			require.NoError(t, s.Start(queryFrontend))
			defer func() {
				require.NoError(t, s.Stop(queryFrontend))
			}()

			// Start querier.
			querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), e2e.MergeFlagsWithoutRemovingEmpty(c.querierFlags, map[string]string{
				"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
			}), c.querierImage)

			require.NoError(t, s.Start(querier))
			defer func() {
				require.NoError(t, s.Stop(querier))
			}()

			// Wait until querier and query-frontend are ready, and the querier has updated the ring.
			require.NoError(t, s.WaitReady(querier, queryFrontend))
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(float64(numIngesters*512)), "cortex_ring_tokens_total"))

			// Query the series.
			for _, endpoint := range []string{queryFrontend.HTTPEndpoint(), querier.HTTPEndpoint()} {
				c, err := e2ecortex.NewClient("", endpoint, "", "", "user-1")
				require.NoError(t, err)

				result, err := c.Query("series_1", now)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVector, result.(model.Vector))
			}
		})
	}
}
