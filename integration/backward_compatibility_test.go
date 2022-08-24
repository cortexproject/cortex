//go:build requires_docker
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
		"quay.io/cortexproject/cortex:v1.6.0":  preCortex110Flags,
		"quay.io/cortexproject/cortex:v1.7.0":  preCortex110Flags,
		"quay.io/cortexproject/cortex:v1.8.0":  preCortex110Flags,
		"quay.io/cortexproject/cortex:v1.9.0":  preCortex110Flags,
		"quay.io/cortexproject/cortex:v1.10.0": nil,
	}
)

func preCortex110Flags(flags map[string]string) map[string]string {
	return e2e.MergeFlagsWithoutRemovingEmpty(flags, map[string]string{
		// Store-gateway "wait ring stability" has been introduced in 1.10.0
		"-store-gateway.sharding-ring.wait-stability-min-duration": "",
		"-store-gateway.sharding-ring.wait-stability-max-duration": "",
	})
}

func TestBackwardCompatibilityWithBlocksStorage(t *testing.T) {
	for previousImage, flagsFn := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			flags := blocksStorageFlagsWithFlushOnShutdown()
			if flagsFn != nil {
				flags = flagsFn(flags)
			}

			runBackwardCompatibilityTestWithBlocksStorage(t, previousImage, flags)
		})
	}
}

func TestNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T) {
	for previousImage, flagsFn := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			flags := blocksStorageFlagsWithFlushOnShutdown()
			if flagsFn != nil {
				flags = flagsFn(flags)
			}

			runNewDistributorsCanPushToOldIngestersWithReplication(t, previousImage, flags)
		})
	}
}

func blocksStorageFlagsWithFlushOnShutdown() map[string]string {
	return mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.flush-blocks-on-shutdown": "true",
	})
}

func runBackwardCompatibilityTestWithBlocksStorage(t *testing.T, previousImage string, flagsForOldImage map[string]string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, flagsForOldImage["-blocks-storage.s3.bucket-name"])
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(minio, consul))

	flagsForNewImage := blocksStorageFlagsWithFlushOnShutdown()

	// Start other Cortex components (ingester running on previous version).
	ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flagsForOldImage, previousImage)
	distributor := e2ecortex.NewDistributor("distributor", "consul", consul.NetworkHTTPEndpoint(), flagsForNewImage, "")
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

	ingester2 := e2ecortex.NewIngester("ingester-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flagsForNewImage, map[string]string{
		"-ingester.join-after": "10s",
	}), "")
	require.NoError(t, s.Start(ingester2))

	// Stop ingester-1. This function will return once the ingester-1 is successfully
	// stopped, which means it has uploaded all its data to the object store.
	require.NoError(t, s.Stop(ingester1))

	checkQueries(t, consul,
		expectedVector,
		previousImage,
		flagsForOldImage,
		flagsForNewImage,
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
	minio := e2edb.NewMinio(9000, flagsForPreviousImage["-blocks-storage.s3.bucket-name"])
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(minio, consul))

	flagsForNewImage := mergeFlags(blocksStorageFlagsWithFlushOnShutdown(), map[string]string{
		"-distributor.replication-factor": "3",
	})

	// Start other Cortex components (ingester running on previous version).
	ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	ingester2 := e2ecortex.NewIngester("ingester-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	ingester3 := e2ecortex.NewIngester("ingester-3", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	distributor := e2ecortex.NewDistributor("distributor", "consul", consul.NetworkHTTPEndpoint(), flagsForNewImage, "")
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
		storeGatewayImage  string
		storeGatewayFlags  map[string]string
	}{
		"old query-frontend, new querier, old store-gateway": {
			queryFrontendImage: previousImage,
			queryFrontendFlags: flagsForOldImage,
			querierImage:       "",
			querierFlags:       flagsForNewImage,
			storeGatewayImage:  previousImage,
			storeGatewayFlags:  flagsForOldImage,
		},
		"new query-frontend, old querier, new store-gateway": {
			queryFrontendImage: "",
			queryFrontendFlags: flagsForNewImage,
			querierImage:       previousImage,
			querierFlags:       flagsForOldImage,
			storeGatewayImage:  "",
			storeGatewayFlags:  flagsForNewImage,
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
			querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), e2e.MergeFlagsWithoutRemovingEmpty(c.querierFlags, map[string]string{
				"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
			}), c.querierImage)

			require.NoError(t, s.Start(querier))
			defer func() {
				require.NoError(t, s.Stop(querier))
			}()

			// Start store gateway.
			storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), c.storeGatewayFlags, c.storeGatewayImage)

			require.NoError(t, s.Start(storeGateway))
			defer func() {
				require.NoError(t, s.Stop(storeGateway))
			}()

			// Wait until querier and query-frontend are ready, and the querier has updated the ring.
			require.NoError(t, s.WaitReady(querier, queryFrontend, storeGateway))
			expectedTokens := float64((numIngesters + 1) * 512) // Ingesters and Store Gateway.
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(expectedTokens), "cortex_ring_tokens_total"))

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
