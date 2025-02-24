//go:build integration_backward_compatibility
// +build integration_backward_compatibility

package integration

import (
	"fmt"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

type versionsImagesFlags struct {
	flagsForOldImage func(map[string]string) map[string]string
	flagsForNewImage func(map[string]string) map[string]string
}

var (
	// If you change the image tag, remember to update it in the preloading done
	// by GitHub Actions too (see .github/workflows/test-build-deploy.yml).
	previousVersionImages = map[string]*versionsImagesFlags{
		"quay.io/cortexproject/cortex:v1.13.1": {
			flagsForOldImage: func(m map[string]string) map[string]string {
				m["-ingester.stream-chunks-when-using-blocks"] = "true"
				return m
			},
			flagsForNewImage: func(m map[string]string) map[string]string {
				m["-ingester.client.grpc-compression"] = "snappy"
				return m
			},
		},
		"quay.io/cortexproject/cortex:v1.13.2": {
			flagsForOldImage: func(m map[string]string) map[string]string {
				m["-ingester.stream-chunks-when-using-blocks"] = "true"
				return m
			},
			flagsForNewImage: func(m map[string]string) map[string]string {
				m["-ingester.client.grpc-compression"] = "snappy"
				return m
			},
		},
		"quay.io/cortexproject/cortex:v1.14.0": {
			flagsForOldImage: func(m map[string]string) map[string]string {
				return m
			},
			flagsForNewImage: func(m map[string]string) map[string]string {
				m["-ingester.client.grpc-compression"] = "snappy"
				return m
			},
		},
		"quay.io/cortexproject/cortex:v1.14.1": {
			flagsForOldImage: func(m map[string]string) map[string]string {
				return m
			},
			flagsForNewImage: func(m map[string]string) map[string]string {
				m["-ingester.client.grpc-compression"] = "snappy"
				return m
			},
		},
		"quay.io/cortexproject/cortex:v1.15.0": nil,
		"quay.io/cortexproject/cortex:v1.15.1": nil,
		"quay.io/cortexproject/cortex:v1.15.2": nil,
		"quay.io/cortexproject/cortex:v1.15.3": nil,
		"quay.io/cortexproject/cortex:v1.16.0": nil,
		"quay.io/cortexproject/cortex:v1.16.1": nil,
		"quay.io/cortexproject/cortex:v1.17.0": nil,
		"quay.io/cortexproject/cortex:v1.17.1": nil,
		"quay.io/cortexproject/cortex:v1.18.0": nil,
		"quay.io/cortexproject/cortex:v1.18.1": nil,
	}
)

func TestBackwardCompatibilityWithBlocksStorage(t *testing.T) {
	for previousImage, imagesFlags := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			flags := blocksStorageFlagsWithFlushOnShutdown()
			var flagsForNewImage func(map[string]string) map[string]string
			if imagesFlags != nil {
				if imagesFlags.flagsForOldImage != nil {
					flags = imagesFlags.flagsForOldImage(flags)
				}

				if imagesFlags.flagsForNewImage != nil {
					flagsForNewImage = imagesFlags.flagsForNewImage
				}
			}

			runBackwardCompatibilityTestWithBlocksStorage(t, previousImage, flags, flagsForNewImage)
		})
	}
}

func TestNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T) {
	for previousImage, imagesFlags := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			flags := blocksStorageFlagsWithFlushOnShutdown()
			var flagsForNewImage func(map[string]string) map[string]string
			if imagesFlags != nil {
				if imagesFlags.flagsForOldImage != nil {
					flags = imagesFlags.flagsForOldImage(flags)
				}

				if imagesFlags.flagsForNewImage != nil {
					flagsForNewImage = imagesFlags.flagsForNewImage
				}
			}

			runNewDistributorsCanPushToOldIngestersWithReplication(t, previousImage, flags, flagsForNewImage)
		})
	}
}

// Test cortex which uses Prometheus v3.x can support holt_winters function
func TestCanSupportHoltWintersFunc(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                     blocksStorageEngine,
			"-blocks-storage.backend":                           "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":     "4m",
			"-blocks-storage.tsdb.block-ranges-period":          "2h",
			"-blocks-storage.tsdb.ship-interval":                "1h",
			"-blocks-storage.bucket-store.sync-interval":        "15m",
			"-blocks-storage.tsdb.retention-period":             "2h",
			"-blocks-storage.bucket-store.index-cache.backend":  tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.bucket-store.bucket-index.enabled": "true",
			"-querier.query-store-for-labels-enabled":           "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
			// enable experimental promQL funcs
			"-querier.enable-promql-experimental-functions": "true",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path := path.Join(s.SharedDir(), "cortex-1")

	flags = mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path})
	// Start Cortex replicas.
	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	// Push some series to Cortex.
	start := now.Add(-time.Minute * 120)
	end := now
	scrapeInterval := 30 * time.Second

	numSeries := 10
	numSamples := 240
	serieses := make([]prompb.TimeSeries, numSeries)
	lbls := make([]labels.Labels, numSeries)
	for i := 0; i < numSeries; i++ {
		series := e2e.GenerateSeriesWithSamples("test_series", start, scrapeInterval, i*numSamples, numSamples, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "series", Value: strconv.Itoa(i)})
		serieses[i] = series

		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}

	res, err := c.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Test holt_winters range query
	hQuery := "holt_winters(test_series[2h], 0.5, 0.5)"
	hRangeResult, err := c.QueryRange(hQuery, start, end, scrapeInterval)
	require.NoError(t, err)
	hMatrix, ok := hRangeResult.(model.Matrix)
	require.True(t, ok)
	require.True(t, hMatrix.Len() > 0)

	// Test holt_winters instant query
	hInstantResult, err := c.Query(hQuery, now)
	require.NoError(t, err)
	hVector, ok := hInstantResult.(model.Vector)
	require.True(t, ok)
	require.True(t, hVector.Len() > 0)

	// Test double_exponential_smoothing range query
	dQuery := "double_exponential_smoothing(test_series[2h], 0.5, 0.5)"
	dRangeResult, err := c.QueryRange(dQuery, start, end, scrapeInterval)
	require.NoError(t, err)
	dMatrix, ok := dRangeResult.(model.Matrix)
	require.True(t, ok)
	require.True(t, dMatrix.Len() > 0)

	// Test double_exponential_smoothing instant query
	dInstantResult, err := c.Query(dQuery, now)
	require.NoError(t, err)
	dVector, ok := dInstantResult.(model.Vector)
	require.True(t, ok)
	require.True(t, dVector.Len() > 0)

	// compare lengths of query results
	require.True(t, hMatrix.Len() == dMatrix.Len())
	require.True(t, hVector.Len() == dVector.Len())
}

func blocksStorageFlagsWithFlushOnShutdown() map[string]string {
	return mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.flush-blocks-on-shutdown": "true",
	})
}

func runBackwardCompatibilityTestWithBlocksStorage(t *testing.T, previousImage string, flagsForOldImage map[string]string, flagsForNewImageFn func(map[string]string) map[string]string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, flagsForOldImage["-blocks-storage.s3.bucket-name"])
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(minio, consul))

	flagsForNewImage := blocksStorageFlagsWithFlushOnShutdown()

	if flagsForNewImageFn != nil {
		flagsForNewImage = flagsForNewImageFn(flagsForNewImage)
	}

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
func runNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T, previousImage string, flagsForPreviousImage map[string]string, flagsForNewImageFn func(map[string]string) map[string]string) {
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

	if flagsForNewImageFn != nil {
		flagsForNewImage = flagsForNewImageFn(flagsForNewImage)
	}

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
			// Start store gateway.
			storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), c.storeGatewayFlags, c.storeGatewayImage)

			require.NoError(t, s.Start(storeGateway))
			defer func() {
				require.NoError(t, s.Stop(storeGateway))
			}()

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

			// Wait until querier and query-frontend are ready, and the querier has updated the ring.
			require.NoError(t, s.WaitReady(querier, queryFrontend, storeGateway))
			expectedTokens := float64((numIngesters + 1) * 512) // Ingesters and Store Gateway.
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(expectedTokens), "cortex_ring_tokens_total"))
			require.NoError(t, storeGateway.WaitSumMetrics(e2e.Greater(0), "cortex_storegateway_bucket_sync_total"))

			// Wait store-gateways and ingesters appears on querier ring
			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "store-gateway-client"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"),
			)))

			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"),
			)))

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
