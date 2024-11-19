//go:build requires_docker
// +build requires_docker

package integration

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestDistriubtorAcceptMixedHASamplesRunningInMicroservicesMode(t *testing.T) {
	const blockRangePeriod = 5 * time.Minute

	t.Run("Distributor accept mixed HA samples in the same request", func(t *testing.T) {
		s, err := e2e.NewScenario(networkName)
		require.NoError(t, err)
		defer s.Close()

		// Start dependencies.
		consul := e2edb.NewConsul()
		etcd := e2edb.NewETCD()
		minio := e2edb.NewMinio(9000, bucketName)
		require.NoError(t, s.StartAndWaitReady(consul, etcd, minio))

		// Configure the querier to only look in ingester
		// and enbale distributor ha tracker with mixed samples.
		distributorFlags := map[string]string{
			"-distributor.ha-tracker.enable":                        "true",
			"-distributor.ha-tracker.enable-for-all-users":          "true",
			"-experimental.distributor.ha-tracker.mixed-ha-samples": "true",
			"-distributor.ha-tracker.cluster":                       "cluster",
			"-distributor.ha-tracker.replica":                       "__replica__",
			"-distributor.ha-tracker.store":                         "etcd",
			"-distributor.ha-tracker.etcd.endpoints":                "etcd:2379",
		}
		querierFlags := mergeFlags(BlocksStorageFlags(), map[string]string{
			"-querier.query-store-after": (1 * time.Hour).String(),
		})
		flags := mergeFlags(BlocksStorageFlags(), map[string]string{
			"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":                "5s",
			"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
			"-blocks-storage.bucket-store.max-chunk-pool-bytes": "1",
		})

		// Start Cortex components.
		distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), distributorFlags, "")
		ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		require.NoError(t, s.StartAndWaitReady(distributor, ingester))

		// Wait until both the distributor and ingester have updated the ring.
		require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

		distributorClient, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
		require.NoError(t, err)

		// Push some series to Cortex.
		series1Timestamp := time.Now()
		series2Timestamp := series1Timestamp.Add(-2 * time.Second)
		series3Timestamp := series1Timestamp.Add(-4 * time.Second)
		series4Timestamp := series1Timestamp.Add(-6 * time.Second)
		series5Timestamp := series1Timestamp.Add(-8 * time.Second)
		series6Timestamp := series1Timestamp.Add(-10 * time.Second)
		series7Timestamp := series1Timestamp.Add(-12 * time.Second)
		series1, _ := generateSeries("foo", series1Timestamp, prompb.Label{Name: "__replica__", Value: "replica0"}, prompb.Label{Name: "cluster", Value: "cluster0"})
		series2, _ := generateSeries("foo", series2Timestamp, prompb.Label{Name: "__replica__", Value: "replica1"}, prompb.Label{Name: "cluster", Value: "cluster0"})
		series3, _ := generateSeries("foo", series3Timestamp, prompb.Label{Name: "__replica__", Value: "replica0"}, prompb.Label{Name: "cluster", Value: "cluster1"})
		series4, _ := generateSeries("foo", series4Timestamp, prompb.Label{Name: "__replica__", Value: "replica1"}, prompb.Label{Name: "cluster", Value: "cluster1"})
		series5, _ := generateSeries("foo", series5Timestamp, prompb.Label{Name: "__replica__", Value: "replicaNoCluster"})
		series6, _ := generateSeries("foo", series6Timestamp, prompb.Label{Name: "cluster", Value: "clusterNoReplica"})
		series7, _ := generateSeries("foo", series7Timestamp, prompb.Label{Name: "other", Value: "label"})

		res, err := distributorClient.Push([]prompb.TimeSeries{series1[0], series2[0], series3[0], series4[0], series5[0], series6[0], series7[0]})
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		// Wait until the samples have been deduped.
		require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(2), "cortex_distributor_deduped_samples_total"))
		require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(3), "cortex_distributor_non_ha_samples_received_total"))

		// Start the querier and store-gateway, and configure them to frequently sync blocks fast enough to trigger consistency check.
		storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(querierFlags, flags), "")
		require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

		// Wait until the querier and store-gateway have updated the ring, and wait until the blocks are old enough for consistency check
		require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
		require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

		// Query back the series.
		querierClient, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
		require.NoError(t, err)

		// Query back the series (only in the ingesters).
		result, err := querierClient.Query("foo[5m]", series1Timestamp)
		require.NoError(t, err)

		require.Equal(t, model.ValMatrix, result.Type())
		m := result.(model.Matrix)
		require.Equal(t, 5, m.Len())
		numValidHA := 0
		numNonHA := 0
		for _, ss := range m {
			replicaLabel, okReplica := ss.Metric["__replica__"]
			if okReplica {
				require.Equal(t, string(replicaLabel), "replicaNoCluster")
			}
			clusterLabel, okCluster := ss.Metric["cluster"]
			if okCluster {
				require.Equal(t, string(clusterLabel) == "cluster1" || string(clusterLabel) == "cluster0" || string(clusterLabel) == "clusterNoReplica", true)
				if clusterLabel == "cluster1" || clusterLabel == "cluster0" {
					numValidHA++
				}
			}
			if (okReplica && !okCluster && replicaLabel == "replicaNoCluster") || (okCluster && !okReplica && clusterLabel == "clusterNoReplica") || (!okCluster && !okReplica) {
				numNonHA++
			}
			require.NotEmpty(t, ss.Values)
			for _, v := range ss.Values {
				require.NotEmpty(t, v)
			}
		}
		require.Equal(t, numNonHA, 3)
		require.Equal(t, numValidHA, 2)

		// Ensure no service-specific metrics prefix is used by the wrong service.
		assertServiceMetricsPrefixes(t, Distributor, distributor)
		assertServiceMetricsPrefixes(t, Ingester, ingester)
		assertServiceMetricsPrefixes(t, StoreGateway, storeGateway)
		assertServiceMetricsPrefixes(t, Querier, querier)
	})
}
