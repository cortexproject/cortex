//go:build requires_docker
// +build requires_docker

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestBlocksStorageWithEtcd(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period":      "1h",
		"-blocks-storage.tsdb.head-compaction-interval": "1m",
		"-store-gateway.sharding-enabled":               "true",
		"-querier.ingester-streaming":                   "true",
	})

	// Start dependencies.
	etcd := e2edb.NewETCD()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(etcd, minio))

	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreEtcd, etcd.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreEtcd, etcd.NetworkHTTPEndpoint(), flags, "")
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreEtcd, etcd.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway))

	// Wait until the distributor and the store-gateway have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Sharding is disabled, pass gateway address.
	querierFlags := mergeFlags(flags, map[string]string{
		// "-querier.store-gateway-addresses": strings.Join([]string{storeGateway.NetworkGRPCEndpoint()}, ","),
		"-blocks-storage.bucket-store.sync-interval": "1m",
		// "-distributor.shard-by-all-labels": "true",
	})
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreEtcd, etcd.NetworkHTTPEndpoint(), querierFlags, "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the distributor, the querier and the store-gateway have updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(1024), "cortex_ring_tokens_total"))

	// Push a series for each user to Cortex.
	now := time.Now()

	distClient, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	series, _ := generateSeries("series_1", now)

	res, err := distClient.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	queClient, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	_, err = queClient.Query("series_1", now)
	require.NoError(t, err)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
}
