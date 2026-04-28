//go:build requires_docker

package integration

import (
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
)

func TestGRPCSnappyCompression(t *testing.T) {
	t.Run("ingester client", func(t *testing.T) {
		s, err := e2e.NewScenario(networkName)
		require.NoError(t, err)
		defer s.Close()

		flags := mergeFlags(BlocksStorageFlags(), map[string]string{
			"-distributor.replication-factor":   "1",
			"-ingester.client.grpc-compression": "snappy",
		})

		consul := e2edb.NewConsul()
		minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(consul, minio))

		distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		ingester := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

		require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
		require.NoError(t, err)

		now := time.Now()
		series, expectedVector := generateSeries("series_1", now, prompb.Label{Name: "foo", Value: "bar"})

		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		result, err := c.Query("series_1", now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	})

	t.Run("store gateway client", func(t *testing.T) {
		const blockRangePeriod = 5 * time.Second

		s, err := e2e.NewScenario(networkName)
		require.NoError(t, err)
		defer s.Close()

		flags := mergeFlags(BlocksStorageFlags(), map[string]string{
			"-distributor.replication-factor":                "1",
			"-blocks-storage.tsdb.block-ranges-period":       blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":             "1s",
			"-blocks-storage.bucket-store.sync-interval":     "1s",
			"-blocks-storage.tsdb.retention-period":          ((blockRangePeriod * 2) - 1).String(),
			"-store-gateway.sharding-enabled":                "false",
			"-querier.store-gateway-client.grpc-compression": "snappy",
		})

		consul := e2edb.NewConsul()
		minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(consul, minio))

		distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		ingester := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway))

		querierFlags := mergeFlags(flags, map[string]string{
			"-querier.store-gateway-addresses": storeGateway.NetworkGRPCEndpoint(),
		})
		querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), querierFlags, "")
		require.NoError(t, s.StartAndWaitReady(querier))

		require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
		require.NoError(t, err)

		// Push two series far enough apart to trigger TSDB head compaction and block shipping.
		series1Timestamp := time.Now()
		series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
		series1, expectedVector1 := generateSeries("series_1", series1Timestamp, prompb.Label{Name: "foo", Value: "bar"})
		series2, _ := generateSeries("series_2", series2Timestamp, prompb.Label{Name: "foo", Value: "baz"})

		res, err := c.Push(series1)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		res, err = c.Push(series2)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		// Wait until the TSDB head is shipped to storage and the store-gateway picks it up.
		require.NoError(t, ingester.WaitSumMetrics(e2e.Greater(0), "cortex_ingester_shipper_uploads_total"))
		require.NoError(t, storeGateway.WaitSumMetrics(e2e.Greater(0), "cortex_storegateway_bucket_sync_total"))

		// Query the first series — this goes through the store-gateway with snappy compression.
		result, err := c.Query("series_1", series1Timestamp)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector1, result.(model.Vector))
	})
}
