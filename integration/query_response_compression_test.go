//go:build requires_docker
// +build requires_docker

package integration

import (
	"compress/gzip"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestQueryResponseCompression(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-api.response-compression-enabled": "true",
	})

	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push series to Cortex.
	now := time.Now()
	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	series, _ := generateSeries("series_1", now)
	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(storeGateway, querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))

	endpoint := fmt.Sprintf("http://%s/api/prom/api/v1/query?query=series_1", querier.HTTPEndpoint())

	t.Run("Compressed", func(t *testing.T) {
		req, err := http.NewRequest("GET", endpoint, nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user-1")
		req.Header.Set("Accept-Encoding", "gzip")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))

		gzipReader, err := gzip.NewReader(resp.Body)
		require.NoError(t, err)
		defer gzipReader.Close()
	})

	t.Run("Uncompressed", func(t *testing.T) {
		req, err := http.NewRequest("GET", endpoint, nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user-1")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Empty(t, resp.Header.Get("Content-Encoding"))
	})
}
