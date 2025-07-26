//go:build requires_docker
// +build requires_docker

package integration

import (
	"compress/gzip"
	"fmt"
	"strings"

	"net/http"
	"net/url"

	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestQuerierResponseCompression(t *testing.T) {
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

	for i := 0; i < 200; i++ {
		series, _ := generateSeries(
			fmt.Sprintf("series_%d", i),
			now,
			prompb.Label{Name: fmt.Sprintf("label_%d", i), Value: strings.Repeat("val_", 10)},
		)
		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	query := `{__name__=~"series_.*"}`
	u := &url.URL{
		Scheme: "http",
		Path:   fmt.Sprintf("%s/api/prom/api/v1/query", querier.HTTPEndpoint()),
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("time", e2ecortex.FormatTime(now))
	u.RawQuery = q.Encode()
	endpoint := u.String()

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

func TestQueryFrontendResponseCompression(t *testing.T) {
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

	// Start the query-frontend.
	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", flags, "")
	require.NoError(t, s.Start(queryFrontend))

	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until both the distributor updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	querier := e2ecortex.NewQuerier("querierWithFrontend", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
	}), "")

	require.NoError(t, s.StartAndWaitReady(querier))
	require.NoError(t, s.WaitReady(queryFrontend))

	now := time.Now()

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	for i := 0; i < 200; i++ {
		series, _ := generateSeries(
			fmt.Sprintf("series_%d", i),
			now,
			prompb.Label{Name: fmt.Sprintf("label_%d", i), Value: strings.Repeat("val_", 10)},
		)
		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	query := `{__name__=~"series_.*"}`
	u := &url.URL{
		Scheme: "http",
		Path:   fmt.Sprintf("%s/api/prom/api/v1/query", queryFrontend.HTTPEndpoint()),
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("time", e2ecortex.FormatTime(now))
	u.RawQuery = q.Encode()
	endpoint := u.String()

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
