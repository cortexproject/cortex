//go:build requires_docker
// +build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestParseQueryAPIQuerier(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-auth.enabled": "true",
	})

	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	endpoint := fmt.Sprintf("http://%s/api/prom/api/v1/parse_query?query=foo/bar", querier.HTTPEndpoint())

	req, err := http.NewRequest("GET", endpoint, nil)
	require.NoError(t, err)
	req.Header.Set("X-Scope-OrgID", "user-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var parsed struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &parsed))
	require.Equal(t, "success", parsed.Status)

	// check for AST contents.
	require.Contains(t, string(parsed.Data), "\"op\":\"/\"")
	require.Contains(t, string(parsed.Data), `"lhs":{"matchers":[{"name":"__name__","type":"=","value":"foo"}]`)
	require.Contains(t, string(parsed.Data), `"rhs":{"matchers":[{"name":"__name__","type":"=","value":"bar"}]`)
}

func TestParseQueryAPIQueryFrontend(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-auth.enabled": "true",
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

	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	endpoint := fmt.Sprintf("http://%s/api/prom/api/v1/parse_query?query=foo/bar", queryFrontend.HTTPEndpoint())

	req, err := http.NewRequest("GET", endpoint, nil)
	require.NoError(t, err)
	req.Header.Set("X-Scope-OrgID", "user-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var parsed struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &parsed))
	require.Equal(t, "success", parsed.Status)

	// check for AST contents.
	require.Contains(t, string(parsed.Data), "\"op\":\"/\"")
	require.Contains(t, string(parsed.Data), `"lhs":{"matchers":[{"name":"__name__","type":"=","value":"foo"}]`)
	require.Contains(t, string(parsed.Data), `"rhs":{"matchers":[{"name":"__name__","type":"=","value":"bar"}]`)
}
