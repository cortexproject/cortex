// +build integration

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestExportedMetrics(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(dynamo, consul))

	// Start Cortex components.
	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

	tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorageFlags, "")
	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), ChunksStorageFlags, "")
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), ChunksStorageFlags, "")
	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", ChunksStorageFlags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, queryFrontend, ingester, tableManager))

	// Start the querier after the query-frontend otherwise we're not
	// able to get the query-frontend network endpoint.
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(ChunksStorageFlags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the first table-manager sync has completed, so that we're
	// sure the tables have been created.
	require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_dynamo_sync_tables_seconds"))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push some series to Cortex (to hit the write path).
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series both from the querier and query-frontend (to hit the read path).
	for _, endpoint := range []string{querier.HTTPEndpoint(), queryFrontend.HTTPEndpoint()} {
		c, err := e2ecortex.NewClient("", endpoint, "", "user-1")
		require.NoError(t, err)

		result, err := c.Query("series_1", now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// For each Cortex service, ensure its service-specific metrics prefix is not used by metrics
	// exported by other services.
	services := map[*e2ecortex.CortexService][]string{
		distributor:   []string{},
		ingester:      []string{},
		querier:       []string{},
		queryFrontend: []string{"cortex_frontend", "cortex_query_frontend"},
		tableManager:  []string{},
	}

	for service, prefixes := range services {
		if len(prefixes) == 0 {
			continue
		}

		// Assert the prefixes against all other services.
		for target, _ := range services {
			if service == target {
				continue
			}

			metrics, err := target.Metrics()
			require.NoError(t, err)

			// Ensure no metric name matches the "reserved" prefixes.
			for _, metricLine := range strings.Split(metrics, "\n") {
				metricLine = strings.TrimSpace(metricLine)
				if metricLine == "" || strings.HasPrefix(metricLine, "#") {
					continue
				}

				for _, prefix := range prefixes {
					assert.NotRegexp(t, "^"+prefix, metricLine, "service: %s", target.Name())
				}
			}
		}
	}
}
