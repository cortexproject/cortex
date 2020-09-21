// +build requires_docker

package integration

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestQuerierSharding(t *testing.T) {
	runQuerierShardingTest(t, true)
}

func TestQuerierNoSharding(t *testing.T) {
	runQuerierShardingTest(t, false)
}

func runQuerierShardingTest(t *testing.T, sharding bool) {
	// Going to high starts hitting filedescriptor limit, since we run all queriers concurrently.
	const numQueries = 100

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	minio := e2edb.NewMinio(9000, BlocksStorageFlags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := BlocksStorageFlags

	flags = mergeFlags(flags, map[string]string{
		"-querier.cache-results":                       "true",
		"-querier.split-queries-by-interval":           "24h",
		"-querier.query-ingesters-within":              "12h", // Required by the test on query /series out of ingesters time range
		"-frontend.memcached.addresses":                "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-querier.max-outstanding-requests-per-tenant": strconv.Itoa(numQueries), // To avoid getting errors.
	})

	if sharding {
		// Use only single querier for each user.
		flags["-frontend.max-queriers-per-user"] = "1"
	}

	// Start Cortex components.
	queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", "", flags, "")
	ingester := e2ecortex.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), "", flags, "")
	distributor := e2ecortex.NewDistributorWithConfigFile("distributor", consul.NetworkHTTPEndpoint(), "", flags, "")

	require.NoError(t, s.Start(queryFrontend))

	querier1 := e2ecortex.NewQuerierWithConfigFile("querier-1", consul.NetworkHTTPEndpoint(), "", mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
	}), "")
	querier2 := e2ecortex.NewQuerierWithConfigFile("querier-2", consul.NetworkHTTPEndpoint(), "", mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
	}), "")

	require.NoError(t, s.StartAndWaitReady(querier1, querier2, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait until distributor and queriers have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier1.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Cortex.
	now := time.Now()

	distClient, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	var series []prompb.TimeSeries
	series, expectedVector := generateSeries("series_1", now)

	res, err := distClient.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Send both queriers a single query, so that they both initialize their cortex_querier_request_duration_seconds metrics.
	for _, q := range []*e2ecortex.CortexService{querier1, querier2} {
		c, err := e2ecortex.NewClient("", q.HTTPEndpoint(), "", "", userID)
		require.NoError(t, err)

		_, err = c.Query("series_1", now)
		require.NoError(t, err)
	}

	// Wait until both workers connect to the query frontend
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "cortex_query_frontend_connected_clients"))

	wg := sync.WaitGroup{}

	// Run all queries concurrently to get better distribution of requests between queriers.
	for i := 0; i < numQueries; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			c, err := e2ecortex.NewClient("", queryFrontend.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			result, err := c.Query("series_1", now)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector, result.(model.Vector))
		}()
	}

	wg.Wait()

	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(numQueries), "cortex_query_frontend_queries_total"))

	// Verify that only single querier handled all the queries when sharding is enabled, otherwise queries have been fairly distributed across queriers.
	q1Values, err := querier1.SumMetrics([]string{"cortex_querier_request_duration_seconds"}, e2e.WithMetricCount)
	require.NoError(t, err)
	require.Len(t, q1Values, 1)

	q2Values, err := querier2.SumMetrics([]string{"cortex_querier_request_duration_seconds"}, e2e.WithMetricCount)
	require.NoError(t, err)
	require.Len(t, q2Values, 1)

	total := q1Values[0] + q2Values[0]
	diff := q1Values[0] - q2Values[0]
	if diff < 0 {
		diff = -diff
	}

	require.Equal(t, float64(numQueries), total-2) // Remove 2 requests used for metrics initialization.

	if sharding {
		require.Equal(t, float64(numQueries), diff)
	} else {
		require.InDelta(t, 0, diff, numQueries*0.20) // Both queriers should have roughly equal number of requests, with possible delta.
	}

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier1)
	assertServiceMetricsPrefixes(t, Querier, querier2)
	assertServiceMetricsPrefixes(t, QueryFrontend, queryFrontend)
}
