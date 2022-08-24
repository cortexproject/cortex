//go:build requires_docker
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

type querierShardingTestConfig struct {
	shuffleShardingEnabled bool
	querySchedulerEnabled  bool
}

func TestQuerierShuffleShardingWithoutQueryScheduler(t *testing.T) {
	runQuerierShardingTest(t, querierShardingTestConfig{
		shuffleShardingEnabled: true,
		querySchedulerEnabled:  false,
	})
}

func TestQuerierShuffleShardingWithQueryScheduler(t *testing.T) {
	runQuerierShardingTest(t, querierShardingTestConfig{
		shuffleShardingEnabled: true,
		querySchedulerEnabled:  true,
	})
}

func TestQuerierNoShardingWithoutQueryScheduler(t *testing.T) {
	runQuerierShardingTest(t, querierShardingTestConfig{
		shuffleShardingEnabled: false,
		querySchedulerEnabled:  false,
	})
}

func TestQuerierNoShardingWithQueryScheduler(t *testing.T) {
	runQuerierShardingTest(t, querierShardingTestConfig{
		shuffleShardingEnabled: false,
		querySchedulerEnabled:  true,
	})
}

func runQuerierShardingTest(t *testing.T, cfg querierShardingTestConfig) {
	// Going to high starts hitting file descriptor limit, since we run all queriers concurrently.
	const numQueries = 100

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-querier.cache-results":                       "true",
		"-querier.split-queries-by-interval":           "24h",
		"-querier.query-ingesters-within":              "12h", // Required by the test on query /series out of ingesters time range
		"-frontend.memcached.addresses":                "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-querier.max-outstanding-requests-per-tenant": strconv.Itoa(numQueries), // To avoid getting errors.
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	if cfg.shuffleShardingEnabled {
		// Use only single querier for each user.
		flags["-frontend.max-queriers-per-tenant"] = "1"
	}

	// Start the query-scheduler if enabled.
	var queryScheduler *e2ecortex.CortexService
	if cfg.querySchedulerEnabled {
		queryScheduler = e2ecortex.NewQueryScheduler("query-scheduler", flags, "")
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	}

	// Start the query-frontend.
	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", flags, "")
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	querier1 := e2ecortex.NewQuerier("querier-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	querier2 := e2ecortex.NewQuerier("querier-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")

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

	// Wait until both workers connect to the query-frontend or query-scheduler
	if cfg.querySchedulerEnabled {
		require.NoError(t, queryScheduler.WaitSumMetrics(e2e.Equals(2), "cortex_query_scheduler_connected_querier_clients"))
	} else {
		require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "cortex_query_frontend_connected_clients"))
	}

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

	if cfg.shuffleShardingEnabled {
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
	assertServiceMetricsPrefixes(t, QueryScheduler, queryScheduler)
}
