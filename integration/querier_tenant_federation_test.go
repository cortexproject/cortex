//go:build integration_querier
// +build integration_querier

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

type querierTenantFederationConfig struct {
	querySchedulerEnabled  bool
	shuffleShardingEnabled bool
}

func TestQuerierTenantFederation(t *testing.T) {
	runQuerierTenantFederationTest(t, querierTenantFederationConfig{})
}

func TestQuerierTenantFederationWithQueryScheduler(t *testing.T) {
	runQuerierTenantFederationTest(t, querierTenantFederationConfig{
		querySchedulerEnabled: true,
	})
}

func TestQuerierTenantFederationWithShuffleSharding(t *testing.T) {
	runQuerierTenantFederationTest(t, querierTenantFederationConfig{
		shuffleShardingEnabled: true,
	})
}

func TestQuerierTenantFederationWithQuerySchedulerAndShuffleSharding(t *testing.T) {
	runQuerierTenantFederationTest(t, querierTenantFederationConfig{
		querySchedulerEnabled:  true,
		shuffleShardingEnabled: true,
	})
}

func runQuerierTenantFederationTest(t *testing.T, cfg querierTenantFederationConfig) {
	const numUsers = 10

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-querier.cache-results":             "true",
		"-querier.split-queries-by-interval": "24h",
		"-querier.query-ingesters-within":    "12h", // Required by the test on query /series out of ingesters time range
		"-frontend.memcached.addresses":      "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-tenant-federation.enabled":         "true",
	})

	// Start the query-scheduler if enabled.
	var queryScheduler *e2ecortex.CortexService
	if cfg.querySchedulerEnabled {
		queryScheduler = e2ecortex.NewQueryScheduler("query-scheduler", flags, "")
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	}

	if cfg.shuffleShardingEnabled {
		// Use only single querier for each user.
		flags["-frontend.max-queriers-per-tenant"] = "1"
	}

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start the query-frontend.
	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", flags, "")
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")

	var querier2 *e2ecortex.CortexService
	if cfg.shuffleShardingEnabled {
		querier2 = e2ecortex.NewQuerier("querier-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	}

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))
	if cfg.shuffleShardingEnabled {
		require.NoError(t, s.StartAndWaitReady(querier2))
	}

	// Wait until distributor and queriers have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	if cfg.shuffleShardingEnabled {
		require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	}

	// Push a series for each user to Cortex.
	now := time.Now()
	expectedVectors := make([]model.Vector, numUsers)
	tenantIDs := make([]string, numUsers)

	for u := 0; u < numUsers; u++ {
		tenantIDs[u] = fmt.Sprintf("user-%d", u)
		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", tenantIDs[u])
		require.NoError(t, err)

		var series []prompb.TimeSeries
		series, expectedVectors[u] = generateSeries("series_1", now)

		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// query all tenants
	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", strings.Join(tenantIDs, "|"))
	require.NoError(t, err)

	result, err := c.Query("series_1", now)
	require.NoError(t, err)

	assert.Equal(t, mergeResults(tenantIDs, expectedVectors), result.(model.Vector))

	// ensure a push to multiple tenants is failing
	series, _ := generateSeries("series_1", now)
	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 500, res.StatusCode)

	// check metric label values for total queries in the query frontend
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_query_frontend_queries_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", strings.Join(tenantIDs, "|")),
		labels.MustNewMatcher(labels.MatchEqual, "op", "query"))))

	// check metric label values for query queue length in either query frontend or query scheduler
	queueComponent := queryFrontend
	queueMetricName := "cortex_query_frontend_queue_length"
	if cfg.querySchedulerEnabled {
		queueComponent = queryScheduler
		queueMetricName = "cortex_query_scheduler_queue_length"
	}
	require.NoError(t, queueComponent.WaitSumMetricsWithOptions(e2e.Equals(0), []string{queueMetricName}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", strings.Join(tenantIDs, "|")))))

	// TODO: check cache invalidation on tombstone cache gen increase
	// TODO: check fairness in queryfrontend
}

func mergeResults(tenantIDs []string, resultsPerTenant []model.Vector) model.Vector {
	var v model.Vector
	for pos, tenantID := range tenantIDs {
		for _, r := range resultsPerTenant[pos] {
			var s model.Sample = *r
			s.Metric = r.Metric.Clone()
			s.Metric[model.LabelName("__tenant_id__")] = model.LabelValue(tenantID)
			v = append(v, &s)
		}
	}
	return v
}
