//go:build requires_docker

package integration

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestIngesterStreamPushConnection(t *testing.T) {

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	maxGlobalSeriesPerMetric := 300
	maxGlobalSeriesPerTenant := 1000

	flags := BlocksStorageFlags()
	flags["-distributor.use-stream-push"] = "true"
	flags["-distributor.replication-factor"] = "1"
	flags["-distributor.shard-by-all-labels"] = "true"
	flags["-distributor.sharding-strategy"] = "shuffle-sharding"
	flags["-distributor.ingestion-tenant-shard-size"] = "1"
	flags["-ingester.max-series-per-user"] = "0"
	flags["-ingester.max-series-per-metric"] = "0"
	flags["-ingester.max-global-series-per-user"] = strconv.Itoa(maxGlobalSeriesPerTenant)
	flags["-ingester.max-global-series-per-metric"] = strconv.Itoa(maxGlobalSeriesPerMetric)
	flags["-ingester.heartbeat-period"] = "1s"

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester2 := e2ecortex.NewIngester("ingester-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester3 := e2ecortex.NewIngester("ingester-3", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until ingesters have heartbeated the ring after all ingesters were active,
	// in order to update the number of instances. Since we have no metric, we have to
	// rely on a ugly sleep.
	time.Sleep(2 * time.Second)

	now := time.Now()
	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	numSeriesWithSameMetricName := 0
	numSeriesTotal := 0
	maxErrorsBeforeStop := 100

	// Try to push as many series with the same metric name as we can.
	for i, errs := 0, 0; i < 10000; i++ {
		series, _ := generateSeries("test_limit_per_metric", now, prompb.Label{
			Name:  "cardinality",
			Value: strconv.Itoa(rand.Int()),
		})

		res, err := client.Push(series)
		require.NoError(t, err)

		if res.StatusCode == 200 {
			numSeriesTotal++
			numSeriesWithSameMetricName++
		} else if errs++; errs >= maxErrorsBeforeStop {
			break
		}
	}

	// Try to push as many series with the different metric name as we can.
	for i, errs := 0, 0; i < 10000; i++ {
		series, _ := generateSeries(fmt.Sprintf("test_limit_per_tenant_%d", rand.Int()), now)
		res, err := client.Push(series)
		require.NoError(t, err)

		if res.StatusCode == 200 {
			numSeriesTotal++
		} else if errs++; errs >= maxErrorsBeforeStop {
			break
		}
	}

	// We expect the number of series we've been successfully pushed to be around
	// the limit. Due to how the global limit implementation works (lack of centralised
	// coordination) the actual number of written series could be slightly different
	// than the global limit, so we allow a 10% difference.
	delta := 0.1
	assert.InDelta(t, maxGlobalSeriesPerMetric, numSeriesWithSameMetricName, float64(maxGlobalSeriesPerMetric)*delta)
	assert.InDelta(t, maxGlobalSeriesPerTenant, numSeriesTotal, float64(maxGlobalSeriesPerTenant)*delta)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester1)
	assertServiceMetricsPrefixes(t, Ingester, ingester2)
	assertServiceMetricsPrefixes(t, Ingester, ingester3)
}

func TestIngesterStreamPushConnectionWithMatchingSigningKey(t *testing.T) {
	const signingKey = "shared-secret-for-integration-test"

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := BlocksStorageFlags()
	flags["-distributor.use-stream-push"] = "true"
	flags["-distributor.replication-factor"] = "1"
	flags["-distributor.sign-write-requests"] = "true"
	flags["-distributor.sign-write-requests-keys"] = signingKey
	flags["-ingester.heartbeat-period"] = "1s"
	flags["-distributor.ring.heartbeat-period"] = "1s"

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1))

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"},
		e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE")),
		e2e.WaitMissingMetrics))

	now := time.Now()
	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	// Push a few series; all should succeed because the signing key matches.
	for i := 0; i < 5; i++ {
		series, _ := generateSeries(fmt.Sprintf("test_signing_ok_%d", i), now)
		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode,
			"push must succeed when distributor and ingester share the same signing key")
	}
}

func TestIngesterStreamPushConnectionWithMismatchedSigningKey(t *testing.T) {
	const distributorKey = "distributor-key"
	const ingesterKey = "ingester-key-different"

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Distributor signs with distributorKey.
	distributorFlags := BlocksStorageFlags()
	distributorFlags["-distributor.use-stream-push"] = "true"
	distributorFlags["-distributor.replication-factor"] = "1"
	distributorFlags["-distributor.sign-write-requests"] = "true"
	distributorFlags["-distributor.sign-write-requests-keys"] = distributorKey
	distributorFlags["-ingester.heartbeat-period"] = "1s"

	// Ingester verifies with ingesterKey (intentionally different).
	ingesterFlags := BlocksStorageFlags()
	ingesterFlags["-distributor.use-stream-push"] = "true"
	ingesterFlags["-distributor.replication-factor"] = "1"
	ingesterFlags["-distributor.sign-write-requests"] = "true"
	ingesterFlags["-distributor.sign-write-requests-keys"] = ingesterKey
	ingesterFlags["-ingester.heartbeat-period"] = "1s"

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, distributorFlags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), distributorFlags, "")
	ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), ingesterFlags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1))

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"},
		e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE")),
		e2e.WaitMissingMetrics))

	now := time.Now()
	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		series, _ := generateSeries(fmt.Sprintf("test_signing_mismatch_%d", i), now)
		res, err := client.Push(series)
		if err == nil {
			require.NotEqual(t, 200, res.StatusCode,
				"push must fail when distributor and ingester use different signing keys")
		}
	}
}

func TestIngesterStreamPushConnectionWithError(t *testing.T) {

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	maxGlobalSeriesPerMetric := 300
	maxGlobalSeriesPerTenant := 1000

	flags := BlocksStorageFlags()
	flags["-distributor.use-stream-push"] = "true"
	flags["-distributor.replication-factor"] = "1"
	flags["-distributor.shard-by-all-labels"] = "true"
	flags["-distributor.sharding-strategy"] = "shuffle-sharding"
	flags["-distributor.ingestion-tenant-shard-size"] = "1"
	flags["-ingester.max-series-per-user"] = "0"
	flags["-ingester.max-series-per-metric"] = "0"
	flags["-ingester.max-global-series-per-user"] = strconv.Itoa(maxGlobalSeriesPerTenant)
	flags["-ingester.max-global-series-per-metric"] = strconv.Itoa(maxGlobalSeriesPerMetric)
	flags["-ingester.heartbeat-period"] = "1s"
	flags["-ingester.instance-limits.max-series"] = "1" // trigger "max series limit reached" error which returns nil response

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester2 := e2ecortex.NewIngester("ingester-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester3 := e2ecortex.NewIngester("ingester-3", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until ingesters have heartbeated the ring after all ingesters were active,
	// in order to update the number of instances. Since we have no metric, we have to
	// rely on a ugly sleep.
	time.Sleep(2 * time.Second)

	now := time.Now()
	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		series, _ := generateSeries("test_limit_per_metric", now,
			prompb.Label{
				Name:  "cardinality",
				Value: strconv.Itoa(rand.Int()),
			},
		)
		_, err = client.Push(series)
		require.NoError(t, err)
		err = ingester1.WaitForRunning()
		require.NoError(t, err)
		err = ingester2.WaitForRunning()
		require.NoError(t, err)
		err = ingester3.WaitForRunning()
		require.NoError(t, err)
	}
}
