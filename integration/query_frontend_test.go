// +build requires_docker

package integration

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/ca"
	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

type queryFrontendSetup func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string)

func TestQueryFrontendWithBlocksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, false, func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
		minio := e2edb.NewMinio(9000, BlocksStorageFlags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))

		return "", BlocksStorageFlags
	})
}

func TestQueryFrontendWithBlocksStorageViaConfigFile(t *testing.T) {
	runQueryFrontendTest(t, false, func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
		require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, []byte(BlocksStorageConfig)))

		minio := e2edb.NewMinio(9000, BlocksStorageFlags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))

		return cortexConfigFile, e2e.EmptyFlags()
	})
}

func TestQueryFrontendWithChunksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, true, func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
		require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

		dynamo := e2edb.NewDynamoDB()
		require.NoError(t, s.StartAndWaitReady(dynamo))

		tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorageFlags, "")
		require.NoError(t, s.StartAndWaitReady(tableManager))

		// Wait until the first table-manager sync has completed, so that we're
		// sure the tables have been created.
		require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))

		return "", ChunksStorageFlags
	})
}

func TestQueryFrontendWithChunksStorageViaConfigFile(t *testing.T) {
	runQueryFrontendTest(t, true, func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
		require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, []byte(ChunksStorageConfig)))
		require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

		dynamo := e2edb.NewDynamoDB()
		require.NoError(t, s.StartAndWaitReady(dynamo))

		tableManager := e2ecortex.NewTableManagerWithConfigFile("table-manager", cortexConfigFile, e2e.EmptyFlags(), "")
		require.NoError(t, s.StartAndWaitReady(tableManager))

		// Wait until the first table-manager sync has completed, so that we're
		// sure the tables have been created.
		require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))

		return cortexConfigFile, e2e.EmptyFlags()
	})
}

func TestQueryFrontendTLSWithBlocksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, false, func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
		minio := e2edb.NewMinio(9000, BlocksStorageFlags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))

		// set the ca
		ca := ca.New("Cortex Test")

		// Ensure the entire path of directories exist.
		require.NoError(t, os.MkdirAll(filepath.Join(s.SharedDir(), "certs"), os.ModePerm))

		require.NoError(t, ca.WriteCACertificate(filepath.Join(s.SharedDir(), caCertFile)))

		// server certificate
		require.NoError(t, ca.WriteCertificate(
			&x509.Certificate{
				Subject:     pkix.Name{CommonName: "client"},
				ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			},
			filepath.Join(s.SharedDir(), clientCertFile),
			filepath.Join(s.SharedDir(), clientKeyFile),
		))
		require.NoError(t, ca.WriteCertificate(
			&x509.Certificate{
				Subject:     pkix.Name{CommonName: "server"},
				DNSNames:    []string{"querier.frontend-client", "ingester.client"},
				ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			},
			filepath.Join(s.SharedDir(), serverCertFile),
			filepath.Join(s.SharedDir(), serverKeyFile),
		))

		return "", mergeFlags(
			BlocksStorageFlags,
			getServerTLSFlags(),
			getClientTLSFlagsWithPrefix("ingester.client"),
			getClientTLSFlagsWithPrefix("querier.frontend-client"),
		)
	})
}

func runQueryFrontendTest(t *testing.T, testMissingMetricName bool, setup queryFrontendSetup) {
	const numUsers = 10
	const numQueriesPerUser = 10

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	configFile, flags := setup(t, s)

	flags = mergeFlags(flags, map[string]string{
		"-querier.cache-results":             "true",
		"-querier.split-queries-by-interval": "24h",
		"-querier.query-ingesters-within":    "12h", // Required by the test on query /series out of ingesters time range
		"-frontend.memcached.addresses":      "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
	})

	// Start Cortex components.
	queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", configFile, flags, "")
	ingester := e2ecortex.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), configFile, flags, "")
	distributor := e2ecortex.NewDistributorWithConfigFile("distributor", consul.NetworkHTTPEndpoint(), configFile, flags, "")

	require.NoError(t, s.Start(queryFrontend))

	querier := e2ecortex.NewQuerierWithConfigFile("querier", consul.NetworkHTTPEndpoint(), configFile, mergeFlags(flags, map[string]string{
		"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
	}), "")

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Check if we're discovering memcache or not.
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "cortex_memcache_client_servers"))
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "cortex_dns_lookups_total"))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Cortex.
	now := time.Now()
	expectedVectors := make([]model.Vector, numUsers)

	for u := 0; u < numUsers; u++ {
		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", fmt.Sprintf("user-%d", u))
		require.NoError(t, err)

		var series []prompb.TimeSeries
		series, expectedVectors[u] = generateSeries("series_1", now)

		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// Query the series for each user in parallel.
	wg := sync.WaitGroup{}
	wg.Add(numUsers * numQueriesPerUser)

	for u := 0; u < numUsers; u++ {
		userID := u

		c, err := e2ecortex.NewClient("", queryFrontend.HTTPEndpoint(), "", "", fmt.Sprintf("user-%d", userID))
		require.NoError(t, err)

		// No need to repeat the test on missing metric name for each user.
		if userID == 0 && testMissingMetricName {
			res, body, err := c.QueryRaw("{instance=~\"hello.*\"}")
			require.NoError(t, err)
			require.Equal(t, 422, res.StatusCode)
			require.Contains(t, string(body), "query must contain metric name")
		}

		// No need to repeat the test on start/end time rounding for each user.
		if userID == 0 {
			start := time.Unix(1595846748, 806*1e6)
			end := time.Unix(1595846750, 806*1e6)

			result, err := c.QueryRange("time()", start, end, time.Second)
			require.NoError(t, err)
			require.Equal(t, model.ValMatrix, result.Type())

			matrix := result.(model.Matrix)
			require.Len(t, matrix, 1)
			require.Len(t, matrix[0].Values, 3)
			assert.Equal(t, model.Time(1595846748806), matrix[0].Values[0].Timestamp)
			assert.Equal(t, model.Time(1595846750806), matrix[0].Values[2].Timestamp)
		}

		// In this test we do ensure that the /series start/end time is ignored and Cortex
		// always returns series in ingesters memory. No need to repeat it for each user.
		if userID == 0 {
			start := now.Add(-1000 * time.Hour)
			end := now.Add(-999 * time.Hour)

			result, err := c.Series([]string{"series_1"}, start, end)
			require.NoError(t, err)
			require.Len(t, result, 1)
			assert.Equal(t, model.LabelSet{labels.MetricName: "series_1"}, result[0])
		}

		for q := 0; q < numQueriesPerUser; q++ {
			go func() {
				defer wg.Done()

				result, err := c.Query("series_1", now)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVectors[userID], result.(model.Vector))
			}()
		}
	}

	wg.Wait()

	extra := float64(2)
	if testMissingMetricName {
		extra++
	}
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(numUsers*numQueriesPerUser+extra), "cortex_query_frontend_queries_total"))

	// The number of received request is greater then the query requests because include
	// requests to /metrics and /ready.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Greater(numUsers*numQueriesPerUser), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Greater(numUsers*numQueriesPerUser), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount))

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
	assertServiceMetricsPrefixes(t, QueryFrontend, queryFrontend)
}
