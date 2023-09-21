//go:build requires_docker
// +build requires_docker

package integration

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/pool"

	"github.com/cortexproject/cortex/integration/ca"
	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

type queryFrontendTestConfig struct {
	testMissingMetricName bool
	querySchedulerEnabled bool
	queryStatsEnabled     bool
	remoteReadEnabled     bool
	testSubQueryStepSize  bool
	setup                 func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string)
}

func TestQueryFrontendWithBlocksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testMissingMetricName: false,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = BlocksStorageFlags()

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndQueryStatsEnabled(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testMissingMetricName: false,
		queryStatsEnabled:     true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = BlocksStorageFlags()

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndWithQueryScheduler(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testMissingMetricName: false,
		querySchedulerEnabled: true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = BlocksStorageFlags()

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndWithQuerySchedulerAndQueryStatsEnabled(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testMissingMetricName: false,
		querySchedulerEnabled: true,
		queryStatsEnabled:     true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = BlocksStorageFlags()

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	})
}

func TestQueryFrontendWithBlocksStorageViaConfigFile(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testMissingMetricName: false,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, []byte(BlocksStorageConfig)))

			minio := e2edb.NewMinio(9000, BlocksStorageFlags()["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return cortexConfigFile, e2e.EmptyFlags()
		},
	})
}

func TestQueryFrontendTLSWithBlocksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testMissingMetricName: false,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(
				BlocksStorageFlags(),
				getServerTLSFlags(),
				getClientTLSFlagsWithPrefix("ingester.client"),
				getClientTLSFlagsWithPrefix("querier.frontend-client"),
			)

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			// set the ca
			cert := ca.New("Cortex Test")

			// Ensure the entire path of directories exist.
			require.NoError(t, os.MkdirAll(filepath.Join(s.SharedDir(), "certs"), os.ModePerm))

			require.NoError(t, cert.WriteCACertificate(filepath.Join(s.SharedDir(), caCertFile)))

			// server certificate
			require.NoError(t, cert.WriteCertificate(
				&x509.Certificate{
					Subject:     pkix.Name{CommonName: "client"},
					ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				},
				filepath.Join(s.SharedDir(), clientCertFile),
				filepath.Join(s.SharedDir(), clientKeyFile),
			))
			require.NoError(t, cert.WriteCertificate(
				&x509.Certificate{
					Subject:     pkix.Name{CommonName: "server"},
					DNSNames:    []string{"querier.frontend-client", "ingester.client"},
					ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
				},
				filepath.Join(s.SharedDir(), serverCertFile),
				filepath.Join(s.SharedDir(), serverKeyFile),
			))

			return "", flags
		},
	})
}

func TestQueryFrontendWithVerticalSharding(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testMissingMetricName: false,
		querySchedulerEnabled: false,
		queryStatsEnabled:     true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, []byte(BlocksStorageConfig)))

			minio := e2edb.NewMinio(9000, BlocksStorageFlags()["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			// Enable vertical sharding.
			flags = mergeFlags(e2e.EmptyFlags(), map[string]string{
				"-frontend.query-vertical-shard-size": "2",
			})
			return cortexConfigFile, flags
		},
	})
}

func TestQueryFrontendWithVerticalShardingQueryScheduler(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testMissingMetricName: false,
		querySchedulerEnabled: true,
		queryStatsEnabled:     true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, []byte(BlocksStorageConfig)))

			minio := e2edb.NewMinio(9000, BlocksStorageFlags()["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			// Enable vertical sharding.
			flags = mergeFlags(e2e.EmptyFlags(), map[string]string{
				"-frontend.query-vertical-shard-size": "2",
			})
			return cortexConfigFile, flags
		},
	})
}

func TestQueryFrontendRemoteRead(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		remoteReadEnabled: true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, []byte(BlocksStorageConfig)))

			minio := e2edb.NewMinio(9000, BlocksStorageFlags()["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))
			return cortexConfigFile, flags
		},
	})
}

func TestQueryFrontendSubQueryStepSize(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		testSubQueryStepSize: true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, []byte(BlocksStorageConfig)))

			minio := e2edb.NewMinio(9000, BlocksStorageFlags()["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))
			return cortexConfigFile, flags
		},
	})
}

func runQueryFrontendTest(t *testing.T, cfg queryFrontendTestConfig) {
	const numUsers = 10
	const numQueriesPerUser = 10

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	configFile, flags := cfg.setup(t, s)

	flags = mergeFlags(flags, map[string]string{
		"-querier.cache-results":             "true",
		"-querier.split-queries-by-interval": "24h",
		"-querier.query-ingesters-within":    "12h", // Required by the test on query /series out of ingesters time range
		"-frontend.memcached.addresses":      "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-frontend.query-stats-enabled":      strconv.FormatBool(cfg.queryStatsEnabled),
	})

	// Start the query-scheduler if enabled.
	var queryScheduler *e2ecortex.CortexService
	if cfg.querySchedulerEnabled {
		queryScheduler = e2ecortex.NewQueryScheduler("query-scheduler", flags, "")
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	}

	// Start the query-frontend.
	queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", configFile, flags, "")
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2ecortex.NewIngesterWithConfigFile("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), configFile, flags, "")
	distributor := e2ecortex.NewDistributorWithConfigFile("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), configFile, flags, "")

	querier := e2ecortex.NewQuerierWithConfigFile("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), configFile, flags, "")

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
		if userID == 0 && cfg.testMissingMetricName {
			res, body, err := c.QueryRaw("{instance=~\"hello.*\"}", time.Now())
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

		// No need to repeat the test on Server-Timing header for each user.
		if userID == 0 && cfg.queryStatsEnabled {
			res, _, err := c.QueryRaw("{instance=~\"hello.*\"}", time.Now())
			require.NoError(t, err)
			require.Regexp(t, "querier_wall_time;dur=[0-9.]*, response_time;dur=[0-9.]*$", res.Header.Values("Server-Timing")[0])
		}

		// No need to repeat the test on remote read for each user.
		if userID == 0 && cfg.remoteReadEnabled {
			start := now.Add(-1 * time.Hour)
			end := now.Add(1 * time.Hour)
			res, err := c.RemoteRead([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "series_1")}, start, end, time.Second)
			require.NoError(t, err)
			require.True(t, len(res.Results) > 0)
			require.True(t, len(res.Results[0].Timeseries) > 0)
			require.True(t, len(res.Results[0].Timeseries[0].Samples) > 0)
			require.True(t, len(res.Results[0].Timeseries[0].Labels) > 0)
		}

		// No need to repeat the test on subquery step size.
		if userID == 0 && cfg.testSubQueryStepSize {
			resp, _, _ := c.QueryRaw(`up[30d:1m]`, now)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
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

		// No need to repeat the query 400 test for each user.
		if userID == 0 {
			start := time.Unix(1595846748, 806*1e6)
			end := time.Unix(1595846750, 806*1e6)

			_, err := c.QueryRange("up)", start, end, time.Second)
			require.Error(t, err)

			apiErr, ok := err.(*v1.Error)
			require.True(t, ok)
			require.Equal(t, apiErr.Type, v1.ErrBadData)
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

	extra := float64(3)
	if cfg.testMissingMetricName {
		extra++
	}

	if cfg.queryStatsEnabled {
		extra++
	}

	if cfg.remoteReadEnabled {
		extra++
	}

	if cfg.testSubQueryStepSize {
		extra++
	}

	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(numUsers*numQueriesPerUser+extra), "cortex_query_frontend_queries_total"))

	// The number of received request is greater than the query requests because include
	// requests to /metrics and /ready.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Greater(numUsers*numQueriesPerUser), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Greater(numUsers*numQueriesPerUser), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Greater(numUsers*numQueriesPerUser), []string{"cortex_querier_request_duration_seconds"}, e2e.WithMetricCount))

	// Ensure query stats metrics are tracked only when enabled.
	if cfg.queryStatsEnabled {
		require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Greater(0),
			[]string{"cortex_query_seconds_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))
	} else {
		require.NoError(t, queryFrontend.WaitRemovedMetric("cortex_query_seconds_total"))
	}

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
	assertServiceMetricsPrefixes(t, QueryFrontend, queryFrontend)
	assertServiceMetricsPrefixes(t, QueryScheduler, queryScheduler)
}

func TestQueryFrontendNoRetryChunkPool(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":                "1s",
		"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
		"-blocks-storage.bucket-store.max-chunk-pool-bytes": "1",
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push some series to Cortex.
	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	seriesTimestamp := time.Now()
	series2Timestamp := seriesTimestamp.Add(blockRangePeriod * 2)
	series1, _ := generateSeries("series_1", seriesTimestamp, prompb.Label{Name: "job", Value: "test"})
	series2, _ := generateSeries("series_2", series2Timestamp, prompb.Label{Name: "job", Value: "test"})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is compacted and shipped to the storage.
	// The shipped block contains the 1st series, while the 2ns series is in the head.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))

	queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", "", flags, "")
	require.NoError(t, s.Start(queryFrontend))

	// Start the querier and store-gateway, and configure them to frequently sync blocks fast enough to trigger consistency check.
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "5s",
	}), "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "5s",
		"-querier.frontend-address":                  queryFrontend.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

	// Wait until the querier and store-gateway have updated the ring, and wait until the blocks are old enough for consistency check
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(4), []string{"cortex_querier_blocks_scan_duration_seconds"}, e2e.WithMetricCount))

	// Query back the series.
	c, err = e2ecortex.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// We expect request to hit chunk pool exhaustion.
	resp, body, err := c.QueryRaw(`{job="test"}`, series2Timestamp)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Contains(t, string(body), pool.ErrPoolExhausted.Error())
	// We shouldn't be able to see any retries.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_query_frontend_retries"}, e2e.WaitMissingMetrics))
}
