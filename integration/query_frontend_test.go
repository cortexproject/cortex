//go:build requires_docker
// +build requires_docker

package integration

import (
	"bytes"
	"context"
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
	"github.com/thanos-io/objstore/providers/s3"
	"github.com/thanos-io/thanos/pkg/pool"

	"github.com/cortexproject/cortex/integration/ca"
	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
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

			// Enable subquery step size check.
			flags = mergeFlags(e2e.EmptyFlags(), map[string]string{
				"-querier.max-subquery-steps": "11000",
			})
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
			res, body, err := c.QueryRaw("{instance=~\"hello.*\"}", time.Now(), map[string]string{})
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
			res, _, err := c.QueryRaw("{instance=~\"hello.*\"}", time.Now(), map[string]string{})
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
			resp, _, _ := c.QueryRaw(`up[30d:1m]`, now, map[string]string{})
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
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

	extra := float64(2)
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
		"-blocks-storage.bucket-store.sync-interval": "1s",
		"-querier.frontend-address":                  queryFrontend.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

	// Wait until the querier and store-gateway have updated the ring, and wait until the blocks are old enough for consistency check
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(4), []string{"cortex_querier_blocks_scan_duration_seconds"}, e2e.WithMetricCount))

	// Sleep 3 * bucket sync interval to make sure consistency checker
	// doesn't consider block is uploaded recently.
	time.Sleep(3 * time.Second)

	// Query back the series.
	c, err = e2ecortex.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// We expect request to hit chunk pool exhaustion.
	resp, body, err := c.QueryRaw(`{job="test"}`, series2Timestamp, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Contains(t, string(body), pool.ErrPoolExhausted.Error())
	// We shouldn't be able to see any retries.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_query_frontend_retries"}, e2e.WaitMissingMetrics))
}

func TestQueryFrontendMaxQueryLengthLimits(t *testing.T) {
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
		"-store.max-query-length":                           "30d",
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

	queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", "", flags, "")
	queryFrontendWithSharding := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend-sharding", "", mergeFlags(flags, map[string]string{
		"-frontend.query-vertical-shard-size": "2",
	}), "")
	require.NoError(t, s.Start(queryFrontend, queryFrontendWithSharding))

	c, err := e2ecortex.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	cSharding, err := e2ecortex.NewClient("", queryFrontendWithSharding.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	// We expect request to hit max query length limit.
	resp, body, err := c.QueryRangeRaw(`rate(test[1m])`, now.Add(-90*time.Hour*24), now, 10*time.Hour, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "the query time range exceeds the limit")

	// We expect request to hit max query length limit.
	resp, body, err = cSharding.QueryRangeRaw(`rate(test[1m])`, now.Add(-90*time.Hour*24), now, 10*time.Hour, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "the query time range exceeds the limit")

	// We expect request to hit max query length limit.
	resp, body, err = c.QueryRaw(`rate(test[90d])`, now, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "the query time range exceeds the limit")

	// We expect request to hit max query length limit.
	resp, body, err = cSharding.QueryRaw(`rate(test[90d])`, now, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "the query time range exceeds the limit")
}

func TestQueryFrontendQueryRejection(t *testing.T) {
	configFileName := "runtime-config.yaml"

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, bucketName, rulestoreBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-runtime-config.backend":              "s3",
		"-runtime-config.s3.access-key-id":     e2edb.MinioAccessKey,
		"-runtime-config.s3.secret-access-key": e2edb.MinioSecretKey,
		"-runtime-config.s3.bucket-name":       bucketName,
		"-runtime-config.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-runtime-config.s3.insecure":          "true",
		"-runtime-config.file":                 configFileName,
		"-runtime-config.reload-period":        "1s",
	})

	client, err := s3.NewBucketWithConfig(nil, s3.Config{
		Endpoint:  minio.HTTPEndpoint(),
		Insecure:  true,
		Bucket:    bucketName,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
	}, "runtime-config-test")

	require.NoError(t, err)

	// update runtime config
	newRuntimeConfig := []byte(`overrides:
  user-1:
    query_rejection:
      enabled: true
      query_attributes:
        - api_type: "query"
          regex: .*rate.*
          query_step_limit:
           min: 6s
           max: 20m
          dashboard_uid: "dash123"
`)
	require.NoError(t, client.Upload(context.Background(), configFileName, bytes.NewReader(newRuntimeConfig)))
	time.Sleep(2 * time.Second)

	queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", "", flags, "")
	require.NoError(t, s.Start(queryFrontend))

	flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

	// Start all other services.
	ingester := e2ecortex.NewIngesterWithConfigFile("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), "", flags, "")
	querier := e2ecortex.NewQuerierWithConfigFile("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), "", flags, "")

	require.NoError(t, s.StartAndWaitReady(querier, ingester))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait until querier have updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	// We expect request to be rejected, as it matches query_attribute of query_rejection (contains rate, contains dashboard header dash123). step limit is ignored for instant queries
	// Query shouldn't be checked against attributes that is not provided in query_attribute config(time_window, time_range_limit, user_agent_regex, panel_id)
	resp, body, err := c.QueryRaw(`min_over_time( rate(http_requests_total[5m])[30m:5s] )`, now, map[string]string{"X-Dashboard-Uid": "dash123", "User-Agent": "grafana"})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected, as it doesn't match api_type
	resp, body, err = c.QueryRangeRaw(`min_over_time( rate(http_requests_total[5m])[30m:5s] )`, now.Add(-11*time.Hour), now.Add(-8*time.Hour), 25*time.Minute, map[string]string{"X-Dashboard-Uid": "dash123", "User-Agent": "grafana"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// update runtime config
	newRuntimeConfig = []byte(`overrides:
  user-1:
    query_rejection:
      enabled: true
      query_attributes:
        - regex: .*rate.*
          time_window:
           start: 12h
           end: 0h
          time_range_limit:
           min: 2h
           max: 6h
          query_step_limit:
           min: 22m
          dashboard_uid: "dash123"
          user_agent_regex: "grafana.*"
`)
	require.NoError(t, client.Upload(context.Background(), configFileName, bytes.NewReader(newRuntimeConfig)))
	time.Sleep(2 * time.Second)

	// We expect request to be rejected, as it matches query_attribute (contains 'rate', within time_window(11h-8h), within time range(3h), within step limit(25m>22m), contains dashboard header(dash123) and user-agent matches regex).
	resp, body, err = c.QueryRangeRaw(`rate(test[1m])`, now.Add(-11*time.Hour), now.Add(-8*time.Hour), 25*time.Minute, map[string]string{"X-Dashboard-Uid": "dash123", "User-Agent": "grafana"})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected, as it doesn't match query step limit (min is 22m, and actual step is 20m)
	resp, body, err = c.QueryRangeRaw(`rate(test[1m])`, now.Add(-11*time.Hour), now.Add(-8*time.Hour), 20*time.Minute, map[string]string{"X-Dashboard-Uid": "dash123", "User-Agent": "grafana"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected, as it goes beyond time_window(-15h is outside of 12h-0h window)
	resp, body, err = c.QueryRangeRaw(`rate(test[1m])`, now.Add(-15*time.Hour), now.Add(-8*time.Hour), 25*time.Minute, map[string]string{"X-Dashboard-Uid": "dash123", "User-Agent": "grafana"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected as it goes beyond time-range(9h is bigger than max time range of 6h)
	resp, body, err = c.QueryRangeRaw(`rate(test[1m])`, now.Add(-11*time.Hour), now.Add(-2*time.Hour), 25*time.Minute, map[string]string{"X-Dashboard-Uid": "dash123", "User-Agent": "grafana"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected, as it doesn't match regex (doesn't contain 'rate')
	resp, body, err = c.QueryRangeRaw(`increase(test[1m])`, now.Add(-11*time.Hour), now.Add(-8*time.Hour), 25*time.Minute, map[string]string{"X-Dashboard-Uid": "dash123", "User-Agent": "grafana"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected, as it doesn't match user-agent regex (doesn't contain 'grafana')
	resp, body, err = c.QueryRangeRaw(`rate(test[1m])`, now.Add(-11*time.Hour), now.Add(-8*time.Hour), 25*time.Minute, map[string]string{"X-Dashboard-Uid": "dash123", "User-Agent": "go-client/v0.19.0"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected, as it doesn't match grafana dashboard uid ('dash123' != 'new-dashboard')
	resp, body, err = c.QueryRangeRaw(`rate(test[1m])`, now.Add(-11*time.Hour), now.Add(-8*time.Hour), 25*time.Minute, map[string]string{"X-Dashboard-Uid": "new-dashboard", "User-Agent": "grafana"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request to be rejected for series request, as it has at least one matcher matching regex(contains 'rate'), within time_window(11h-8h, within time_range(3h).
	// query_step_limit, dashboard_uid, panel_id fields are ignored for metadata queries.
	resp, body, err = c.SeriesRaw([]string{`http_requests_rate_total{job="prometheus"}`}, now.Add(-11*time.Hour), now.Add(-8*time.Hour), map[string]string{"User-Agent": "grafana-agent/v0.19.0"})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected for series request as it does not have at least one matcher matching regex
	resp, body, err = c.SeriesRaw([]string{`http_requests_total{job="prometheus"}`}, now.Add(-11*time.Hour), now.Add(-8*time.Hour), map[string]string{"User-Agent": "grafana-agent/v0.19.0"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request to be rejected for labels request, as it has at least one matcher matching regex(contains 'rate'), within time_window(11h-8h, within time_range(3h).
	// query_step_limit, dashboard_uid, panel_id properties are ignored for metadata queries.
	resp, body, err = c.LabelNamesRaw([]string{`http_requests_rate_total{job="prometheus"}`}, now.Add(-11*time.Hour), now.Add(-8*time.Hour), map[string]string{"User-Agent": "grafana-agent/v0.19.0"})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected if label/label_values request has no matcher, but rejection query_attribute has regex property specified.
	// All the provided query_attributes fields that can be applied to metadata queries should match
	resp, body, err = c.LabelNamesRaw([]string{}, now.Add(-11*time.Hour), now.Add(-8*time.Hour), map[string]string{"User-Agent": "grafana-agent/v0.19.0"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected if label/label_values request doesn't provide time but rejection query_attribute has time_window property
	resp, body, err = c.LabelNamesRaw([]string{`http_requests_rate_total{job="prometheus"}`}, time.Time{}, time.Time{}, map[string]string{"User-Agent": "grafana-agent/v0.19.0"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// We expect request not to be rejected if label/label_values request doesn't provide one of the time(startTime or endTime) but rejection query_attribute has both time_window limits
	resp, body, err = c.LabelNamesRaw([]string{`http_requests_rate_total{job="prometheus"}`}, now.Add(-11*time.Hour), time.Time{}, map[string]string{"User-Agent": "grafana-agent/v0.19.0"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), tripperware.QueryRejectErrorMessage)

	// update runtime config
	newRuntimeConfig = []byte(`overrides:
  user-1:
    query_rejection:
      enabled: true
      query_attributes:
        - regex: .*rate.*
          time_window:
           start: 12h
           end: 0h
        - user_agent_regex: "grafana.*"
`)
	require.NoError(t, client.Upload(context.Background(), configFileName, bytes.NewReader(newRuntimeConfig)))
	time.Sleep(2 * time.Second)

	// We expect request to be rejected if any of the listed query_attributes configuration matches. Two query_attributes provided here, and doesn't match regex from first one, but matches second one.
	// query rejection should consider only provided attributes.
	// There is no regex, time_window, time_range_limit on second query_attribute. Only user_agent_regex provided so any query with this agent should be rejected
	resp, body, err = c.LabelValuesRaw("cluster", []string{}, time.Time{}, time.Time{}, map[string]string{"User-Agent": "grafana-agent/v0.19.0"})
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	require.Contains(t, string(body), tripperware.QueryRejectErrorMessage)

}
