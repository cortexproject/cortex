//go:build integration_query_fuzz

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cortexproject/promqlsmith"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/log"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
)

func TestParquetFuzz(t *testing.T) {

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())
	flags := mergeFlags(
		baseFlags,
		map[string]string{
			"-target": "all,parquet-converter",
			"-blocks-storage.tsdb.block-ranges-period":                             "1m,24h",
			"-blocks-storage.tsdb.ship-interval":                                   "1s",
			"-blocks-storage.bucket-store.sync-interval":                           "1s",
			"-blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl": "1s",
			"-blocks-storage.bucket-store.bucket-index.idle-timeout":               "1s",
			"-blocks-storage.bucket-store.bucket-index.enabled":                    "true",
			"-blocks-storage.bucket-store.index-cache.backend":                     tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":                              "true",
			// compactor
			"-compactor.cleanup-interval": "1s",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled":   "false",
			"--querier.store-gateway-addresses": "nonExistent", // Make sure we do not call Store gateways
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
			// Enable vertical sharding.
			"-frontend.query-vertical-shard-size": "3",
			"-frontend.max-cache-freshness":       "1m",
			// enable experimental promQL funcs
			"-querier.enable-promql-experimental-functions": "true",
			// parquet-converter
			"-parquet-converter.ring.consul.hostname": consul.NetworkHTTPEndpoint(),
			"-parquet-converter.conversion-interval":  "1s",
			"-parquet-converter.enabled":              "true",
			// Querier
			"-querier.enable-parquet-queryable": "true",
			// Enable cache for parquet labels and chunks
			"-blocks-storage.bucket-store.parquet-labels-cache.backend":             "inmemory,memcached",
			"-blocks-storage.bucket-store.parquet-labels-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
			"-blocks-storage.bucket-store.chunks-cache.backend":                     "inmemory,memcached",
			"-blocks-storage.bucket-store.chunks-cache.memcached.addresses":         "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		},
	)

	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	ctx := context.Background()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	dir := filepath.Join(s.SharedDir(), "data")
	numSeries := 10
	numSamples := 60
	lbls := make([]labels.Labels, 0, numSeries*2)
	scrapeInterval := time.Minute
	statusCodes := []string{"200", "400", "404", "500", "502"}
	now := time.Now()
	start := now.Add(-time.Hour * 24)
	end := now.Add(-time.Hour)

	for i := 0; i < numSeries; i++ {
		lbls = append(lbls, labels.FromStrings(labels.MetricName, "test_series_a", "job", "test", "series", strconv.Itoa(i%3), "status_code", statusCodes[i%5]))
		lbls = append(lbls, labels.FromStrings(labels.MetricName, "test_series_b", "job", "test", "series", strconv.Itoa((i+1)%3), "status_code", statusCodes[(i+1)%5]))
	}
	id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)

	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait until we convert the blocks
	cortex_testutil.Poll(t, 30*time.Second, true, func() interface{} {
		found := false
		foundBucketIndex := false

		err := bkt.Iter(context.Background(), "", func(name string) error {
			fmt.Println(name)
			if name == fmt.Sprintf("parquet-markers/%v-parquet-converter-mark.json", id.String()) {
				found = true
			}
			if name == "bucket-index.json.gz" {
				foundBucketIndex = true
			}
			return nil
		}, objstore.WithRecursiveIter())
		require.NoError(t, err)
		return found && foundBucketIndex
	})

	att, err := bkt.Attributes(context.Background(), "bucket-index.json.gz")
	require.NoError(t, err)
	numberOfIndexesUpdate := 0
	lastUpdate := att.LastModified

	cortex_testutil.Poll(t, 30*time.Second, 5, func() interface{} {
		att, err := bkt.Attributes(context.Background(), "bucket-index.json.gz")
		require.NoError(t, err)
		if lastUpdate != att.LastModified {
			lastUpdate = att.LastModified
			numberOfIndexesUpdate++
		}
		return numberOfIndexesUpdate
	})

	c1, err := e2ecortex.NewClient("", cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	err = writeFileToSharedDir(s, "prometheus.yml", []byte(""))
	require.NoError(t, err)
	prom := e2edb.NewPrometheus("", map[string]string{
		"--enable-feature": "promql-experimental-functions",
	})
	require.NoError(t, s.StartAndWaitReady(prom))

	c2, err := e2ecortex.NewPromQueryClient(prom.HTTPEndpoint())
	require.NoError(t, err)
	waitUntilReady(t, ctx, c1, c2, `{job="test"}`, start, end)

	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(enabledFunctions),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, end, start, end, scrapeInterval, 1000, false)

	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_parquet_queryable_blocks_queried_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "type", "parquet"))))
}

func TestParquetProjectionPushdown(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())
	flags := mergeFlags(
		baseFlags,
		map[string]string{
			"-target": "all,parquet-converter",
			"-blocks-storage.tsdb.block-ranges-period":                             "1m,24h",
			"-blocks-storage.tsdb.ship-interval":                                   "1s",
			"-blocks-storage.bucket-store.sync-interval":                           "1s",
			"-blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl": "1s",
			"-blocks-storage.bucket-store.bucket-index.idle-timeout":               "1s",
			"-blocks-storage.bucket-store.bucket-index.enabled":                    "true",
			"-blocks-storage.bucket-store.index-cache.backend":                     tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":                              "true",
			// compactor
			"-compactor.cleanup-interval": "1s",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled":   "false",
			"--querier.store-gateway-addresses": "nonExistent", // Make sure we do not call Store gateways
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
			// parquet-converter
			"-parquet-converter.ring.consul.hostname": consul.NetworkHTTPEndpoint(),
			"-parquet-converter.conversion-interval":  "1s",
			"-parquet-converter.enabled":              "true",
			// Querier - Enable Thanos engine with projection optimizer
			"-querier.thanos-engine":                            "true",
			"-querier.optimizers":                               "default,projection", // Enable projection optimizer
			"-querier.enable-parquet-queryable":                 "true",
			"-querier.parquet-queryable-honor-projection-hints": "true", // Honor projection hints
			// Set query-ingesters-within to 2h so queries older than 2h don't hit ingesters
			// Since test queries are 24-48h old, they won't query ingesters and projection will be enabled
			"-querier.query-ingesters-within": "2h",
			// Enable cache for parquet labels and chunks
			"-blocks-storage.bucket-store.parquet-labels-cache.backend":             "inmemory,memcached",
			"-blocks-storage.bucket-store.parquet-labels-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
			"-blocks-storage.bucket-store.chunks-cache.backend":                     "inmemory,memcached",
			"-blocks-storage.bucket-store.chunks-cache.memcached.addresses":         "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		},
	)

	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	ctx := context.Background()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	dir := filepath.Join(s.SharedDir(), "data")
	numSeries := 20
	numSamples := 100
	lbls := make([]labels.Labels, 0, numSeries)
	scrapeInterval := time.Minute
	statusCodes := []string{"200", "400", "404", "500", "502"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	now := time.Now()
	// Make sure query time is old enough to not overlap with ingesters
	// With query-ingesters-within=2h, queries with maxT < now-2h won't hit ingesters
	// Using 24h-48h ago ensures no ingester overlap, allowing projection to be enabled
	start := now.Add(-time.Hour * 48)
	end := now.Add(-time.Hour * 24)

	// Create series with multiple labels
	for i := 0; i < numSeries; i++ {
		lbls = append(lbls, labels.FromStrings(
			labels.MetricName, "http_requests_total",
			"job", "api-server",
			"instance", fmt.Sprintf("instance-%d", i%5),
			"status_code", statusCodes[i%len(statusCodes)],
			"method", methods[i%len(methods)],
			"path", fmt.Sprintf("/api/v1/endpoint%d", i%3),
			"cluster", "test-cluster",
		))
	}

	id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)

	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait until we convert the blocks to parquet
	cortex_testutil.Poll(t, 30*time.Second, true, func() interface{} {
		found := false
		foundBucketIndex := false

		err := bkt.Iter(context.Background(), "", func(name string) error {
			if name == fmt.Sprintf("parquet-markers/%v-parquet-converter-mark.json", id.String()) {
				found = true
			}
			if name == "bucket-index.json.gz" {
				foundBucketIndex = true
			}
			return nil
		}, objstore.WithRecursiveIter())
		require.NoError(t, err)
		return found && foundBucketIndex
	})

	c, err := e2ecortex.NewClient("", cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	testCases := []struct {
		name           string
		query          string
		expectedLabels []string // Labels that should be present in result
	}{
		{
			name:           "vector selector query should not use projection",
			query:          `http_requests_total`,
			expectedLabels: []string{"__name__", "job", "instance", "status_code", "method", "path", "cluster"},
		},
		{
			name:           "simple_sum_by_job",
			query:          `sum by (job) (http_requests_total)`,
			expectedLabels: []string{"job"},
		},
		{
			name:           "rate_with_aggregation",
			query:          `sum by (method) (rate(http_requests_total[5m]))`,
			expectedLabels: []string{"method"},
		},
		{
			name:           "multiple_grouping_labels",
			query:          `sum by (job, status_code) (http_requests_total)`,
			expectedLabels: []string{"job", "status_code"},
		},
		{
			name:           "aggregation without query",
			query:          `sum without (instance, method) (http_requests_total)`,
			expectedLabels: []string{"job", "status_code", "path", "cluster"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing: %s", tc.query)

			// Execute instant query
			result, err := c.Query(tc.query, end)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Verify we got results
			matrix, ok := result.(model.Matrix)
			require.True(t, ok, "result should be a matrix")
			require.NotEmpty(t, matrix, "query should return results")

			t.Logf("Query returned %d series", len(matrix))

			// Verify projection worked: series should only have the expected labels
			for _, series := range matrix {
				actualLabels := make(map[string]struct{})
				for _, label := range series.Metric {
					actualLabels[string(label.Name)] = struct{}{}
				}

				// Check that all expected labels are present
				for _, expectedLabel := range tc.expectedLabels {
					_, ok := actualLabels[expectedLabel]
					require.True(t, ok,
						"series should have %s label", expectedLabel)
				}

				// Check that no unexpected labels are present
				for lbl := range actualLabels {
					if !slices.Contains(tc.expectedLabels, lbl) {
						require.Fail(t, "series should not have unexpected label: %s", lbl)
					}
				}
			}
		})
	}

	// Verify that parquet blocks were queried
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_parquet_queryable_blocks_queried_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "type", "parquet"))))
}
