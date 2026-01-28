//go:build integration_querier

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util/log"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
)

func TestParquetBucketStore_ProjectionHint(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	minio := e2edb.NewMinio(9000, bucketName)
	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(consul, minio, memcached))

	// Define configuration flags.
	flags := BlocksStorageFlags()
	flags = mergeFlags(flags, map[string]string{
		// Enable Thanos engine and projection optimization.
		"-querier.thanos-engine": "true",
		"-querier.optimizers":    "projection",

		// enable honor-projection-hints querier and store gateway
		"-querier.honor-projection-hints":                     "true",
		"-blocks-storage.bucket-store.honor-projection-hints": "true",
		// enable Store Gateway Parquet mode
		"-blocks-storage.bucket-store.bucket-store-type": "parquet",

		// Set query-ingesters-within to 1h so queries older than 1h don't hit ingesters
		"-querier.query-ingesters-within": "1h",

		// Configure Parquet Converter
		"-parquet-converter.enabled":              "true",
		"-parquet-converter.conversion-interval":  "1s",
		"-parquet-converter.ring.consul.hostname": consul.NetworkHTTPEndpoint(),
		"-compactor.block-ranges":                 "1ms,12h",
		// Enable cache
		"-blocks-storage.bucket-store.parquet-labels-cache.backend":             "inmemory,memcached",
		"-blocks-storage.bucket-store.parquet-labels-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
	})

	// Store Gateway
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(storeGateway))

	// Parquet Converter
	parquetConverter := e2ecortex.NewParquetConverter("parquet-converter", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(parquetConverter))

	// Querier
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.store-gateway-addresses": storeGateway.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Create block
	now := time.Now()
	// Time range: [Now - 24h] to [Now - 20h]
	start := now.Add(-24 * time.Hour)
	end := now.Add(-20 * time.Hour)

	ctx := context.Background()

	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	dir := filepath.Join(s.SharedDir(), "data")
	scrapeInterval := time.Minute
	statusCodes := []string{"200", "400", "404", "500", "502"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}

	numSeries := 10
	numSamples := 100

	lbls := make([]labels.Labels, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		lbls = append(lbls, labels.FromStrings(
			labels.MetricName, "http_requests_total",
			"job", "api-server",
			"instance", fmt.Sprintf("instance-%d", i),
			"status_code", statusCodes[i%len(statusCodes)],
			"method", methods[i%len(methods)],
			"path", fmt.Sprintf("/api/v1/endpoint%d", i%3),
			"cluster", "test-cluster",
		))
	}

	id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)

	storage, err := e2ecortex.NewS3ClientForMinio(minio, bucketName)
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)

	// Upload TSDB Block
	require.NoError(t, block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))

	// Wait until parquet converter convert block
	require.NoError(t, parquetConverter.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_parquet_converter_blocks_converted_total"}, e2e.WaitMissingMetrics))

	// Create client
	c, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	cortex_testutil.Poll(t, 60*time.Second, true, func() interface{} {
		labelSets, err := c.Series([]string{`{job="api-server"}`}, start, end)
		if err != nil {
			t.Logf("Series query failed: %v", err)
			return false
		}
		return len(labelSets) > 0
	})

	testCases := []struct {
		name           string
		query          string
		expectedLabels []string // query result should contain these labels
	}{
		{
			name:  "vector selector query",
			query: `http_requests_total`,
			expectedLabels: []string{
				"__name__", "job", "instance", "status_code", "method", "path", "cluster",
			},
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
			vector, ok := result.(model.Vector)
			require.True(t, ok, "result should be a vector")
			require.NotEmpty(t, vector, "query should return results")

			for _, sample := range vector {
				actualLabels := make(map[string]struct{})
				for label := range sample.Metric {
					actualLabels[string(label)] = struct{}{}
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
}
