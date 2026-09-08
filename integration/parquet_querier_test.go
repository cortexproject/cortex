//go:build integration_query_fuzz

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/promqlsmith"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
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
	rnd := newFuzzRand(t)
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

	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)

	// Upload the block before starting cortex so the first compactor scan finds
	// the complete block and includes it in the bucket index immediately.
	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until we convert the blocks
	cortex_testutil.Poll(t, 60*time.Second, true, func() interface{} {
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
		// @ modifier and offset disabled: known bug in Prometheus (e.g. predict_linear with @/offset can panic).
		promqlsmith.WithEnabledFunctions(enabledFunctions),
		promqlsmith.WithEnabledAggrs(enabledAggrs),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, end, start, end, scrapeInterval, 1000, true)

	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_parquet_queryable_blocks_queried_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "type", "parquet"))))
}

func TestParquetProjectionPushdownFuzz(t *testing.T) {
	t.Skip("Disabled due to flakiness")

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
			"-querier.thanos-engine":            "true",
			"-querier.optimizers":               "propagate-matchers,sort-matchers,merge-selects,detect-histogram-stats,projection", // Enable all optimizers including projection
			"-querier.enable-parquet-queryable": "true",
			"-querier.honor-projection-hints":   "true", // Honor projection hints
			// Set query-ingesters-within to 2h so queries older than 2h don't hit ingesters
			// Since test queries are 24-48h old, they won't query ingesters and projection will be enabled
			"-limits.query-ingesters-within": "2h",
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
	rnd := newFuzzRand(t)
	dir := filepath.Join(s.SharedDir(), "data")
	numSeries := 20
	numSamples := 100
	lbls := make([]labels.Labels, 0, numSeries)
	scrapeInterval := time.Minute
	statusCodes := []string{"200", "400", "404", "500", "502"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	now := time.Now()
	// Make sure query time is old enough to not overlap with ingesters.
	start := now.Add(-time.Hour * 72)
	end := now.Add(-time.Hour * 48)

	// Create series with multiple labels
	for i := range numSeries {
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
	bkt := storage.GetBucket()
	userBucket := bucket.NewUserBucketClient("user-1", bkt, nil)

	err = block.Upload(ctx, log.Logger, userBucket, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait until we convert the blocks to parquet AND bucket index is updated
	cortex_testutil.Poll(t, 300*time.Second, true, func() interface{} {
		// Check if parquet marker exists
		markerFound := false
		err := userBucket.Iter(context.Background(), "", func(name string) error {
			if name == fmt.Sprintf("parquet-markers/%v-parquet-converter-mark.json", id.String()) {
				markerFound = true
			}
			return nil
		}, objstore.WithRecursiveIter())
		if err != nil || !markerFound {
			return false
		}

		// Check if bucket index exists AND contains the parquet block metadata
		idx, err := bucketindex.ReadIndex(ctx, bkt, "user-1", nil, log.Logger)
		if err != nil {
			return false
		}

		// Verify the block is in the bucket index with parquet metadata
		for _, b := range idx.Blocks {
			if b.ID == id && b.Parquet != nil {
				require.True(t, b.Parquet.Version == cortex_parquet.CurrentVersion)
				return true
			}
		}
		return false
	})

	c, err := e2ecortex.NewClient("", cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Wait for data to be queryable before running the projection hints tests
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
			vector, ok := result.(model.Vector)
			require.True(t, ok, "result should be a vector")
			require.NotEmpty(t, vector, "query should return results")

			t.Logf("Query returned %d series", len(vector))

			// Verify projection worked: series should only have the expected labels
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

	// Verify that parquet blocks were queried
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_parquet_queryable_blocks_queried_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "type", "parquet"))))
}

func TestParquetMultiShardQuery(t *testing.T) {
	for name, tc := range map[string]struct {
		viaStoreGateway bool
		// replicationFactor is the store-gateway sharding ring replication factor.
		replicationFactor string
		// extraStoreGateways is the number of additional store-gateway replicas to
		// start alongside the one embedded in the single binary (target "all").
		extraStoreGateways int
	}{
		"querier parquet queryable":                           {viaStoreGateway: false, replicationFactor: "1", extraStoreGateways: 0},
		"store-gateway parquet bucket store":                  {viaStoreGateway: true, replicationFactor: "1", extraStoreGateways: 0},
		"store-gateway parquet bucket store with replication": {viaStoreGateway: true, replicationFactor: "2", extraStoreGateways: 1},
	} {
		t.Run(name, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			consul := e2edb.NewConsulWithName("consul")
			require.NoError(t, s.StartAndWaitReady(consul))

			const (
				// 2 metrics * seriesPerMetric unique series. Sized together with the
				// converter flags below so the block is split into exactly 2 shards.
				seriesPerMetric    = 10
				totalSeries        = seriesPerMetric * 2 // 20
				maxRowsPerRowGroup = 10
				numRowGroups       = 1
				expectedShards     = 2 // ceil(20 / (1 * 10))
				seriesSize         = 10
			)

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
					// compactor
					"-compactor.cleanup-interval": "1s",
					// Ingester.
					"-ring.store":      "consul",
					"-consul.hostname": consul.NetworkHTTPEndpoint(),
					// Distributor.
					"-distributor.replication-factor": "1",
					// alert manager
					"-alertmanager.web.external-url": "http://localhost/alertmanager",
					// Don't query ingesters: the queried time range is older than this,
					// so all data is served exclusively from parquet blocks.
					"-limits.query-ingesters-within": "2h",
					// parquet-converter
					"-parquet-converter.ring.consul.hostname":   consul.NetworkHTTPEndpoint(),
					"-parquet-converter.conversion-interval":    "1s",
					"-parquet-converter.enabled":                "true",
					"-parquet-converter.num-row-groups":         strconv.Itoa(numRowGroups),
					"-parquet-converter.max-rows-per-row-group": strconv.Itoa(maxRowsPerRowGroup),
				},
			)

			if tc.viaStoreGateway {
				// Route reads through the store-gateway's parquet bucket store.
				flags = mergeFlags(flags, map[string]string{
					"-blocks-storage.bucket-store.bucket-store-type": "parquet",
					// Enable sharding so the querier discovers the store-gateway via the
					// ring and routes block queries to it.
					"-store-gateway.sharding-enabled":                 "true",
					"-store-gateway.sharding-ring.store":              "consul",
					"-store-gateway.sharding-ring.consul.hostname":    consul.NetworkHTTPEndpoint(),
					"-store-gateway.sharding-ring.replication-factor": tc.replicationFactor,
					// Disable the embedded parquet queryable so reads go to the store-gateway.
					"-querier.enable-parquet-queryable": "false",
				})
			} else {
				// Query directly via the querier's embedded parquet queryable.
				flags = mergeFlags(flags, map[string]string{
					"-store-gateway.sharding-enabled":                  "false",
					"--querier.store-gateway-addresses":                "nonExistent", // Make sure we do not call Store gateways
					"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
					"-querier.enable-parquet-queryable":                "true",
				})
			}

			// make alert manager config dir
			require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

			ctx := context.Background()
			rnd := newFuzzRand(t)
			dir := filepath.Join(s.SharedDir(), "data")
			numSamples := 60
			scrapeInterval := time.Minute
			now := time.Now()
			// Keep the whole range older than -limits.query-ingesters-within (2h)
			// so queries are served exclusively from parquet blocks.
			start := now.Add(-time.Hour * 24)
			end := now.Add(-time.Hour * 3)

			// Generate unique series so the converter produces a deterministic series count.
			lbls := make([]labels.Labels, 0, totalSeries)
			for i := 0; i < seriesPerMetric; i++ {
				lbls = append(lbls, labels.FromStrings(labels.MetricName, "test_series_a", "job", "test", "instance", strconv.Itoa(i)))
				lbls = append(lbls, labels.FromStrings(labels.MetricName, "test_series_b", "job", "test", "instance", strconv.Itoa(i)))
			}

			id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), seriesSize)
			require.NoError(t, err)
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, err)
			bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)

			// Upload the block before starting cortex so the first compactor scan finds
			// the complete block and includes it in the bucket index immediately.
			err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
			require.NoError(t, err)

			cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
			require.NoError(t, s.StartAndWaitReady(cortex))

			// Start additional store-gateway replicas
			for i := 0; i < tc.extraStoreGateways; i++ {
				storeGateway := e2ecortex.NewStoreGateway(
					fmt.Sprintf("store-gateway-%d", i+1),
					e2ecortex.RingStoreConsul,
					consul.NetworkHTTPEndpoint(),
					// Override the target so this instance only runs the store-gateway
					mergeFlags(flags, map[string]string{"-target": "store-gateway"}),
					"",
				)
				require.NoError(t, s.StartAndWaitReady(storeGateway))
			}

			// Ensure all store-gateways (embedded + extra replicas) are ACTIVE
			if tc.extraStoreGateways > 0 {
				expectedStoreGateways := float64(1 + tc.extraStoreGateways)
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(expectedStoreGateways), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "name", "store-gateway"),
					labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
			}

			// Wait until the block is converted to parquet and the bucket index is updated.
			cortex_testutil.Poll(t, 120*time.Second, true, func() interface{} {
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

			// Verify the converter actually split the block into the expected number of shards.
			marker, err := cortex_parquet.ReadConverterMark(ctx, id, bkt, log.Logger)
			require.NoError(t, err)
			require.Equal(t, expectedShards, marker.Shards, "block should be split into multiple parquet shards")

			// Verify each shard's parquet files (labels + chunks) exist in object storage.
			for shardID := 0; shardID < expectedShards; shardID++ {
				labelsFile := fmt.Sprintf("%s/%d.labels.parquet", id.String(), shardID)
				chunksFile := fmt.Sprintf("%s/%d.chunks.parquet", id.String(), shardID)

				exists, err := bkt.Exists(ctx, labelsFile)
				require.NoError(t, err)
				require.True(t, exists, "labels parquet file should exist for shard %d", shardID)

				exists, err = bkt.Exists(ctx, chunksFile)
				require.NoError(t, err)
				require.True(t, exists, "chunks parquet file should exist for shard %d", shardID)
			}

			// Verify the block is registered in the bucket index as a parquet block with the expected shard count.
			cortex_testutil.Poll(t, 60*time.Second, true, func() interface{} {
				idx, err := bucketindex.ReadIndex(ctx, storage.GetBucket(), "user-1", nil, log.Logger)
				if err != nil {
					return false
				}
				for _, b := range idx.Blocks {
					if b.ID == id && b.Parquet != nil && b.Parquet.Shards == expectedShards {
						return true
					}
				}
				return false
			})

			c, err := e2ecortex.NewClient("", cortex.HTTPEndpoint(), "", "", "user-1")
			require.NoError(t, err)

			// Wait until all series are queryable across both shards.
			cortex_testutil.Poll(t, 120*time.Second, true, func() interface{} {
				labelSets, err := c.Series([]string{`{job="test"}`}, start, end)
				if err != nil {
					return false
				}
				return len(labelSets) == totalSeries
			})

			if tc.viaStoreGateway {
				// wait until the parquet block is converted
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_blocks_meta_synced"},
					e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "state", "parquet-converted"))))
			}

			rangeRes, err := c.QueryRange(`test_series_a`, start, end, scrapeInterval)
			require.NoError(t, err)
			rangeMatrix, ok := rangeRes.(model.Matrix)
			require.True(t, ok)
			require.Len(t, rangeMatrix, seriesPerMetric)

			if tc.viaStoreGateway {
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_querier_storegateway_instances_hit_per_query"}, e2e.WithMetricCount, e2e.SkipMissingMetrics))
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_querier_blocks_consistency_checks_total"}, e2e.SkipMissingMetrics))
				// The store-gateway parquet bucket store resolves the shard count via the
				// bucket index loader, which is tagged with component="store-gateway".
				if tc.extraStoreGateways == 0 {
					// Only assert on the single binary when there are no extra replicas, since
					// with replication the query may be served by another store-gateway.
					require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_bucket_index_loads_total"}, e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "component", "store-gateway"))))
				}
			} else {
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_parquet_queryable_blocks_queried_total"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "type", "parquet"))))
				// The querier's bucket index loader is tagged with component="store-queryable".
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_bucket_index_loads_total"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "component", "store-queryable"))))
			}
		})
	}
}

func TestParquetStoreGateway_HybridMode(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())
	flags := mergeFlags(baseFlags, map[string]string{
		// No parquet-converter service: TSDB block will never be auto-converted.
		"-target": "all",
		"-blocks-storage.tsdb.block-ranges-period":                             "1m,24h",
		"-blocks-storage.tsdb.ship-interval":                                   "1s",
		"-blocks-storage.bucket-store.sync-interval":                           "1s",
		"-blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl": "1s",
		"-blocks-storage.bucket-store.bucket-index.idle-timeout":               "1s",
		"-blocks-storage.bucket-store.bucket-index.enabled":                    "true",
		// Route reads through the store-gateway Parquet bucket store.
		"-blocks-storage.bucket-store.bucket-store-type":  "parquet",
		"-compactor.cleanup-interval":                     "1s",
		"-ring.store":                                     "consul",
		"-consul.hostname":                                consul.NetworkHTTPEndpoint(),
		"-distributor.replication-factor":                 "1",
		"-store-gateway.sharding-enabled":                 "true",
		"-store-gateway.sharding-ring.store":              "consul",
		"-store-gateway.sharding-ring.consul.hostname":    consul.NetworkHTTPEndpoint(),
		"-store-gateway.sharding-ring.replication-factor": "1",
		"-querier.enable-parquet-queryable":               "false",
		"-limits.query-ingesters-within":                  "2h",
		"-alertmanager.web.external-url":                  "http://localhost/alertmanager",
		"-parquet-converter.enabled":                      "true", // enables EnableParquet() in the compactor's bucket index updater
	})

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	const (
		userID        = "user-1"
		metricParquet = "series_parquet"
		metricTSDB    = "series_tsdb"
		metricMerge   = "series_merge"
		numSamples    = 60
	)

	ctx := context.Background()
	rnd := newFuzzRand(t)
	dir := filepath.Join(s.SharedDir(), "data")
	scrapeInterval := time.Minute
	now := time.Now()
	// Both time ranges must be older than -limits.query-ingesters-within (2h).
	midPoint := now.Add(-time.Hour * 10)
	start := now.Add(-time.Hour * 24)
	end := now.Add(-time.Hour * 3)

	// Block A: series_parquet [start, midPoint) — will be converted to Parquet block.
	// Also carries two series_merge series (labeled "pk", a Parquet-only label) to verify
	// that Series/LabelNames/LabelValues correctly merge results across both blocks.
	idA, err := e2e.CreateBlock(ctx, rnd, dir,
		[]labels.Labels{
			labels.FromStrings(labels.MetricName, metricParquet, "job", "test"),
			labels.FromStrings(labels.MetricName, metricMerge, "job", "test", "series", "a", "pk", "1"),
			labels.FromStrings(labels.MetricName, metricMerge, "job", "test", "series", "c", "pk", "1"),
		},
		numSamples, start.UnixMilli(), midPoint.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)

	// Block B: series_tsdb [midPoint, end) — stays as TSDB block.
	// Also carries three series_merge series (labeled "tk", a TSDB-only label). The "series"
	// value "c" is shared with block A (under a different label set) to exercise
	// de-duplication in the merged LabelValues response.
	idB, err := e2e.CreateBlock(ctx, rnd, dir,
		[]labels.Labels{
			labels.FromStrings(labels.MetricName, metricTSDB, "job", "test"),
			labels.FromStrings(labels.MetricName, metricMerge, "job", "test", "series", "b", "tk", "1"),
			labels.FromStrings(labels.MetricName, metricMerge, "job", "test", "series", "c", "tk", "1"),
			labels.FromStrings(labels.MetricName, metricMerge, "job", "test", "series", "d", "tk", "1"),
		},
		numSamples, midPoint.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	userBkt := bucket.NewUserBucketClient(userID, storage.GetBucket(), nil)

	// Upload both TSDB blocks to object storage.
	require.NoError(t, block.Upload(ctx, log.Logger, userBkt, filepath.Join(dir, idA.String()), metadata.NoneFunc))
	require.NoError(t, block.Upload(ctx, log.Logger, userBkt, filepath.Join(dir, idB.String()), metadata.NoneFunc))

	// Manually convert block A to Parquet in object storage.
	{
		tsdbBlock, openErr := prom_tsdb.OpenBlock(nil, filepath.Join(dir, idA.String()), chunkenc.NewPool(), prom_tsdb.DefaultPostingsDecoderFactory)
		require.NoError(t, openErr)
		numShards, convertErr := convert.ConvertTSDBBlock(ctx, userBkt,
			tsdbBlock.MinTime(), tsdbBlock.MaxTime(),
			[]convert.Convertible{tsdbBlock}, promslog.NewNopLogger(),
			convert.WithName(idA.String()))
		require.NoError(t, tsdbBlock.Close())
		require.NoError(t, convertErr)
		marker := cortex_parquet.ConverterMark{
			Version: cortex_parquet.CurrentVersion,
			Shards:  numShards,
		}
		markerBytes, marshalErr := json.Marshal(marker)
		require.NoError(t, marshalErr)
		markerPath := path.Join(idA.String(), cortex_parquet.ConverterMarkerFileName)
		require.NoError(t, userBkt.Upload(ctx, markerPath, bytes.NewReader(markerBytes)))
		// Upload at the global marker path so the bucket index updater discovers it.
		require.NoError(t, userBkt.Upload(ctx, bucketindex.ConverterMarkFilePath(idA), strings.NewReader("{}")))
	}

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	c, err := e2ecortex.NewClient("", cortex.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Wait until the compactor has built the bucket index with the correct state:
	// block A must be tagged as Parquet, block B must be TSDB.
	cortex_testutil.Poll(t, 60*time.Second, true, func() any {
		idx, idxErr := bucketindex.ReadIndex(ctx, storage.GetBucket(), userID, nil, log.Logger)
		if idxErr != nil {
			return false
		}
		foundParquetBlock, foundTSDBBlock := false, false
		for _, b := range idx.Blocks {
			switch b.ID {
			case idA:
				if b.Parquet != nil {
					foundParquetBlock = true
				}
			case idB:
				if b.Parquet == nil {
					foundTSDBBlock = true
				}
			}
		}
		return foundParquetBlock && foundTSDBBlock
	})

	// Wait until both metrics are queryable via the store-gateway.
	cortex_testutil.Poll(t, 120*time.Second, true, func() any {
		labelSets, err := c.Series([]string{`{job="test"}`}, start, end)
		if err != nil {
			return false
		}
		foundParquet, foundTSDB := false, false
		for _, ls := range labelSets {
			switch string(ls[model.MetricNameLabel]) {
			case metricParquet:
				foundParquet = true
			case metricTSDB:
				foundTSDB = true
			}
		}
		return foundParquet && foundTSDB
	})

	// series_parquet must be served by the Parquet store.
	resParquet, err := c.QueryRange(metricParquet, start, midPoint, scrapeInterval)
	require.NoError(t, err)
	matrixParquet, ok := resParquet.(model.Matrix)
	require.True(t, ok)
	require.Len(t, matrixParquet, 1, "series_parquet must return one series (served from Parquet store)")

	// series_tsdb must be served by the TSDB store.
	resTSDB, err := c.QueryRange(metricTSDB, midPoint, end, scrapeInterval)
	require.NoError(t, err)
	matrixTSDB, ok := resTSDB.(model.Matrix)
	require.True(t, ok)
	require.Len(t, matrixTSDB, 1, "series_tsdb must return one series (served from TSDB store)")

	// Series() must return the union of series_merge series from both the Parquet block
	// (pk-labeled: a, c) and the TSDB block (tk-labeled: b, c, d).
	mergeMatcher := fmt.Sprintf(`{__name__=%q}`, metricMerge)
	mergedSeries, err := c.Series([]string{mergeMatcher}, start, end)
	require.NoError(t, err)
	require.Len(t, mergedSeries, 5, "series_merge must return the union of series from both stores")
	var gotSeriesValues []string
	for _, ls := range mergedSeries {
		gotSeriesValues = append(gotSeriesValues, string(ls["series"]))
	}
	sort.Strings(gotSeriesValues)
	require.Equal(t, []string{"a", "b", "c", "c", "d"}, gotSeriesValues,
		"series_merge series values must include both blocks' series (c appears twice, once per block)")

	// LabelNames() must merge label names from both stores: "pk" only exists in the Parquet
	// block, "tk" only in the TSDB block.
	names, err := c.LabelNames(start, end, mergeMatcher)
	require.NoError(t, err)
	require.Equal(t, []string{labels.MetricName, "job", "pk", "series", "tk"}, names,
		"LabelNames must merge Parquet-only and TSDB-only label names")

	// LabelValues() must merge and de-duplicate values from both stores: "c" is present in
	// both blocks but must appear only once in the merged, sorted result.
	values, err := c.LabelValues("series", start, end, []string{mergeMatcher})
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"a", "b", "c", "d"}, values,
		"LabelValues must merge and de-duplicate values across both stores")

	// TSDB sub-store must have loaded only block B (loaded=1) and excluded block A (parquet-converted=1).
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_blocks_meta_synced"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "state", "loaded"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_blocks_meta_synced"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "state", "parquet-converted"))))
}

func TestParquetStoreGateway_HybridModeFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())
	flags := mergeFlags(baseFlags, map[string]string{
		// No parquet-converter service: the second block will never be auto-converted, so the
		// store-gateway keeps serving it from the TSDB sub-store (hybrid).
		"-target": "all",
		"-blocks-storage.tsdb.block-ranges-period":                             "1m,24h",
		"-blocks-storage.tsdb.ship-interval":                                   "1s",
		"-blocks-storage.bucket-store.sync-interval":                           "1s",
		"-blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl": "1s",
		"-blocks-storage.bucket-store.bucket-index.idle-timeout":               "1s",
		"-blocks-storage.bucket-store.bucket-index.enabled":                    "true",
		"-blocks-storage.bucket-store.index-cache.backend":                     tsdb.IndexCacheBackendInMemory,
		// Route reads through the store-gateway Parquet (hybrid) bucket store.
		"-blocks-storage.bucket-store.bucket-store-type":  "parquet",
		"-compactor.cleanup-interval":                     "1s",
		"-ring.store":                                     "consul",
		"-consul.hostname":                                consul.NetworkHTTPEndpoint(),
		"-distributor.replication-factor":                 "1",
		"-store-gateway.sharding-enabled":                 "true",
		"-store-gateway.sharding-ring.store":              "consul",
		"-store-gateway.sharding-ring.consul.hostname":    consul.NetworkHTTPEndpoint(),
		"-store-gateway.sharding-ring.replication-factor": "1",
		"-querier.enable-parquet-queryable":               "false",
		// Keep the queried range older than this so all data is served from blocks, not ingesters.
		"-limits.query-ingesters-within": "2h",
		"-alertmanager.web.external-url": "http://localhost/alertmanager",
		// Enables EnableParquet() in the compactor's bucket index updater so the manually-created
		// Parquet marker is honoured in the bucket index.
		"-parquet-converter.enabled":          "true",
		"-frontend.query-vertical-shard-size": "3",
	})

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	const (
		userID     = "user-1"
		numShared  = 6 // series present in both blocks
		numPOnly   = 4 // series present only in the Parquet block
		numTOnly   = 4 // series present only in the TSDB block
		numSamples = 60
	)

	ctx := context.Background()
	rnd := newFuzzRand(t)
	dir := filepath.Join(s.SharedDir(), "data")
	scrapeInterval := time.Minute
	statusCodes := []string{"200", "400", "404", "500", "502"}

	now := time.Now()
	// The whole range must be older than -limits.query-ingesters-within (2h). Block A covers
	// [start, mid) and is converted to Parquet; block B covers [mid, end) and stays TSDB.
	start := now.Add(-time.Hour * 24)
	mid := now.Add(-time.Hour * 13)
	end := now.Add(-time.Hour * 3)

	// shared: same series in both blocks. In the store-gateway these are served by the Parquet
	// store (block A) and the TSDB store (block B) and their chunks are merged for the same
	// series.
	sharedLbls := make([]labels.Labels, 0, numShared)
	for i := 0; i < numShared; i++ {
		sharedLbls = append(sharedLbls, labels.FromStrings(
			labels.MetricName, "test_shared", "job", "test",
			"series", strconv.Itoa(i%3), "status_code", statusCodes[i%5]))
	}
	// parquet-only: only in block A. Carries a Parquet-only label name "pk".
	parquetOnlyLbls := make([]labels.Labels, 0, numPOnly)
	for i := 0; i < numPOnly; i++ {
		parquetOnlyLbls = append(parquetOnlyLbls, labels.FromStrings(
			labels.MetricName, "test_parquet_only", "job", "test",
			"series", strconv.Itoa(i%3), "pk", strconv.Itoa(i)))
	}
	// tsdb-only: only in block B. Carries a TSDB-only label name "tk".
	tsdbOnlyLbls := make([]labels.Labels, 0, numTOnly)
	for i := 0; i < numTOnly; i++ {
		tsdbOnlyLbls = append(tsdbOnlyLbls, labels.FromStrings(
			labels.MetricName, "test_tsdb_only", "job", "test",
			"series", strconv.Itoa(i%3), "tk", strconv.Itoa(i)))
	}

	// Block A (Parquet) gets shared + parquet-only, block B (TSDB) gets shared + tsdb-only.
	lblsA := append(append([]labels.Labels{}, sharedLbls...), parquetOnlyLbls...)
	lblsB := append(append([]labels.Labels{}, sharedLbls...), tsdbOnlyLbls...)
	lblsAll := append(append(append([]labels.Labels{}, sharedLbls...), parquetOnlyLbls...), tsdbOnlyLbls...)

	// Block A [start, mid): converted to Parquet. Block B [mid, end): stays TSDB.
	idA, err := e2e.CreateBlock(ctx, rnd, dir, lblsA, numSamples, start.UnixMilli(), mid.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)
	idB, err := e2e.CreateBlock(ctx, rnd, dir, lblsB, numSamples, mid.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	userBkt := bucket.NewUserBucketClient(userID, storage.GetBucket(), nil)

	// Upload both TSDB blocks to object storage.
	require.NoError(t, block.Upload(ctx, log.Logger, userBkt, filepath.Join(dir, idA.String()), metadata.NoneFunc))
	require.NoError(t, block.Upload(ctx, log.Logger, userBkt, filepath.Join(dir, idB.String()), metadata.NoneFunc))

	// Manually convert block A to Parquet in object storage (block B is left as TSDB).
	{
		tsdbBlock, openErr := prom_tsdb.OpenBlock(nil, filepath.Join(dir, idA.String()), chunkenc.NewPool(), prom_tsdb.DefaultPostingsDecoderFactory)
		require.NoError(t, openErr)
		numShards, convertErr := convert.ConvertTSDBBlock(ctx, userBkt,
			tsdbBlock.MinTime(), tsdbBlock.MaxTime(),
			[]convert.Convertible{tsdbBlock}, promslog.NewNopLogger(),
			convert.WithName(idA.String()))
		require.NoError(t, tsdbBlock.Close())
		require.NoError(t, convertErr)
		marker := cortex_parquet.ConverterMark{
			Version: cortex_parquet.CurrentVersion,
			Shards:  numShards,
		}
		markerBytes, marshalErr := json.Marshal(marker)
		require.NoError(t, marshalErr)
		markerPath := path.Join(idA.String(), cortex_parquet.ConverterMarkerFileName)
		require.NoError(t, userBkt.Upload(ctx, markerPath, bytes.NewReader(markerBytes)))
		// Upload at the global marker path so the bucket index updater discovers it.
		require.NoError(t, userBkt.Upload(ctx, bucketindex.ConverterMarkFilePath(idA), strings.NewReader("{}")))
	}

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until the bucket index reflects the hybrid state: block A tagged Parquet, block B TSDB.
	cortex_testutil.Poll(t, 60*time.Second, true, func() any {
		idx, idxErr := bucketindex.ReadIndex(ctx, storage.GetBucket(), userID, nil, log.Logger)
		if idxErr != nil {
			return false
		}
		foundParquetBlock, foundTSDBBlock := false, false
		for _, b := range idx.Blocks {
			switch b.ID {
			case idA:
				if b.Parquet != nil {
					foundParquetBlock = true
				}
			case idB:
				if b.Parquet == nil {
					foundTSDBBlock = true
				}
			}
		}
		return foundParquetBlock && foundTSDBBlock
	})

	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_blocks_meta_synced"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "state", "loaded"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_blocks_meta_synced"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "state", "parquet-converted"))))

	c1, err := e2ecortex.NewClient("", cortex.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Prometheus reads both blocks directly from the shared data dir, giving it the same data.
	require.NoError(t, writeFileToSharedDir(s, "prometheus.yml", []byte("")))
	prom := e2edb.NewPrometheus("", nil)
	require.NoError(t, s.StartAndWaitReady(prom))

	c2, err := e2ecortex.NewPromQueryClient(prom.HTTPEndpoint())
	require.NoError(t, err)
	waitUntilReady(t, ctx, c1, c2, `{job="test"}`, start, end)

	opts := []promqlsmith.Option{
		promqlsmith.WithEnabledFunctions(enabledFunctions),
		promqlsmith.WithEnabledAggrs(enabledAggrs),
	}
	ps := promqlsmith.New(rnd, lblsAll, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, end, start, end, scrapeInterval, 500, true)

	// Confirm the hybrid path actually routed blocks to both sub-stores.
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_hybrid_bucket_stores_blocks_routed_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "store", "parquet"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_hybrid_bucket_stores_blocks_routed_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "store", "tsdb"))))
}
