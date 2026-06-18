//go:build integration_query_fuzz

package integration

import (
	"context"
	"fmt"
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

			rangeRes, err := c.QueryRange(`test_series_a`, start, end, scrapeInterval)
			require.NoError(t, err)
			rangeMatrix, ok := rangeRes.(model.Matrix)
			require.True(t, ok)
			require.Len(t, rangeMatrix, seriesPerMetric)

			if tc.viaStoreGateway {
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_querier_storegateway_instances_hit_per_query"}, e2e.WithMetricCount, e2e.SkipMissingMetrics))
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_querier_blocks_consistency_checks_total"}, e2e.SkipMissingMetrics))
			} else {
				require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_parquet_queryable_blocks_queried_total"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "type", "parquet"))))
			}
		})
	}
}
