//go:build integration_query_fuzz
// +build integration_query_fuzz

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/promqlsmith"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/pkg/util/log"
)

var enabledFunctions []*parser.Function

func init() {
	for _, f := range parser.Functions {
		// Ignore native histogram functions for now as our test cases are only float samples.
		if strings.Contains(f.Name, "histogram") && f.Name != "histogram_quantile" {
			continue
		}
		// Ignore experimental functions for now.
		if !f.Experimental {
			enabledFunctions = append(enabledFunctions, f)
		}
	}
}

func TestNativeHistogramFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())
	flags := mergeFlags(
		baseFlags,
		map[string]string{
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.tsdb.block-ranges-period":         "2h",
			"-blocks-storage.tsdb.ship-interval":               "1h",
			"-blocks-storage.bucket-store.sync-interval":       "1s",
			"-blocks-storage.tsdb.retention-period":            "24h",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":          "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	now := time.Now()
	start := now.Add(-time.Hour * 2)
	end := now.Add(-time.Hour)
	numSeries := 10
	numSamples := 60
	lbls := make([]labels.Labels, 0, numSeries*2)
	scrapeInterval := time.Minute
	statusCodes := []string{"200", "400", "404", "500", "502"}
	for i := 0; i < numSeries; i++ {
		lbls = append(lbls, labels.Labels{
			{Name: labels.MetricName, Value: "test_series_a"},
			{Name: "job", Value: "test"},
			{Name: "series", Value: strconv.Itoa(i % 3)},
			{Name: "status_code", Value: statusCodes[i%5]},
		})

		lbls = append(lbls, labels.Labels{
			{Name: labels.MetricName, Value: "test_series_b"},
			{Name: "job", Value: "test"},
			{Name: "series", Value: strconv.Itoa((i + 1) % 3)},
			{Name: "status_code", Value: statusCodes[(i+1)%5]},
		})
	}

	ctx := context.Background()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	dir := filepath.Join(s.SharedDir(), "data")
	err = os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)
	id, err := e2e.CreateNHBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)
	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait for querier and store to sync blocks.
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "component", "store-gateway"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "component", "querier"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))

	c1, err := e2ecortex.NewClient("", cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	err = writeFileToSharedDir(s, "prometheus.yml", []byte(""))
	require.NoError(t, err)
	prom := e2edb.NewPrometheus("", nil)
	require.NoError(t, s.StartAndWaitReady(prom))

	c2, err := e2ecortex.NewPromQueryClient(prom.HTTPEndpoint())
	require.NoError(t, err)

	waitUntilReady(t, ctx, c1, c2, `{job="test"}`, start, end)

	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledAggrs([]parser.ItemType{
			parser.SUM, parser.MIN, parser.MAX, parser.AVG, parser.GROUP, parser.COUNT, parser.COUNT_VALUES, parser.QUANTILE,
		}),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, end, start, end, scrapeInterval, 1000, false)
}

func TestExperimentalPromQLFuncsWithPrometheus(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())
	flags := mergeFlags(
		baseFlags,
		map[string]string{
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.tsdb.block-ranges-period":         "2h",
			"-blocks-storage.tsdb.ship-interval":               "1h",
			"-blocks-storage.bucket-store.sync-interval":       "1s",
			"-blocks-storage.tsdb.retention-period":            "24h",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":          "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url":      "http://localhost/alertmanager",
			"-frontend.query-vertical-shard-size": "1",
			"-frontend.max-cache-freshness":       "1m",
			// enable experimental promQL funcs
			"-querier.enable-promql-experimental-functions": "true",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	now := time.Now()
	start := now.Add(-time.Hour * 2)
	end := now.Add(-time.Hour)
	numSeries := 10
	numSamples := 60
	lbls := make([]labels.Labels, 0, numSeries*2)
	scrapeInterval := time.Minute
	statusCodes := []string{"200", "400", "404", "500", "502"}
	for i := 0; i < numSeries; i++ {
		lbls = append(lbls, labels.Labels{
			{Name: labels.MetricName, Value: "test_series_a"},
			{Name: "job", Value: "test"},
			{Name: "series", Value: strconv.Itoa(i % 3)},
			{Name: "status_code", Value: statusCodes[i%5]},
		})

		lbls = append(lbls, labels.Labels{
			{Name: labels.MetricName, Value: "test_series_b"},
			{Name: "job", Value: "test"},
			{Name: "series", Value: strconv.Itoa((i + 1) % 3)},
			{Name: "status_code", Value: statusCodes[(i+1)%5]},
		})
	}

	ctx := context.Background()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	dir := filepath.Join(s.SharedDir(), "data")
	err = os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)
	id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)
	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait for querier and store to sync blocks.
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "component", "store-gateway"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "component", "querier"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))

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
		promqlsmith.WithEnableExperimentalPromQLFunctions(true),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, end, start, end, scrapeInterval, 1000, false)
}

func TestDisableChunkTrimmingFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul1 := e2edb.NewConsulWithName("consul1")
	consul2 := e2edb.NewConsulWithName("consul2")
	require.NoError(t, s.StartAndWaitReady(consul1, consul2))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.tsdb.block-ranges-period":         "2h",
			"-blocks-storage.tsdb.ship-interval":               "1h",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.tsdb.retention-period":            "2h",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":          "true",
			// Ingester.
			"-ring.store": "consul",
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)

	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path1 := path.Join(s.SharedDir(), "cortex-1")
	path2 := path.Join(s.SharedDir(), "cortex-2")

	flags1 := mergeFlags(flags, map[string]string{
		"-blocks-storage.filesystem.dir": path1,
		"-consul.hostname":               consul1.NetworkHTTPEndpoint(),
	})
	// Disable chunk trimming for Cortex 2.
	flags2 := mergeFlags(flags, map[string]string{
		"-blocks-storage.filesystem.dir":   path2,
		"-consul.hostname":                 consul2.NetworkHTTPEndpoint(),
		"-ingester.disable-chunk-trimming": "true",
	})
	// Start Cortex replicas.
	cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags1, "")
	cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags2, "")
	require.NoError(t, s.StartAndWaitReady(cortex1, cortex2))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c1, err := e2ecortex.NewClient(cortex1.HTTPEndpoint(), cortex1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	c2, err := e2ecortex.NewClient(cortex2.HTTPEndpoint(), cortex2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	// Push some series to Cortex.
	start := now.Add(-time.Minute * 120)
	scrapeInterval := 30 * time.Second

	numSeries := 10
	numSamples := 240
	serieses := make([]prompb.TimeSeries, numSeries)
	lbls := make([]labels.Labels, numSeries)
	for i := 0; i < numSeries; i++ {
		series := e2e.GenerateSeriesWithSamples("test_series", start, scrapeInterval, i*numSamples, numSamples, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "series", Value: strconv.Itoa(i)})
		serieses[i] = series

		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}

	res, err := c1.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c2.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	waitUntilReady(t, context.Background(), c1, c2, `{job="test"}`, start, now)

	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(enabledFunctions),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	type testCase struct {
		query      string
		res1, res2 model.Value
		err1, err2 error
	}

	queryStart := time.Now().Add(-time.Minute * 40)
	queryEnd := time.Now().Add(-time.Minute * 20)
	cases := make([]*testCase, 0, 200)
	testRun := 500
	var (
		expr  parser.Expr
		query string
	)
	for i := 0; i < testRun; i++ {
		for {
			expr = ps.WalkRangeQuery()
			query = expr.Pretty(0)
			// timestamp is a known function that break with disable chunk trimming.
			if isValidQuery(expr, false) && !strings.Contains(query, "timestamp") {
				break
			}
		}
		res1, err1 := c1.QueryRange(query, queryStart, queryEnd, scrapeInterval)
		res2, err2 := c2.QueryRange(query, queryStart, queryEnd, scrapeInterval)
		cases = append(cases, &testCase{
			query: query,
			res1:  res1,
			res2:  res2,
			err1:  err1,
			err2:  err2,
		})
	}

	failures := 0
	for i, tc := range cases {
		qt := "range query"
		if tc.err1 != nil || tc.err2 != nil {
			if !cmp.Equal(tc.err1, tc.err2) {
				t.Logf("case %d error mismatch.\n%s: %s\nerr1: %v\nerr2: %v\n", i, qt, tc.query, tc.err1, tc.err2)
				failures++
			}
		} else if shouldUseSampleNumComparer(tc.query) {
			if !cmp.Equal(tc.res1, tc.res2, sampleNumComparer) {
				t.Logf("case %d # of samples mismatch.\n%s: %s\nres1: %s\nres2: %s\n", i, qt, tc.query, tc.res1.String(), tc.res2.String())
				failures++
			}
		} else if !cmp.Equal(tc.res1, tc.res2, comparer) {
			t.Logf("case %d results mismatch.\n%s: %s\nres1: %s\nres2: %s\n", i, qt, tc.query, tc.res1.String(), tc.res2.String())
			failures++
		}
	}
	if failures > 0 {
		require.Failf(t, "finished query fuzzing tests", "%d test cases failed", failures)
	}
}

func TestExpandedPostingsCacheFuzz(t *testing.T) {
	stableCortexImage := "quay.io/cortexproject/cortex:v1.18.0"
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul1 := e2edb.NewConsulWithName("consul1")
	consul2 := e2edb.NewConsulWithName("consul2")
	require.NoError(t, s.StartAndWaitReady(consul1, consul2))

	flags1 := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                     blocksStorageEngine,
			"-blocks-storage.backend":                           "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":     "4m",
			"-blocks-storage.tsdb.block-ranges-period":          "2h",
			"-blocks-storage.tsdb.ship-interval":                "1h",
			"-blocks-storage.bucket-store.sync-interval":        "15m",
			"-blocks-storage.tsdb.retention-period":             "2h",
			"-blocks-storage.bucket-store.index-cache.backend":  tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.bucket-store.bucket-index.enabled": "true",
			"-querier.query-store-for-labels-enabled":           "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul1.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)
	flags2 := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                         blocksStorageEngine,
			"-blocks-storage.backend":                               "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":         "4m",
			"-blocks-storage.tsdb.block-ranges-period":              "2h",
			"-blocks-storage.tsdb.ship-interval":                    "1h",
			"-blocks-storage.bucket-store.sync-interval":            "15m",
			"-blocks-storage.tsdb.retention-period":                 "2h",
			"-blocks-storage.bucket-store.index-cache.backend":      tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.bucket-store.bucket-index.enabled":     "true",
			"-querier.query-store-for-labels-enabled":               "true",
			"-blocks-storage.expanded_postings_cache.head.enabled":  "true",
			"-blocks-storage.expanded_postings_cache.block.enabled": "true",
			// Ingester.
			"-ring.store":                        "consul",
			"-consul.hostname":                   consul2.NetworkHTTPEndpoint(),
			"-ingester.matchers-cache-max-items": "10000",
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path1 := path.Join(s.SharedDir(), "cortex-1")
	path2 := path.Join(s.SharedDir(), "cortex-2")

	flags1 = mergeFlags(flags1, map[string]string{"-blocks-storage.filesystem.dir": path1})
	flags2 = mergeFlags(flags2, map[string]string{"-blocks-storage.filesystem.dir": path2})
	// Start Cortex replicas.
	cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags1, stableCortexImage)
	cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags2, "")
	require.NoError(t, s.StartAndWaitReady(cortex1, cortex2))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	var clients []*e2ecortex.Client
	c1, err := e2ecortex.NewClient(cortex1.HTTPEndpoint(), cortex1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	c2, err := e2ecortex.NewClient(cortex2.HTTPEndpoint(), cortex2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	clients = append(clients, c1, c2)

	now := time.Now()
	// Push some series to Cortex.
	start := now.Add(-24 * time.Hour)
	scrapeInterval := 30 * time.Second

	numSeries := 10
	numberOfLabelsPerSeries := 5
	numSamples := 10
	ss := make([]prompb.TimeSeries, numSeries*numberOfLabelsPerSeries)
	lbls := make([]labels.Labels, numSeries*numberOfLabelsPerSeries)

	for i := 0; i < numSeries; i++ {
		for j := 0; j < numberOfLabelsPerSeries; j++ {
			series := e2e.GenerateSeriesWithSamples(
				fmt.Sprintf("test_series_%d", i),
				start,
				scrapeInterval,
				i*numSamples,
				numSamples,
				prompb.Label{Name: "test_label", Value: fmt.Sprintf("test_label_value_%d", j)},
			)
			ss[i*numberOfLabelsPerSeries+j] = series

			builder := labels.NewBuilder(labels.EmptyLabels())
			for _, lbl := range series.Labels {
				builder.Set(lbl.Name, lbl.Value)
			}
			lbls[i*numberOfLabelsPerSeries+j] = builder.Labels()
		}
	}

	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	// Create the queries with the original labels
	testRun := 300
	queries := make([]string, 0, testRun)
	matchers := make([]string, 0, testRun)
	for i := 0; i < testRun; i++ {
		expr := ps.WalkRangeQuery()
		if isValidQuery(expr, true) {
			break
		}
		queries = append(queries, expr.Pretty(0))
		matchers = append(matchers, storepb.PromMatchersToString(
			append(
				ps.WalkSelectors(),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", fmt.Sprintf("test_series_%d", i%numSeries)),
			)...))
	}

	// Lets run multiples iterations and create new series every iteration
	for k := 0; k < 5; k++ {

		nss := make([]prompb.TimeSeries, numSeries*numberOfLabelsPerSeries)
		for i := 0; i < numSeries; i++ {
			for j := 0; j < numberOfLabelsPerSeries; j++ {
				nss[i*numberOfLabelsPerSeries+j] = e2e.GenerateSeriesWithSamples(
					fmt.Sprintf("test_series_%d", i),
					start.Add(scrapeInterval*time.Duration(numSamples*j)),
					scrapeInterval,
					i*numSamples,
					numSamples,
					prompb.Label{Name: "test_label", Value: fmt.Sprintf("test_label_value_%d", j)},
					prompb.Label{Name: "k", Value: fmt.Sprintf("%d", k)},
				)
			}
		}

		for _, client := range clients {
			res, err := client.Push(nss)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)
		}

		type testCase struct {
			query        string
			qt           string
			res1, res2   model.Value
			sres1, sres2 []model.LabelSet
			err1, err2   error
		}

		cases := make([]*testCase, 0, len(queries)*3)

		for _, query := range queries {
			fuzzyTime := time.Duration(rand.Int63n(time.Now().UnixMilli() - start.UnixMilli()))
			queryEnd := start.Add(fuzzyTime * time.Millisecond)
			res1, err1 := c1.Query(query, queryEnd)
			res2, err2 := c2.Query(query, queryEnd)
			cases = append(cases, &testCase{
				query: query,
				qt:    "instant",
				res1:  res1,
				res2:  res2,
				err1:  err1,
				err2:  err2,
			})
			res1, err1 = c1.QueryRange(query, start, queryEnd, scrapeInterval)
			res2, err2 = c2.QueryRange(query, start, queryEnd, scrapeInterval)
			cases = append(cases, &testCase{
				query: query,
				qt:    "range query",
				res1:  res1,
				res2:  res2,
				err1:  err1,
				err2:  err2,
			})
		}

		for _, m := range matchers {
			fuzzyTime := time.Duration(rand.Int63n(time.Now().UnixMilli() - start.UnixMilli()))
			queryEnd := start.Add(fuzzyTime * time.Millisecond)
			res1, err := c1.Series([]string{m}, start, queryEnd)
			require.NoError(t, err)
			res2, err := c2.Series([]string{m}, start, queryEnd)
			require.NoError(t, err)
			cases = append(cases, &testCase{
				query: m,
				qt:    "get series",
				sres1: res1,
				sres2: res2,
			})
		}

		failures := 0
		for i, tc := range cases {
			if tc.err1 != nil || tc.err2 != nil {
				if !cmp.Equal(tc.err1, tc.err2) {
					t.Logf("case %d error mismatch.\n%s: %s\nerr1: %v\nerr2: %v\n", i, tc.qt, tc.query, tc.err1, tc.err2)
					failures++
				}
			} else if shouldUseSampleNumComparer(tc.query) {
				if !cmp.Equal(tc.res1, tc.res2, sampleNumComparer) {
					t.Logf("case %d # of samples mismatch.\n%s: %s\nres1: %s\nres2: %s\n", i, tc.qt, tc.query, tc.res1.String(), tc.res2.String())
					failures++
				}
			} else if !cmp.Equal(tc.res1, tc.res2, comparer) {
				t.Logf("case %d results mismatch.\n%s: %s\nres1: %s\nres2: %s\n", i, tc.qt, tc.query, tc.res1.String(), tc.res2.String())
				failures++
			} else if !cmp.Equal(tc.sres1, tc.sres1, labelSetsComparer) {
				t.Logf("case %d results mismatch.\n%s: %s\nsres1: %s\nsres2: %s\n", i, tc.qt, tc.query, tc.sres1, tc.sres2)
				failures++
			}
		}
		if failures > 0 {
			require.Failf(t, "finished query fuzzing tests", "%d test cases failed", failures)
		}
	}
}

func TestVerticalShardingFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul1 := e2edb.NewConsulWithName("consul1")
	consul2 := e2edb.NewConsulWithName("consul2")
	require.NoError(t, s.StartAndWaitReady(consul1, consul2))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.tsdb.block-ranges-period":         "2h",
			"-blocks-storage.tsdb.ship-interval":               "1h",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.tsdb.retention-period":            "2h",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":          "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul1.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path1 := path.Join(s.SharedDir(), "cortex-1")
	path2 := path.Join(s.SharedDir(), "cortex-2")

	flags1 := mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path1})
	// Start Cortex replicas.
	cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags1, "")
	// Enable vertical sharding for the second Cortex instance.
	flags2 := mergeFlags(flags, map[string]string{
		"-frontend.query-vertical-shard-size": "2",
		"-blocks-storage.filesystem.dir":      path2,
		"-consul.hostname":                    consul2.NetworkHTTPEndpoint(),
	})
	cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags2, "")
	require.NoError(t, s.StartAndWaitReady(cortex1, cortex2))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c1, err := e2ecortex.NewClient(cortex1.HTTPEndpoint(), cortex1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	c2, err := e2ecortex.NewClient(cortex2.HTTPEndpoint(), cortex2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	// Push some series to Cortex.
	start := now.Add(-time.Minute * 10)
	end := now.Add(-time.Minute * 1)
	numSeries := 3
	numSamples := 20
	lbls := make([]labels.Labels, numSeries*2)
	serieses := make([]prompb.TimeSeries, numSeries*2)
	scrapeInterval := 30 * time.Second
	for i := 0; i < numSeries; i++ {
		series := e2e.GenerateSeriesWithSamples("test_series_a", start, scrapeInterval, i*numSamples, numSamples, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "series", Value: strconv.Itoa(i)})
		serieses[i] = series
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}
	// Generate another set of series for testing binary expression and vector matching.
	for i := numSeries; i < 2*numSeries; i++ {
		prompbLabels := []prompb.Label{{Name: "job", Value: "test"}, {Name: "series", Value: strconv.Itoa(i)}}
		if i%3 == 0 {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "200"})
		} else if i%3 == 1 {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "400"})
		} else {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "500"})
		}
		series := e2e.GenerateSeriesWithSamples("test_series_b", start, scrapeInterval, i*numSamples, numSamples, prompbLabels...)
		serieses[i] = series
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}
	res, err := c1.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c2.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	waitUntilReady(t, context.Background(), c1, c2, `{job="test"}`, start, end)

	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(enabledFunctions),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, now, start, end, scrapeInterval, 1000, false)
}

func TestProtobufCodecFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul1 := e2edb.NewConsulWithName("consul1")
	consul2 := e2edb.NewConsulWithName("consul2")
	require.NoError(t, s.StartAndWaitReady(consul1, consul2))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.tsdb.block-ranges-period":         "2h",
			"-blocks-storage.tsdb.ship-interval":               "1h",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.tsdb.retention-period":            "2h",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":          "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul1.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path1 := path.Join(s.SharedDir(), "cortex-1")
	path2 := path.Join(s.SharedDir(), "cortex-2")

	flags1 := mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path1})
	// Start Cortex replicas.
	cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags1, "")
	// Enable protobuf codec for the second Cortex instance.
	flags2 := mergeFlags(flags, map[string]string{
		"-api.querier-default-codec":     "protobuf",
		"-blocks-storage.filesystem.dir": path2,
		"-consul.hostname":               consul2.NetworkHTTPEndpoint(),
	})
	cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags2, "")
	require.NoError(t, s.StartAndWaitReady(cortex1, cortex2))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c1, err := e2ecortex.NewClient(cortex1.HTTPEndpoint(), cortex1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	c2, err := e2ecortex.NewClient(cortex2.HTTPEndpoint(), cortex2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	// Push some series to Cortex.
	start := now.Add(-time.Minute * 10)
	end := now.Add(-time.Minute * 1)
	numSeries := 3
	numSamples := 20
	lbls := make([]labels.Labels, numSeries*2)
	serieses := make([]prompb.TimeSeries, numSeries*2)
	scrapeInterval := 30 * time.Second
	for i := 0; i < numSeries; i++ {
		series := e2e.GenerateSeriesWithSamples("test_series_a", start, scrapeInterval, i*numSamples, numSamples, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "series", Value: strconv.Itoa(i)})
		serieses[i] = series
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}
	// Generate another set of series for testing binary expression and vector matching.
	for i := numSeries; i < 2*numSeries; i++ {
		prompbLabels := []prompb.Label{{Name: "job", Value: "test"}, {Name: "series", Value: strconv.Itoa(i)}}
		if i%3 == 0 {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "200"})
		} else if i%3 == 1 {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "400"})
		} else {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "500"})
		}
		series := e2e.GenerateSeriesWithSamples("test_series_b", start, scrapeInterval, i*numSamples, numSamples, prompbLabels...)
		serieses[i] = series
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}
	res, err := c1.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c2.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	waitUntilReady(t, context.Background(), c1, c2, `{job="test"}`, start, end)

	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(enabledFunctions),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, now, start, end, scrapeInterval, 1000, false)
}

var sampleNumComparer = cmp.Comparer(func(x, y model.Value) bool {
	if x.Type() != y.Type() {
		return false
	}

	vx, xvec := x.(model.Vector)
	vy, yvec := y.(model.Vector)

	if xvec && yvec {
		return len(vx) == len(vy)
	}

	mx, xmat := x.(model.Matrix)
	my, ymat := y.(model.Matrix)

	mxSamples := 0
	mySamples := 0

	if xmat && ymat {
		for i := 0; i < len(mx); i++ {
			mxSamples += len(mx[i].Values)
		}
		for i := 0; i < len(my); i++ {
			mySamples += len(my[i].Values)
		}
	}

	return mxSamples == mySamples
})

// comparer should be used to compare promql results between engines.
var comparer = cmp.Comparer(func(x, y model.Value) bool {
	if x.Type() != y.Type() {
		return false
	}
	compareFloats := func(l, r float64) bool {
		const epsilon = 1e-6
		const fraction = 1.e-10 // 0.00000001%
		return cmp.Equal(l, r, cmpopts.EquateNaNs(), cmpopts.EquateApprox(fraction, epsilon))
	}
	compareHistogramBucket := func(l, r *model.HistogramBucket) bool {
		return l == r || (l.Boundaries == r.Boundaries && compareFloats(float64(l.Lower), float64(r.Lower)) && compareFloats(float64(l.Upper), float64(r.Upper)) && compareFloats(float64(l.Count), float64(r.Count)))
	}

	compareHistogramBuckets := func(l, r model.HistogramBuckets) bool {
		if len(l) != len(r) {
			return false
		}

		for i := range l {
			if !compareHistogramBucket(l[i], r[i]) {
				return false
			}
		}
		return true
	}

	compareHistograms := func(l, r *model.SampleHistogram) bool {
		return l == r || (compareFloats(float64(l.Count), float64(r.Count)) && compareFloats(float64(l.Sum), float64(r.Sum)) && compareHistogramBuckets(l.Buckets, r.Buckets))
	}

	// count_values returns a metrics with one label {"value": "1.012321"}
	compareValueMetrics := func(l, r model.Metric) (valueMetric bool, equals bool) {
		lLabels := model.LabelSet(l).Clone()
		rLabels := model.LabelSet(r).Clone()
		var (
			lVal, rVal     model.LabelValue
			lFloat, rFloat float64
			ok             bool
			err            error
		)

		if lVal, ok = lLabels["value"]; !ok {
			return false, false
		}

		if rVal, ok = rLabels["value"]; !ok {
			return false, false
		}

		if lFloat, err = strconv.ParseFloat(string(lVal), 64); err != nil {
			return false, false
		}
		if rFloat, err = strconv.ParseFloat(string(rVal), 64); err != nil {
			return false, false
		}

		// Exclude the value label in comparison.
		delete(lLabels, "value")
		delete(rLabels, "value")

		if !lLabels.Equal(rLabels) {
			return false, false
		}

		return true, compareFloats(lFloat, rFloat)
	}
	compareMetrics := func(l, r model.Metric) bool {
		if valueMetric, equals := compareValueMetrics(l, r); valueMetric {
			return equals
		}
		return l.Equal(r)
	}

	vx, xvec := x.(model.Vector)
	vy, yvec := y.(model.Vector)

	if xvec && yvec {
		if len(vx) != len(vy) {
			return false
		}

		// Sort vector before comparing.
		sort.Sort(vx)
		sort.Sort(vy)

		for i := 0; i < len(vx); i++ {
			if !compareMetrics(vx[i].Metric, vy[i].Metric) {
				return false
			}
			if vx[i].Timestamp != vy[i].Timestamp {
				return false
			}
			if !compareFloats(float64(vx[i].Value), float64(vy[i].Value)) {
				return false
			}
			if !compareHistograms(vx[i].Histogram, vy[i].Histogram) {
				return false
			}
		}
		return true
	}

	mx, xmat := x.(model.Matrix)
	my, ymat := y.(model.Matrix)

	if xmat && ymat {
		if len(mx) != len(my) {
			return false
		}
		// Sort matrix before comparing.
		sort.Sort(mx)
		sort.Sort(my)
		for i := 0; i < len(mx); i++ {
			mxs := mx[i]
			mys := my[i]

			if !compareMetrics(mxs.Metric, mys.Metric) {
				return false
			}

			xps := mxs.Values
			yps := mys.Values

			if len(xps) != len(yps) {
				return false
			}
			for j := 0; j < len(xps); j++ {
				if xps[j].Timestamp != yps[j].Timestamp {
					return false
				}
				if !compareFloats(float64(xps[j].Value), float64(yps[j].Value)) {
					return false
				}
			}

			xhs := mxs.Histograms
			yhs := mys.Histograms

			if len(xhs) != len(yhs) {
				return false
			}
			for j := 0; j < len(xhs); j++ {
				if xhs[j].Timestamp != yhs[j].Timestamp {
					return false
				}
				if !compareHistograms(xhs[j].Histogram, yhs[j].Histogram) {
					return false
				}
			}
		}
		return true
	}

	sx, xscalar := x.(*model.Scalar)
	sy, yscalar := y.(*model.Scalar)
	if xscalar && yscalar {
		if sx.Timestamp != sy.Timestamp {
			return false
		}
		return compareFloats(float64(sx.Value), float64(sy.Value))
	}
	return false
})

func TestStoreGatewayLazyExpandedPostingsSeriesFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul1 := e2edb.NewConsulWithName("consul1")
	consul2 := e2edb.NewConsulWithName("consul2")
	require.NoError(t, s.StartAndWaitReady(consul1, consul2))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
		"-querier.query-store-for-labels-enabled":          "true",
		"-ring.store":                                "consul",
		"-consul.hostname":                           consul1.NetworkHTTPEndpoint(),
		"-store-gateway.sharding-enabled":            "false",
		"-blocks-storage.bucket-store.sync-interval": "1s",
	})
	// Enable lazy expanded postings.
	flags2 := mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.lazy-expanded-postings-enabled": "true",
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Cortex replicas.
	ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul1.NetworkHTTPEndpoint(), flags, "")
	ingester2 := e2ecortex.NewIngester("ingester-2", e2ecortex.RingStoreConsul, consul2.NetworkHTTPEndpoint(), flags2, "")
	storeGateway1 := e2ecortex.NewStoreGateway("store-gateway-1", e2ecortex.RingStoreConsul, consul1.NetworkHTTPEndpoint(), flags, "")
	storeGateway2 := e2ecortex.NewStoreGateway("store-gateway-2", e2ecortex.RingStoreConsul, consul2.NetworkHTTPEndpoint(), flags2, "")
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, storeGateway1, storeGateway2))

	querier1 := e2ecortex.NewQuerier("querier-1", e2ecortex.RingStoreConsul, consul1.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.store-gateway-addresses": strings.Join([]string{storeGateway1.NetworkGRPCEndpoint()}, ","),
	}), "")
	querier2 := e2ecortex.NewQuerier("querier-2", e2ecortex.RingStoreConsul, consul2.NetworkHTTPEndpoint(), mergeFlags(flags2, map[string]string{
		"-querier.store-gateway-addresses": strings.Join([]string{storeGateway2.NetworkGRPCEndpoint()}, ","),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier1, querier2))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, querier1.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))
	require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	now := time.Now()
	start := now.Add(-time.Minute * 20)
	startMs := start.UnixMilli()
	end := now.Add(-time.Minute * 10)
	endMs := end.UnixMilli()
	numSeries := 1000
	numSamples := 50
	lbls := make([]labels.Labels, 0, numSeries)
	scrapeInterval := (10 * time.Second).Milliseconds()
	metricName := "http_requests_total"
	statusCodes := []string{"200", "400", "404", "500", "502"}
	for i := 0; i < numSeries; i++ {
		lbl := labels.Labels{
			{Name: labels.MetricName, Value: metricName},
			{Name: "job", Value: "test"},
			{Name: "series", Value: strconv.Itoa(i % 200)},
			{Name: "status_code", Value: statusCodes[i%5]},
		}
		lbls = append(lbls, lbl)
	}
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	dir := t.TempDir()
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)
	id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval, 10)
	require.NoError(t, err)
	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait for store to sync blocks.
	require.NoError(t, storeGateway1.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics))
	require.NoError(t, storeGateway2.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics))
	require.NoError(t, storeGateway1.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))
	require.NoError(t, storeGateway2.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))

	c1, err := e2ecortex.NewClient("", querier1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	c2, err := e2ecortex.NewClient("", querier2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: 5 * time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 5,
	})

	var (
		labelSet1 []model.LabelSet
		labelSet2 []model.LabelSet
	)
	// Wait until both Store Gateways load the block.
	for retries.Ongoing() {
		labelSet1, err = c1.Series([]string{`{job="test"}`}, start, end)
		require.NoError(t, err)
		labelSet2, err = c2.Series([]string{`{job="test"}`}, start, end)
		require.NoError(t, err)

		if cmp.Equal(labelSet1, labelSet2, labelSetsComparer) {
			break
		}

		retries.Wait()
	}

	opts := []promqlsmith.Option{
		promqlsmith.WithEnforceLabelMatchers([]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
			labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
		}),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	type testCase struct {
		matchers                     string
		res1, newRes1, res2, newRes2 []model.LabelSet
	}

	cases := make([]*testCase, 0, 1000)
	for i := 0; i < 1000; i++ {
		matchers := ps.WalkSelectors()
		matcherStrings := storepb.PromMatchersToString(matchers...)
		minT := e2e.RandRange(rnd, startMs, endMs)
		maxT := e2e.RandRange(rnd, minT, endMs)

		res1, err := c1.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)
		res2, err := c2.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)

		// Try again with a different timestamp and let requests hit posting cache.
		minT = e2e.RandRange(rnd, startMs, endMs)
		maxT = e2e.RandRange(rnd, minT, endMs)
		newRes1, err := c1.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)
		newRes2, err := c2.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)

		cases = append(cases, &testCase{
			matchers: matcherStrings,
			res1:     res1,
			newRes1:  newRes1,
			res2:     res2,
			newRes2:  newRes2,
		})
	}

	failures := 0
	for i, tc := range cases {
		if !cmp.Equal(tc.res1, tc.res2, labelSetsComparer) {
			t.Logf("case %d results mismatch for the first attempt.\n%s\nres1 len: %d data: %s\nres2 len: %d data: %s\n", i, tc.matchers, len(tc.res1), tc.res1, len(tc.res2), tc.res2)
			failures++
		} else if !cmp.Equal(tc.newRes1, tc.newRes2, labelSetsComparer) {
			t.Logf("case %d results mismatch for the second attempt.\n%s\nres1 len: %d data: %s\nres2 len: %d data: %s\n", i, tc.matchers, len(tc.newRes1), tc.newRes1, len(tc.newRes2), tc.newRes2)
			failures++
		}
	}
	if failures > 0 {
		require.Failf(t, "finished store gateway lazy expanded posting fuzzing tests", "%d test cases failed", failures)
	}
}

func TestStoreGatewayLazyExpandedPostingsSeriesFuzzWithPrometheus(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
		"-querier.query-store-for-labels-enabled":          "true",
		"-ring.store":                                "consul",
		"-consul.hostname":                           consul.NetworkHTTPEndpoint(),
		"-store-gateway.sharding-enabled":            "false",
		"-blocks-storage.bucket-store.sync-interval": "1s",
		"-blocks-storage.bucket-store.lazy-expanded-postings-enabled": "true",
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Cortex replicas.
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(ingester, storeGateway))

	querier := e2ecortex.NewQuerier("querier-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.store-gateway-addresses": strings.Join([]string{storeGateway.NetworkGRPCEndpoint()}, ","),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	now := time.Now()
	start := now.Add(-time.Minute * 20)
	startMs := start.UnixMilli()
	end := now.Add(-time.Minute * 10)
	endMs := end.UnixMilli()
	numSeries := 1000
	numSamples := 50
	lbls := make([]labels.Labels, 0, numSeries)
	scrapeInterval := (10 * time.Second).Milliseconds()
	metricName := "http_requests_total"
	statusCodes := []string{"200", "400", "404", "500", "502"}
	for i := 0; i < numSeries; i++ {
		lbl := labels.Labels{
			{Name: labels.MetricName, Value: metricName},
			{Name: "job", Value: "test"},
			{Name: "series", Value: strconv.Itoa(i % 200)},
			{Name: "status_code", Value: statusCodes[i%5]},
		}
		lbls = append(lbls, lbl)
	}
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	dir := filepath.Join(s.SharedDir(), "data")
	err = os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)
	id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval, 10)
	require.NoError(t, err)
	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait for store to sync blocks.
	require.NoError(t, storeGateway.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics))
	require.NoError(t, storeGateway.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))

	c1, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	err = writeFileToSharedDir(s, "prometheus.yml", []byte(""))
	require.NoError(t, err)
	prom := e2edb.NewPrometheus("", map[string]string{})
	require.NoError(t, s.StartAndWaitReady(prom))

	c2, err := e2ecortex.NewPromQueryClient(prom.HTTPEndpoint())
	require.NoError(t, err)
	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: 5 * time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 5,
	})

	var (
		labelSet1 []model.LabelSet
		labelSet2 []model.LabelSet
	)
	// Wait until both Store Gateway and Prometheus load the block.
	for retries.Ongoing() {
		labelSet1, err = c1.Series([]string{`{job="test"}`}, start, end)
		require.NoError(t, err)
		labelSet2, err = c2.Series([]string{`{job="test"}`}, start, end)
		require.NoError(t, err)

		if cmp.Equal(labelSet1, labelSet2, labelSetsComparer) {
			break
		}

		retries.Wait()
	}

	opts := []promqlsmith.Option{
		promqlsmith.WithEnforceLabelMatchers([]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
			labels.MustNewMatcher(labels.MatchEqual, "job", "test"),
		}),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	type testCase struct {
		matchers                     string
		res1, newRes1, res2, newRes2 []model.LabelSet
	}

	cases := make([]*testCase, 0, 1000)
	for i := 0; i < 1000; i++ {
		matchers := ps.WalkSelectors()
		matcherStrings := storepb.PromMatchersToString(matchers...)
		minT := e2e.RandRange(rnd, startMs, endMs)
		maxT := e2e.RandRange(rnd, minT, endMs)

		res1, err := c1.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)
		res2, err := c2.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)

		// Try again with a different timestamp and let requests hit posting cache.
		minT = e2e.RandRange(rnd, startMs, endMs)
		maxT = e2e.RandRange(rnd, minT, endMs)
		newRes1, err := c1.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)
		newRes2, err := c2.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)

		cases = append(cases, &testCase{
			matchers: matcherStrings,
			res1:     res1,
			newRes1:  newRes1,
			res2:     res2,
			newRes2:  newRes2,
		})
	}

	failures := 0
	for i, tc := range cases {
		if !cmp.Equal(tc.res1, tc.res2, labelSetsComparer) {
			t.Logf("case %d results mismatch for the first attempt.\n%s\nres1 len: %d data: %s\nres2 len: %d data: %s\n", i, tc.matchers, len(tc.res1), tc.res1, len(tc.res2), tc.res2)
			failures++
		} else if !cmp.Equal(tc.newRes1, tc.newRes2, labelSetsComparer) {
			t.Logf("case %d results mismatch for the second attempt.\n%s\nres1 len: %d data: %s\nres2 len: %d data: %s\n", i, tc.matchers, len(tc.newRes1), tc.newRes1, len(tc.newRes2), tc.newRes2)
			failures++
		}
	}
	if failures > 0 {
		require.Failf(t, "finished store gateway lazy expanded posting fuzzing tests with Prometheus", "%d test cases failed", failures)
	}
}

var labelSetsComparer = cmp.Comparer(func(x, y []model.LabelSet) bool {
	if len(x) != len(y) {
		return false
	}
	for i := 0; i < len(x); i++ {
		if !x[i].Equal(y[i]) {
			return false
		}
	}
	return true
})

// TestBackwardCompatibilityQueryFuzz compares query results with the latest Cortex release.
func TestBackwardCompatibilityQueryFuzz(t *testing.T) {
	// TODO: expose the image tag to be passed from Makefile or Github Action Config.
	previousCortexReleaseImage := "quay.io/cortexproject/cortex:v1.18.1"
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul1 := e2edb.NewConsulWithName("consul1")
	consul2 := e2edb.NewConsulWithName("consul2")
	require.NoError(t, s.StartAndWaitReady(consul1, consul2))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.tsdb.block-ranges-period":         "2h",
			"-blocks-storage.tsdb.ship-interval":               "1h",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.tsdb.retention-period":            "2h",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":          "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul1.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url":      "http://localhost/alertmanager",
			"-frontend.query-vertical-shard-size": "2",
			"-frontend.max-cache-freshness":       "1m",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path1 := path.Join(s.SharedDir(), "cortex-1")
	path2 := path.Join(s.SharedDir(), "cortex-2")

	flags1 := mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path1})
	// Start Cortex replicas.
	cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags1, "")
	flags2 := mergeFlags(flags, map[string]string{
		"-blocks-storage.filesystem.dir": path2,
		"-consul.hostname":               consul2.NetworkHTTPEndpoint(),
	})
	cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags2, previousCortexReleaseImage)
	require.NoError(t, s.StartAndWaitReady(cortex1, cortex2))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c1, err := e2ecortex.NewClient(cortex1.HTTPEndpoint(), cortex1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	c2, err := e2ecortex.NewClient(cortex2.HTTPEndpoint(), cortex2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	// Push some series to Cortex.
	start := now.Add(-time.Hour * 2)
	end := now.Add(-time.Hour)
	numSeries := 3
	numSamples := 60
	lbls := make([]labels.Labels, numSeries*2)
	serieses := make([]prompb.TimeSeries, numSeries*2)
	scrapeInterval := time.Minute
	for i := 0; i < numSeries; i++ {
		series := e2e.GenerateSeriesWithSamples("test_series_a", start, scrapeInterval, i*numSamples, numSamples, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "series", Value: strconv.Itoa(i)})
		serieses[i] = series
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}
	// Generate another set of series for testing binary expression and vector matching.
	for i := numSeries; i < 2*numSeries; i++ {
		prompbLabels := []prompb.Label{{Name: "job", Value: "test"}, {Name: "series", Value: strconv.Itoa(i)}}
		if i%3 == 0 {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "200"})
		} else if i%3 == 1 {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "400"})
		} else {
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "500"})
		}
		series := e2e.GenerateSeriesWithSamples("test_series_b", start, scrapeInterval, i*numSamples, numSamples, prompbLabels...)
		serieses[i] = series
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}
	res, err := c1.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c2.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	ctx := context.Background()
	waitUntilReady(t, ctx, c1, c2, `{job="test"}`, start, end)

	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(enabledFunctions),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, end, start, end, scrapeInterval, 1000, true)
}

// TestPrometheusCompatibilityQueryFuzz compares Cortex with latest Prometheus release.
func TestPrometheusCompatibilityQueryFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())
	flags := mergeFlags(
		baseFlags,
		map[string]string{
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.tsdb.block-ranges-period":         "2h",
			"-blocks-storage.tsdb.ship-interval":               "1h",
			"-blocks-storage.bucket-store.sync-interval":       "1s",
			"-blocks-storage.tsdb.retention-period":            "24h",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":          "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url":      "http://localhost/alertmanager",
			"-frontend.query-vertical-shard-size": "2",
			"-frontend.max-cache-freshness":       "1m",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	now := time.Now()
	start := now.Add(-time.Hour * 2)
	end := now.Add(-time.Hour)
	numSeries := 10
	numSamples := 60
	lbls := make([]labels.Labels, 0, numSeries*2)
	scrapeInterval := time.Minute
	statusCodes := []string{"200", "400", "404", "500", "502"}
	for i := 0; i < numSeries; i++ {
		lbls = append(lbls, labels.Labels{
			{Name: labels.MetricName, Value: "test_series_a"},
			{Name: "job", Value: "test"},
			{Name: "series", Value: strconv.Itoa(i % 3)},
			{Name: "status_code", Value: statusCodes[i%5]},
		})

		lbls = append(lbls, labels.Labels{
			{Name: labels.MetricName, Value: "test_series_b"},
			{Name: "job", Value: "test"},
			{Name: "series", Value: strconv.Itoa((i + 1) % 3)},
			{Name: "status_code", Value: statusCodes[(i+1)%5]},
		})
	}

	ctx := context.Background()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	dir := filepath.Join(s.SharedDir(), "data")
	err = os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	storage, err := e2ecortex.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	bkt := bucket.NewUserBucketClient("user-1", storage.GetBucket(), nil)
	id, err := e2e.CreateBlock(ctx, rnd, dir, lbls, numSamples, start.UnixMilli(), end.UnixMilli(), scrapeInterval.Milliseconds(), 10)
	require.NoError(t, err)
	err = block.Upload(ctx, log.Logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc)
	require.NoError(t, err)

	// Wait for querier and store to sync blocks.
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "component", "store-gateway"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_blocks_meta_synced"}, e2e.WaitMissingMetrics, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "component", "querier"))))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(1)), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))

	c1, err := e2ecortex.NewClient("", cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	err = writeFileToSharedDir(s, "prometheus.yml", []byte(""))
	require.NoError(t, err)
	prom := e2edb.NewPrometheus("", map[string]string{})
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
}

// waitUntilReady is a helper function to wait and check if both servers to test load the expected data.
func waitUntilReady(t *testing.T, ctx context.Context, c1, c2 *e2ecortex.Client, query string, start, end time.Time) {
	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: 5 * time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 5,
	})

	var (
		labelSet1 []model.LabelSet
		labelSet2 []model.LabelSet
		err       error
	)
	// Wait until both Cortex and Prometheus load the block.
	for retries.Ongoing() {
		labelSet1, err = c1.Series([]string{query}, start, end)
		require.NoError(t, err)
		labelSet2, err = c2.Series([]string{query}, start, end)
		require.NoError(t, err)

		// Make sure series can be queried.
		if len(labelSet1) > 0 {
			if cmp.Equal(labelSet1, labelSet2, labelSetsComparer) {
				break
			}
		}

		retries.Wait()
	}
	if err := retries.Err(); err != nil {
		t.Fatalf("failed to wait for ready, error: %v", err)
	}
}

// runQueryFuzzTestCases executes the fuzz test for the specified number of runs for both instant and range queries.
func runQueryFuzzTestCases(t *testing.T, ps *promqlsmith.PromQLSmith, c1, c2 *e2ecortex.Client, queryTime, start, end time.Time, step time.Duration, run int, skipStdAggregations bool) {
	type testCase struct {
		query        string
		res1, res2   model.Value
		err1, err2   error
		instantQuery bool
	}

	cases := make([]*testCase, 0, 2*run)
	var (
		expr  parser.Expr
		query string
	)
	for i := 0; i < run; i++ {
		for {
			expr = ps.WalkInstantQuery()
			if isValidQuery(expr, skipStdAggregations) {
				query = expr.Pretty(0)
				break
			}
		}

		res1, err1 := c1.Query(query, queryTime)
		res2, err2 := c2.Query(query, queryTime)
		cases = append(cases, &testCase{
			query:        query,
			res1:         res1,
			res2:         res2,
			err1:         err1,
			err2:         err2,
			instantQuery: true,
		})
	}

	for i := 0; i < run; i++ {
		for {
			expr = ps.WalkRangeQuery()
			if isValidQuery(expr, skipStdAggregations) {
				query = expr.Pretty(0)
				break
			}
		}

		res1, err1 := c1.QueryRange(query, start, end, step)
		res2, err2 := c2.QueryRange(query, start, end, step)
		cases = append(cases, &testCase{
			query:        query,
			res1:         res1,
			res2:         res2,
			err1:         err1,
			err2:         err2,
			instantQuery: false,
		})
	}

	failures := 0
	for i, tc := range cases {
		qt := "instant query"
		if !tc.instantQuery {
			qt = "range query"
		}
		if tc.err1 != nil || tc.err2 != nil {
			if !cmp.Equal(tc.err1, tc.err2) {
				t.Logf("case %d error mismatch.\n%s: %s\nerr1: %v\nerr2: %v\n", i, qt, tc.query, tc.err1, tc.err2)
				failures++
			}
		} else if shouldUseSampleNumComparer(tc.query) {
			if !cmp.Equal(tc.res1, tc.res2, sampleNumComparer) {
				t.Logf("case %d # of samples mismatch.\n%s: %s\nres1: %s\nres2: %s\n", i, qt, tc.query, tc.res1.String(), tc.res2.String())
				failures++
			}
		} else if !cmp.Equal(tc.res1, tc.res2, comparer) {
			t.Logf("case %d results mismatch.\n%s: %s\nres1: %s\nres2: %s\n", i, qt, tc.query, tc.res1.String(), tc.res2.String())
			failures++
		}
	}
	if failures > 0 {
		require.Failf(t, "finished query fuzzing tests", "%d test cases failed", failures)
	}
}

func shouldUseSampleNumComparer(query string) bool {
	if strings.Contains(query, "bottomk") || strings.Contains(query, "topk") {
		return true
	}
	return false
}

func isValidQuery(generatedQuery parser.Expr, skipStdAggregations bool) bool {
	isValid := true
	// TODO(SungJin1212): Test limitk, limit_ratio
	if strings.Contains(generatedQuery.String(), "limitk") {
		// current skip the limitk
		return false
	}
	if strings.Contains(generatedQuery.String(), "limit_ratio") {
		// current skip the limit_ratio
		return false
	}
	if skipStdAggregations && (strings.Contains(generatedQuery.String(), "stddev") || strings.Contains(generatedQuery.String(), "stdvar")) {
		// The behavior of stdvar and stddev changes in https://github.com/prometheus/prometheus/pull/14941
		// If skipStdAggregations enabled, we skip to evaluate for stddev and stdvar aggregations.
		return false
	}
	return isValid
}
