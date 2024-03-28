//go:build integration_query_fuzz
// +build integration_query_fuzz

package integration

import (
	"context"
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

func TestVerticalShardingFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul1 := e2edb.NewConsulWithName("consul1")
	consul2 := e2edb.NewConsulWithName("consul2")
	require.NoError(t, s.StartAndWaitReady(consul1, consul2))

	flags := map[string]string{
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
	}

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

	labelSet1, err := c1.Series([]string{`{job="test"}`}, start, end)
	require.NoError(t, err)
	labelSet2, err := c2.Series([]string{`{job="test"}`}, start, end)
	require.NoError(t, err)
	require.Equal(t, labelSet1, labelSet2)

	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	type testCase struct {
		query        string
		res1, res2   model.Value
		err1, err2   error
		instantQuery bool
	}

	now = time.Now()
	cases := make([]*testCase, 0, 200)
	for i := 0; i < 100; i++ {
		expr := ps.WalkInstantQuery()
		query := expr.Pretty(0)
		res1, err1 := c1.Query(query, now)
		res2, err2 := c2.Query(query, now)
		cases = append(cases, &testCase{
			query:        query,
			res1:         res1,
			res2:         res2,
			err1:         err1,
			err2:         err2,
			instantQuery: true,
		})
	}

	for i := 0; i < 100; i++ {
		expr := ps.WalkRangeQuery()
		query := expr.Pretty(0)
		res1, err1 := c1.QueryRange(query, start, end, scrapeInterval)
		res2, err2 := c2.QueryRange(query, start, end, scrapeInterval)
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
		} else if !cmp.Equal(tc.res1, tc.res2, comparer) {
			t.Logf("case %d results mismatch.\n%s: %s\nres1: %s\nres2: %s\n", i, qt, tc.query, tc.res1.String(), tc.res2.String())
			failures++
		}
	}
	if failures > 0 {
		require.Failf(t, "finished query fuzzing tests", "%d test cases failed", failures)
	}
}

// comparer should be used to compare promql results between engines.
var comparer = cmp.Comparer(func(x, y model.Value) bool {
	if x.Type() != y.Type() {
		return false
	}
	compareFloats := func(l, r float64) bool {
		const epsilon = 1e-6
		return cmp.Equal(l, r, cmpopts.EquateNaNs(), cmpopts.EquateApprox(0, epsilon))
	}
	compareMetrics := func(l, r model.Metric) bool {
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
	// Push some series to Cortex.
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
		maxT := e2e.RandRange(rnd, minT+1, endMs)

		res1, err := c1.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)
		res2, err := c2.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)

		// Try again with a different timestamp and let requests hit posting cache.
		minT = e2e.RandRange(rnd, startMs, endMs)
		maxT = e2e.RandRange(rnd, minT+1, endMs)
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
	// Push some series to Cortex.
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
	prom := e2edb.NewPrometheus(map[string]string{})
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
		maxT := e2e.RandRange(rnd, minT+1, endMs)

		res1, err := c1.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)
		res2, err := c2.Series([]string{matcherStrings}, time.UnixMilli(minT), time.UnixMilli(maxT))
		require.NoError(t, err)

		// Try again with a different timestamp and let requests hit posting cache.
		minT = e2e.RandRange(rnd, startMs, endMs)
		maxT = e2e.RandRange(rnd, minT+1, endMs)
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
