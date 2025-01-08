package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	encodings = []encoding.Encoding{
		encoding.PrometheusXorChunk,
		encoding.PrometheusHistogramChunk,
		encoding.PrometheusFloatHistogramChunk,
	}
)

func runTestQuery(ctx context.Context, t *testing.T, ing *Ingester, ty labels.MatchType, n, v string) (model.Matrix, *client.QueryRequest, error) {
	return runTestQueryTimes(ctx, t, ing, ty, n, v, model.Earliest, model.Latest)
}

func runTestQueryTimes(ctx context.Context, t *testing.T, ing *Ingester, ty labels.MatchType, n, v string, start, end model.Time) (model.Matrix, *client.QueryRequest, error) {
	matcher, err := labels.NewMatcher(ty, n, v)
	if err != nil {
		return nil, nil, err
	}
	req, err := client.ToQueryRequest(start, end, []*labels.Matcher{matcher})
	if err != nil {
		return nil, nil, err
	}
	s := &mockQueryStreamServer{ctx: ctx}
	err = ing.QueryStream(req, s)
	if err != nil {
		return nil, nil, err
	}
	set, err := seriesSetFromResponseStream(s)
	if err != nil {
		return nil, nil, err
	}
	res, err := client.MatrixFromSeriesSet(set)
	sort.Sort(res)
	return res, req, err
}

func seriesSetFromResponseStream(s *mockQueryStreamServer) (storage.SeriesSet, error) {
	serieses := make([]storage.Series, 0, len(s.series))

	for _, result := range s.series {
		ls := cortexpb.FromLabelAdaptersToLabels(result.Labels)

		chunks, err := chunkcompat.FromChunks(ls, result.Chunks)
		if err != nil {
			return nil, err
		}

		serieses = append(serieses, &storage.SeriesEntry{
			Lset: ls,
			SampleIteratorFn: func(it chunkenc.Iterator) chunkenc.Iterator {
				return batch.NewChunkMergeIterator(it, chunks, math.MinInt64, math.MaxInt64)
			},
		})
	}
	set := series.NewConcreteSeriesSet(false, serieses)
	return set, nil
}

func TestMatcherCache(t *testing.T) {
	limits := defaultLimitsTestConfig()
	userID := "1"
	tenantLimits := newMockTenantLimits(map[string]*validation.Limits{userID: &limits})
	registry := prometheus.NewRegistry()

	dir := t.TempDir()
	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))
	cfg := defaultIngesterTestConfig(t)
	cfg.MatchersCacheMaxItems = 50
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, tenantLimits, blocksDir, registry, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return ing.lifecycler.GetState()
	})
	ctx := user.InjectOrgID(context.Background(), userID)
	// Lets have 1 key evicted
	numberOfDifferentMatchers := cfg.MatchersCacheMaxItems + 1
	callPerMatcher := 10
	for j := 0; j < numberOfDifferentMatchers; j++ {
		for i := 0; i < callPerMatcher; i++ {
			s := &mockQueryStreamServer{ctx: ctx}
			err = ing.QueryStream(&client.QueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: fmt.Sprintf("%d", j)}},
			}, s)
			require.NoError(t, err)
		}
	}

	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(fmt.Sprintf(`
				# HELP cortex_ingester_matchers_cache_evicted_total Total number of items evicted from the cache
				# TYPE cortex_ingester_matchers_cache_evicted_total counter
				cortex_ingester_matchers_cache_evicted_total 1
				# HELP cortex_ingester_matchers_cache_hits_total Total number of cache hits for series matchers
				# TYPE cortex_ingester_matchers_cache_hits_total counter
				cortex_ingester_matchers_cache_hits_total %v
				# HELP cortex_ingester_matchers_cache_items Total number of cached items
				# TYPE cortex_ingester_matchers_cache_items gauge
				cortex_ingester_matchers_cache_items %v
				# HELP cortex_ingester_matchers_cache_max_items Maximum number of items that can be cached
				# TYPE cortex_ingester_matchers_cache_max_items gauge
				cortex_ingester_matchers_cache_max_items 50
				# HELP cortex_ingester_matchers_cache_requests_total Total number of cache requests for series matchers
				# TYPE cortex_ingester_matchers_cache_requests_total counter
				cortex_ingester_matchers_cache_requests_total %v
	`, callPerMatcher*numberOfDifferentMatchers-numberOfDifferentMatchers, cfg.MatchersCacheMaxItems, callPerMatcher*numberOfDifferentMatchers)), "ingester_matchers_cache_requests_total", "ingester_matchers_cache_hits_total", "ingester_matchers_cache_items", "ingester_matchers_cache_max_items", "ingester_matchers_cache_evicted_total"))
}

func TestIngesterPerLabelsetLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	userID := "1"
	registry := prometheus.NewRegistry()

	limits.LimitsPerLabelSet = []validation.LimitsPerLabelSet{
		{
			LabelSet: labels.FromMap(map[string]string{
				"label1": "value1",
			}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 3,
			},
		},
		{
			LabelSet: labels.FromMap(map[string]string{
				"label2": "value2",
			}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 2,
			},
		},
	}
	tenantLimits := newMockTenantLimits(map[string]*validation.Limits{userID: &limits})

	b, err := json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))

	dir := t.TempDir()
	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, tenantLimits, blocksDir, registry, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	// Wait until it's ACTIVE
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return ing.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	samples := []cortexpb.Sample{{Value: 2, TimestampMs: 10}}

	// Create first series within the limits
	for _, set := range limits.LimitsPerLabelSet {
		lbls := []string{labels.MetricName, "metric_name"}
		for _, lbl := range set.LabelSet {
			lbls = append(lbls, lbl.Name, lbl.Value)
		}
		for i := 0; i < set.Limits.MaxSeries; i++ {
			_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
				[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", fmt.Sprintf("extraValue%v", i))...)}, samples, nil, nil, cortexpb.API))
			require.NoError(t, err)
		}
	}

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Should impose limits
	for _, set := range limits.LimitsPerLabelSet {
		lbls := []string{labels.MetricName, "metric_name"}
		for _, lbl := range set.LabelSet {
			lbls = append(lbls, lbl.Name, lbl.Value)
		}
		_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
			[]labels.Labels{labels.FromStrings(append(lbls, "newLabel", "newValue")...)}, samples, nil, nil, cortexpb.API))
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok, "returned error is not an httpgrpc response")
		assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
		require.ErrorContains(t, err, set.Id)
	}

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_discarded_samples_per_labelset_total The total number of samples that were discarded for each labelset.
				# TYPE cortex_discarded_samples_per_labelset_total counter
				cortex_discarded_samples_per_labelset_total{labelset="{label1=\"value1\"}",reason="per_labelset_series_limit",user="1"} 1
				cortex_discarded_samples_per_labelset_total{labelset="{label2=\"value2\"}",reason="per_labelset_series_limit",user="1"} 1
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="per_labelset_series_limit",user="1"} 2
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset", "cortex_discarded_samples_total", "cortex_discarded_samples_per_labelset_total"))

	// Should apply composite limits
	limits.LimitsPerLabelSet = append(limits.LimitsPerLabelSet,
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			"comp1": "compValue1",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 10,
			},
		},
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			"comp2": "compValue2",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 10,
			},
		},
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			"comp1": "compValue1",
			"comp2": "compValue2",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 2,
			},
		},
	)

	b, err = json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))
	tenantLimits.setLimits(userID, &limits)

	// Should backfill
	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_discarded_samples_per_labelset_total The total number of samples that were discarded for each labelset.
				# TYPE cortex_discarded_samples_per_labelset_total counter
				cortex_discarded_samples_per_labelset_total{labelset="{label1=\"value1\"}",reason="per_labelset_series_limit",user="1"} 1
				cortex_discarded_samples_per_labelset_total{labelset="{label2=\"value2\"}",reason="per_labelset_series_limit",user="1"} 1
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="per_labelset_series_limit",user="1"} 2
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 0
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 0
				cortex_ingester_usage_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 0
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset", "cortex_discarded_samples_total", "cortex_discarded_samples_per_labelset_total"))

	// Adding 5 metrics with only 1 label
	for i := 0; i < 5; i++ {
		lbls := []string{labels.MetricName, "metric_name", "comp1", "compValue1"}
		_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
			[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", fmt.Sprintf("extraValue%v", i))...)}, samples, nil, nil, cortexpb.API))
		require.NoError(t, err)
	}

	// Adding 2 metrics with both labels (still below the limit)
	lbls := []string{labels.MetricName, "metric_name", "comp1", "compValue1", "comp2", "compValue2"}
	for i := 0; i < 2; i++ {
		_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
			[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", fmt.Sprintf("extraValue%v", i))...)}, samples, nil, nil, cortexpb.API))
		require.NoError(t, err)
	}

	// Now we should hit the limit as we already have 2 metrics with comp1=compValue1, comp2=compValue2
	_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
		[]labels.Labels{labels.FromStrings(append(lbls, "newLabel", "newValue")...)}, samples, nil, nil, cortexpb.API))
	httpResp, ok := httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok, "returned error is not an httpgrpc response")
	assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
	require.ErrorContains(t, err, labels.FromStrings("comp1", "compValue1", "comp2", "compValue2").String())

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_discarded_samples_per_labelset_total The total number of samples that were discarded for each labelset.
				# TYPE cortex_discarded_samples_per_labelset_total counter
				cortex_discarded_samples_per_labelset_total{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",reason="per_labelset_series_limit",user="1"} 1
				cortex_discarded_samples_per_labelset_total{labelset="{comp1=\"compValue1\"}",reason="per_labelset_series_limit",user="1"} 1
				cortex_discarded_samples_per_labelset_total{labelset="{comp2=\"compValue2\"}",reason="per_labelset_series_limit",user="1"} 1
				cortex_discarded_samples_per_labelset_total{labelset="{label1=\"value1\"}",reason="per_labelset_series_limit",user="1"} 1
				cortex_discarded_samples_per_labelset_total{labelset="{label2=\"value2\"}",reason="per_labelset_series_limit",user="1"} 1
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="per_labelset_series_limit",user="1"} 3
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 7
				cortex_ingester_usage_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 2
		`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset", "cortex_discarded_samples_total", "cortex_discarded_samples_per_labelset_total"))

	// Should bootstrap and apply limits when configuration change
	limits.LimitsPerLabelSet = append(limits.LimitsPerLabelSet,
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			labels.MetricName: "metric_name",
			"comp2":           "compValue2",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 3, // we already have 2 so we need to allow 1 more
			},
		},
	)

	b, err = json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))
	tenantLimits.setLimits(userID, &limits)

	lbls = []string{labels.MetricName, "metric_name", "comp2", "compValue2"}
	_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
		[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", "extraValueUpdate")...)}, samples, nil, nil, cortexpb.API))
	require.NoError(t, err)

	_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
		[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", "extraValueUpdate2")...)}, samples, nil, nil, cortexpb.API))
	httpResp, ok = httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok, "returned error is not an httpgrpc response")
	assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
	require.ErrorContains(t, err, labels.FromStrings(lbls...).String())

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{__name__=\"metric_name\", comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{__name__=\"metric_name\", comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 7
				cortex_ingester_usage_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 3
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Add default partition -> no label set configured working as a fallback when a series
	// doesn't match any existing label set limit.
	emptyLabels := labels.EmptyLabels()
	defaultPartitionLimits := validation.LimitsPerLabelSet{LabelSet: emptyLabels,
		Limits: validation.LimitsPerLabelSetEntry{
			MaxSeries: 2,
		},
	}
	limits.LimitsPerLabelSet = append(limits.LimitsPerLabelSet, defaultPartitionLimits)
	b, err = json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))
	tenantLimits.setLimits(userID, &limits)

	lbls = []string{labels.MetricName, "test_default"}
	for i := 0; i < 2; i++ {
		_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
			[]labels.Labels{labels.FromStrings(append(lbls, "series", strconv.Itoa(i))...)}, samples, nil, nil, cortexpb.API))
		require.NoError(t, err)
	}

	// Max series limit for default partition is 2 so 1 more series will be throttled.
	_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
		[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", "extraValueUpdate2")...)}, samples, nil, nil, cortexpb.API))
	httpResp, ok = httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok, "returned error is not an httpgrpc response")
	assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
	require.ErrorContains(t, err, emptyLabels.String())

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{__name__=\"metric_name\", comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{__name__=\"metric_name\", comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 7
				cortex_ingester_usage_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Add a new label set limit.
	limits.LimitsPerLabelSet = append(limits.LimitsPerLabelSet,
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			"series": "0",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 3,
			},
		},
	)
	b, err = json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))
	tenantLimits.setLimits(userID, &limits)
	ing.updateActiveSeries(ctx)
	// Default partition usage reduced from 2 to 1 as one series in default partition
	// now counted into the new partition.
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{__name__=\"metric_name\", comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{series=\"0\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{__name__=\"metric_name\", comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 7
				cortex_ingester_usage_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{series=\"0\"}",limit="max_series",user="1"} 1
				cortex_ingester_usage_per_labelset{labelset="{}",limit="max_series",user="1"} 1
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Should remove metrics when the limits is removed, keep default partition limit
	limits.LimitsPerLabelSet = limits.LimitsPerLabelSet[:2]
	limits.LimitsPerLabelSet = append(limits.LimitsPerLabelSet, defaultPartitionLimits)
	b, err = json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))
	tenantLimits.setLimits(userID, &limits)
	ing.updateActiveSeries(ctx)
	// Default partition usage increased from 2 to 10 as some existing partitions got removed.
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{}",limit="max_series",user="1"} 10
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Should persist between restarts
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	registry = prometheus.NewRegistry()
	ing, err = prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, tenantLimits, blocksDir, registry, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{}",limit="max_series",user="1"} 10
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Force set tenant to be deleted.
	ing.getTSDB(userID).deletionMarkFound.Store(true)
	require.Equal(t, tsdbTenantMarkedForDeletion, ing.closeAndDeleteUserTSDBIfIdle(userID))
	// LabelSet metrics cleaned up.
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(``), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

}

func TestPushRace(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	l := defaultLimitsTestConfig()
	cfg.LabelsStringInterningEnabled = true
	cfg.LifecyclerConfig.JoinAfter = 0

	l.LimitsPerLabelSet = []validation.LimitsPerLabelSet{
		{
			LabelSet: labels.FromMap(map[string]string{
				labels.MetricName: "foo",
			}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 10e10,
			},
		},
		{
			// Default partition.
			LabelSet: labels.EmptyLabels(),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 10e10,
			},
		},
	}

	dir := t.TempDir()
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, l, nil, blocksDir, prometheus.NewRegistry(), true)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	// Wait until it's ACTIVE
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return ing.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	sample1 := cortexpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}

	concurrentRequest := 100
	numberOfSeries := 100
	wg := sync.WaitGroup{}
	wg.Add(numberOfSeries * concurrentRequest)
	for k := 0; k < numberOfSeries; k++ {
		for i := 0; i < concurrentRequest; i++ {
			go func() {
				defer wg.Done()
				_, err := ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels.FromStrings(labels.MetricName, "foo", "userId", userID, "k", strconv.Itoa(k))}, []cortexpb.Sample{sample1}, nil, nil, cortexpb.API))
				require.NoError(t, err)

				// Go to default partition.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels.FromStrings(labels.MetricName, "bar", "userId", userID, "k", strconv.Itoa(k))}, []cortexpb.Sample{sample1}, nil, nil, cortexpb.API))
				require.NoError(t, err)
			}()
		}
	}

	wg.Wait()

	db := ing.getTSDB(userID)
	ir, err := db.db.Head().Index()
	require.NoError(t, err)

	p, err := ir.Postings(ctx, "", "")
	require.NoError(t, err)
	p = ir.SortedPostings(p)
	total := 0
	var builder labels.ScratchBuilder

	for p.Next() {
		total++
		err = ir.Series(p.At(), &builder, nil)
		require.NoError(t, err)
		lbls := builder.Labels()
		require.True(t, lbls.Get(labels.MetricName) == "foo" || lbls.Get(labels.MetricName) == "bar")
		require.Equal(t, "1", lbls.Get("userId"))
		require.NotEmpty(t, lbls.Get("k"))
		builder.Reset()
	}
	require.Equal(t, 2*numberOfSeries, total)
	require.Equal(t, uint64(2*numberOfSeries), db.Head().NumSeries())
}

func TestIngesterUserLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerUser = 1
	limits.MaxLocalMetricsWithMetadataPerUser = 1

	userID := "1"
	// Series
	labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := cortexpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := cortexpb.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := cortexpb.Sample{
		TimestampMs: 1,
		Value:       3,
	}
	// Metadata
	metadata1 := &cortexpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: cortexpb.COUNTER}
	metadata2 := &cortexpb.MetricMetadata{MetricFamilyName: "testmetric2", Help: "a help for testmetric2", Type: cortexpb.COUNTER}

	dir := t.TempDir()

	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	blocksIngesterGenerator := func(reg prometheus.Registerer) *Ingester {
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, blocksDir, reg, true)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
		// Wait until it's ACTIVE
		test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
			return ing.lifecycler.GetState()
		})

		return ing
	}

	tests := []string{"blocks"}
	for i, ingGenerator := range []func(reg prometheus.Registerer) *Ingester{blocksIngesterGenerator} {
		t.Run(tests[i], func(t *testing.T) {
			reg := prometheus.NewRegistry()
			ing := ingGenerator(reg)

			// Append only one series and one metadata first, expect no error.
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels1}, []cortexpb.Sample{sample1}, []*cortexpb.MetricMetadata{metadata1}, nil, cortexpb.API))
			require.NoError(t, err)

			testLimits := func(reg prometheus.Gatherer) {
				// Append to two series, expect series-exceeded error.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels1, labels3}, []cortexpb.Sample{sample2, sample3}, nil, nil, cortexpb.API))
				httpResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "returned error is not an httpgrpc response")
				assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
				assert.Equal(t, wrapWithUser(makeLimitError(perUserSeriesLimit, ing.limiter.FormatError(userID, errMaxSeriesPerUserLimitExceeded, labels1)), userID).Error(), string(httpResp.Body))

				// Append two metadata, expect no error since metadata is a best effort approach.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest(nil, nil, []*cortexpb.MetricMetadata{metadata1, metadata2}, nil, cortexpb.API))
				require.NoError(t, err)

				// Read samples back via ingester queries.
				res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
				require.NoError(t, err)

				expected := model.Matrix{
					{
						Metric: cortexpb.FromLabelAdaptersToMetric(cortexpb.FromLabelsToLabelAdapters(labels1)),
						Values: []model.SamplePair{
							{
								Timestamp: model.Time(sample1.TimestampMs),
								Value:     model.SampleValue(sample1.Value),
							},
							{
								Timestamp: model.Time(sample2.TimestampMs),
								Value:     model.SampleValue(sample2.Value),
							},
						},
					},
				}

				// Verify samples
				require.Equal(t, expected, res)

				// Verify metadata
				m, err := ing.MetricsMetadata(ctx, nil)
				require.NoError(t, err)
				assert.Equal(t, []*cortexpb.MetricMetadata{metadata1}, m.Metadata)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="per_user_series_limit",user="1"} 1
	`), "cortex_discarded_samples_total"))
			}

			testLimits(reg)

			// Limits should hold after restart.
			services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
			// Use new registry to prevent metrics registration panic.
			reg = prometheus.NewRegistry()
			ing = ingGenerator(reg)
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			testLimits(reg)
		})
	}

}

func benchmarkData(nSeries int) (allLabels []labels.Labels, allSamples []cortexpb.Sample) {
	for j := 0; j < nSeries; j++ {
		labels := chunk.BenchmarkLabels.Copy()
		for i := range labels {
			if labels[i].Name == "cpu" {
				labels[i].Value = fmt.Sprintf("cpu%02d", j)
			}
		}
		allLabels = append(allLabels, labels)
		allSamples = append(allSamples, cortexpb.Sample{TimestampMs: 0, Value: float64(j)})
	}
	return
}

func TestIngesterMetricLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerMetric = 1
	limits.MaxLocalMetadataPerMetric = 1

	userID := "1"
	labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := cortexpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := cortexpb.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := cortexpb.Sample{
		TimestampMs: 1,
		Value:       3,
	}

	// Metadata
	metadata1 := &cortexpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: cortexpb.COUNTER}
	metadata2 := &cortexpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric2", Type: cortexpb.COUNTER}

	dir := t.TempDir()

	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	blocksIngesterGenerator := func(reg prometheus.Registerer) *Ingester {
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, blocksDir, reg, true)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
		// Wait until it's ACTIVE
		test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
			return ing.lifecycler.GetState()
		})

		return ing
	}

	tests := []string{"chunks", "blocks"}
	for i, ingGenerator := range []func(reg prometheus.Registerer) *Ingester{blocksIngesterGenerator} {
		t.Run(tests[i], func(t *testing.T) {
			reg := prometheus.NewRegistry()
			ing := ingGenerator(reg)

			// Append only one series and one metadata first, expect no error.
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels1}, []cortexpb.Sample{sample1}, []*cortexpb.MetricMetadata{metadata1}, nil, cortexpb.API))
			require.NoError(t, err)

			testLimits := func(reg prometheus.Gatherer) {
				// Append two series, expect series-exceeded error.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels1, labels3}, []cortexpb.Sample{sample2, sample3}, nil, nil, cortexpb.API))
				httpResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "returned error is not an httpgrpc response")
				assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
				assert.Equal(t, wrapWithUser(makeMetricLimitError(perMetricSeriesLimit, labels3, ing.limiter.FormatError(userID, errMaxSeriesPerMetricLimitExceeded, labels1)), userID).Error(), string(httpResp.Body))

				// Append two metadata for the same metric. Drop the second one, and expect no error since metadata is a best effort approach.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest(nil, nil, []*cortexpb.MetricMetadata{metadata1, metadata2}, nil, cortexpb.API))
				require.NoError(t, err)

				// Read samples back via ingester queries.
				res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
				require.NoError(t, err)

				// Verify Series
				expected := model.Matrix{
					{
						Metric: cortexpb.FromLabelAdaptersToMetric(cortexpb.FromLabelsToLabelAdapters(labels1)),
						Values: []model.SamplePair{
							{
								Timestamp: model.Time(sample1.TimestampMs),
								Value:     model.SampleValue(sample1.Value),
							},
							{
								Timestamp: model.Time(sample2.TimestampMs),
								Value:     model.SampleValue(sample2.Value),
							},
						},
					},
				}

				assert.Equal(t, expected, res)

				// Verify metadata
				m, err := ing.MetricsMetadata(ctx, nil)
				require.NoError(t, err)
				assert.Equal(t, []*cortexpb.MetricMetadata{metadata1}, m.Metadata)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="per_metric_series_limit",user="1"} 1
	`), "cortex_discarded_samples_total"))
			}

			testLimits(reg)

			// Limits should hold after restart.
			services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
			reg = prometheus.NewRegistry()
			ing = ingGenerator(reg)
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			testLimits(reg)
		})
	}
}

func TestGetIgnoreSeriesLimitForMetricNamesMap(t *testing.T) {
	cfg := Config{}

	require.Nil(t, cfg.getIgnoreSeriesLimitForMetricNamesMap())

	cfg.IgnoreSeriesLimitForMetricNames = ", ,,,"
	require.Nil(t, cfg.getIgnoreSeriesLimitForMetricNamesMap())

	cfg.IgnoreSeriesLimitForMetricNames = "foo, bar, ,"
	require.Equal(t, map[string]struct{}{"foo": {}, "bar": {}}, cfg.getIgnoreSeriesLimitForMetricNamesMap())
}

func TestIngester_Push(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
	}
	userID := "test"

	testHistogram := cortexpb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(1))
	testFloatHistogram := cortexpb.FloatHistogramToHistogramProto(11, tsdbutil.GenerateTestFloatHistogram(1))
	tests := map[string]struct {
		reqs                      []*cortexpb.WriteRequest
		expectedErr               error
		expectedIngested          []cortexpb.TimeSeries
		expectedMetadataIngested  []*cortexpb.MetricMetadata
		expectedExemplarsIngested []cortexpb.TimeSeries
		expectedMetrics           string
		additionalMetrics         []string
		disableActiveSeries       bool
		maxExemplars              int
		oooTimeWindow             time.Duration
		disableNativeHistogram    bool
	}{
		"should record native histogram discarded": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					[]*cortexpb.MetricMetadata{
						{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: cortexpb.GAUGE},
					},
					[]cortexpb.Histogram{
						{
							TimestampMs: 10,
						},
					},
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 10}}},
			},
			expectedMetadataIngested: []*cortexpb.MetricMetadata{
				{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: cortexpb.GAUGE},
			},
			additionalMetrics:      []string{"cortex_discarded_samples_total", "cortex_ingester_active_series"},
			disableNativeHistogram: true,
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="native-histogram-sample",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should succeed on valid series and metadata": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					[]*cortexpb.MetricMetadata{
						{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: cortexpb.COUNTER},
					},
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					[]*cortexpb.MetricMetadata{
						{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: cortexpb.GAUGE},
					},
					nil,
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 9}, {Value: 2, TimestampMs: 10}}},
			},
			expectedMetadataIngested: []*cortexpb.MetricMetadata{
				{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: cortexpb.GAUGE},
				{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: cortexpb.COUNTER},
			},
			additionalMetrics: []string{
				// Metadata.
				"cortex_ingester_memory_metadata",
				"cortex_ingester_memory_metadata_created_total",
				"cortex_ingester_ingested_metadata_total",
				"cortex_ingester_ingested_metadata_failures_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
				# TYPE cortex_ingester_ingested_metadata_failures_total counter
				cortex_ingester_ingested_metadata_failures_total 0
				# HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
				# TYPE cortex_ingester_ingested_metadata_total counter
				cortex_ingester_ingested_metadata_total 2
				# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
				# TYPE cortex_ingester_memory_metadata gauge
				cortex_ingester_memory_metadata 2
				# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
				# TYPE cortex_ingester_memory_metadata_created_total counter
				cortex_ingester_memory_metadata_created_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should succeed on valid series with exemplars": {
			maxExemplars: 2,
			reqs: []*cortexpb.WriteRequest{
				// Ingesting an exemplar requires a sample to create the series first
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					cortexpb.API),
				{
					Timeseries: []cortexpb.PreallocTimeseries{
						{
							TimeSeries: &cortexpb.TimeSeries{
								Labels: []cortexpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []cortexpb.Exemplar{
									{
										Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
									{
										Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: "456"}},
										TimestampMs: 1001,
										Value:       1001,
									},
								},
							},
						},
					},
				},
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 9}}},
			},
			expectedExemplarsIngested: []cortexpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []cortexpb.Exemplar{
						{
							Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 1001,
							Value:       1001,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total 2

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 2

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out of order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
			`,
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 9}, {Value: 2, TimestampMs: 10}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
			`,
		},
		"ooo disabled, should soft fail on sample out of order": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					[]cortexpb.Histogram{
						cortexpb.HistogramToHistogramProto(9, tsdbutil.GenerateTestHistogram(1)),
					},
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfOrderSample, model.Time(9), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 10}}},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_out_of_order_samples_total",
				"cortex_ingester_tsdb_head_out_of_order_samples_appended_total",
				"cortex_discarded_samples_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
				# HELP cortex_ingester_ingested_native_histograms_total The total number of native histograms ingested.
				# TYPE cortex_ingester_ingested_native_histograms_total counter
				cortex_ingester_ingested_native_histograms_total 0
				# HELP cortex_ingester_ingested_native_histograms_failures_total The total number of native histograms that errored on ingestion.
				# TYPE cortex_ingester_ingested_native_histograms_failures_total counter
				cortex_ingester_ingested_native_histograms_failures_total 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
        	    # HELP cortex_ingester_tsdb_head_out_of_order_samples_appended_total Total number of appended out of order samples.
        	    # TYPE cortex_ingester_tsdb_head_out_of_order_samples_appended_total counter
        	    cortex_ingester_tsdb_head_out_of_order_samples_appended_total{type="float",user="test"} 0
        	    # HELP cortex_ingester_tsdb_out_of_order_samples_total Total number of out of order samples ingestion failed attempts due to out of order being disabled.
        	    # TYPE cortex_ingester_tsdb_out_of_order_samples_total counter
        	    cortex_ingester_tsdb_out_of_order_samples_total{type="float",user="test"} 1
        	    cortex_ingester_tsdb_out_of_order_samples_total{type="histogram",user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-order",user="test"} 2
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"ooo disabled, should soft fail on sample out of bound": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (86400 * 1000)}},
					nil,
					[]cortexpb.Histogram{
						cortexpb.HistogramToHistogramProto(1575043969-(86400*1000), tsdbutil.GenerateTestHistogram(1)),
					},
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfBounds, model.Time(1575043969-(86400*1000)), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}}},
			},
			additionalMetrics: []string{"cortex_ingester_active_series"},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
				# HELP cortex_ingester_ingested_native_histograms_total The total number of native histograms ingested.
				# TYPE cortex_ingester_ingested_native_histograms_total counter
				cortex_ingester_ingested_native_histograms_total 1
				# HELP cortex_ingester_ingested_native_histograms_failures_total The total number of native histograms that errored on ingestion.
				# TYPE cortex_ingester_ingested_native_histograms_failures_total counter
				cortex_ingester_ingested_native_histograms_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-bounds",user="test"} 2
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"ooo enabled, should soft fail on sample too old": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (600 * 1000)}},
					nil,
					nil,
					cortexpb.API),
			},
			oooTimeWindow: 5 * time.Minute,
			expectedErr:   httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrTooOldSample, model.Time(1575043969-(600*1000)), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}}},
			},
			additionalMetrics: []string{
				"cortex_discarded_samples_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-too-old",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"ooo enabled, should succeed": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (60 * 1000)}},
					nil,
					nil,
					cortexpb.API),
			},
			oooTimeWindow: 5 * time.Minute,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (60 * 1000)}, {Value: 2, TimestampMs: 1575043969}}},
			},
			additionalMetrics: []string{"cortex_ingester_active_series"},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on two different sample values at the same timestamp": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.NewDuplicateFloatErr(1575043969, 2, 1), model.Time(1575043969), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}}},
			},
			additionalMetrics: []string{"cortex_discarded_samples_total", "cortex_ingester_active_series"},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="new-value-for-timestamp",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on exemplar with unknown series": {
			maxExemplars: 1,
			reqs: []*cortexpb.WriteRequest{
				// Ingesting an exemplar requires a sample to create the series first
				// This is not done here.
				{
					Timeseries: []cortexpb.PreallocTimeseries{
						{
							TimeSeries: &cortexpb.TimeSeries{
								Labels: []cortexpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []cortexpb.Exemplar{
									{
										Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
					},
				},
			},
			expectedErr:              httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestExemplarErr(errExemplarRef, model.Time(1000), cortexpb.FromLabelsToLabelAdapters(metricLabels), []cortexpb.LabelAdapter{{Name: "traceID", Value: "123"}}), userID).Error()),
			expectedIngested:         nil,
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 0
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 0

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out of order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
			`,
		},
		"should succeed when only native histogram present if enabled": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					nil,
					nil,
					[]cortexpb.Histogram{testHistogram},
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Histograms: []cortexpb.Histogram{testHistogram}},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_head_samples_appended_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 0
				# HELP cortex_ingester_ingested_native_histograms_total The total number of native histograms ingested.
				# TYPE cortex_ingester_ingested_native_histograms_total counter
				cortex_ingester_ingested_native_histograms_total 1
				# HELP cortex_ingester_ingested_native_histograms_failures_total The total number of native histograms that errored on ingestion.
				# TYPE cortex_ingester_ingested_native_histograms_failures_total counter
				cortex_ingester_ingested_native_histograms_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
        	    # HELP cortex_ingester_tsdb_head_out_of_order_samples_appended_total Total number of appended out of order samples.
        	    # TYPE cortex_ingester_tsdb_head_out_of_order_samples_appended_total counter
        	    cortex_ingester_tsdb_head_out_of_order_samples_appended_total{type="float",user="test"} 0
        	    # HELP cortex_ingester_tsdb_head_samples_appended_total Total number of appended samples.
        	    # TYPE cortex_ingester_tsdb_head_samples_appended_total counter
        	    cortex_ingester_tsdb_head_samples_appended_total{type="float",user="test"} 0
        	    cortex_ingester_tsdb_head_samples_appended_total{type="histogram",user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should succeed when only float native histogram present if enabled": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					nil,
					nil,
					[]cortexpb.Histogram{testFloatHistogram},
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Histograms: []cortexpb.Histogram{testFloatHistogram}},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_head_samples_appended_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 0
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_ingested_native_histograms_total The total number of native histograms ingested.
				# TYPE cortex_ingester_ingested_native_histograms_total counter
				cortex_ingester_ingested_native_histograms_total 1
				# HELP cortex_ingester_ingested_native_histograms_failures_total The total number of native histograms that errored on ingestion.
				# TYPE cortex_ingester_ingested_native_histograms_failures_total counter
				cortex_ingester_ingested_native_histograms_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
        	    # HELP cortex_ingester_tsdb_head_out_of_order_samples_appended_total Total number of appended out of order samples.
        	    # TYPE cortex_ingester_tsdb_head_out_of_order_samples_appended_total counter
        	    cortex_ingester_tsdb_head_out_of_order_samples_appended_total{type="float",user="test"} 0
        	    # HELP cortex_ingester_tsdb_head_samples_appended_total Total number of appended samples.
        	    # TYPE cortex_ingester_tsdb_head_samples_appended_total counter
        	    cortex_ingester_tsdb_head_samples_appended_total{type="float",user="test"} 0
        	    cortex_ingester_tsdb_head_samples_appended_total{type="histogram",user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should fail to ingest histogram due to OOO native histogram. Sample and histogram has same timestamp but sample got ingested first": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					[]cortexpb.Histogram{testHistogram},
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 10}}},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_head_samples_appended_total",
				"cortex_ingester_tsdb_out_of_order_samples_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_ingested_native_histograms_total The total number of native histograms ingested.
				# TYPE cortex_ingester_ingested_native_histograms_total counter
				cortex_ingester_ingested_native_histograms_total 1
				# HELP cortex_ingester_ingested_native_histograms_failures_total The total number of native histograms that errored on ingestion.
				# TYPE cortex_ingester_ingested_native_histograms_failures_total counter
				cortex_ingester_ingested_native_histograms_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
        	    # HELP cortex_ingester_tsdb_head_samples_appended_total Total number of appended samples.
        	    # TYPE cortex_ingester_tsdb_head_samples_appended_total counter
        	    cortex_ingester_tsdb_head_samples_appended_total{type="float",user="test"} 1
        	    cortex_ingester_tsdb_head_samples_appended_total{type="histogram",user="test"} 0
        	    # HELP cortex_ingester_tsdb_out_of_order_samples_total Total number of out of order samples ingestion failed attempts due to out of order being disabled.
        	    # TYPE cortex_ingester_tsdb_out_of_order_samples_total counter
        	    cortex_ingester_tsdb_out_of_order_samples_total{type="float",user="test"} 0
        	    cortex_ingester_tsdb_out_of_order_samples_total{type="histogram",user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.ActiveSeriesMetricsEnabled = !testData.disableActiveSeries

			limits := defaultLimitsTestConfig()
			limits.MaxExemplars = testData.maxExemplars
			limits.OutOfOrderTimeWindow = model.Duration(testData.oooTimeWindow)
			limits.LimitsPerLabelSet = []validation.LimitsPerLabelSet{
				{
					LabelSet: labels.FromMap(map[string]string{model.MetricNameLabel: "test"}),
					Hash:     0,
				},
				{
					LabelSet: labels.EmptyLabels(),
					Hash:     1,
				},
			}
			i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry, !testData.disableNativeHistogram)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), userID)

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// Push timeseries
			for idx, req := range testData.reqs {
				_, err := i.Push(ctx, req)

				// We expect no error on any request except the last one
				// which may error (and in that case we assert on it)
				if idx < len(testData.reqs)-1 {
					assert.NoError(t, err)
				} else {
					assert.Equal(t, testData.expectedErr, err)
				}
			}

			// Read back samples to see what has been really ingested
			s := &mockQueryStreamServer{ctx: ctx}
			err = i.QueryStream(&client.QueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}},
			}, s)
			require.NoError(t, err)
			set, err := seriesSetFromResponseStream(s)
			require.NoError(t, err)

			require.NotNil(t, set)
			r, err := client.SeriesSetToQueryResponse(set)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedIngested, r.Timeseries)

			// Read back samples to see what has been really ingested
			exemplarRes, err := i.QueryExemplars(ctx, &client.ExemplarQueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers: []*client.LabelMatchers{
					{Matchers: []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}}},
				},
			})

			require.NoError(t, err)
			require.NotNil(t, exemplarRes)
			assert.Equal(t, testData.expectedExemplarsIngested, exemplarRes.Timeseries)

			// Read back metadata to see what has been really ingested.
			mres, err := i.MetricsMetadata(ctx, &client.MetricsMetadataRequest{})

			require.NoError(t, err)
			require.NotNil(t, mres)

			// Order is never guaranteed.
			assert.ElementsMatch(t, testData.expectedMetadataIngested, mres.Metadata)

			// Update active series for metrics check.
			if !testData.disableActiveSeries {
				i.updateActiveSeries(ctx)
			}

			// Append additional metrics to assert on.
			mn := append(metricNames, testData.additionalMetrics...)

			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), mn...)
			assert.NoError(t, err)
		})
	}
}

// Referred from https://github.com/prometheus/prometheus/blob/v2.52.1/model/histogram/histogram_test.go#L985.
func TestIngester_PushNativeHistogramErrors(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	for _, tc := range []struct {
		name        string
		histograms  []cortexpb.Histogram
		expectedErr error
	}{
		{
			name: "rejects histogram with NaN observations that has its Count (2) lower than the actual total of buckets (2 + 1)",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					ZeroCount:       2,
					Count:           2,
					Sum:             math.NaN(),
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					PositiveBuckets: []int64{1},
				}),
			},
			expectedErr: fmt.Errorf("3 observations found in buckets, but the Count field is 2: %w", histogram.ErrHistogramCountNotBigEnough),
		},
		{
			name: "rejects histogram without NaN observations that has its Count (4) higher than the actual total of buckets (2 + 1)",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					ZeroCount:       2,
					Count:           4,
					Sum:             333,
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					PositiveBuckets: []int64{1},
				}),
			},
			expectedErr: fmt.Errorf("3 observations found in buckets, but the Count field is 4: %w", histogram.ErrHistogramCountMismatch),
		},
		{
			name: "rejects histogram that has too few negative buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					NegativeSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					NegativeBuckets: []int64{},
				}),
			},
			expectedErr: fmt.Errorf("negative side: spans need 1 buckets, have 0 buckets: %w", histogram.ErrHistogramSpansBucketsMismatch),
		},
		{
			name: "rejects histogram that has too few positive buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					PositiveBuckets: []int64{},
				}),
			},
			expectedErr: fmt.Errorf("positive side: spans need 1 buckets, have 0 buckets: %w", histogram.ErrHistogramSpansBucketsMismatch),
		},
		{
			name: "rejects histogram that has too many negative buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					NegativeSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					NegativeBuckets: []int64{1, 2},
				}),
			},
			expectedErr: fmt.Errorf("negative side: spans need 1 buckets, have 2 buckets: %w", histogram.ErrHistogramSpansBucketsMismatch),
		},
		{
			name: "rejects histogram that has too many positive buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					PositiveBuckets: []int64{1, 2},
				}),
			},
			expectedErr: fmt.Errorf("positive side: spans need 1 buckets, have 2 buckets: %w", histogram.ErrHistogramSpansBucketsMismatch),
		},
		{
			name: "rejects a histogram that has a negative span with a negative offset",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					NegativeSpans:   []histogram.Span{{Offset: -1, Length: 1}, {Offset: -1, Length: 1}},
					NegativeBuckets: []int64{1, 2},
				}),
			},
			expectedErr: fmt.Errorf("negative side: span number 2 with offset -1: %w", histogram.ErrHistogramSpanNegativeOffset),
		},
		{
			name: "rejects a histogram that has a positive span with a negative offset",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					PositiveSpans:   []histogram.Span{{Offset: -1, Length: 1}, {Offset: -1, Length: 1}},
					PositiveBuckets: []int64{1, 2},
				}),
			},
			expectedErr: fmt.Errorf("positive side: span number 2 with offset -1: %w", histogram.ErrHistogramSpanNegativeOffset),
		},
		{
			name: "rejects a histogram that has a negative span with a negative count",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					NegativeSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					NegativeBuckets: []int64{-1},
				}),
			},
			expectedErr: fmt.Errorf("negative side: bucket number 1 has observation count of -1: %w", histogram.ErrHistogramNegativeBucketCount),
		},
		{
			name: "rejects a histogram that has a positive span with a negative count",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					PositiveSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					PositiveBuckets: []int64{-1},
				}),
			},
			expectedErr: fmt.Errorf("positive side: bucket number 1 has observation count of -1: %w", histogram.ErrHistogramNegativeBucketCount),
		},
		{
			name: "rejects a histogram that has a lower count than count in buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					Count:           0,
					NegativeSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					PositiveSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					NegativeBuckets: []int64{1},
					PositiveBuckets: []int64{1},
				}),
			},
			expectedErr: fmt.Errorf("2 observations found in buckets, but the Count field is 0: %w", histogram.ErrHistogramCountMismatch),
		},
		{
			name: "rejects a histogram that doesn't count the zero bucket in its count",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					Count:           2,
					ZeroCount:       1,
					NegativeSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					PositiveSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					NegativeBuckets: []int64{1},
					PositiveBuckets: []int64{1},
				}),
			},
			expectedErr: fmt.Errorf("3 observations found in buckets, but the Count field is 2: %w", histogram.ErrHistogramCountMismatch),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0

			limits := defaultLimitsTestConfig()
			i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry, true)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), userID)

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			req := cortexpb.ToWriteRequest([]labels.Labels{metricLabels}, nil, nil, tc.histograms, cortexpb.API)
			// Push timeseries
			_, err = i.Push(ctx, req)
			assert.Equal(t, httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(tc.expectedErr, model.Time(10), metricLabelAdapters), userID).Error()), err)

			require.Equal(t, testutil.ToFloat64(i.metrics.ingestedHistogramsFail), float64(1))
		})
	}
}

func TestIngester_Push_ShouldCorrectlyTrackMetricsInMultiTenantScenario(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*cortexpb.WriteRequest{
			cortexpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				nil,
				cortexpb.API),
			cortexpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				nil,
				cortexpb.API),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Update active series for metrics check.
	i.updateActiveSeries(context.Background())

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total 4
		# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
		# TYPE cortex_ingester_ingested_samples_failures_total counter
		cortex_ingester_ingested_samples_failures_total 0
		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 2
		# HELP cortex_ingester_memory_series The current number of series in memory.
		# TYPE cortex_ingester_memory_series gauge
		cortex_ingester_memory_series 2
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 1
		cortex_ingester_memory_series_created_total{user="test-2"} 1
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="test-1"} 1
		cortex_ingester_active_series{user="test-2"} 1
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func TestIngester_Push_DecreaseInactiveSeries(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetricsIdleTimeout = 100 * time.Millisecond
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*cortexpb.WriteRequest{
			cortexpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				nil,
				cortexpb.API),
			cortexpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				nil,
				cortexpb.API),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Wait a bit to make series inactive (set to 100ms above).
	time.Sleep(200 * time.Millisecond)

	// Update active series for metrics check. This will remove inactive series.
	i.updateActiveSeries(context.Background())

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 1
		cortex_ingester_memory_series_created_total{user="test-2"} 1
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="test-1"} 0
		cortex_ingester_active_series{user="test-2"} 0
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func BenchmarkIngesterPush(b *testing.B) {
	limits := defaultLimitsTestConfig()
	benchmarkIngesterPush(b, limits, false)
}

func benchmarkIngesterPush(b *testing.B, limits validation.Limits, errorsExpected bool) {
	registry := prometheus.NewRegistry()
	ctx := user.InjectOrgID(context.Background(), userID)

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(b)
	cfg.LifecyclerConfig.JoinAfter = 0

	ingester, err := prepareIngesterWithBlocksStorage(b, cfg, registry)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
	defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(b, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return ingester.lifecycler.GetState()
	})

	// Push a single time series to set the TSDB min time.
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	startTime := util.TimeToMillis(time.Now())

	currTimeReq := cortexpb.ToWriteRequest(
		[]labels.Labels{metricLabels},
		[]cortexpb.Sample{{Value: 1, TimestampMs: startTime}},
		nil,
		nil,
		cortexpb.API)
	_, err = ingester.Push(ctx, currTimeReq)
	require.NoError(b, err)

	const (
		series  = 10000
		samples = 10
	)

	allLabels, allSamples := benchmarkData(series)

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		// Bump the timestamp on each of our test samples each time round the loop
		for j := 0; j < samples; j++ {
			for i := range allSamples {
				allSamples[i].TimestampMs = startTime + int64(iter*samples+j+1)
			}
			_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(allLabels, allSamples, nil, nil, cortexpb.API))
			if !errorsExpected {
				require.NoError(b, err)
			}
		}
	}
}

func verifyErrorString(tb testing.TB, err error, expectedErr string) {
	if err == nil || !strings.Contains(err.Error(), expectedErr) {
		tb.Helper()
		tb.Fatalf("unexpected error. expected: %s actual: %v", expectedErr, err)
	}
}

func Benchmark_Ingester_PushOnError(b *testing.B) {
	var (
		ctx             = user.InjectOrgID(context.Background(), userID)
		sampleTimestamp = int64(100)
		metricName      = "test"
	)

	scenarios := map[string]struct {
		numSeriesPerRequest  int
		numConcurrentClients int
	}{
		"no concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 1,
		},
		"low concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 100,
		},
		"high concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 1000,
		},
	}

	instanceLimits := map[string]*InstanceLimits{
		"no limits":  nil,
		"limits set": {MaxIngestionRate: 1000, MaxInMemoryTenants: 1, MaxInMemorySeries: 1000, MaxInflightPushRequests: 1000}, // these match max values from scenarios
	}

	tests := map[string]struct {
		// If this returns false, test is skipped.
		prepareConfig   func(limits *validation.Limits, instanceLimits *InstanceLimits) bool
		beforeBenchmark func(b *testing.B, ingester *Ingester, numSeriesPerRequest int)
		runBenchmark    func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample)
	}{
		"out of bound samples": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool { return true },
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a single time series to set the TSDB min time.
				currTimeReq := cortexpb.ToWriteRequest(
					[]labels.Labels{{{Name: labels.MetricName, Value: metricName}}},
					[]cortexpb.Sample{{Value: 1, TimestampMs: util.TimeToMillis(time.Now())}},
					nil,
					nil,
					cortexpb.API)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				expectedErr := storage.ErrOutOfBounds.Error()

				// Push out of bound samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API)) // nolint:errcheck

					verifyErrorString(b, err, expectedErr)
				}
			},
		},
		"out of order samples": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool { return true },
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// For each series, push a single sample with a timestamp greater than next pushes.
				for i := 0; i < numSeriesPerRequest; i++ {
					currTimeReq := cortexpb.ToWriteRequest(
						[]labels.Labels{{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}}},
						[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
						nil,
						nil,
						cortexpb.API)

					_, err := ingester.Push(ctx, currTimeReq)
					require.NoError(b, err)
				}
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				expectedErr := storage.ErrOutOfOrderSample.Error()

				// Push out of order samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API)) // nolint:errcheck

					verifyErrorString(b, err, expectedErr)
				}
			},
		},
		"per-user series limit reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				limits.MaxLocalSeriesPerUser = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a series with a metric name different than the one used during the benchmark.
				currTimeReq := cortexpb.ToWriteRequest(
					[]labels.Labels{labels.FromStrings(labels.MetricName, "another")},
					[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					nil,
					cortexpb.API)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				// Push series with a different name than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API)) // nolint:errcheck
					verifyErrorString(b, err, "per-user series limit")
				}
			},
		},
		"per-metric series limit reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				limits.MaxLocalSeriesPerMetric = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a series with the same metric name but different labels than the one used during the benchmark.
				currTimeReq := cortexpb.ToWriteRequest(
					[]labels.Labels{labels.FromStrings(labels.MetricName, metricName, "cardinality", "another")},
					[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					nil,
					cortexpb.API)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API)) // nolint:errcheck
					verifyErrorString(b, err, "per-metric series limit")
				}
			},
		},
		"very low ingestion rate limit": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxIngestionRate = 0.00001 // very low
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Send a lot of samples
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 10000, 1))
				require.NoError(b, err)

				ingester.ingestionRate.Tick()
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API))
					verifyErrorString(b, err, "push rate reached")
				}
			},
		},
		"max number of tenants reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInMemoryTenants = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Send some samples for one tenant (not the same that is used during the test)
				ctx := user.InjectOrgID(context.Background(), "different_tenant")
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 10000, 1))
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API))
					verifyErrorString(b, err, "max tenants limit reached")
				}
			},
		},
		"max number of series reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInMemorySeries = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 10000, 1))
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API))
					verifyErrorString(b, err, "max series limit reached")
				}
			},
		},
		"max inflight requests reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInflightPushRequests = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				ingester.inflightPushRequests.Inc()
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API))
					verifyErrorString(b, err, "too many inflight push requests")
				}
			},
		},
	}

	for testName, testData := range tests {
		for scenarioName, scenario := range scenarios {
			for limitsName, limits := range instanceLimits {
				b.Run(fmt.Sprintf("failure: %s, scenario: %s, limits: %s", testName, scenarioName, limitsName), func(b *testing.B) {
					registry := prometheus.NewRegistry()

					instanceLimits := limits
					if instanceLimits != nil {
						// make a copy, to avoid changing value in the instanceLimits map.
						newLimits := &InstanceLimits{}
						*newLimits = *instanceLimits
						instanceLimits = newLimits
					}

					// Create a mocked ingester
					cfg := defaultIngesterTestConfig(b)
					cfg.LifecyclerConfig.JoinAfter = 0

					limits := defaultLimitsTestConfig()
					if !testData.prepareConfig(&limits, instanceLimits) {
						b.SkipNow()
					}

					cfg.InstanceLimitsFn = func() *InstanceLimits {
						return instanceLimits
					}

					ingester, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", registry, true)
					require.NoError(b, err)
					require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
					defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

					// Wait until the ingester is ACTIVE
					test.Poll(b, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
						return ingester.lifecycler.GetState()
					})

					testData.beforeBenchmark(b, ingester, scenario.numSeriesPerRequest)

					// Prepare the request.
					metrics := make([]labels.Labels, 0, scenario.numSeriesPerRequest)
					samples := make([]cortexpb.Sample, 0, scenario.numSeriesPerRequest)
					for i := 0; i < scenario.numSeriesPerRequest; i++ {
						metrics = append(metrics, labels.Labels{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}})
						samples = append(samples, cortexpb.Sample{Value: float64(i), TimestampMs: sampleTimestamp})
					}

					// Run the benchmark.
					wg := sync.WaitGroup{}
					wg.Add(scenario.numConcurrentClients)
					start := make(chan struct{})

					b.ReportAllocs()
					b.ResetTimer()

					for c := 0; c < scenario.numConcurrentClients; c++ {
						go func() {
							defer wg.Done()
							<-start

							testData.runBenchmark(b, ingester, metrics, samples)
						}()
					}

					b.ResetTimer()
					close(start)
					wg.Wait()
				})
			}
		}
	}
}

func Test_Ingester_LabelNames(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	expected := []string{"__name__", "route", "status"}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	tests := map[string]struct {
		limit    int
		expected []string
	}{
		"should return all label names if no limit is set": {
			expected: expected,
		},
		"should return limited label names if a limit is set": {
			limit:    2,
			expected: expected[:2],
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Get label names
			res, err := i.LabelNames(ctx, &client.LabelNamesRequest{Limit: int64(testData.limit)})
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.LabelNames)
		})
	}
}

func Test_Ingester_LabelValues(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	expected := map[string][]string{
		"__name__": {"test_1", "test_2"},
		"status":   {"200", "500"},
		"route":    {"get_user"},
		"unknown":  {},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	tests := map[string]struct {
		limit int64
		match []*labels.Matcher
	}{
		"should return all label values if no limit is set": {
			limit: 0,
		},
		"should return limited label values if a limit is set": {
			limit: 1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for labelName, expectedValues := range expected {
				req := &client.LabelValuesRequest{LabelName: labelName, Limit: testData.limit}
				res, err := i.LabelValues(ctx, req)
				require.NoError(t, err)
				if testData.limit > 0 && len(expectedValues) > int(testData.limit) {
					expectedValues = expectedValues[:testData.limit]
				}
				assert.ElementsMatch(t, expectedValues, res.LabelValues)
			}
		})
	}

}

func Test_Ingester_LabelValue_MaxInflightQueryRequest(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.DefaultLimits.MaxInflightQueryRequests = 1
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	i.inflightQueryRequests.Add(1)

	// Mock request
	ctx := user.InjectOrgID(context.Background(), "test")

	wreq, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000)
	_, err = i.Push(ctx, wreq)
	require.NoError(t, err)

	rreq := &client.LabelValuesRequest{}
	_, err = i.LabelValues(ctx, rreq)
	require.Error(t, err)
	require.Equal(t, err, errTooManyInflightQueryRequests)
}

func Test_Ingester_Query(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatcher
		expected []cortexpb.TimeSeries
	}{
		"should return an empty response if no metric matches": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
			},
			expected: []cortexpb.TimeSeries{},
		},
		"should filter series by == matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by != matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.NOT_EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[2].lbls), Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 200000}}},
			},
		},
		"should filter series by =~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by !~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_NO_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[2].lbls), Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 200000}}},
			},
		},
		"should filter series by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				{Type: client.REGEX_MATCH, Name: "status", Value: "5.."},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by matcher and time range": {
			from: 100000,
			to:   100000,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &client.QueryRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				Matchers:         testData.matchers,
			}

			s := &mockQueryStreamServer{ctx: ctx}
			err = i.QueryStream(req, s)
			require.NoError(t, err)
			set, err := seriesSetFromResponseStream(s)
			require.NoError(t, err)
			r, err := client.SeriesSetToQueryResponse(set)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, r.Timeseries)
		})
	}
}

func Test_Ingester_Query_MaxInflightQueryRequest(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.DefaultLimits.MaxInflightQueryRequests = 1
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	i.inflightQueryRequests.Add(1)

	// Mock request
	ctx := user.InjectOrgID(context.Background(), "test")

	wreq, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000)
	_, err = i.Push(ctx, wreq)
	require.NoError(t, err)

	rreq := &client.QueryRequest{}
	s := &mockQueryStreamServer{ctx: ctx}
	err = i.QueryStream(rreq, s)
	require.Error(t, err)
	require.Equal(t, err, errTooManyInflightQueryRequests)
}

func TestIngester_Query_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.QueryRequest{}
	s := &mockQueryStreamServer{ctx: ctx}
	err = i.QueryStream(req, s)
	require.NoError(t, err)
	set, err := seriesSetFromResponseStream(s)
	require.NoError(t, err)
	r, err := client.SeriesSetToQueryResponse(set)
	require.NoError(t, err)
	require.Len(t, r.Timeseries, 0)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_LabelValues_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelValuesRequest{}

	res, err := i.LabelValues(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelValuesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_LabelNames_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelNamesRequest{}

	res, err := i.LabelNames(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelNamesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_Push_ShouldNotCreateTSDBIfNotInActiveState(t *testing.T) {
	// Configure the lifecycler to not immediately join the ring, to make sure
	// the ingester will NOT be in the ACTIVE state when we'll push samples.
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 10 * time.Second

	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	require.Equal(t, ring.PENDING, i.lifecycler.GetState())

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &cortexpb.WriteRequest{}

	res, err := i.Push(ctx, req)
	assert.Equal(t, wrapWithUser(fmt.Errorf(errTSDBCreateIncompatibleState, "PENDING"), userID).Error(), err.Error())
	assert.Nil(t, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_getOrCreateTSDB_ShouldNotAllowToCreateTSDBIfIngesterStateIsNotActive(t *testing.T) {
	tests := map[string]struct {
		state       ring.InstanceState
		expectedErr error
	}{
		"not allow to create TSDB if in PENDING state": {
			state:       ring.PENDING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.PENDING),
		},
		"not allow to create TSDB if in JOINING state": {
			state:       ring.JOINING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.JOINING),
		},
		"not allow to create TSDB if in LEAVING state": {
			state:       ring.LEAVING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.LEAVING),
		},
		"allow to create TSDB if in ACTIVE state": {
			state:       ring.ACTIVE,
			expectedErr: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 60 * time.Second

			i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Switch ingester state to the expected one in the test
			if i.lifecycler.GetState() != testData.state {
				var stateChain []ring.InstanceState

				if testData.state == ring.LEAVING {
					stateChain = []ring.InstanceState{ring.ACTIVE, ring.LEAVING}
				} else {
					stateChain = []ring.InstanceState{testData.state}
				}

				for _, s := range stateChain {
					err = i.lifecycler.ChangeState(context.Background(), s)
					require.NoError(t, err)
				}
			}

			db, err := i.getOrCreateTSDB("test", false)
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr != nil {
				assert.Nil(t, db)
			} else {
				assert.NotNil(t, db)
			}
		})
	}
}

func Test_Ingester_MetricsForLabelMatchers(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
	}

	tests := map[string]struct {
		from                 int64
		to                   int64
		limit                int64
		matchers             []*client.LabelMatchers
		expected             []*cortexpb.Metric
		queryIngestersWithin time.Duration
	}{
		"should return an empty response if no metric match": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
				},
			}},
			expected: []*cortexpb.Metric{},
		},
		"should filter metrics by single matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should filter metrics by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: "status", Value: "200"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_2"},
					},
				},
			},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should NOT filter metrics by time range to always return known metrics even when queried for older time ranges": {
			from: 100,
			to:   1000,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should filter metrics by time range if queryIngestersWithin is enabled": {
			from: 99999,
			to:   100001,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
			},
			queryIngestersWithin: time.Hour,
		},
		"should not return duplicated metrics on overlapping matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
					},
				},
			},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "collision"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[3].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[4].lbls)},
			},
		},
		"should return only limited results": {
			from:  math.MinInt64,
			to:    math.MaxInt64,
			limit: 1,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: "status", Value: "200"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_2"},
					},
				},
			},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			req := &client.MetricsForLabelMatchersRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				MatchersSet:      testData.matchers,
				Limit:            testData.limit,
			}
			i.cfg.QueryIngestersWithin = testData.queryIngestersWithin
			res, err := i.MetricsForLabelMatchers(ctx, req)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.Metric)

			// Stream
			ss := mockMetricsForLabelMatchersStreamServer{ctx: ctx}
			err = i.MetricsForLabelMatchersStream(req, &ss)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, ss.res.Metric)
		})
	}
}

func Test_Ingester_MetricsForLabelMatchers_Deduplication(t *testing.T) {
	const (
		userID    = "test"
		numSeries = 100000
	)

	now := util.TimeToMillis(time.Now())
	i := createIngesterWithSeries(t, userID, numSeries, 1, now, 1)
	ctx := user.InjectOrgID(context.Background(), "test")

	req := &client.MetricsForLabelMatchersRequest{
		StartTimestampMs: now,
		EndTimestampMs:   now,
		// Overlapping matchers to make sure series are correctly deduplicated.
		MatchersSet: []*client.LabelMatchers{
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}},
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*0"},
			}},
		},
	}

	res, err := i.MetricsForLabelMatchers(ctx, req)
	require.NoError(t, err)
	require.Len(t, res.GetMetric(), numSeries)
}

func Benchmark_Ingester_MetricsForLabelMatchers(b *testing.B) {
	var (
		userID              = "test"
		numSeries           = 10000
		numSamplesPerSeries = 60 * 6 // 6h on 1 sample per minute
		startTimestamp      = util.TimeToMillis(time.Now())
		step                = int64(60000) // 1 sample per minute
	)

	i := createIngesterWithSeries(b, userID, numSeries, numSamplesPerSeries, startTimestamp, step)
	ctx := user.InjectOrgID(context.Background(), "test")

	// Flush the ingester to ensure blocks have been compacted, so we'll test
	// fetching labels from blocks.
	i.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		req := &client.MetricsForLabelMatchersRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			MatchersSet: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}}},
		}

		res, err := i.MetricsForLabelMatchers(ctx, req)
		require.NoError(b, err)
		require.Len(b, res.GetMetric(), numSeries)
	}
}

// createIngesterWithSeries creates an ingester and push numSeries with numSamplesPerSeries each.
func createIngesterWithSeries(t testing.TB, userID string, numSeries, numSamplesPerSeries int, startTimestamp, step int64) *Ingester {
	const maxBatchSize = 1000

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push fixtures.
	ctx := user.InjectOrgID(context.Background(), userID)

	for ts := startTimestamp; ts < startTimestamp+(step*int64(numSamplesPerSeries)); ts += step {
		for o := 0; o < numSeries; o += maxBatchSize {
			batchSize := min(maxBatchSize, numSeries-o)

			// Generate metrics and samples (1 for each series).
			metrics := make([]labels.Labels, 0, batchSize)
			samples := make([]cortexpb.Sample, 0, batchSize)

			for s := 0; s < batchSize; s++ {
				metrics = append(metrics, labels.Labels{
					{Name: labels.MetricName, Value: fmt.Sprintf("test_%d", o+s)},
				})

				samples = append(samples, cortexpb.Sample{
					TimestampMs: ts,
					Value:       1,
				})
			}

			// Send metrics to the ingester.
			req := cortexpb.ToWriteRequest(metrics, samples, nil, nil, cortexpb.API)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	return i
}

func TestIngester_QueryStream(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)

	for _, enc := range encodings {
		t.Run(enc.String(), func(t *testing.T) {
			i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until it's ACTIVE.
			test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// Push series.
			ctx := user.InjectOrgID(context.Background(), userID)
			lbls := labels.Labels{{Name: labels.MetricName, Value: "foo"}}
			var (
				req                    *cortexpb.WriteRequest
				expectedResponseChunks *client.QueryStreamResponse
			)
			switch enc {
			case encoding.PrometheusXorChunk:
				req, expectedResponseChunks = mockWriteRequest(t, lbls, 123000, 456)
			case encoding.PrometheusHistogramChunk:
				req, expectedResponseChunks = mockHistogramWriteRequest(t, lbls, 123000, 456, false)
			case encoding.PrometheusFloatHistogramChunk:
				req, expectedResponseChunks = mockHistogramWriteRequest(t, lbls, 123000, 456, true)
			}
			_, err = i.Push(ctx, req)
			require.NoError(t, err)

			// Create a GRPC server used to query back the data.
			serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
			defer serv.GracefulStop()
			client.RegisterIngesterServer(serv, i)

			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)

			go func() {
				require.NoError(t, serv.Serve(listener))
			}()

			// Query back the series using GRPC streaming.
			c, err := client.MakeIngesterClient(listener.Addr().String(), defaultClientTestConfig())
			require.NoError(t, err)
			defer c.Close()

			queryRequest := &client.QueryRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   200000,
				Matchers: []*client.LabelMatcher{{
					Type:  client.EQUAL,
					Name:  model.MetricNameLabel,
					Value: "foo",
				}},
			}

			chunksTest := func(t *testing.T) {
				s, err := c.QueryStream(ctx, queryRequest)
				require.NoError(t, err)

				count := 0
				var lastResp *client.QueryStreamResponse
				for {
					resp, err := s.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					count += len(resp.Chunkseries)
					lastResp = resp
				}
				require.Equal(t, 1, count)
				require.Equal(t, expectedResponseChunks, lastResp)
			}

			t.Run("chunks", chunksTest)
		})
	}
}

func TestIngester_QueryStreamManySamplesChunks(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)

	reg := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 1000000
	samples := make([]cortexpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	// 100k samples in chunks use about 154 KiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "1"}}, samples[0:100000]))
	require.NoError(t, err)

	// 1M samples in chunks use about 1.51 MiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "2"}}, samples))
	require.NoError(t, err)

	// 500k samples in chunks need 775 KiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "3"}}, samples[0:500000]))
	require.NoError(t, err)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	c, err := client.MakeIngesterClient(listener.Addr().String(), defaultClientTestConfig())
	require.NoError(t, err)
	defer c.Close()

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	recvMsgs := 0
	series := 0
	totalSamples := 0
	totalChunks := 0

	for {
		resp, err := s.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.Chunkseries) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.Chunkseries)

		for _, ts := range resp.Chunkseries {
			for _, c := range ts.Chunks {
				enc := encoding.Encoding(c.Encoding).PromChunkEncoding()
				require.True(t, enc != chunkenc.EncNone)
				chk, err := chunkenc.FromData(enc, c.Data)
				require.NoError(t, err)
				totalSamples += chk.NumSamples()
			}
			totalChunks += len(ts.Chunks)
		}
	}

	// As ingester doesn't guarantee sorting of series, we can get 2 (100k + 500k in first, 1M in second)
	// or 3 messages (100k or 500k first, 1M second, and 500k or 100k last).

	require.True(t, 2 <= recvMsgs && recvMsgs <= 3)
	require.Equal(t, 3, series)
	require.Equal(t, 100000+500000+samplesCount, totalSamples)
	require.Equal(t, 13335, totalChunks)
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_queried_chunks The total number of chunks returned from queries.
		# TYPE cortex_ingester_queried_chunks histogram
		cortex_ingester_queried_chunks_bucket{le="10"} 0
		cortex_ingester_queried_chunks_bucket{le="80"} 0
		cortex_ingester_queried_chunks_bucket{le="640"} 0
		cortex_ingester_queried_chunks_bucket{le="5120"} 0
		cortex_ingester_queried_chunks_bucket{le="40960"} 1
		cortex_ingester_queried_chunks_bucket{le="327680"} 1
		cortex_ingester_queried_chunks_bucket{le="2.62144e+06"} 1
		cortex_ingester_queried_chunks_bucket{le="+Inf"} 1
		cortex_ingester_queried_chunks_sum 13335
		cortex_ingester_queried_chunks_count 1
	`), `cortex_ingester_queried_chunks`))
}

func writeRequestSingleSeries(lbls labels.Labels, samples []cortexpb.Sample) *cortexpb.WriteRequest {
	req := &cortexpb.WriteRequest{
		Source: cortexpb.API,
	}

	ts := cortexpb.TimeSeries{}
	ts.Labels = cortexpb.FromLabelsToLabelAdapters(lbls)
	ts.Samples = samples
	req.Timeseries = append(req.Timeseries, cortexpb.PreallocTimeseries{TimeSeries: &ts})

	return req
}

type mockMetricsForLabelMatchersStreamServer struct {
	grpc.ServerStream
	ctx context.Context
	res client.MetricsForLabelMatchersStreamResponse
}

func (m *mockMetricsForLabelMatchersStreamServer) Send(response *client.MetricsForLabelMatchersStreamResponse) error {
	m.res.Metric = append(m.res.Metric, response.Metric...)
	return nil
}

func (m *mockMetricsForLabelMatchersStreamServer) Context() context.Context {
	return m.ctx
}

type mockQueryStreamServer struct {
	grpc.ServerStream
	ctx context.Context

	series []client.TimeSeriesChunk
}

func (m *mockQueryStreamServer) Send(response *client.QueryStreamResponse) error {
	m.series = append(m.series, response.Chunkseries...)
	return nil
}

func (m *mockQueryStreamServer) Context() context.Context {
	return m.ctx
}

func BenchmarkIngester_QueryStream_Chunks(b *testing.B) {
	tc := []struct {
		samplesCount, seriesCount int
	}{
		{samplesCount: 10, seriesCount: 10},
		{samplesCount: 10, seriesCount: 50},
		{samplesCount: 10, seriesCount: 100},
		{samplesCount: 50, seriesCount: 10},
		{samplesCount: 50, seriesCount: 50},
		{samplesCount: 50, seriesCount: 100},
	}

	for _, c := range tc {
		b.Run(fmt.Sprintf("samplesCount=%v; seriesCount=%v", c.samplesCount, c.seriesCount), func(b *testing.B) {
			benchmarkQueryStream(b, c.samplesCount, c.seriesCount)
		})
	}
}

func benchmarkQueryStream(b *testing.B, samplesCount, seriesCount int) {
	cfg := defaultIngesterTestConfig(b)

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(b, cfg, prometheus.NewRegistry())
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE.
	test.Poll(b, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	samples := make([]cortexpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	for s := 0; s < seriesCount; s++ {
		_, err = i.Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: strconv.Itoa(s)}}, samples))
		require.NoError(b, err)
	}

	req := &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   int64(samplesCount + 1),

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	}

	mockStream := &mockQueryStreamServer{ctx: ctx}

	b.ResetTimer()
	b.ReportAllocs()

	for ix := 0; ix < b.N; ix++ {
		err := i.QueryStream(req, mockStream)
		require.NoError(b, err)
	}
}

func mockWriteRequest(t *testing.T, lbls labels.Labels, value float64, timestampMs int64) (*cortexpb.WriteRequest, *client.QueryStreamResponse) {
	samples := []cortexpb.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	req := cortexpb.ToWriteRequest([]labels.Labels{lbls}, samples, nil, nil, cortexpb.API)

	chunk := chunkenc.NewXORChunk()
	app, err := chunk.Appender()
	require.NoError(t, err)
	app.Append(timestampMs, value)
	chunk.Compact()

	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestampMs,
						EndTimestampMs:   timestampMs,
						Encoding:         int32(encoding.PrometheusXorChunk),
						Data:             chunk.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryStreamResChunks
}

func mockHistogramWriteRequest(t *testing.T, lbls labels.Labels, value int, timestampMs int64, float bool) (*cortexpb.WriteRequest, *client.QueryStreamResponse) {
	var (
		histograms []cortexpb.Histogram
		h          *histogram.Histogram
		fh         *histogram.FloatHistogram
		c          chunkenc.Chunk
	)
	if float {
		fh = tsdbutil.GenerateTestFloatHistogram(value)
		histograms = []cortexpb.Histogram{
			cortexpb.FloatHistogramToHistogramProto(timestampMs, fh),
		}
		c = chunkenc.NewFloatHistogramChunk()
	} else {
		h = tsdbutil.GenerateTestHistogram(value)
		histograms = []cortexpb.Histogram{
			cortexpb.HistogramToHistogramProto(timestampMs, h),
		}
		c = chunkenc.NewHistogramChunk()
	}

	app, err := c.Appender()
	require.NoError(t, err)
	if float {
		_, _, _, err = app.AppendFloatHistogram(nil, timestampMs, fh, true)
	} else {
		_, _, _, err = app.AppendHistogram(nil, timestampMs, h, true)
	}
	require.NoError(t, err)
	c.Compact()

	req := cortexpb.ToWriteRequest([]labels.Labels{lbls}, nil, nil, histograms, cortexpb.API)
	enc := int32(encoding.PrometheusHistogramChunk)
	if float {
		enc = int32(encoding.PrometheusFloatHistogramChunk)
	}
	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestampMs,
						EndTimestampMs:   timestampMs,
						Encoding:         enc,
						Data:             c.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryStreamResChunks
}

func prepareIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, registerer prometheus.Registerer) (*Ingester, error) {
	return prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, defaultLimitsTestConfig(), nil, "", registerer, true)
}

func prepareIngesterWithBlocksStorageAndLimits(t testing.TB, ingesterCfg Config, limits validation.Limits, tenantLimits validation.TenantLimits, dataDir string, registerer prometheus.Registerer, nativeHistograms bool) (*Ingester, error) {
	// Create a data dir if none has been provided.
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	bucketDir := t.TempDir()

	overrides, err := validation.NewOverrides(limits, tenantLimits)
	if err != nil {
		return nil, err
	}

	ingesterCfg.BlocksStorageConfig.TSDB.Dir = dataDir
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	ingesterCfg.BlocksStorageConfig.Bucket.Filesystem.Directory = bucketDir
	ingesterCfg.BlocksStorageConfig.TSDB.EnableNativeHistograms = nativeHistograms

	ingester, err := New(ingesterCfg, overrides, registerer, log.NewNopLogger())
	if err != nil {
		return nil, err
	}

	return ingester, nil
}

func TestIngester_OpenExistingTSDBOnStartup(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		concurrency int
		setup       func(*testing.T, string)
		check       func(*testing.T, *Ingester)
		expectedErr string
	}{
		"should not load TSDB if the user directory is empty": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user0"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Nil(t, i.getTSDB("user0"))
			},
		},
		"should not load any TSDB if the root directory is empty": {
			concurrency: 10,
			setup:       func(t *testing.T, dir string) {},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.TSDBState.dbs))
			},
		},
		"should not load any TSDB is the root directory is missing": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Remove(dir))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.TSDBState.dbs))
			},
		},
		"should load TSDB for any non-empty user directory": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user2"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 2, len(i.TSDBState.dbs))
				require.NotNil(t, i.getTSDB("user0"))
				require.NotNil(t, i.getTSDB("user1"))
				require.Nil(t, i.getTSDB("user2"))
			},
		},
		"should load all TSDBs on concurrency < number of TSDBs": {
			concurrency: 2,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user3", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user4", "dummy"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 5, len(i.TSDBState.dbs))
				require.NotNil(t, i.getTSDB("user0"))
				require.NotNil(t, i.getTSDB("user1"))
				require.NotNil(t, i.getTSDB("user2"))
				require.NotNil(t, i.getTSDB("user3"))
				require.NotNil(t, i.getTSDB("user4"))
			},
		},
		"should fail and rollback if an error occur while loading a TSDB on concurrency > number of TSDBs": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				// Create a fake TSDB on disk with an empty chunks head segment file (it's invalid unless
				// it's the last one and opening TSDB should fail).
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "wal", ""), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "chunks_head", ""), 0700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "user0", "chunks_head", "00000001"), nil, 0700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "user0", "chunks_head", "00000002"), nil, 0700))

				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 0, len(i.TSDBState.dbs))
				require.Nil(t, i.getTSDB("user0"))
				require.Nil(t, i.getTSDB("user1"))
			},
			expectedErr: "unable to open TSDB for user user0",
		},
		"should fail and rollback if an error occur while loading a TSDB on concurrency < number of TSDBs": {
			concurrency: 2,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user3", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user4", "dummy"), 0700))

				// Create a fake TSDB on disk with an empty chunks head segment file (it's invalid unless
				// it's the last one and opening TSDB should fail).
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "wal", ""), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "chunks_head", ""), 0700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "user2", "chunks_head", "00000001"), nil, 0700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "user2", "chunks_head", "00000002"), nil, 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 0, len(i.TSDBState.dbs))
				require.Nil(t, i.getTSDB("user0"))
				require.Nil(t, i.getTSDB("user1"))
				require.Nil(t, i.getTSDB("user2"))
				require.Nil(t, i.getTSDB("user3"))
				require.Nil(t, i.getTSDB("user4"))
			},
			expectedErr: "unable to open TSDB for user user2",
		},
	}

	for name, test := range tests {
		testName := name
		testData := test
		t.Run(testName, func(t *testing.T) {
			limits := defaultLimitsTestConfig()

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// Create a temporary directory for TSDB
			tempDir := t.TempDir()

			ingesterCfg := defaultIngesterTestConfig(t)
			ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
			ingesterCfg.BlocksStorageConfig.TSDB.MaxTSDBOpeningConcurrencyOnStartup = testData.concurrency
			ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
			ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"

			// setup the tsdbs dir
			testData.setup(t, tempDir)

			ingester, err := New(ingesterCfg, overrides, prometheus.NewRegistry(), log.NewNopLogger())
			require.NoError(t, err)

			startErr := services.StartAndAwaitRunning(context.Background(), ingester)
			if testData.expectedErr == "" {
				require.NoError(t, startErr)
			} else {
				require.Error(t, startErr)
				assert.Contains(t, startErr.Error(), testData.expectedErr)
			}

			defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck
			testData.check(t, ingester)
		})
	}
}

func TestIngester_shipBlocks(t *testing.T) {
	testCases := map[string]struct {
		ss                   bucketindex.Status
		expectetNumberOfCall int
	}{
		"should ship blocks if status ok": {
			ss:                   bucketindex.Status{Version: bucketindex.IndexVersion1, Status: bucketindex.Ok},
			expectetNumberOfCall: 1,
		},
		"should not ship on cmk errors": {
			ss:                   bucketindex.Status{Version: bucketindex.IndexVersion1, Status: bucketindex.CustomerManagedKeyError},
			expectetNumberOfCall: 0,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

			// Create ingester
			i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until it's ACTIVE
			test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// Create the TSDB for 3 users and then replace the shipper with the mocked one
			mocks := []*shipperMock{}
			for _, userID := range []string{"user-1", "user-2", "user-3"} {
				bucketindex.WriteSyncStatus(context.Background(), i.TSDBState.bucket, userID, tc.ss, log.NewNopLogger())
				userDB, err := i.getOrCreateTSDB(userID, false)
				require.NoError(t, err)
				require.NotNil(t, userDB)

				m := &shipperMock{}
				m.On("Sync", mock.Anything).Return(0, nil)
				mocks = append(mocks, m)

				userDB.shipper = m
			}

			// Ship blocks and assert on the mocked shipper
			i.shipBlocks(context.Background(), nil)

			for _, m := range mocks {
				m.AssertNumberOfCalls(t, "Sync", tc.expectetNumberOfCall)
			}
		})
	}
}

func TestIngester_dontShipBlocksWhenTenantDeletionMarkerIsPresent(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	i.TSDBState.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.TSDBState.seriesCount.Load())
	i.compactBlocks(context.Background(), true, nil)
	require.Equal(t, int64(0), i.TSDBState.seriesCount.Load())
	i.shipBlocks(context.Background(), nil)

	numObjects := len(bucket.Objects())
	require.NotZero(t, numObjects)

	require.NoError(t, cortex_tsdb.WriteTenantDeletionMark(context.Background(), objstore.WithNoopInstr(bucket), userID, cortex_tsdb.NewTenantDeletionMark(time.Now())))
	numObjects++ // For deletion marker

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	db.lastDeletionMarkCheck.Store(0)

	// After writing tenant deletion mark,
	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.TSDBState.seriesCount.Load())
	i.compactBlocks(context.Background(), true, nil)
	require.Equal(t, int64(0), i.TSDBState.seriesCount.Load())
	i.shipBlocks(context.Background(), nil)

	numObjectsAfterMarkingTenantForDeletion := len(bucket.Objects())
	require.Equal(t, numObjects, numObjectsAfterMarkingTenantForDeletion)
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngester_seriesCountIsCorrectAfterClosingTSDBForDeletedTenant(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	// Write tenant deletion mark.
	require.NoError(t, cortex_tsdb.WriteTenantDeletionMark(context.Background(), objstore.WithNoopInstr(bucket), userID, cortex_tsdb.NewTenantDeletionMark(time.Now())))

	i.TSDBState.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.TSDBState.seriesCount.Load())

	// We call shipBlocks to check for deletion marker (it happens inside this method).
	i.shipBlocks(context.Background(), nil)

	// Verify that tenant deletion mark was found.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.True(t, db.deletionMarkFound.Load())

	// If we try to close TSDB now, it should succeed, even though TSDB is not idle and empty.
	require.Equal(t, uint64(1), db.Head().NumSeries())
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))

	// Closing should decrease series count.
	require.Equal(t, int64(0), i.TSDBState.seriesCount.Load())
}

func TestIngester_sholdUpdateCacheShippedBlocks(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	mockUserShipper(t, i)

	// Mock the shipper meta (no blocks).
	db := i.getTSDB(userID)
	err = db.updateCachedShippedBlocks()
	require.NoError(t, err)

	require.Equal(t, len(db.getCachedShippedBlocks()), 0)
	shippedBlock, _ := ulid.Parse("01D78XZ44G0000000000000000")

	require.NoError(t, shipper.WriteMetaFile(log.NewNopLogger(), db.shipperMetadataFilePath, &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{shippedBlock},
	}))

	err = db.updateCachedShippedBlocks()
	require.NoError(t, err)

	require.Equal(t, len(db.getCachedShippedBlocks()), 1)
}

func TestIngester_closeAndDeleteUserTSDBIfIdle_shouldNotCloseTSDBIfShippingIsInProgress(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Mock the shipper to slow down Sync() execution.
	s := mockUserShipper(t, i)
	s.On("Sync", mock.Anything).Run(func(args mock.Arguments) {
		time.Sleep(3 * time.Second)
	}).Return(0, nil)

	// Mock the shipper meta (no blocks).
	db := i.getTSDB(userID)
	require.NoError(t, shipper.WriteMetaFile(log.NewNopLogger(), db.shipperMetadataFilePath, &shipper.Meta{
		Version: shipper.MetaVersion1,
	}))

	// Run blocks shipping in a separate go routine.
	go i.shipBlocks(ctx, nil)

	// Wait until shipping starts.
	test.Poll(t, 1*time.Second, activeShipping, func() interface{} {
		db.stateMtx.RLock()
		defer db.stateMtx.RUnlock()
		return db.state
	})
	assert.Equal(t, tsdbNotActive, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngester_closingAndOpeningTsdbConcurrently(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	_, err = i.getOrCreateTSDB(userID, false)
	require.NoError(t, err)

	iterations := 5000
	chanErr := make(chan error, 1)
	quit := make(chan bool)

	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				_, err = i.getOrCreateTSDB(userID, false)
				if err != nil {
					chanErr <- err
				}
			}
		}
	}()

	for k := 0; k < iterations; k++ {
		i.closeAndDeleteUserTSDBIfIdle(userID)
	}

	select {
	case err := <-chanErr:
		assert.Fail(t, err.Error())
		quit <- true
	default:
		quit <- true
	}
}

func TestIngester_idleCloseEmptyTSDB(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	db, err := i.getOrCreateTSDB(userID, true)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Run compaction and shipping.
	i.compactBlocks(context.Background(), true, nil)
	i.shipBlocks(context.Background(), nil)

	// Make sure we can close completely empty TSDB without problems.
	require.Equal(t, tsdbIdleClosed, i.closeAndDeleteUserTSDBIfIdle(userID))

	// Verify that it was closed.
	db = i.getTSDB(userID)
	require.Nil(t, db)

	// And we can recreate it again, if needed.
	db, err = i.getOrCreateTSDB(userID, true)
	require.NoError(t, err)
	require.NotNil(t, db)
}

type shipperMock struct {
	mock.Mock
}

// Sync mocks Shipper.Sync()
func (m *shipperMock) Sync(ctx context.Context) (uploaded int, err error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func TestIngester_invalidSamplesDontChangeLastUpdateTime(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	sampleTimestamp := int64(model.Now())

	{
		req, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, sampleTimestamp)
		_, err = i.Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	lastUpdate := db.lastUpdate.Load()

	// Wait until 1 second passes.
	test.Poll(t, 1*time.Second, time.Now().Unix()+1, func() interface{} {
		return time.Now().Unix()
	})

	// Push another sample to the same metric and timestamp, with different value. We expect to get error.
	{
		req, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 1, sampleTimestamp)
		_, err = i.Push(ctx, req)
		require.Error(t, err)
	}

	// Make sure last update hasn't changed.
	require.Equal(t, lastUpdate, db.lastUpdate.Load())
}

func TestIngester_flushing(t *testing.T) {
	for name, tc := range map[string]struct {
		setupIngester func(cfg *Config)
		action        func(t *testing.T, i *Ingester, reg *prometheus.Registry)
	}{
		"ingesterShutdown": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = true
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},
			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				// Shutdown ingester. This triggers flushing of the block.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

				verifyCompactedHead(t, i, true)

				// Verify that block has been shipped.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"shutdownHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

				i.ShutdownHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/shutdown", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				// Using wait=true makes this a synchronous call.
				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush?wait=true", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushHandlerWithListOfTenants": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				users := url.Values{}
				users.Add(tenantParam, "unknown-user")
				users.Add(tenantParam, "another-unknown-user")

				// Using wait=true makes this a synchronous call.
				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush?wait=true&"+users.Encode(), nil))

				// Still nothing shipped or compacted.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))
				verifyCompactedHead(t, i, false)

				users = url.Values{}
				users.Add(tenantParam, "different-user")
				users.Add(tenantParam, userID) // Our user
				users.Add(tenantParam, "yet-another-user")

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush?wait=true&"+users.Encode(), nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushMultipleBlocksWithDataSpanning3Days": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				// Pushing 5 samples, spanning over 3 days.
				// First block
				pushSingleSampleAtTime(t, i, 23*time.Hour.Milliseconds())
				pushSingleSampleAtTime(t, i, 24*time.Hour.Milliseconds()-1)

				// Second block
				pushSingleSampleAtTime(t, i, 24*time.Hour.Milliseconds()+1)
				pushSingleSampleAtTime(t, i, 25*time.Hour.Milliseconds())

				// Third block, far in the future.
				pushSingleSampleAtTime(t, i, 50*time.Hour.Milliseconds())

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush?wait=true", nil))

				verifyCompactedHead(t, i, true)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 3
				`), "cortex_ingester_shipper_uploads_total"))

				userDB := i.getTSDB(userID)
				require.NotNil(t, userDB)

				blocks := userDB.Blocks()
				require.Equal(t, 3, len(blocks))
				require.Equal(t, 23*time.Hour.Milliseconds(), blocks[0].Meta().MinTime)
				require.Equal(t, 24*time.Hour.Milliseconds(), blocks[0].Meta().MaxTime) // Block maxt is exclusive.

				require.Equal(t, 24*time.Hour.Milliseconds()+1, blocks[1].Meta().MinTime)
				require.Equal(t, 26*time.Hour.Milliseconds(), blocks[1].Meta().MaxTime)

				require.Equal(t, 50*time.Hour.Milliseconds()+1, blocks[2].Meta().MaxTime) // Block maxt is exclusive.
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
			cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute // Long enough to not be reached during the test.

			if tc.setupIngester != nil {
				tc.setupIngester(&cfg)
			}

			// Create ingester
			reg := prometheus.NewPedanticRegistry()
			i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			t.Cleanup(func() {
				_ = services.StopAndAwaitTerminated(context.Background(), i)
			})

			// Wait until it's ACTIVE
			test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// mock user's shipper
			tc.action(t, i, reg)
		})
	}
}

func TestIngester_ForFlush(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 10 * time.Minute // Long enough to not be reached during the test.

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push some data.
	pushSingleSampleWithMetadata(t, i)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Nothing shipped yet.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

	// Restart ingester in "For Flusher" mode. We reuse the same config (esp. same dir)
	reg = prometheus.NewPedanticRegistry()
	i, err = NewForFlusher(i.cfg, i.limits, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

	// Our single sample should be reloaded from WAL
	verifyCompactedHead(t, i, false)
	i.Flush()

	// Head should be empty after flushing.
	verifyCompactedHead(t, i, true)

	// Verify that block has been shipped.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
}

func mockUserShipper(t *testing.T, i *Ingester) *shipperMock {
	m := &shipperMock{}
	userDB, err := i.getOrCreateTSDB(userID, false)
	require.NoError(t, err)
	require.NotNil(t, userDB)

	userDB.shipper = m
	return m
}

func Test_Ingester_UserStats(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.TSDBState.dbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	res, err := i.UserStats(ctx, &client.UserStatsRequest{})
	require.NoError(t, err)
	assert.InDelta(t, 0.2, res.ApiIngestionRate, 0.0001)
	assert.InDelta(t, float64(0), res.RuleIngestionRate, 0.0001)
	assert.Equal(t, uint64(3), res.NumSeries)
}

func Test_Ingester_AllUserStats(t *testing.T) {
	series := []struct {
		user      string
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_2"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_1"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_2"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})
	for _, series := range series {
		ctx := user.InjectOrgID(context.Background(), series.user)
		req, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.TSDBState.dbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	res, err := i.AllUserStats(context.Background(), &client.UserStatsRequest{})
	require.NoError(t, err)

	expect := []*client.UserIDStatsResponse{
		{
			UserId: "user-1",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.2,
				NumSeries:         3,
				ApiIngestionRate:  0.2,
				RuleIngestionRate: 0,
				ActiveSeries:      3,
				LoadedBlocks:      0,
			},
		},
		{
			UserId: "user-2",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.13333333333333333,
				NumSeries:         2,
				ApiIngestionRate:  0.13333333333333333,
				RuleIngestionRate: 0,
				ActiveSeries:      2,
				LoadedBlocks:      0,
			},
		},
	}
	assert.ElementsMatch(t, expect, res.Stats)
}

func Test_Ingester_AllUserStatsHandler(t *testing.T) {
	series := []struct {
		user      string
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_2"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_1"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_2"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})
	for _, series := range series {
		ctx := user.InjectOrgID(context.Background(), series.user)
		req, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Force compaction to test loaded blocks
	compactionCallbackCh := make(chan struct{})
	i.TSDBState.forceCompactTrigger <- requestWithUsersAndCallback{users: nil, callback: compactionCallbackCh}
	<-compactionCallbackCh

	// force update statistics
	for _, db := range i.TSDBState.dbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	response := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/all_user_stats", nil)
	request.Header.Add("Accept", "application/json")
	i.AllUserStatsHandler(response, request)
	var resp UserStatsByTimeseries
	err = json.Unmarshal(response.Body.Bytes(), &resp)
	require.NoError(t, err)

	expect := UserStatsByTimeseries{
		{
			UserID: "user-1",
			UserStats: UserStats{
				IngestionRate:     0.2,
				NumSeries:         0,
				APIIngestionRate:  0.2,
				RuleIngestionRate: 0,
				ActiveSeries:      3,
				LoadedBlocks:      1,
			},
		},
		{
			UserID: "user-2",
			UserStats: UserStats{
				IngestionRate:     0.13333333333333333,
				NumSeries:         0,
				APIIngestionRate:  0.13333333333333333,
				RuleIngestionRate: 0,
				ActiveSeries:      2,
				LoadedBlocks:      1,
			},
		},
	}
	assert.ElementsMatch(t, expect, resp)
}

func TestIngesterCompactIdleBlock(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Hour      // Long enough to not be reached during the test.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second // Testing this.

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadata(t, i)

	i.compactBlocks(context.Background(), false, nil)
	verifyCompactedHead(t, i, false)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))

	// wait one second (plus maximum jitter) -- TSDB is now idle.
	time.Sleep(time.Duration(float64(cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout) * (1 + compactionIdleTimeoutJitter)))

	i.compactBlocks(context.Background(), false, nil)
	verifyCompactedHead(t, i, true)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))

	// Pushing another sample still works.
	pushSingleSampleWithMetadata(t, i)
	verifyCompactedHead(t, i, false)

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 2

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))
}

func TestIngesterCompactAndCloseIdleTSDB(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Second // Required to enable shipping.
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval = 100 * time.Millisecond

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadata(t, i)
	i.updateActiveSeries(context.Background())

	require.Equal(t, int64(1), i.TSDBState.seriesCount.Load())

	userMetrics := []string{memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_active_series"}

	globalMetrics := []string{"cortex_ingester_memory_users", "cortex_ingester_memory_metadata"}
	metricsToCheck := append(userMetrics, globalMetrics...)

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))

	// Wait until TSDB has been closed and removed.
	test.Poll(t, 10*time.Second, 0, func() interface{} {
		i.stoppedMtx.Lock()
		defer i.stoppedMtx.Unlock()
		return len(i.TSDBState.dbs)
	})

	require.Greater(t, testutil.ToFloat64(i.TSDBState.idleTsdbChecks.WithLabelValues(string(tsdbIdleClosed))), float64(0))
	i.updateActiveSeries(context.Background())
	require.Equal(t, int64(0), i.TSDBState.seriesCount.Load()) // Flushing removed all series from memory.

	// Verify that user has disappeared from metrics.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(""), userMetrics...))

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 0

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
    `), "cortex_ingester_memory_users", "cortex_ingester_memory_metadata"))

	// Pushing another sample will recreate TSDB.
	pushSingleSampleWithMetadata(t, i)
	i.updateActiveSeries(context.Background())

	// User is back.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))
}

func verifyCompactedHead(t *testing.T, i *Ingester, expected bool) {
	db := i.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()
	require.Equal(t, expected, h.NumSeries() == 0)
}

func pushSingleSampleWithMetadata(t *testing.T, i *Ingester) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()))
	req.Metadata = append(req.Metadata, &cortexpb.MetricMetadata{MetricFamilyName: "test", Help: "a help for metric", Unit: "", Type: cortexpb.COUNTER})
	_, err := i.Push(ctx, req)
	require.NoError(t, err)
}

func pushSingleSampleAtTime(t *testing.T, i *Ingester, ts int64) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, ts)
	_, err := i.Push(ctx, req)
	require.NoError(t, err)
}

func TestHeadCompactionOnStartup(t *testing.T) {
	// Create a temporary directory for TSDB
	tempDir := t.TempDir()

	// Build TSDB for user, with data covering 24 hours.
	{
		// Number of full chunks, 12 chunks for 24hrs.
		numFullChunks := 12
		chunkRange := 2 * time.Hour.Milliseconds()

		userDir := filepath.Join(tempDir, userID)
		require.NoError(t, os.Mkdir(userDir, 0700))

		db, err := tsdb.Open(userDir, nil, nil, &tsdb.Options{
			RetentionDuration: int64(time.Hour * 25 / time.Millisecond),
			NoLockfile:        true,
			MinBlockDuration:  chunkRange,
			MaxBlockDuration:  chunkRange,
		}, nil)
		require.NoError(t, err)

		db.DisableCompactions()
		head := db.Head()

		l := labels.Labels{{Name: "n", Value: "v"}}
		for i := 0; i < numFullChunks; i++ {
			// Not using db.Appender() as it checks for compaction.
			app := head.Appender(context.Background())
			_, err := app.Append(0, l, int64(i)*chunkRange+1, 9.99)
			require.NoError(t, err)
			_, err = app.Append(0, l, int64(i+1)*chunkRange, 9.99)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
		}

		dur := time.Duration(head.MaxTime()-head.MinTime()) * time.Millisecond
		require.True(t, dur > 23*time.Hour)
		require.Equal(t, 0, len(db.Blocks()))
		require.NoError(t, db.Close())
	}

	limits := defaultLimitsTestConfig()

	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	ingesterCfg := defaultIngesterTestConfig(t)
	ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
	ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"
	ingesterCfg.BlocksStorageConfig.TSDB.Retention = 2 * 24 * time.Hour // Make sure that no newly created blocks are deleted.

	ingester, err := New(ingesterCfg, overrides, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))

	defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()

	dur := time.Duration(h.MaxTime()-h.MinTime()) * time.Millisecond
	require.True(t, dur <= 2*time.Hour)
	require.Equal(t, 11, len(db.Blocks()))
}

func TestIngester_CloseTSDBsOnShutdown(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push some data.
	pushSingleSampleWithMetadata(t, i)

	db := i.getTSDB(userID)
	require.NotNil(t, db)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Verify that DB is no longer in memory, but was closed
	db = i.getTSDB(userID)
	require.Nil(t, db)
}

func TestIngesterNotDeleteUnshippedBlocks(t *testing.T) {
	chunkRange := 2 * time.Hour
	chunkRangeMilliSec := chunkRange.Milliseconds()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{chunkRange}
	cfg.BlocksStorageConfig.TSDB.Retention = time.Millisecond // Which means delete all but first block.
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds 0
	`), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Push some data to create 3 blocks.
	ctx := user.InjectOrgID(context.Background(), userID)
	for j := int64(0); j < 5; j++ {
		req, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.Nil(t, db.Compact(ctx))

	oldBlocks := db.Blocks()
	require.Equal(t, 3, len(oldBlocks))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, oldBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Saying that we have shipped the second block, so only that should get deleted.
	require.Nil(t, shipper.WriteMetaFile(nil, db.shipperMetadataFilePath, &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{oldBlocks[1].Meta().ULID},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(5); j < 6; j++ {
		req, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}
	require.Nil(t, db.Compact(ctx))

	// Only the second block should be gone along with a new block.
	newBlocks := db.Blocks()
	require.Equal(t, 3, len(newBlocks))
	require.Equal(t, oldBlocks[0].Meta().ULID, newBlocks[0].Meta().ULID)    // First block remains same.
	require.Equal(t, oldBlocks[2].Meta().ULID, newBlocks[1].Meta().ULID)    // 3rd block becomes 2nd now.
	require.NotEqual(t, oldBlocks[1].Meta().ULID, newBlocks[2].Meta().ULID) // The new block won't match previous 2nd block.

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Shipping 2 more blocks, hence all the blocks from first round.
	require.Nil(t, shipper.WriteMetaFile(nil, db.shipperMetadataFilePath, &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{oldBlocks[1].Meta().ULID, newBlocks[0].Meta().ULID, newBlocks[1].Meta().ULID},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(6); j < 7; j++ {
		req, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}
	require.Nil(t, db.Compact(ctx))

	// All blocks from the old blocks should be gone now.
	newBlocks2 := db.Blocks()
	require.Equal(t, 2, len(newBlocks2))

	require.Equal(t, newBlocks[2].Meta().ULID, newBlocks2[0].Meta().ULID) // Block created in last round.
	for _, b := range oldBlocks {
		// Second block is not one among old blocks.
		require.NotEqual(t, b.Meta().ULID, newBlocks2[1].Meta().ULID)
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks2[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))
}

func TestIngesterPushErrorDuringForcedCompaction(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push a sample, it should succeed.
	pushSingleSampleWithMetadata(t, i)

	// We mock a flushing by setting the boolean.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.True(t, db.casState(active, forceCompacting))

	// Ingestion should fail with a 503.
	req, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()))
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = i.Push(ctx, req)
	require.Equal(t, httpgrpc.Errorf(http.StatusServiceUnavailable, wrapWithUser(errors.New("forced compaction in progress"), userID).Error()), err)

	// Ingestion is successful after a flush.
	require.True(t, db.casState(forceCompacting, active))
	pushSingleSampleWithMetadata(t, i)
}

func TestIngesterNoFlushWithInFlightRequest(t *testing.T) {
	registry := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), registry)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push few samples.
	for j := 0; j < 5; j++ {
		pushSingleSampleWithMetadata(t, i)
	}

	// Verifying that compaction won't happen when a request is in flight.

	// This mocks a request in flight.
	db := i.getTSDB(userID)
	require.NoError(t, db.acquireAppendLock())

	// Flush handler only triggers compactions, but doesn't wait for them to finish. We cannot use ?wait=true here,
	// because it would deadlock -- flush will wait for appendLock to be released.
	i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush", nil))

	// Flushing should not have succeeded even after 5 seconds.
	time.Sleep(5 * time.Second)
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 0
	`), "cortex_ingester_tsdb_compactions_total"))

	// No requests in flight after this.
	db.releaseAppendLock()

	// Let's wait until all head series have been flushed.
	test.Poll(t, 5*time.Second, uint64(0), func() interface{} {
		db := i.getTSDB(userID)
		if db == nil {
			return false
		}
		return db.Head().NumSeries()
	})

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 1
	`), "cortex_ingester_tsdb_compactions_total"))
}

func TestIngester_PushInstanceLimits(t *testing.T) {
	tests := map[string]struct {
		limits          InstanceLimits
		reqs            map[string][]*cortexpb.WriteRequest
		expectedErr     error
		expectedErrType interface{}
	}{
		"should succeed creating one user and series": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},
			reqs: map[string][]*cortexpb.WriteRequest{
				"test": {
					cortexpb.ToWriteRequest(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
						[]*cortexpb.MetricMetadata{
							{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: cortexpb.COUNTER},
						},
						nil,
						cortexpb.API),
				},
			},
			expectedErr: nil,
		},

		"should fail creating two series": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},

			reqs: map[string][]*cortexpb.WriteRequest{
				"test": {
					cortexpb.ToWriteRequest(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						cortexpb.API),

					cortexpb.ToWriteRequest(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test2"}})}, // another series
						[]cortexpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						cortexpb.API),
				},
			},

			expectedErr: wrapWithUser(errMaxSeriesLimitReached, "test"),
		},

		"should fail creating two users": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},

			reqs: map[string][]*cortexpb.WriteRequest{
				"user1": {
					cortexpb.ToWriteRequest(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						cortexpb.API),
				},

				"user2": {
					cortexpb.ToWriteRequest(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test2"}})}, // another series
						[]cortexpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						cortexpb.API),
				},
			},
			expectedErr: wrapWithUser(errMaxUsersLimitReached, "user2"),
		},

		"should fail pushing samples in two requests due to rate limit": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1, MaxIngestionRate: 0.001},

			reqs: map[string][]*cortexpb.WriteRequest{
				"user1": {
					cortexpb.ToWriteRequest(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						cortexpb.API),

					cortexpb.ToWriteRequest(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						cortexpb.API),
				},
			},
			expectedErr: errMaxSamplesPushRateLimitReached,
		},
	}

	defaultInstanceLimits = nil

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.InstanceLimitsFn = func() *InstanceLimits {
				return &testData.limits
			}

			i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// Iterate through users in sorted order (by username).
			uids := []string{}
			totalPushes := 0
			for uid, requests := range testData.reqs {
				uids = append(uids, uid)
				totalPushes += len(requests)
			}
			sort.Strings(uids)

			pushIdx := 0
			for _, uid := range uids {
				ctx := user.InjectOrgID(context.Background(), uid)

				for _, req := range testData.reqs[uid] {
					pushIdx++
					_, err := i.Push(ctx, req)

					if pushIdx < totalPushes {
						require.NoError(t, err)
					} else {
						// Last push may expect error.
						if testData.expectedErr != nil {
							assert.Equal(t, testData.expectedErr, err)
						} else if testData.expectedErrType != nil {
							assert.True(t, errors.As(err, testData.expectedErrType), "expected error type %T, got %v", testData.expectedErrType, err)
						} else {
							assert.NoError(t, err)
						}
					}

					// imitate time ticking between each push
					i.ingestionRate.Tick()

					rate := testutil.ToFloat64(i.metrics.ingestionRate)
					require.NotZero(t, rate)
				}
			}
		})
	}
}

func TestIngester_instanceLimitsMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	l := InstanceLimits{
		MaxIngestionRate:   10,
		MaxInMemoryTenants: 20,
		MaxInMemorySeries:  30,
	}

	cfg := defaultIngesterTestConfig(t)
	cfg.InstanceLimitsFn = func() *InstanceLimits {
		return &l
	}
	cfg.LifecyclerConfig.JoinAfter = 0

	_, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_instance_limits Instance limits used by this ingester.
		# TYPE cortex_ingester_instance_limits gauge
		cortex_ingester_instance_limits{limit="max_inflight_push_requests"} 0
		cortex_ingester_instance_limits{limit="max_ingestion_rate"} 10
		cortex_ingester_instance_limits{limit="max_series"} 30
		cortex_ingester_instance_limits{limit="max_tenants"} 20
	`), "cortex_ingester_instance_limits"))

	l.MaxInMemoryTenants = 1000
	l.MaxInMemorySeries = 2000

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_instance_limits Instance limits used by this ingester.
		# TYPE cortex_ingester_instance_limits gauge
		cortex_ingester_instance_limits{limit="max_inflight_push_requests"} 0
		cortex_ingester_instance_limits{limit="max_ingestion_rate"} 10
		cortex_ingester_instance_limits{limit="max_series"} 2000
		cortex_ingester_instance_limits{limit="max_tenants"} 1000
	`), "cortex_ingester_instance_limits"))
}

func TestExpendedPostingsCacheIsolation(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{2 * time.Hour}
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.PostingsCache = cortex_tsdb.TSDBPostingsCacheConfig{
		SeedSize: 3, // lets make sure all metric names collide
		Head: cortex_tsdb.PostingsCacheConfig{
			Enabled:  true,
			Ttl:      time.Hour,
			MaxBytes: 1024 * 1024 * 1024,
		},
		Blocks: cortex_tsdb.PostingsCacheConfig{
			Enabled:  true,
			Ttl:      time.Hour,
			MaxBytes: 1024 * 1024 * 1024,
		},
	}

	r := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	numberOfTenants := 100
	wg := sync.WaitGroup{}

	for k := 0; k < 10; k++ {
		wg.Add(numberOfTenants)
		for j := 0; j < numberOfTenants; j++ {
			go func() {
				defer wg.Done()
				userId := fmt.Sprintf("user%v", j)
				ctx := user.InjectOrgID(context.Background(), userId)
				_, err := i.Push(ctx, cortexpb.ToWriteRequest(
					[]labels.Labels{labels.FromStrings(labels.MetricName, "foo", "userId", userId, "k", strconv.Itoa(k))}, []cortexpb.Sample{{Value: 2, TimestampMs: 4 * 60 * 60 * 1000}}, nil, nil, cortexpb.API))
				require.NoError(t, err)
			}()
		}
		wg.Wait()
	}

	wg.Add(numberOfTenants)
	for j := 0; j < numberOfTenants; j++ {
		go func() {
			defer wg.Done()
			userId := fmt.Sprintf("user%v", j)
			ctx := user.InjectOrgID(context.Background(), userId)
			s := &mockQueryStreamServer{ctx: ctx}

			err := i.QueryStream(&client.QueryRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   math.MaxInt64,
				Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: "foo"}},
			}, s)
			require.NoError(t, err)
			require.Len(t, s.series, 10)
			require.Len(t, s.series[0].Labels, 3)
			require.Equal(t, userId, cortexpb.FromLabelAdaptersToLabels(s.series[0].Labels).Get("userId"))
		}()
	}
	wg.Wait()
}

func TestExpendedPostingsCache(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{2 * time.Hour}

	runQuery := func(t *testing.T, ctx context.Context, i *Ingester, matchers []*client.LabelMatcher) []client.TimeSeriesChunk {
		s := &mockQueryStreamServer{ctx: ctx}

		err := i.QueryStream(&client.QueryRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   math.MaxInt64,
			Matchers:         matchers,
		}, s)
		require.NoError(t, err)
		return s.series
	}

	tc := map[string]struct {
		cacheConfig              cortex_tsdb.TSDBPostingsCacheConfig
		expectedBlockPostingCall int
		expectedHeadPostingCall  int
	}{
		"cacheDisabled": {
			expectedBlockPostingCall: 0,
			expectedHeadPostingCall:  0,
			cacheConfig: cortex_tsdb.TSDBPostingsCacheConfig{
				Head: cortex_tsdb.PostingsCacheConfig{
					Enabled: false,
				},
				Blocks: cortex_tsdb.PostingsCacheConfig{
					Enabled: false,
				},
			},
		},
		"enabled cache on compacted blocks": {
			expectedBlockPostingCall: 1,
			expectedHeadPostingCall:  0,
			cacheConfig: cortex_tsdb.TSDBPostingsCacheConfig{
				Blocks: cortex_tsdb.PostingsCacheConfig{
					Ttl:      time.Hour,
					MaxBytes: 1024 * 1024 * 1024,
					Enabled:  true,
				},
			},
		},
		"enabled cache on head": {
			expectedBlockPostingCall: 0,
			expectedHeadPostingCall:  1,
			cacheConfig: cortex_tsdb.TSDBPostingsCacheConfig{
				Head: cortex_tsdb.PostingsCacheConfig{
					Ttl:      time.Hour,
					MaxBytes: 1024 * 1024 * 1024,
					Enabled:  true,
				},
			},
		},
		"enabled cache on compacted blocks and head": {
			expectedBlockPostingCall: 1,
			expectedHeadPostingCall:  1,
			cacheConfig: cortex_tsdb.TSDBPostingsCacheConfig{
				Blocks: cortex_tsdb.PostingsCacheConfig{
					Ttl:      time.Hour,
					MaxBytes: 1024 * 1024 * 1024,
					Enabled:  true,
				},
				Head: cortex_tsdb.PostingsCacheConfig{
					Ttl:      time.Hour,
					MaxBytes: 1024 * 1024 * 1024,
					Enabled:  true,
				},
			},
		},
	}

	for name, c := range tc {
		t.Run(name, func(t *testing.T) {
			postingsForMatchersCalls := atomic.Int64{}
			cfg.BlocksStorageConfig.TSDB.PostingsCache = c.cacheConfig

			cfg.BlocksStorageConfig.TSDB.PostingsCache.PostingsForMatchers = func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
				postingsForMatchersCalls.Add(1)
				return tsdb.PostingsForMatchers(ctx, ix, ms...)
			}
			cfg.LifecyclerConfig.JoinAfter = 0

			ctx := user.InjectOrgID(context.Background(), "test")

			r := prometheus.NewRegistry()
			i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			metricNames := []string{"metric1", "metric2"}

			// Generate 4 hours of data so we have 1 block + head
			totalSamples := 4 * 60
			var samples = make([]cortexpb.Sample, 0, totalSamples)

			for i := 0; i < totalSamples; i++ {
				samples = append(samples, cortexpb.Sample{
					Value:       float64(i),
					TimestampMs: int64(i * 60 * 1000),
				})
			}

			lbls := make([]labels.Labels, 0, len(samples))
			for j := 0; j < 10; j++ {
				for i := 0; i < len(samples); i++ {
					lbls = append(lbls, labels.FromStrings(labels.MetricName, metricNames[i%len(metricNames)], "a", fmt.Sprintf("aaa%v", j)))
				}
			}

			for i := len(samples); i < len(lbls); i++ {
				samples = append(samples, samples[i%len(samples)])
			}

			req := cortexpb.ToWriteRequest(lbls, samples, nil, nil, cortexpb.API)
			_, err = i.Push(ctx, req)
			require.NoError(t, err)

			i.compactBlocks(ctx, false, nil)

			extraMatcher := []struct {
				matchers       []*client.LabelMatcher
				expectedLenght int
			}{
				{
					expectedLenght: 10,
					matchers: []*client.LabelMatcher{
						{
							Type:  client.REGEX_MATCH,
							Name:  "a",
							Value: "aaa.*",
						},
					},
				},
				{
					expectedLenght: 1,
					matchers: []*client.LabelMatcher{
						{
							Type:  client.EQUAL,
							Name:  "a",
							Value: "aaa1",
						},
					},
				},
			}

			// Run queries with no cache
			for _, name := range metricNames {
				for _, m := range extraMatcher {
					postingsForMatchersCalls.Store(0)
					require.Len(t, runQuery(t, ctx, i, append(m.matchers, &client.LabelMatcher{Type: client.EQUAL, Name: labels.MetricName, Value: name})), m.expectedLenght)
					// Query block and Head
					require.Equal(t, int64(c.expectedBlockPostingCall+c.expectedHeadPostingCall), postingsForMatchersCalls.Load())
				}
			}

			if c.expectedHeadPostingCall > 0 || c.expectedBlockPostingCall > 0 {
				metric := `
		# HELP cortex_ingester_expanded_postings_cache_requests_total Total number of requests to the cache.
		# TYPE cortex_ingester_expanded_postings_cache_requests_total counter
`
				if c.expectedBlockPostingCall > 0 {
					metric += `
		cortex_ingester_expanded_postings_cache_requests_total{cache="block"} 4
`
				}

				if c.expectedHeadPostingCall > 0 {
					metric += `
		cortex_ingester_expanded_postings_cache_requests_total{cache="head"} 4
`
				}

				err = testutil.GatherAndCompare(r, bytes.NewBufferString(metric), "cortex_ingester_expanded_postings_cache_requests_total")
				require.NoError(t, err)
			}

			// Calling again and it should hit the cache
			for _, name := range metricNames {
				for _, m := range extraMatcher {
					postingsForMatchersCalls.Store(0)
					require.Len(t, runQuery(t, ctx, i, append(m.matchers, &client.LabelMatcher{Type: client.EQUAL, Name: labels.MetricName, Value: name})), m.expectedLenght)
					require.Equal(t, int64(0), postingsForMatchersCalls.Load())
				}
			}

			if c.expectedHeadPostingCall > 0 || c.expectedBlockPostingCall > 0 {
				metric := `
		# HELP cortex_ingester_expanded_postings_cache_hits_total Total number of hit requests to the cache.
		# TYPE cortex_ingester_expanded_postings_cache_hits_total counter
`
				if c.expectedBlockPostingCall > 0 {
					metric += `
		cortex_ingester_expanded_postings_cache_hits_total{cache="block"} 4
`
				}

				if c.expectedHeadPostingCall > 0 {
					metric += `
		cortex_ingester_expanded_postings_cache_hits_total{cache="head"} 4
`
				}

				err = testutil.GatherAndCompare(r, bytes.NewBufferString(metric), "cortex_ingester_expanded_postings_cache_hits_total")
				require.NoError(t, err)
			}

			// Check the number total of series with the first metric name
			require.Len(t, runQuery(t, ctx, i, []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: metricNames[0]}}), 10)
			// Query block and head
			require.Equal(t, postingsForMatchersCalls.Load(), int64(c.expectedBlockPostingCall+c.expectedHeadPostingCall))

			// Adding a metric for the first metric name so we expire all caches for that metric name
			_, err = i.Push(ctx, cortexpb.ToWriteRequest(
				[]labels.Labels{labels.FromStrings(labels.MetricName, metricNames[0], "extra", "1")}, []cortexpb.Sample{{Value: 2, TimestampMs: 4 * 60 * 60 * 1000}}, nil, nil, cortexpb.API))
			require.NoError(t, err)

			for in, name := range metricNames {
				for _, m := range extraMatcher {
					postingsForMatchersCalls.Store(0)

					require.Len(t, runQuery(t, ctx, i, append(m.matchers, &client.LabelMatcher{Type: client.EQUAL, Name: labels.MetricName, Value: name})), m.expectedLenght)

					// first metric name should be expired
					if in == 0 {
						// Query only head as the block is already cached and the head was expired
						require.Equal(t, postingsForMatchersCalls.Load(), int64(c.expectedHeadPostingCall))
					} else {
						require.Equal(t, postingsForMatchersCalls.Load(), int64(0))
					}
				}
			}

			// Check if the new metric name was added
			require.Len(t, runQuery(t, ctx, i, []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: metricNames[0]}}), 11)
			// Query only head as the block is already cached and the head was expired
			require.Equal(t, postingsForMatchersCalls.Load(), int64(c.expectedHeadPostingCall))
			postingsForMatchersCalls.Store(0)
			require.Len(t, runQuery(t, ctx, i, []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: metricNames[0]}}), 11)
			// Return all from the caches
			require.Equal(t, postingsForMatchersCalls.Load(), int64(0))

			// Should never cache head postings there is not matcher for the metric name
			postingsForMatchersCalls.Store(0)
			require.Len(t, runQuery(t, ctx, i, []*client.LabelMatcher{{Type: client.EQUAL, Name: "extra", Value: "1"}}), 1)
			// Query block and head but bypass head
			require.Equal(t, postingsForMatchersCalls.Load(), int64(c.expectedBlockPostingCall))
			if c.cacheConfig.Head.Enabled {
				err = testutil.GatherAndCompare(r, bytes.NewBufferString(`
		# HELP cortex_ingester_expanded_postings_non_cacheable_queries_total Total number of non cacheable queries.
		# TYPE cortex_ingester_expanded_postings_non_cacheable_queries_total counter
        cortex_ingester_expanded_postings_non_cacheable_queries_total{cache="head"} 1
`), "cortex_ingester_expanded_postings_non_cacheable_queries_total")
				require.NoError(t, err)
			}

			postingsForMatchersCalls.Store(0)
			require.Len(t, runQuery(t, ctx, i, []*client.LabelMatcher{{Type: client.EQUAL, Name: "extra", Value: "1"}}), 1)
			// Return cached value from block and bypass head
			require.Equal(t, int64(0), postingsForMatchersCalls.Load())
		})
	}
}

func TestIngester_inflightPushRequests(t *testing.T) {
	limits := InstanceLimits{MaxInflightPushRequests: 1}

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.InstanceLimitsFn = func() *InstanceLimits { return &limits }
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), "test")

	startCh := make(chan struct{})

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		count := 3500000
		req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, fmt.Sprintf("real-%d", count)), count, 1)
		// Signal that we're going to do the real push now.
		close(startCh)

		_, err := i.Push(ctx, req)
		return err
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
		// failed to setup
		case <-startCh:
			// we can start the test.
		}

		time.Sleep(10 * time.Millisecond) // Give first goroutine a chance to start pushing...
		req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, "testcase"), 1024, 1)

		_, err := i.Push(ctx, req)
		require.Equal(t, errTooManyInflightPushRequests, err)
		return nil
	})

	require.NoError(t, g.Wait())
}

func TestIngester_MaxExemplarsFallBack(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.MaxExemplars = 2

	dir := t.TempDir()
	blocksDir := filepath.Join(dir, "blocks")
	limits := defaultLimitsTestConfig()
	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, blocksDir, prometheus.NewRegistry(), true)
	require.NoError(t, err)

	maxExemplars := i.getMaxExemplars("someTenant")
	require.Equal(t, maxExemplars, int64(2))

	// set max exemplars value in limits, and re-initialize the ingester
	limits.MaxExemplars = 5
	i, err = prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, blocksDir, prometheus.NewRegistry(), true)
	require.NoError(t, err)

	// validate this value is picked up now
	maxExemplars = i.getMaxExemplars("someTenant")
	require.Equal(t, maxExemplars, int64(5))
}

func Test_Ingester_QueryExemplar_MaxInflightQueryRequest(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.DefaultLimits.MaxInflightQueryRequests = 1
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	i.inflightQueryRequests.Add(1)

	// Mock request
	ctx := user.InjectOrgID(context.Background(), "test")

	wreq, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000)
	_, err = i.Push(ctx, wreq)
	require.NoError(t, err)

	rreq := &client.ExemplarQueryRequest{}
	_, err = i.QueryExemplars(ctx, rreq)
	require.Error(t, err)
	require.Equal(t, err, errTooManyInflightQueryRequests)
}

func generateSamplesForLabel(l labels.Labels, count int, sampleIntervalInMs int) *cortexpb.WriteRequest {
	var lbls = make([]labels.Labels, 0, count)
	var samples = make([]cortexpb.Sample, 0, count)

	for i := 0; i < count; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i * sampleIntervalInMs),
		})
		lbls = append(lbls, l)
	}

	return cortexpb.ToWriteRequest(lbls, samples, nil, nil, cortexpb.API)
}

func Test_Ingester_ModeHandler(t *testing.T) {
	tests := map[string]struct {
		method           string
		requestBody      io.Reader
		requestUrl       string
		initialState     ring.InstanceState
		mode             string
		expectedState    ring.InstanceState
		expectedResponse int
		expectedIsReady  bool
	}{
		"should change to READONLY mode": {
			method:           "POST",
			initialState:     ring.ACTIVE,
			requestUrl:       "/mode?mode=reAdOnLy",
			expectedState:    ring.READONLY,
			expectedResponse: http.StatusOK,
			expectedIsReady:  true,
		},
		"should change mode on GET method": {
			method:           "GET",
			initialState:     ring.ACTIVE,
			requestUrl:       "/mode?mode=READONLY",
			expectedState:    ring.READONLY,
			expectedResponse: http.StatusOK,
			expectedIsReady:  true,
		},
		"should change mode on POST method via body": {
			method:           "POST",
			initialState:     ring.ACTIVE,
			requestUrl:       "/mode",
			requestBody:      strings.NewReader("mode=readonly"),
			expectedState:    ring.READONLY,
			expectedResponse: http.StatusOK,
			expectedIsReady:  true,
		},
		"should change to ACTIVE mode": {
			method:           "POST",
			initialState:     ring.READONLY,
			requestUrl:       "/mode?mode=active",
			expectedState:    ring.ACTIVE,
			expectedResponse: http.StatusOK,
			expectedIsReady:  true,
		},
		"should fail to unknown mode": {
			method:           "POST",
			initialState:     ring.ACTIVE,
			requestUrl:       "/mode?mode=NotSupported",
			expectedState:    ring.ACTIVE,
			expectedResponse: http.StatusBadRequest,
			expectedIsReady:  true,
		},
		"should maintain in readonly": {
			method:           "POST",
			initialState:     ring.READONLY,
			requestUrl:       "/mode?mode=READONLY",
			expectedState:    ring.READONLY,
			expectedResponse: http.StatusOK,
			expectedIsReady:  true,
		},
		"should maintain in active": {
			method:           "POST",
			initialState:     ring.ACTIVE,
			requestUrl:       "/mode?mode=ACTIVE",
			expectedState:    ring.ACTIVE,
			expectedResponse: http.StatusOK,
			expectedIsReady:  true,
		},
		"should fail mode READONLY if LEAVING state": {
			method:           "POST",
			initialState:     ring.LEAVING,
			requestUrl:       "/mode?mode=READONLY",
			expectedState:    ring.LEAVING,
			expectedResponse: http.StatusBadRequest,
			expectedIsReady:  false,
		},
		"should fail with malformatted request": {
			method:           "GET",
			initialState:     ring.ACTIVE,
			requestUrl:       "/mode?mod;e=READONLY",
			expectedResponse: http.StatusBadRequest,
			expectedIsReady:  true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.MinReadyDuration = 0
			i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until it's ACTIVE
			test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			if testData.initialState != ring.ACTIVE {
				err = i.lifecycler.ChangeState(context.Background(), testData.initialState)
				require.NoError(t, err)

				// Wait until initial state
				test.Poll(t, 1*time.Second, testData.initialState, func() interface{} {
					return i.lifecycler.GetState()
				})
			}

			response := httptest.NewRecorder()
			request := httptest.NewRequest(testData.method, testData.requestUrl, testData.requestBody)
			if testData.requestBody != nil {
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
			}
			i.ModeHandler(response, request)

			require.Equal(t, testData.expectedResponse, response.Code)
			require.Equal(t, testData.expectedState, i.lifecycler.GetState())
			if testData.expectedIsReady {
				// Wait for instance to own tokens
				test.Poll(t, 1*time.Second, nil, func() interface{} {
					return i.CheckReady(context.Background())
				})
				require.NoError(t, i.CheckReady(context.Background()))
			} else {
				require.NotNil(t, i.CheckReady(context.Background()))
			}
		})
	}
}

func TestIngester_UserTSDB_BlocksToDelete(t *testing.T) {
	tempDir := t.TempDir()
	db, err := tsdb.Open(tempDir, log.NewNopLogger(), prometheus.NewPedanticRegistry(), &tsdb.Options{}, nil)
	require.NoError(t, err)

	t.Run("should delete all block beyond block retention period and were shipped", func(t *testing.T) {
		currentTime := time.Now()
		var blocks []*tsdb.Block
		block1 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-3*time.Hour).UnixMilli(), currentTime.Add(-2*time.Hour).UnixMilli())
		blocks = append(blocks, block1)
		block2 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-4*time.Hour).UnixMilli(), currentTime.Add(-3*time.Hour).UnixMilli())
		blocks = append(blocks, block2)
		block3 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-5*time.Hour).UnixMilli(), currentTime.Add(-4*time.Hour).UnixMilli())
		blocks = append(blocks, block3)
		block4 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-6*time.Hour).UnixMilli(), currentTime.Add(-5*time.Hour).UnixMilli())
		blocks = append(blocks, block4)

		shippedBlocks := map[ulid.ULID]struct{}{
			block1.Meta().ULID: {},
			block2.Meta().ULID: {},
			block3.Meta().ULID: {},
			block4.Meta().ULID: {},
		}
		userDB := &userTSDB{
			db:                   db,
			shipper:              &shipperMock{},
			shippedBlocks:        shippedBlocks,
			blockRetentionPeriod: 2 * time.Hour.Milliseconds(),
		}

		blocksToDelete := userDB.blocksToDelete(blocks)
		require.Equal(t, 4, len(blocksToDelete))
		require.Contains(t, blocksToDelete, block1.Meta().ULID)
		require.Contains(t, blocksToDelete, block2.Meta().ULID)
		require.Contains(t, blocksToDelete, block3.Meta().ULID)
		require.Contains(t, blocksToDelete, block4.Meta().ULID)
	})

	t.Run("should not delete not-shipped block even it is beyond block retention period", func(t *testing.T) {
		currentTime := time.Now()
		var blocks []*tsdb.Block
		block1 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-3*time.Hour).UnixMilli(), currentTime.Add(-2*time.Hour).UnixMilli())
		blocks = append(blocks, block1)
		block2 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-4*time.Hour).UnixMilli(), currentTime.Add(-3*time.Hour).UnixMilli())
		blocks = append(blocks, block2)
		block3 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-5*time.Hour).UnixMilli(), currentTime.Add(-4*time.Hour).UnixMilli())
		blocks = append(blocks, block3)
		block4 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-6*time.Hour).UnixMilli(), currentTime.Add(-5*time.Hour).UnixMilli())
		blocks = append(blocks, block4)

		shippedBlocks := map[ulid.ULID]struct{}{
			block1.Meta().ULID: {},
			block2.Meta().ULID: {},
			block3.Meta().ULID: {},
		}
		userDB := &userTSDB{
			db:                   db,
			shipper:              &shipperMock{},
			shippedBlocks:        shippedBlocks,
			blockRetentionPeriod: 2 * time.Hour.Milliseconds(),
		}

		blocksToDelete := userDB.blocksToDelete(blocks)
		require.Equal(t, 3, len(blocksToDelete))
		require.Contains(t, blocksToDelete, block1.Meta().ULID)
		require.Contains(t, blocksToDelete, block2.Meta().ULID)
		require.Contains(t, blocksToDelete, block3.Meta().ULID)
		require.NotContains(t, blocksToDelete, block4.Meta().ULID)
	})

	t.Run("should not delete any block not reaching block retention period and not shipped", func(t *testing.T) {
		currentTime := time.Now()
		var blocks []*tsdb.Block
		block1 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-1*time.Hour).UnixMilli(), currentTime.Add(0).UnixMilli())
		blocks = append(blocks, block1)
		block2 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-1*time.Hour).UnixMilli(), currentTime.Add(0).UnixMilli())
		blocks = append(blocks, block2)
		block3 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-2*time.Hour).UnixMilli(), currentTime.Add(-1*time.Hour).UnixMilli())
		blocks = append(blocks, block3)
		block4 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-2*time.Hour).UnixMilli(), currentTime.Add(-1*time.Hour).UnixMilli())
		blocks = append(blocks, block4)

		shippedBlocks := map[ulid.ULID]struct{}{}
		userDB := &userTSDB{
			db:                   db,
			shipper:              &shipperMock{},
			shippedBlocks:        shippedBlocks,
			blockRetentionPeriod: 2 * time.Hour.Milliseconds(),
		}

		blocksToDelete := userDB.blocksToDelete(blocks)
		require.Equal(t, 0, len(blocksToDelete))
		require.NotContains(t, blocksToDelete, block1.Meta().ULID)
		require.NotContains(t, blocksToDelete, block2.Meta().ULID)
		require.NotContains(t, blocksToDelete, block3.Meta().ULID)
		require.NotContains(t, blocksToDelete, block4.Meta().ULID)
	})

	t.Run("should not delete block not reaching block retention period", func(t *testing.T) {
		currentTime := time.Now()
		var blocks []*tsdb.Block
		block1 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-1*time.Hour).UnixMilli(), currentTime.Add(0).UnixMilli())
		blocks = append(blocks, block1)
		block2 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-4*time.Hour).UnixMilli(), currentTime.Add(-3*time.Hour).UnixMilli())
		blocks = append(blocks, block2)
		block3 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-5*time.Hour).UnixMilli(), currentTime.Add(-4*time.Hour).UnixMilli())
		blocks = append(blocks, block3)
		block4 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-6*time.Hour).UnixMilli(), currentTime.Add(-5*time.Hour).UnixMilli())
		blocks = append(blocks, block4)

		shippedBlocks := map[ulid.ULID]struct{}{
			block2.Meta().ULID: {},
			block3.Meta().ULID: {},
			block4.Meta().ULID: {},
		}
		userDB := &userTSDB{
			db:                   db,
			shipper:              &shipperMock{},
			shippedBlocks:        shippedBlocks,
			blockRetentionPeriod: 2 * time.Hour.Milliseconds(),
		}

		blocksToDelete := userDB.blocksToDelete(blocks)
		require.Equal(t, 3, len(blocksToDelete))
		require.NotContains(t, blocksToDelete, block1.Meta().ULID)
		require.Contains(t, blocksToDelete, block2.Meta().ULID)
		require.Contains(t, blocksToDelete, block3.Meta().ULID)
		require.Contains(t, blocksToDelete, block4.Meta().ULID)
	})

	t.Run("should not delete block not reaching block retention period even it is shipped", func(t *testing.T) {
		currentTime := time.Now()
		var blocks []*tsdb.Block
		block1 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-2*time.Hour).UnixMilli(), currentTime.Add(-1*time.Hour).UnixMilli())
		blocks = append(blocks, block1)
		block2 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-4*time.Hour).UnixMilli(), currentTime.Add(-3*time.Hour).UnixMilli())
		blocks = append(blocks, block2)
		block3 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-5*time.Hour).UnixMilli(), currentTime.Add(-4*time.Hour).UnixMilli())
		blocks = append(blocks, block3)
		block4 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-6*time.Hour).UnixMilli(), currentTime.Add(-5*time.Hour).UnixMilli())
		blocks = append(blocks, block4)

		shippedBlocks := map[ulid.ULID]struct{}{
			block1.Meta().ULID: {},
			block2.Meta().ULID: {},
			block3.Meta().ULID: {},
			block4.Meta().ULID: {},
		}
		userDB := &userTSDB{
			db:                   db,
			shipper:              &shipperMock{},
			shippedBlocks:        shippedBlocks,
			blockRetentionPeriod: 2 * time.Hour.Milliseconds(),
		}

		blocksToDelete := userDB.blocksToDelete(blocks)
		require.Equal(t, 3, len(blocksToDelete))
		require.NotContains(t, blocksToDelete, block1.Meta().ULID)
		require.Contains(t, blocksToDelete, block2.Meta().ULID)
		require.Contains(t, blocksToDelete, block3.Meta().ULID)
		require.Contains(t, blocksToDelete, block4.Meta().ULID)
	})

	t.Run("should delete all block beyond block retention period if there is no shipper", func(t *testing.T) {
		currentTime := time.Now()
		var blocks []*tsdb.Block
		block1 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-3*time.Hour).UnixMilli(), currentTime.Add(-2*time.Hour).UnixMilli())
		blocks = append(blocks, block1)
		block2 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-4*time.Hour).UnixMilli(), currentTime.Add(-3*time.Hour).UnixMilli())
		blocks = append(blocks, block2)
		block3 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-5*time.Hour).UnixMilli(), currentTime.Add(-4*time.Hour).UnixMilli())
		blocks = append(blocks, block3)
		block4 := CreateBlock(t, context.Background(), tempDir, currentTime.Add(-6*time.Hour).UnixMilli(), currentTime.Add(-5*time.Hour).UnixMilli())
		blocks = append(blocks, block4)
		userDB := &userTSDB{
			db:                   db,
			blockRetentionPeriod: 2 * time.Hour.Milliseconds(),
		}

		blocksToDelete := userDB.blocksToDelete(blocks)
		require.Equal(t, 4, len(blocksToDelete))
		require.Contains(t, blocksToDelete, block1.Meta().ULID)
		require.Contains(t, blocksToDelete, block2.Meta().ULID)
		require.Contains(t, blocksToDelete, block3.Meta().ULID)
		require.Contains(t, blocksToDelete, block4.Meta().ULID)
	})
}

func TestIngester_UpdateLabelSetMetrics(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{2 * time.Hour}
	reg := prometheus.NewRegistry()
	limits := defaultLimitsTestConfig()
	userID := "1"
	ctx := user.InjectOrgID(context.Background(), userID)

	limits.LimitsPerLabelSet = []validation.LimitsPerLabelSet{
		{
			LabelSet: labels.FromMap(map[string]string{
				"foo": "bar",
			}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 1,
			},
		},
		{
			LabelSet: labels.EmptyLabels(),
		},
	}
	tenantLimits := newMockTenantLimits(map[string]*validation.Limits{userID: &limits})

	dir := t.TempDir()
	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, tenantLimits, blocksDir, reg, false)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	// Wait until it's ACTIVE
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})
	// Add user ID.
	wreq := &cortexpb.WriteRequest{
		Timeseries: []cortexpb.PreallocTimeseries{
			{TimeSeries: &cortexpb.TimeSeries{
				Labels:  cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{model.MetricNameLabel: "test", "foo": "bar"})),
				Samples: []cortexpb.Sample{{Value: 0, TimestampMs: 1}},
			}},
		},
	}
	_, err = i.Push(ctx, wreq)
	require.NoError(t, err)

	// Push one more series will trigger throttle by label set.
	wreq = &cortexpb.WriteRequest{
		Timeseries: []cortexpb.PreallocTimeseries{
			{TimeSeries: &cortexpb.TimeSeries{
				Labels:  cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{model.MetricNameLabel: "test2", "foo": "bar"})),
				Samples: []cortexpb.Sample{{Value: 0, TimestampMs: 0}},
			}},
		},
	}
	_, err = i.Push(ctx, wreq)
	require.Error(t, err)
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_discarded_samples_per_labelset_total The total number of samples that were discarded for each labelset.
		# TYPE cortex_discarded_samples_per_labelset_total counter
		cortex_discarded_samples_per_labelset_total{labelset="{foo=\"bar\"}",reason="per_labelset_series_limit",user="1"} 1
		# HELP cortex_discarded_samples_total The total number of samples that were discarded.
		# TYPE cortex_discarded_samples_total counter
		cortex_discarded_samples_total{reason="per_labelset_series_limit",user="1"} 1
	`), "cortex_discarded_samples_total", "cortex_discarded_samples_per_labelset_total"))

	// Expect per labelset validate metrics cleaned up.
	i.closeAllTSDB()
	i.updateLabelSetMetrics()
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_discarded_samples_total The total number of samples that were discarded.
		# TYPE cortex_discarded_samples_total counter
		cortex_discarded_samples_total{reason="per_labelset_series_limit",user="1"} 1
	`), "cortex_discarded_samples_total", "cortex_discarded_samples_per_labelset_total"))
}

// mockTenantLimits exposes per-tenant limits based on a provided map
type mockTenantLimits struct {
	limits map[string]*validation.Limits
	m      sync.Mutex
}

// newMockTenantLimits creates a new mockTenantLimits that returns per-tenant limits based on
// the given map
func newMockTenantLimits(limits map[string]*validation.Limits) *mockTenantLimits {
	return &mockTenantLimits{
		limits: limits,
	}
}

func (l *mockTenantLimits) ByUserID(userID string) *validation.Limits {
	l.m.Lock()
	defer l.m.Unlock()
	return l.limits[userID]
}

func (l *mockTenantLimits) AllByUserID() map[string]*validation.Limits {
	l.m.Lock()
	defer l.m.Unlock()
	return l.limits
}

func (l *mockTenantLimits) setLimits(userID string, limits *validation.Limits) {
	l.m.Lock()
	defer l.m.Unlock()
	l.limits[userID] = limits
}

func CreateBlock(t *testing.T, ctx context.Context, dir string, mint, maxt int64) *tsdb.Block {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = 10000000000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	require.NoError(t, err)
	defer func() {
		runutil.CloseWithErrCapture(&err, h, "TSDB Head")
		e := os.RemoveAll(headOpts.ChunkDirRoot)
		require.NoError(t, e)
	}()

	app := h.Appender(ctx)

	var ref storage.SeriesRef
	start := (maxt-mint)/2 + mint
	_, err = app.Append(ref, labels.Labels{labels.Label{Name: "test_label", Value: "test_value"}}, start, float64(1))
	require.NoError(t, err)
	err = app.Commit()
	require.NoError(t, err)

	c, err := tsdb.NewLeveledCompactor(ctx, nil, log.NewNopLogger(), []int64{maxt - mint}, nil, nil)
	require.NoError(t, err)

	ids, err := c.Write(dir, h, mint, maxt, nil)
	require.NoError(t, err)
	blockId := ids[0]

	blockDir := filepath.Join(dir, blockId.String())
	logger := log.NewNopLogger()
	block, err := tsdb.OpenBlock(logger, blockDir, nil)
	require.NoError(t, err)

	return block
}
