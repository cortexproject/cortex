package ingester

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
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
	resp, err := ing.Query(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	res := client.FromQueryResponse(resp)
	sort.Sort(res)
	return res, req, nil
}

func TestIngesterUserLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerUser = 1
	limits.MaxLocalMetricsWithMetadataPerUser = 1

	dir, err := ioutil.TempDir("", "limits")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	blocksIngesterGenerator := func() *Ingester {
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, blocksDir, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
		// Wait until it's ACTIVE
		test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
			return ing.lifecycler.GetState()
		})

		return ing
	}

	tests := []string{"blocks"}
	for i, ingGenerator := range []func() *Ingester{blocksIngesterGenerator} {
		t.Run(tests[i], func(t *testing.T) {
			ing := ingGenerator()

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

			// Append only one series and one metadata first, expect no error.
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err = ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels1}, []cortexpb.Sample{sample1}, []*cortexpb.MetricMetadata{metadata1}, cortexpb.API))
			require.NoError(t, err)

			testLimits := func() {
				// Append to two series, expect series-exceeded error.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels1, labels3}, []cortexpb.Sample{sample2, sample3}, nil, cortexpb.API))
				httpResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "returned error is not an httpgrpc response")
				assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
				assert.Equal(t, wrapWithUser(makeLimitError(perUserSeriesLimit, ing.limiter.FormatError(userID, errMaxSeriesPerUserLimitExceeded)), userID).Error(), string(httpResp.Body))

				// Append two metadata, expect no error since metadata is a best effort approach.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest(nil, nil, []*cortexpb.MetricMetadata{metadata1, metadata2}, cortexpb.API))
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
			}

			testLimits()

			// Limits should hold after restart.
			services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
			ing = ingGenerator()
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			testLimits()
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

	dir, err := ioutil.TempDir("", "limits")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	blocksIngesterGenerator := func() *Ingester {
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, blocksDir, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
		// Wait until it's ACTIVE
		test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
			return ing.lifecycler.GetState()
		})

		return ing
	}

	tests := []string{"chunks", "blocks"}
	for i, ingGenerator := range []func() *Ingester{blocksIngesterGenerator} {
		t.Run(tests[i], func(t *testing.T) {
			ing := ingGenerator()

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

			// Append only one series and one metadata first, expect no error.
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err = ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels1}, []cortexpb.Sample{sample1}, []*cortexpb.MetricMetadata{metadata1}, cortexpb.API))
			require.NoError(t, err)

			testLimits := func() {
				// Append two series, expect series-exceeded error.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest([]labels.Labels{labels1, labels3}, []cortexpb.Sample{sample2, sample3}, nil, cortexpb.API))
				httpResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "returned error is not an httpgrpc response")
				assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
				assert.Equal(t, wrapWithUser(makeMetricLimitError(perMetricSeriesLimit, labels3, ing.limiter.FormatError(userID, errMaxSeriesPerMetricLimitExceeded)), userID).Error(), string(httpResp.Body))

				// Append two metadata for the same metric. Drop the second one, and expect no error since metadata is a best effort approach.
				_, err = ing.Push(ctx, cortexpb.ToWriteRequest(nil, nil, []*cortexpb.MetricMetadata{metadata1, metadata2}, cortexpb.API))
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
			}

			testLimits()

			// Limits should hold after restart.
			services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
			ing = ingGenerator()
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			testLimits()
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
