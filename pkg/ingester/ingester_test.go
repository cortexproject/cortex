package ingester

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	net_context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
)

type testStore struct {
	mtx sync.Mutex
	// Chunks keyed by userID.
	chunks map[string][]chunk.Chunk
}

func newTestStore(t require.TestingT, cfg Config, clientConfig client.Config, limits validation.Limits) (*testStore, *Ingester) {
	store := &testStore{
		chunks: map[string][]chunk.Chunk{},
	}
	overrides, err := validation.NewOverrides(limits)
	require.NoError(t, err)

	ing, err := New(cfg, clientConfig, overrides, store, nil)
	require.NoError(t, err)

	return store, ing
}

func newDefaultTestStore(t require.TestingT) (*testStore, *Ingester) {
	return newTestStore(t,
		defaultIngesterTestConfig(),
		defaultClientTestConfig(),
		defaultLimitsTestConfig())
}

func (s *testStore) Put(ctx context.Context, chunks []chunk.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, chunk := range chunks {
		for _, v := range chunk.Metric {
			if v.Value == "" {
				return fmt.Errorf("Chunk has blank label %q", v.Name)
			}
		}
	}
	userID := chunks[0].UserID
	s.chunks[userID] = append(s.chunks[userID], chunks...)
	return nil
}

func (s *testStore) Stop() {}

// check that the store is holding data equivalent to what we expect
func (s *testStore) checkData(t *testing.T, userIDs []string, testData map[string]model.Matrix) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, userID := range userIDs {
		res, err := chunk.ChunksToMatrix(context.Background(), s.chunks[userID], model.Time(0), model.Time(math.MaxInt64))
		require.NoError(t, err)
		sort.Sort(res)
		assert.Equal(t, testData[userID], res)
	}
}

func buildTestMatrix(numSeries int, samplesPerSeries int, offset int) model.Matrix {
	m := make(model.Matrix, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		ss := model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: model.LabelValue(fmt.Sprintf("testmetric_%d", i)),
				model.JobLabel:        model.LabelValue(fmt.Sprintf("testjob%d", i%2)),
			},
			Values: make([]model.SamplePair, 0, samplesPerSeries),
		}
		for j := 0; j < samplesPerSeries; j++ {
			ss.Values = append(ss.Values, model.SamplePair{
				Timestamp: model.Time(i + j + offset),
				Value:     model.SampleValue(i + j + offset),
			})
		}
		m = append(m, &ss)
	}
	sort.Sort(m)
	return m
}

func matrixToSamples(m model.Matrix) []client.Sample {
	var samples []client.Sample
	for _, ss := range m {
		for _, sp := range ss.Values {
			samples = append(samples, client.Sample{
				TimestampMs: int64(sp.Timestamp),
				Value:       float64(sp.Value),
			})
		}
	}
	return samples
}

// Return one copy of the labels per sample
func matrixToLables(m model.Matrix) []labels.Labels {
	var labels []labels.Labels
	for _, ss := range m {
		for range ss.Values {
			labels = append(labels, client.FromLabelAdaptersToLabels(client.FromMetricsToLabelAdapters(ss.Metric)))
		}
	}
	return labels
}

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

func pushTestSamples(t *testing.T, ing *Ingester, numSeries, samplesPerSeries, offset int) ([]string, map[string]model.Matrix) {
	userIDs := []string{"1", "2", "3"}

	// Create test samples.
	testData := map[string]model.Matrix{}
	for i, userID := range userIDs {
		testData[userID] = buildTestMatrix(numSeries, samplesPerSeries, i+offset)
	}

	// Append samples.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, client.ToWriteRequest(matrixToLables(testData[userID]), matrixToSamples(testData[userID]), client.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func TestIngesterAppend(t *testing.T) {
	store, ing := newDefaultTestStore(t)
	userIDs, testData := pushTestSamples(t, ing, 10, 1000, 0)

	// Read samples back via ingester queries.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		res, req, err := runTestQuery(ctx, t, ing, labels.MatchRegexp, model.JobLabel, ".+")
		require.NoError(t, err)
		assert.Equal(t, testData[userID], res)

		s := stream{
			ctx: ctx,
		}
		err = ing.QueryStream(req, &s)
		require.NoError(t, err)

		res, err = chunkcompat.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
		require.NoError(t, err)
		assert.Equal(t, testData[userID].String(), res.String())
	}

	// Read samples back via chunk store.
	ing.Shutdown()
	store.checkData(t, userIDs, testData)
}

func TestIngesterSendsOnlySeriesWithData(t *testing.T) {
	_, ing := newDefaultTestStore(t)

	userIDs, _ := pushTestSamples(t, ing, 10, 1000, 0)

	// Read samples back via ingester queries.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, req, err := runTestQueryTimes(ctx, t, ing, labels.MatchRegexp, model.JobLabel, ".+", model.Latest.Add(-15*time.Second), model.Latest)
		require.NoError(t, err)

		s := stream{
			ctx: ctx,
		}
		err = ing.QueryStream(req, &s)
		require.NoError(t, err)

		// Nothing should be selected.
		require.Equal(t, 0, len(s.responses))
	}

	// Read samples back via chunk store.
	ing.Shutdown()
}

func TestIngesterIdleFlush(t *testing.T) {
	// Create test ingester with short flush cycle
	cfg := defaultIngesterTestConfig()
	cfg.FlushCheckPeriod = 20 * time.Millisecond
	cfg.MaxChunkIdle = 100 * time.Millisecond
	cfg.RetainPeriod = 500 * time.Millisecond
	store, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())

	userIDs, testData := pushTestSamples(t, ing, 4, 100, 0)

	// wait beyond idle time so samples flush
	time.Sleep(cfg.MaxChunkIdle * 2)

	store.checkData(t, userIDs, testData)

	// Check data is still retained by ingester
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		res, _, err := runTestQuery(ctx, t, ing, labels.MatchRegexp, model.JobLabel, ".+")
		require.NoError(t, err)
		assert.Equal(t, testData[userID], res)
	}

	// now wait beyond retain time so chunks are removed from memory
	time.Sleep(cfg.RetainPeriod)

	// Check data has gone from ingester
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		res, _, err := runTestQuery(ctx, t, ing, labels.MatchRegexp, model.JobLabel, ".+")
		require.NoError(t, err)
		assert.Equal(t, model.Matrix{}, res)
	}
}

func TestIngesterSpreadFlush(t *testing.T) {
	// Create test ingester with short flush cycle
	cfg := defaultIngesterTestConfig()
	cfg.SpreadFlushes = true
	cfg.FlushCheckPeriod = 20 * time.Millisecond
	store, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())

	userIDs, testData := pushTestSamples(t, ing, 4, 100, 0)

	// add another sample with timestamp at the end of the cycle to trigger
	// head closes and get an extra chunk so we will flush the first one
	_, _ = pushTestSamples(t, ing, 4, 1, int(cfg.MaxChunkAge.Seconds()-1)*1000)

	// wait beyond flush time so first set of samples should be sent to store
	time.Sleep(cfg.FlushCheckPeriod * 2)

	// check the first set of samples has been sent to the store
	store.checkData(t, userIDs, testData)
}

type stream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*client.QueryStreamResponse
}

func (s *stream) Context() net_context.Context {
	return s.ctx
}

func (s *stream) Send(response *client.QueryStreamResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func TestIngesterAppendOutOfOrderAndDuplicate(t *testing.T) {
	_, ing := newDefaultTestStore(t)
	defer ing.Shutdown()

	m := labelPairs{
		{Name: model.MetricNameLabel, Value: "testmetric"},
	}
	state := ing.userStates.getOrCreate(userID)
	require.NotNil(t, state)
	fp, series, err := state.getSeries(m)
	require.NoError(t, err)
	err = ing.append(state, fp, series, 1, 0, client.API)
	require.NoError(t, err)

	// Two times exactly the same sample (noop).
	err = ing.append(state, fp, series, 1, 0, client.API)
	require.Error(t, err)
	mse, ok := err.(*memorySeriesError)
	require.True(t, ok)
	require.True(t, mse.noReport)

	// Earlier sample than previous one.
	err = ing.append(state, fp, series, 0, 0, client.API)
	require.Contains(t, err.Error(), "sample timestamp out of order")

	// Same timestamp as previous sample, but different value.
	err = ing.append(state, fp, series, 1, 1, client.API)
	require.Contains(t, err.Error(), "sample with repeated timestamp but different value")
}

// Test that blank labels are removed by the ingester
func TestIngesterAppendBlankLabel(t *testing.T) {
	_, ing := newDefaultTestStore(t)
	defer ing.Shutdown()

	lp := labelPairs{
		{Name: model.MetricNameLabel, Value: "testmetric"},
		{Name: "foo", Value: ""},
		{Name: "bar", Value: ""},
	}
	ctx := user.InjectOrgID(context.Background(), userID)
	state := ing.userStates.getOrCreate(userID)
	require.NotNil(t, state)
	fp, series, err := state.getSeries(lp)
	require.NoError(t, err)
	err = ing.append(state, fp, series, 1, 0, client.API)
	require.NoError(t, err)

	res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, labels.MetricName, "testmetric")
	require.NoError(t, err)

	expected := model.Matrix{
		{
			Metric: model.Metric{labels.MetricName: "testmetric"},
			Values: []model.SamplePair{
				{Timestamp: 1, Value: 0},
			},
		},
	}

	assert.Equal(t, expected, res)
}

func TestIngesterUserSeriesLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerUser = 1

	_, ing := newTestStore(t, defaultIngesterTestConfig(), defaultClientTestConfig(), limits)
	defer ing.Shutdown()

	userID := "1"
	labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := client.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := client.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := client.Sample{
		TimestampMs: 1,
		Value:       3,
	}

	// Append only one series first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, client.ToWriteRequest([]labels.Labels{labels1}, []client.Sample{sample1}, client.API))
	require.NoError(t, err)

	// Append to two series, expect series-exceeded error.
	_, err = ing.Push(ctx, client.ToWriteRequest([]labels.Labels{labels1, labels3}, []client.Sample{sample2, sample3}, client.API))
	if resp, ok := httpgrpc.HTTPResponseFromError(err); !ok || resp.Code != http.StatusTooManyRequests {
		t.Fatalf("expected error about exceeding metrics per user, got %v", err)
	}

	// Read samples back via ingester queries.
	res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
	require.NoError(t, err)

	expected := model.Matrix{
		{
			Metric: client.FromLabelAdaptersToMetric(client.FromLabelsToLabelAdapters(labels1)),
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
}

func TestIngesterMetricSeriesLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerMetric = 1

	_, ing := newTestStore(t, defaultIngesterTestConfig(), defaultClientTestConfig(), limits)
	defer ing.Shutdown()

	userID := "1"
	labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := client.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := client.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := client.Sample{
		TimestampMs: 1,
		Value:       3,
	}

	// Append only one series first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, client.ToWriteRequest([]labels.Labels{labels1}, []client.Sample{sample1}, client.API))
	require.NoError(t, err)

	// Append to two series, expect series-exceeded error.
	_, err = ing.Push(ctx, client.ToWriteRequest([]labels.Labels{labels1, labels3}, []client.Sample{sample2, sample3}, client.API))
	if resp, ok := httpgrpc.HTTPResponseFromError(err); !ok || resp.Code != http.StatusTooManyRequests {
		t.Fatalf("expected error about exceeding series per metric, got %v", err)
	}

	// Read samples back via ingester queries.
	res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
	require.NoError(t, err)

	expected := model.Matrix{
		{
			Metric: client.FromLabelAdaptersToMetric(client.FromLabelsToLabelAdapters(labels1)),
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
}

func BenchmarkIngesterSeriesCreationLocking(b *testing.B) {
	for i := 1; i <= 32; i++ {
		b.Run(strconv.Itoa(i), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				benchmarkIngesterSeriesCreationLocking(b, i)
			}
		})
	}
}

func benchmarkIngesterSeriesCreationLocking(b *testing.B, parallelism int) {
	_, ing := newDefaultTestStore(b)
	defer ing.Shutdown()

	var (
		wg     sync.WaitGroup
		series = int(1e4)
		ctx    = context.Background()
	)
	wg.Add(parallelism)
	ctx = user.InjectOrgID(ctx, "1")
	for i := 0; i < parallelism; i++ {
		seriesPerGoroutine := series / parallelism
		go func(from, through int) {
			defer wg.Done()

			for j := from; j < through; j++ {
				_, err := ing.Push(ctx, &client.WriteRequest{
					Timeseries: []client.PreallocTimeseries{
						{
							TimeSeries: &client.TimeSeries{
								Labels: []client.LabelAdapter{
									{Name: model.MetricNameLabel, Value: fmt.Sprintf("metric_%d", j)},
								},
								Samples: []client.Sample{
									{TimestampMs: int64(j), Value: float64(j)},
								},
							},
						},
					},
				})
				require.NoError(b, err)
			}

		}(i*seriesPerGoroutine, (i+1)*seriesPerGoroutine)
	}

	wg.Wait()
}

func BenchmarkIngesterPush(b *testing.B) {
	cfg := defaultIngesterTestConfig()
	clientCfg := defaultClientTestConfig()
	limits := defaultLimitsTestConfig()

	const (
		series  = 100
		samples = 100
	)

	// Construct a set of realistic-looking samples, all with slightly different label sets
	var allLabels []labels.Labels
	var allSamples []client.Sample
	for j := 0; j < series; j++ {
		labels := chunk.BenchmarkLabels.Copy()
		for i := range labels {
			if labels[i].Name == "cpu" {
				labels[i].Value = fmt.Sprintf("cpu%02d", j)
			}
		}
		allLabels = append(allLabels, labels)
		allSamples = append(allSamples, client.Sample{TimestampMs: 0, Value: float64(j)})
	}
	ctx := user.InjectOrgID(context.Background(), "1")

	encodings := []struct {
		name string
		e    promchunk.Encoding
	}{
		{"DoubleDelta", promchunk.DoubleDelta},
		{"Varbit", promchunk.Varbit},
		{"Bigchunk", promchunk.Bigchunk},
	}

	for _, enc := range encodings {
		b.Run(fmt.Sprintf("encoding=%s", enc.name), func(b *testing.B) {
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				_, ing := newTestStore(b, cfg, clientCfg, limits)
				// Bump the timestamp on each of our test samples each time round the loop
				for j := 0; j < samples; j++ {
					for i := range allSamples {
						allSamples[i].TimestampMs = int64(j + 1)
					}
					_, err := ing.Push(ctx, client.ToWriteRequest(allLabels, allSamples, client.API))
					require.NoError(b, err)
				}
				ing.Shutdown()
			}
		})
	}

}
