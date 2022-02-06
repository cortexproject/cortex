package ingester

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type testStore struct {
	mtx sync.Mutex
	// Chunks keyed by userID.
	chunks map[string][]chunk.Chunk
}

func newTestStore(t require.TestingT, cfg Config, clientConfig client.Config, limits validation.Limits, reg prometheus.Registerer) (*testStore, *Ingester) {
	store := &testStore{
		chunks: map[string][]chunk.Chunk{},
	}
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	ing, err := New(cfg, clientConfig, overrides, store, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

	return store, ing
}

func newDefaultTestStore(t testing.TB) (*testStore, *Ingester) {
	t.Helper()

	return newTestStore(t,
		defaultIngesterTestConfig(t),
		defaultClientTestConfig(),
		defaultLimitsTestConfig(), nil)
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
		assert.Equal(t, testData[userID], res, "userID %s", userID)
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

func matrixToSamples(m model.Matrix) []cortexpb.Sample {
	var samples []cortexpb.Sample
	for _, ss := range m {
		for _, sp := range ss.Values {
			samples = append(samples, cortexpb.Sample{
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
			labels = append(labels, cortexpb.FromLabelAdaptersToLabels(cortexpb.FromMetricsToLabelAdapters(ss.Metric)))
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

func pushTestMetadata(t *testing.T, ing *Ingester, numMetadata, metadataPerMetric int) ([]string, map[string][]*cortexpb.MetricMetadata) {
	userIDs := []string{"1", "2", "3"}

	// Create test metadata.
	// Map of userIDs, to map of metric => metadataSet
	testData := map[string][]*cortexpb.MetricMetadata{}
	for _, userID := range userIDs {
		metadata := make([]*cortexpb.MetricMetadata, 0, metadataPerMetric)
		for i := 0; i < numMetadata; i++ {
			metricName := fmt.Sprintf("testmetric_%d", i)
			for j := 0; j < metadataPerMetric; j++ {
				m := &cortexpb.MetricMetadata{MetricFamilyName: metricName, Help: fmt.Sprintf("a help for %d", j), Unit: "", Type: cortexpb.COUNTER}
				metadata = append(metadata, m)
			}
		}
		testData[userID] = metadata
	}

	// Append metadata.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, cortexpb.ToWriteRequest(nil, nil, testData[userID], cortexpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func pushTestSamples(t testing.TB, ing *Ingester, numSeries, samplesPerSeries, offset int) ([]string, map[string]model.Matrix) {
	userIDs := []string{"1", "2", "3"}

	// Create test samples.
	testData := map[string]model.Matrix{}
	for i, userID := range userIDs {
		testData[userID] = buildTestMatrix(numSeries, samplesPerSeries, i+offset)
	}

	// Append samples.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, cortexpb.ToWriteRequest(matrixToLables(testData[userID]), matrixToSamples(testData[userID]), nil, cortexpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func retrieveTestSamples(t *testing.T, ing *Ingester, userIDs []string, testData map[string]model.Matrix) {
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
}

func TestIngesterAppend(t *testing.T) {
	store, ing := newDefaultTestStore(t)
	userIDs, testData := pushTestSamples(t, ing, 10, 1000, 0)
	retrieveTestSamples(t, ing, userIDs, testData)

	// Read samples back via chunk store.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	store.checkData(t, userIDs, testData)
}

func TestIngesterMetadataAppend(t *testing.T) {
	for _, tc := range []struct {
		desc              string
		numMetadata       int
		metadataPerMetric int
		expectedMetrics   int
		expectedMetadata  int
		err               error
	}{
		{"with no metadata", 0, 0, 0, 0, nil},
		{"with one metadata per metric", 10, 1, 10, 10, nil},
		{"with multiple metadata per metric", 10, 3, 10, 30, nil},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			limits := defaultLimitsTestConfig()
			limits.MaxLocalMetadataPerMetric = 50
			_, ing := newTestStore(t, defaultIngesterTestConfig(t), defaultClientTestConfig(), limits, nil)
			userIDs, _ := pushTestMetadata(t, ing, tc.numMetadata, tc.metadataPerMetric)

			for _, userID := range userIDs {
				ctx := user.InjectOrgID(context.Background(), userID)
				resp, err := ing.MetricsMetadata(ctx, nil)

				if tc.err != nil {
					require.Equal(t, tc.err, err)
				} else {
					require.NoError(t, err)
					require.NotNil(t, resp)

					metricTracker := map[string]bool{}
					for _, m := range resp.Metadata {
						_, ok := metricTracker[m.GetMetricFamilyName()]
						if !ok {
							metricTracker[m.GetMetricFamilyName()] = true
						}
					}

					require.Equal(t, tc.expectedMetrics, len(metricTracker))
					require.Equal(t, tc.expectedMetadata, len(resp.Metadata))
				}
			}
		})
	}
}

func TestIngesterPurgeMetadata(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.MetadataRetainPeriod = 20 * time.Millisecond
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)
	userIDs, _ := pushTestMetadata(t, ing, 10, 3)

	time.Sleep(40 * time.Millisecond)
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		ing.purgeUserMetricsMetadata()

		resp, err := ing.MetricsMetadata(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, len(resp.GetMetadata()))
	}
}

func TestIngesterMetadataMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.MetadataRetainPeriod = 20 * time.Millisecond
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), reg)
	_, _ = pushTestMetadata(t, ing, 10, 3)

	pushTestMetadata(t, ing, 10, 3)
	pushTestMetadata(t, ing, 10, 3) // We push the _exact_ same metrics again to ensure idempotency. Metadata is kept as a set so there shouldn't be a change of metrics.

	metricNames := []string{
		"cortex_ingester_memory_metadata_created_total",
		"cortex_ingester_memory_metadata_removed_total",
		"cortex_ingester_memory_metadata",
	}

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 90
		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 30
		cortex_ingester_memory_metadata_created_total{user="2"} 30
		cortex_ingester_memory_metadata_created_total{user="3"} 30
	`), metricNames...))

	time.Sleep(40 * time.Millisecond)
	ing.purgeUserMetricsMetadata()
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 30
		cortex_ingester_memory_metadata_created_total{user="2"} 30
		cortex_ingester_memory_metadata_created_total{user="3"} 30
		# HELP cortex_ingester_memory_metadata_removed_total The total number of metadata that were removed per user.
		# TYPE cortex_ingester_memory_metadata_removed_total counter
		cortex_ingester_memory_metadata_removed_total{user="1"} 30
		cortex_ingester_memory_metadata_removed_total{user="2"} 30
		cortex_ingester_memory_metadata_removed_total{user="3"} 30
	`), metricNames...))

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
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
}

func TestIngesterIdleFlush(t *testing.T) {
	// Create test ingester with short flush cycle
	cfg := defaultIngesterTestConfig(t)
	cfg.FlushCheckPeriod = 20 * time.Millisecond
	cfg.MaxChunkIdle = 100 * time.Millisecond
	cfg.RetainPeriod = 500 * time.Millisecond
	store, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)

	userIDs, testData := pushTestSamples(t, ing, 4, 100, 0)

	// wait beyond idle time so samples flush
	time.Sleep(cfg.MaxChunkIdle * 3)

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

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
}

func TestIngesterSpreadFlush(t *testing.T) {
	// Create test ingester with short flush cycle
	cfg := defaultIngesterTestConfig(t)
	cfg.SpreadFlushes = true
	cfg.FlushCheckPeriod = 20 * time.Millisecond
	store, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)

	userIDs, testData := pushTestSamples(t, ing, 4, 100, 0)

	// add another sample with timestamp at the end of the cycle to trigger
	// head closes and get an extra chunk so we will flush the first one
	_, _ = pushTestSamples(t, ing, 4, 1, int(cfg.MaxChunkAge.Seconds()-1)*1000)

	// wait beyond flush time so first set of samples should be sent to store
	// (you'd think a shorter wait, like period*2, would work, but Go timers are not reliable enough for that)
	time.Sleep(cfg.FlushCheckPeriod * 10)

	// check the first set of samples has been sent to the store
	store.checkData(t, userIDs, testData)

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
}

type stream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*client.QueryStreamResponse
}

func (s *stream) Context() context.Context {
	return s.ctx
}

func (s *stream) Send(response *client.QueryStreamResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func TestIngesterAppendOutOfOrderAndDuplicate(t *testing.T) {
	_, ing := newDefaultTestStore(t)
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	m := labelPairs{
		{Name: model.MetricNameLabel, Value: "testmetric"},
	}
	ctx := context.Background()
	err := ing.append(ctx, userID, m, 1, 0, cortexpb.API, nil)
	require.NoError(t, err)

	// Two times exactly the same sample (noop).
	err = ing.append(ctx, userID, m, 1, 0, cortexpb.API, nil)
	require.NoError(t, err)

	// Earlier sample than previous one.
	err = ing.append(ctx, userID, m, 0, 0, cortexpb.API, nil)
	require.Contains(t, err.Error(), "sample timestamp out of order")
	errResp, ok := err.(*validationError)
	require.True(t, ok)
	require.Equal(t, errResp.code, 400)

	// Same timestamp as previous sample, but different value.
	err = ing.append(ctx, userID, m, 1, 1, cortexpb.API, nil)
	require.Contains(t, err.Error(), "sample with repeated timestamp but different value")
	errResp, ok = err.(*validationError)
	require.True(t, ok)
	require.Equal(t, errResp.code, 400)
}

// Test that blank labels are removed by the ingester
func TestIngesterAppendBlankLabel(t *testing.T) {
	_, ing := newDefaultTestStore(t)
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	lp := labelPairs{
		{Name: model.MetricNameLabel, Value: "testmetric"},
		{Name: "foo", Value: ""},
		{Name: "bar", Value: ""},
	}
	ctx := user.InjectOrgID(context.Background(), userID)
	err := ing.append(ctx, userID, lp, 1, 0, cortexpb.API, nil)
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

	chunksIngesterGenerator := func() *Ingester {
		cfg := defaultIngesterTestConfig(t)
		cfg.WALConfig.WALEnabled = true
		cfg.WALConfig.Recover = true
		cfg.WALConfig.Dir = chunksDir
		cfg.WALConfig.CheckpointDuration = 100 * time.Minute

		_, ing := newTestStore(t, cfg, defaultClientTestConfig(), limits, nil)
		return ing
	}

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
	for i, ingGenerator := range []func() *Ingester{chunksIngesterGenerator, blocksIngesterGenerator} {
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

	chunksIngesterGenerator := func() *Ingester {
		cfg := defaultIngesterTestConfig(t)
		cfg.WALConfig.WALEnabled = true
		cfg.WALConfig.Recover = true
		cfg.WALConfig.Dir = chunksDir
		cfg.WALConfig.CheckpointDuration = 100 * time.Minute

		_, ing := newTestStore(t, cfg, defaultClientTestConfig(), limits, nil)
		return ing
	}

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
	for i, ingGenerator := range []func() *Ingester{chunksIngesterGenerator, blocksIngesterGenerator} {
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

func TestIngesterValidation(t *testing.T) {
	_, ing := newDefaultTestStore(t)
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	userID := "1"
	ctx := user.InjectOrgID(context.Background(), userID)
	m := labelPairs{{Name: labels.MetricName, Value: "testmetric"}}

	// As a setup, let's append samples.
	err := ing.append(context.Background(), userID, m, 1, 0, cortexpb.API, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc    string
		lbls    []labels.Labels
		samples []cortexpb.Sample
		err     error
	}{
		{
			desc: "With multiple append failures, only return the first error.",
			lbls: []labels.Labels{
				{{Name: labels.MetricName, Value: "testmetric"}},
				{{Name: labels.MetricName, Value: "testmetric"}},
			},
			samples: []cortexpb.Sample{
				{TimestampMs: 0, Value: 0}, // earlier timestamp, out of order.
				{TimestampMs: 1, Value: 2}, // same timestamp different value.
			},
			err: httpgrpc.Errorf(http.StatusBadRequest, `user=1: sample timestamp out of order; last timestamp: 0.001, incoming timestamp: 0 for series {__name__="testmetric"}`),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := ing.Push(ctx, cortexpb.ToWriteRequest(tc.lbls, tc.samples, nil, cortexpb.API))
			require.Equal(t, tc.err, err)
		})
	}
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
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

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
				_, err := ing.Push(ctx, &cortexpb.WriteRequest{
					Timeseries: []cortexpb.PreallocTimeseries{
						{
							TimeSeries: &cortexpb.TimeSeries{
								Labels: []cortexpb.LabelAdapter{
									{Name: model.MetricNameLabel, Value: fmt.Sprintf("metric_%d", j)},
								},
								Samples: []cortexpb.Sample{
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
	limits := defaultLimitsTestConfig()
	benchmarkIngesterPush(b, limits, false)
}

func BenchmarkIngesterPushErrors(b *testing.B) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerMetric = 1
	benchmarkIngesterPush(b, limits, true)
}

// Construct a set of realistic-looking samples, all with slightly different label sets
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

func benchmarkIngesterPush(b *testing.B, limits validation.Limits, errorsExpected bool) {
	cfg := defaultIngesterTestConfig(b)
	clientCfg := defaultClientTestConfig()

	const (
		series  = 100
		samples = 100
	)

	allLabels, allSamples := benchmarkData(series)
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
				_, ing := newTestStore(b, cfg, clientCfg, limits, nil)
				// Bump the timestamp on each of our test samples each time round the loop
				for j := 0; j < samples; j++ {
					for i := range allSamples {
						allSamples[i].TimestampMs = int64(j + 1)
					}
					_, err := ing.Push(ctx, cortexpb.ToWriteRequest(allLabels, allSamples, nil, cortexpb.API))
					if !errorsExpected {
						require.NoError(b, err)
					}
				}
				_ = services.StopAndAwaitTerminated(context.Background(), ing)
			}
		})
	}

}

func BenchmarkIngester_QueryStream(b *testing.B) {
	cfg := defaultIngesterTestConfig(b)
	clientCfg := defaultClientTestConfig()
	limits := defaultLimitsTestConfig()
	_, ing := newTestStore(b, cfg, clientCfg, limits, nil)
	ctx := user.InjectOrgID(context.Background(), "1")

	const (
		series  = 2000
		samples = 1000
	)

	allLabels, allSamples := benchmarkData(series)

	// Bump the timestamp and set a random value on each of our test samples each time round the loop
	for j := 0; j < samples; j++ {
		for i := range allSamples {
			allSamples[i].TimestampMs = int64(j + 1)
			allSamples[i].Value = rand.Float64()
		}
		_, err := ing.Push(ctx, cortexpb.ToWriteRequest(allLabels, allSamples, nil, cortexpb.API))
		require.NoError(b, err)
	}

	req := &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samples + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "container_cpu_usage_seconds_total",
		}},
	}

	mockStream := &mockQueryStreamServer{ctx: ctx}

	b.ResetTimer()

	for ix := 0; ix < b.N; ix++ {
		err := ing.QueryStream(req, mockStream)
		require.NoError(b, err)
	}
}

func TestIngesterActiveSeries(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_active_series",
	}
	userID := "test"

	tests := map[string]struct {
		reqs                []*cortexpb.WriteRequest
		expectedMetrics     string
		disableActiveSeries bool
	}{
		"should succeed on valid series and metadata": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					cortexpb.API),
			},
			expectedMetrics: `
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					cortexpb.API),
			},
			expectedMetrics: ``,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.ActiveSeriesMetricsEnabled = !testData.disableActiveSeries

			_, i := newTestStore(t,
				cfg,
				defaultClientTestConfig(),
				defaultLimitsTestConfig(), registry)

			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), userID)

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// Push timeseries
			for _, req := range testData.reqs {
				_, err := i.Push(ctx, req)
				assert.NoError(t, err)
			}

			// Update active series for metrics check.
			if !testData.disableActiveSeries {
				i.userStatesMtx.Lock()
				i.userStates.purgeAndUpdateActiveSeries(time.Now().Add(-i.cfg.ActiveSeriesMetricsIdleTimeout))
				i.userStatesMtx.Unlock()
			}

			// Check tracked Prometheus metrics
			err := testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), metricNames...)
			assert.NoError(t, err)
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
