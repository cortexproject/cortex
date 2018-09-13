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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	net_context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util/chunkcompat"
	"github.com/weaveworks/cortex/pkg/util/wire"
)

type testStore struct {
	mtx sync.Mutex
	// Chunks keyed by userID.
	chunks map[string][]chunk.Chunk
}

func newTestStore(t require.TestingT, cfg Config) (*testStore, *Ingester) {
	store := &testStore{
		chunks: map[string][]chunk.Chunk{},
	}
	ing, err := New(cfg, store)
	require.NoError(t, err)
	return store, ing
}

func (s *testStore) Put(ctx context.Context, chunks []chunk.Chunk) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}
	s.chunks[userID] = append(s.chunks[userID], chunks...)
	return nil
}

func (s *testStore) Stop() {}

func buildTestMatrix(numSeries int, samplesPerSeries int, offset int) model.Matrix {
	m := make(model.Matrix, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		ss := model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: model.LabelValue(fmt.Sprintf("testmetric_%d", i)),
				model.JobLabel:        "testjob",
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

func matrixToSamples(m model.Matrix) []model.Sample {
	var samples []model.Sample
	for _, ss := range m {
		for _, sp := range ss.Values {
			samples = append(samples, model.Sample{
				Metric:    ss.Metric,
				Timestamp: sp.Timestamp,
				Value:     sp.Value,
			})
		}
	}
	return samples
}

func TestIngesterAppend(t *testing.T) {
	store, ing := newTestStore(t, defaultIngesterTestConfig())

	userIDs := []string{"1", "2", "3"}

	// Create test samples.
	testData := map[string]model.Matrix{}
	for i, userID := range userIDs {
		testData[userID] = buildTestMatrix(10, 1000, i)
	}

	// Append samples.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, client.ToWriteRequest(matrixToSamples(testData[userID]), client.API))
		require.NoError(t, err)
	}

	// Read samples back via ingester queries.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		matcher, err := labels.NewMatcher(labels.MatchRegexp, model.JobLabel, ".+")
		require.NoError(t, err)

		req, err := client.ToQueryRequest(model.Earliest, model.Latest, []*labels.Matcher{matcher})
		require.NoError(t, err)

		resp, err := ing.Query(ctx, req)
		require.NoError(t, err)

		res := client.FromQueryResponse(resp)
		sort.Sort(res)
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
	for _, userID := range userIDs {
		res, err := chunk.ChunksToMatrix(context.Background(), store.chunks[userID], model.Time(0), model.Time(math.MaxInt64))
		require.NoError(t, err)
		sort.Sort(res)
		assert.Equal(t, testData[userID], res)
	}
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
	_, ing := newTestStore(t, defaultIngesterTestConfig())
	defer ing.Shutdown()

	m := model.Metric{
		model.MetricNameLabel: "testmetric",
	}
	ctx := user.InjectOrgID(context.Background(), userID)
	err := ing.append(ctx, &model.Sample{Metric: m, Timestamp: 1, Value: 0}, client.API)
	require.NoError(t, err)

	// Two times exactly the same sample (noop).
	err = ing.append(ctx, &model.Sample{Metric: m, Timestamp: 1, Value: 0}, client.API)
	require.NoError(t, err)

	// Earlier sample than previous one.
	err = ing.append(ctx, &model.Sample{Metric: m, Timestamp: 0, Value: 0}, client.API)
	require.Contains(t, err.Error(), "sample timestamp out of order")
	errResp, ok := httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok)
	require.Equal(t, errResp.Code, int32(400))

	// Same timestamp as previous sample, but different value.
	err = ing.append(ctx, &model.Sample{Metric: m, Timestamp: 1, Value: 1}, client.API)
	require.Contains(t, err.Error(), "sample with repeated timestamp but different value")
	errResp, ok = httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok)
	require.Equal(t, errResp.Code, int32(400))
}

func TestIngesterUserSeriesLimitExceeded(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.limits.MaxSeriesPerUser = 1

	_, ing := newTestStore(t, cfg)
	defer ing.Shutdown()

	userID := "1"
	sample1 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: 0,
		Value:     1,
	}
	sample2 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: 1,
		Value:     2,
	}
	sample3 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "biz"},
		Timestamp: 1,
		Value:     3,
	}

	// Append only one series first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample1}, client.API))
	require.NoError(t, err)

	// Append to two series, expect series-exceeded error.
	_, err = ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample2, sample3}, client.API))
	if resp, ok := httpgrpc.HTTPResponseFromError(err); !ok || resp.Code != http.StatusTooManyRequests {
		t.Fatalf("expected error about exceeding metrics per user, got %v", err)
	}

	// Read samples back via ingester queries.
	matcher, err := labels.NewMatcher(labels.MatchEqual, model.MetricNameLabel, "testmetric")
	require.NoError(t, err)

	req, err := client.ToQueryRequest(model.Earliest, model.Latest, []*labels.Matcher{matcher})
	require.NoError(t, err)

	resp, err := ing.Query(ctx, req)
	require.NoError(t, err)

	res := client.FromQueryResponse(resp)
	sort.Sort(res)

	expected := model.Matrix{
		{
			Metric: sample1.Metric,
			Values: []model.SamplePair{
				{
					Timestamp: sample1.Timestamp,
					Value:     sample1.Value,
				},
				{
					Timestamp: sample2.Timestamp,
					Value:     sample2.Value,
				},
			},
		},
	}

	assert.Equal(t, expected, res)
}

func TestIngesterMetricSeriesLimitExceeded(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.limits.MaxSeriesPerMetric = 1

	_, ing := newTestStore(t, cfg)
	defer ing.Shutdown()

	userID := "1"
	sample1 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: 0,
		Value:     1,
	}
	sample2 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: 1,
		Value:     2,
	}
	sample3 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "biz"},
		Timestamp: 1,
		Value:     3,
	}

	// Append only one series first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample1}, client.API))
	require.NoError(t, err)

	// Append to two series, expect series-exceeded error.
	_, err = ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample2, sample3}, client.API))
	if resp, ok := httpgrpc.HTTPResponseFromError(err); !ok || resp.Code != http.StatusTooManyRequests {
		t.Fatalf("expected error about exceeding series per metric, got %v", err)
	}

	// Read samples back via ingester queries.
	matcher, err := labels.NewMatcher(labels.MatchEqual, model.MetricNameLabel, "testmetric")
	require.NoError(t, err)

	req, err := client.ToQueryRequest(model.Earliest, model.Latest, []*labels.Matcher{matcher})
	require.NoError(t, err)

	resp, err := ing.Query(ctx, req)
	require.NoError(t, err)

	res := client.FromQueryResponse(resp)
	sort.Sort(res)

	expected := model.Matrix{
		{
			Metric: sample1.Metric,
			Values: []model.SamplePair{
				{
					Timestamp: sample1.Timestamp,
					Value:     sample1.Value,
				},
				{
					Timestamp: sample2.Timestamp,
					Value:     sample2.Value,
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
	cfg := defaultIngesterTestConfig()
	_, ing := newTestStore(b, cfg)
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
							TimeSeries: client.TimeSeries{
								Labels: []client.LabelPair{
									{Name: wire.Bytes("__name__"), Value: wire.Bytes(fmt.Sprintf("metric_%d", j))},
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
