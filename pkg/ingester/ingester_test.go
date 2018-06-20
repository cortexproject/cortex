package ingester

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util"
)

type testStore struct {
	mtx sync.Mutex
	// Chunks keyed by userID.
	chunks map[string][]chunk.Chunk
}

func newTestStore(t *testing.T, cfg Config) (*testStore, *Ingester) {
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

// chunksToMatrix converts a slice of chunks into a model.Matrix.
func chunksToMatrix(chunks []chunk.Chunk) (model.Matrix, error) {
	// Group chunks by series, sort and dedupe samples.
	sampleStreams := map[model.Fingerprint]*model.SampleStream{}
	for _, c := range chunks {
		fp := c.Metric.Fingerprint()
		ss, ok := sampleStreams[fp]
		if !ok {
			ss = &model.SampleStream{
				Metric: c.Metric,
			}
			sampleStreams[fp] = ss
		}

		samples, err := c.Samples(c.From, c.Through)
		if err != nil {
			return nil, err
		}

		ss.Values = util.MergeSampleSets(ss.Values, samples)
	}

	matrix := make(model.Matrix, 0, len(sampleStreams))
	for _, ss := range sampleStreams {
		matrix = append(matrix, ss)
	}

	return matrix, nil
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
		_, err := ing.Push(ctx, client.ToWriteRequest(matrixToSamples(testData[userID])))
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
	}

	// Read samples back via chunk store.
	ing.Shutdown()
	for _, userID := range userIDs {
		res, err := chunksToMatrix(store.chunks[userID])
		require.NoError(t, err)
		sort.Sort(res)
		assert.Equal(t, testData[userID], res)
	}
}

func TestIngesterAppendOutOfOrderAndDuplicate(t *testing.T) {
	_, ing := newTestStore(t, defaultIngesterTestConfig())
	defer ing.Shutdown()

	m := model.Metric{
		model.MetricNameLabel: "testmetric",
	}
	ctx := user.InjectOrgID(context.Background(), userID)
	err := ing.append(ctx, &model.Sample{Metric: m, Timestamp: 1, Value: 0})
	require.NoError(t, err)

	// Two times exactly the same sample (noop).
	err = ing.append(ctx, &model.Sample{Metric: m, Timestamp: 1, Value: 0})
	require.NoError(t, err)

	// Earlier sample than previous one.
	err = ing.append(ctx, &model.Sample{Metric: m, Timestamp: 0, Value: 0})
	require.Contains(t, err.Error(), "sample timestamp out of order")

	// Same timestamp as previous sample, but different value.
	err = ing.append(ctx, &model.Sample{Metric: m, Timestamp: 1, Value: 1})
	require.Contains(t, err.Error(), "sample with repeated timestamp but different value")
}

func TestIngesterUserSeriesLimitExceeded(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.userStatesConfig = UserStatesConfig{
		MaxSeriesPerUser: 1,
	}

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
	_, err := ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample1}))
	require.NoError(t, err)

	// Append to two series, expect series-exceeded error.
	_, err = ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample2, sample3}))
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
	cfg.userStatesConfig = UserStatesConfig{
		MaxSeriesPerMetric: 1,
	}

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
	_, err := ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample1}))
	require.NoError(t, err)

	// Append to two series, expect series-exceeded error.
	_, err = ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample2, sample3}))
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

func TestIngesterRejectOldSamples(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.RejectOldSamples = true
	cfg.RejectOldSamplesMaxAge = 24 * time.Hour

	_, ing := newTestStore(t, cfg)
	defer ing.Shutdown()

	now := model.Now()
	sample1 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: now,
		Value:     1,
	}
	sample2 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: now.Add(1 * time.Second),
		Value:     2,
	}
	sample3 := model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: now.Add(-25 * time.Hour), // before old sample max age
		Value:     2,
	}

	// Append recent sample, expect no error.
	userID := "1"
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample1}))
	require.NoError(t, err)

	// Append old sample, expect bad request error.
	_, err = ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample2, sample3}))
	if resp, ok := httpgrpc.HTTPResponseFromError(err); !ok || resp.Code != http.StatusBadRequest {
		t.Fatalf("expected error about old samples not accepted, got %v", err)
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

	// Expect recent sample including partial but no old sample.
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

func TestIngesterMaxLabelNamesPerSeries(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.validationConfig.MaxLabelNamesPerSeries = 2
	_, ing := newTestStore(t, cfg)
	defer ing.Shutdown()

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
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar", "foo2": "bar2"},
		Timestamp: 2,
		Value:     2,
	}

	// Append recent sample, expect no error.
	userID := "1"
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample1}))
	require.NoError(t, err)

	// Append old sample, expect bad request error.
	_, err = ing.Push(ctx, client.ToWriteRequest([]model.Sample{sample2, sample3}))
	if resp, ok := httpgrpc.HTTPResponseFromError(err); !ok || resp.Code != http.StatusBadRequest {
		t.Fatalf("expected error about old samples not accepted, got %v", err)
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

	// Expect recent sample including partial but no old sample.
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
