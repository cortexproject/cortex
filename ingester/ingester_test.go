package ingester

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/util"
)

type testStore struct {
	mtx sync.Mutex
	// Chunks keyed by userID.
	chunks map[string][]chunk.Chunk
}

func (s *testStore) Put(ctx context.Context, chunks []chunk.Chunk) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	userID, err := user.GetID(ctx)
	if err != nil {
		return err
	}
	s.chunks[userID] = append(s.chunks[userID], chunks...)
	return nil
}

func (s *testStore) Get(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]chunk.Chunk, error) {
	return nil, nil
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

func matrixToSamples(m model.Matrix) []*model.Sample {
	var samples []*model.Sample
	for _, ss := range m {
		for _, sp := range ss.Values {
			samples = append(samples, &model.Sample{
				Metric:    ss.Metric,
				Timestamp: sp.Timestamp,
				Value:     sp.Value,
			})
		}
	}
	return samples
}

func TestIngesterAppend(t *testing.T) {
	cfg := Config{
		FlushCheckPeriod: 99999 * time.Hour,
		MaxChunkIdle:     99999 * time.Hour,
	}
	store := &testStore{
		chunks: map[string][]chunk.Chunk{},
	}
	ing, err := New(cfg, store, nil)
	if err != nil {
		t.Fatal(err)
	}

	userIDs := []string{"1", "2", "3"}

	// Create test samples.
	testData := map[string]model.Matrix{}
	for i, userID := range userIDs {
		testData[userID] = buildTestMatrix(10, 1000, i)
	}

	// Append samples.
	for _, userID := range userIDs {
		ctx := user.WithID(context.Background(), userID)
		_, err = ing.Push(ctx, util.ToWriteRequest(matrixToSamples(testData[userID])))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Read samples back via ingester queries.
	for _, userID := range userIDs {
		ctx := user.WithID(context.Background(), userID)
		matcher, err := metric.NewLabelMatcher(metric.RegexMatch, model.JobLabel, ".+")
		if err != nil {
			t.Fatal(err)
		}

		req, err := util.ToQueryRequest(model.Earliest, model.Latest, []*metric.LabelMatcher{matcher})
		if err != nil {
			t.Fatal(err)
		}

		resp, err := ing.Query(ctx, req)
		if err != nil {
			t.Fatal(err)
		}

		res := util.FromQueryResponse(resp)
		sort.Sort(res)
		if !reflect.DeepEqual(res, testData[userID]) {
			t.Fatalf("unexpected query result\n\nwant:\n\n%v\n\ngot:\n\n%v\n\n", testData[userID], res)
		}
	}

	// Read samples back via chunk store.
	ing.Stop()
	for _, userID := range userIDs {
		res, err := chunk.ChunksToMatrix(store.chunks[userID])
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(res)

		if !reflect.DeepEqual(res, testData[userID]) {
			t.Fatalf("unexpected chunk store result\n\nwant:\n\n%v\n\ngot:\n\n%v\n\n", testData[userID], res)
		}
	}
}

func TestIngesterSeriesLimitExceeded(t *testing.T) {
	cfg := Config{
		FlushCheckPeriod: 99999 * time.Hour,
		MaxChunkIdle:     99999 * time.Hour,
		UserStatesConfig: UserStatesConfig{
			MaxSeriesPerUser: 1,
		},
	}
	store := &testStore{
		chunks: map[string][]chunk.Chunk{},
	}
	ing, err := New(cfg, store, nil)
	if err != nil {
		t.Fatal(err)
	}

	userID := "1"
	sample1 := &model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: 0,
		Value:     1,
	}
	sample2 := &model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
		Timestamp: 1,
		Value:     2,
	}
	sample3 := &model.Sample{
		Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "biz"},
		Timestamp: 1,
		Value:     3,
	}

	// Append only one series first, expect no error.
	ctx := user.WithID(context.Background(), userID)
	_, err = ing.Push(ctx, util.ToWriteRequest([]*model.Sample{sample1}))
	if err != nil {
		t.Fatal(err)
	}

	// Append to two series, expect series-exceeded error.
	_, err = ing.Push(ctx, util.ToWriteRequest([]*model.Sample{sample2, sample3}))
	if grpc.ErrorDesc(err) != util.ErrUserSeriesLimitExceeded.Error() {
		t.Fatalf("expected series-exceeded error, got %v", err)
	}

	// Read samples back via ingester queries.
	matcher, err := metric.NewLabelMatcher(metric.Equal, model.MetricNameLabel, "testmetric")
	if err != nil {
		t.Fatal(err)
	}

	req, err := util.ToQueryRequest(model.Earliest, model.Latest, []*metric.LabelMatcher{matcher})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := ing.Query(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	res := util.FromQueryResponse(resp)
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

	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("unexpected query result\n\nwant:\n\n%v\n\ngot:\n\n%v\n\n", expected, res)
	}
}
