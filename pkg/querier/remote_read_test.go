package querier

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestRemoteReadHandler(t *testing.T) {
	t.Parallel()
	q := storage.QueryableFunc(func(mint, maxt int64) (storage.Querier, error) {
		return mockQuerier{
			matrix: model.Matrix{
				{
					Metric: model.Metric{"foo": "bar"},
					Values: []model.SamplePair{
						{Timestamp: 0, Value: 0},
						{Timestamp: 1, Value: 1},
						{Timestamp: 2, Value: 2},
						{Timestamp: 3, Value: 3},
					},
				},
			},
		}, nil
	})
	handler := RemoteReadHandler(q, log.NewNopLogger())

	requestBody, err := proto.Marshal(&client.ReadRequest{
		Queries: []*client.QueryRequest{
			{StartTimestampMs: 0, EndTimestampMs: 10},
		},
	})
	require.NoError(t, err)
	requestBody = snappy.Encode(nil, requestBody)
	request, err := http.NewRequest("GET", "/query", bytes.NewReader(requestBody))
	require.NoError(t, err)
	request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, 200, recorder.Result().StatusCode)
	require.Equal(t, []string([]string{"application/x-protobuf"}), recorder.Result().Header["Content-Type"])
	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)
	responseBody, err = snappy.Decode(nil, responseBody)
	require.NoError(t, err)
	var response client.ReadResponse
	err = proto.Unmarshal(responseBody, &response)
	require.NoError(t, err)

	expected := client.ReadResponse{
		Results: []*client.QueryResponse{
			{
				Timeseries: []cortexpb.TimeSeries{
					{
						Labels: []cortexpb.LabelAdapter{
							{Name: "foo", Value: "bar"},
						},
						Samples: []cortexpb.Sample{
							{Value: 0, TimestampMs: 0},
							{Value: 1, TimestampMs: 1},
							{Value: 2, TimestampMs: 2},
							{Value: 3, TimestampMs: 3},
						},
					},
				},
			},
		},
	}
	require.Equal(t, expected, response)
}

func TestRemoteReadHandler_Closes_Querier(t *testing.T) {
	t.Parallel()
	var closed atomic.Int64
	q := storage.QueryableFunc(func(mint, maxt int64) (storage.Querier, error) {
		return &closeCountingQuerier{closed: &closed}, nil
	})
	handler := RemoteReadHandler(q, log.NewNopLogger())

	const numQueries = 3
	queries := make([]*client.QueryRequest, numQueries)
	for i := range queries {
		queries[i] = &client.QueryRequest{StartTimestampMs: 0, EndTimestampMs: 10}
	}
	requestBody, err := proto.Marshal(&client.ReadRequest{Queries: queries})
	require.NoError(t, err)
	requestBody = snappy.Encode(nil, requestBody)
	request, err := http.NewRequest("GET", "/query", bytes.NewReader(requestBody))
	require.NoError(t, err)
	request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, 200, recorder.Result().StatusCode)
	// Each per-query querier is closed by a deferred call in its own goroutine,
	// which may run after ServeHTTP returns, so poll until every querier is closed.
	test.Poll(t, time.Second, int64(numQueries), func() any {
		return closed.Load()
	})
}

type mockQuerier struct {
	matrix model.Matrix
}

func (m mockQuerier) Select(ctx context.Context, sortSeries bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if sp == nil {
		panic(fmt.Errorf("select params must be set"))
	}
	return series.MatrixToSeriesSet(sortSeries, m.matrix)
}

func (m mockQuerier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m mockQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (mockQuerier) Close() error {
	return nil
}

// closeCountingQuerier records how many times Close is called so a test can
// assert the remote read handler releases every querier it creates.
type closeCountingQuerier struct {
	mockQuerier
	closed *atomic.Int64
}

func (q *closeCountingQuerier) Close() error {
	q.closed.Add(1)
	return nil
}
