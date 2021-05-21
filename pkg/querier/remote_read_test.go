package querier

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/series"
)

func TestRemoteReadHandler(t *testing.T) {
	q := mockSampleAndChunkQueryable{
		querierMatrix: model.Matrix{
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
	}
	handler := NewRemoteReadHandler(q, log.NewNopLogger(), 0)

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
	responseBody, err := ioutil.ReadAll(recorder.Result().Body)
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

type mockSampleAndChunkQueryable struct {
	querierMatrix model.Matrix
}

func (m mockSampleAndChunkQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return mockQuerier{matrix: m.querierMatrix}, nil
}

func (m mockSampleAndChunkQueryable) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return mockChunkQuerier{}, nil
}

type mockQuerier struct {
	matrix model.Matrix
}

type mockChunkQuerier struct {}

func (m mockQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if sp == nil {
		panic(fmt.Errorf("select params must be set"))
	}
	return series.MatrixToSeriesSet(m.matrix)
}

func (m mockQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m mockQuerier) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m mockQuerier) Close() error {
	return nil
}

func (m mockChunkQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	if sp == nil {
		panic(fmt.Errorf("select params must be set"))
	}
	return nil
}

func (m mockChunkQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m mockChunkQuerier) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m mockChunkQuerier) Close() error {
	return nil
}

