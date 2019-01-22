package querier

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/wire"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteReadHandler(t *testing.T) {
	q := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
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
	handler := RemoteReadHandler(q)

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
				Timeseries: []client.TimeSeries{
					{
						Labels: []client.LabelPair{
							{
								Name:  wire.Bytes([]byte("foo")),
								Value: wire.Bytes([]byte("bar")),
							},
						},
						Samples: []client.Sample{
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

type mockQuerier struct {
	matrix model.Matrix
}

func (m mockQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	if sp == nil {
		panic(fmt.Errorf("select params must be set"))
	}
	return matrixToSeriesSet(m.matrix), nil, nil
}

func (m mockQuerier) LabelValues(name string) ([]string, error) {
	return nil, nil
}

func (m mockQuerier) LabelNames() ([]string, error) {
	return nil, nil
}

func (mockQuerier) Close() error {
	return nil
}
