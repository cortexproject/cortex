package querier

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"
	"github.com/weaveworks/cortex/pkg/util/wire"
)

func TestRemoteReadHandler(t *testing.T) {
	q := MergeQueryable{
		queriers: []Querier{
			mockQuerier{
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
			},
		},
	}

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
	q.RemoteReadHandler(recorder, request)

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

func TestMergeQuerierSortsMetricLabels(t *testing.T) {
	mq := mergeQuerier{
		ctx: context.Background(),
		queriers: []Querier{
			mockQuerier{
				matrix: model.Matrix{
					{
						Metric: model.Metric{
							model.MetricNameLabel: "testmetric",
							"e": "f",
							"a": "b",
							"g": "h",
							"c": "d",
						},
						Values: []model.SamplePair{{Timestamp: 0, Value: 0}},
					},
				},
			},
		},
		mint:         0,
		maxt:         0,
		metadataOnly: false,
	}
	m, err := labels.NewMatcher(labels.MatchEqual, model.MetricNameLabel, "testmetric")
	require.NoError(t, err)
	ss, err := mq.Select(nil, m)
	require.NoError(t, err)
	require.NoError(t, ss.Err())
	ss.Next()
	require.NoError(t, ss.Err())
	l := ss.At().Labels()
	require.Equal(t, labels.Labels{
		{Name: string(model.MetricNameLabel), Value: "testmetric"},
		{Name: "a", Value: "b"},
		{Name: "c", Value: "d"},
		{Name: "e", Value: "f"},
		{Name: "g", Value: "h"},
	}, l)
}

type mockQuerier struct {
	matrix model.Matrix
}

func (m mockQuerier) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return m.matrix, nil
}

func (mockQuerier) LabelValuesForLabelName(context.Context, model.LabelName) (model.LabelValues, error) {
	return nil, nil
}

func (mockQuerier) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matcherSets ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}
