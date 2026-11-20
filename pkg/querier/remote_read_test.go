package querier

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/series"
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
	handler := RemoteReadHandler(q, nil, log.NewNopLogger())

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

type mockRemoteReadLimits struct {
	haEnabled map[string]bool
	label     string
}

func (m mockRemoteReadLimits) AcceptHASamples(userID string) bool { return m.haEnabled[userID] }
func (m mockRemoteReadLimits) HAReplicaLabel(string) string       { return m.label }

type matcherCapturingQuerier struct {
	mockQuerier
	got *[]*labels.Matcher
}

func (m matcherCapturingQuerier) Select(ctx context.Context, sortSeries bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	*m.got = matchers
	return m.mockQuerier.Select(ctx, sortSeries, sp, matchers...)
}

func TestRemoteReadHandler_HAReplicaLabel(t *testing.T) {
	t.Parallel()

	queryMatchers := []*client.LabelMatcher{
		{Type: client.EQUAL, Name: labels.MetricName, Value: "up"},
		{Type: client.EQUAL, Name: "cluster", Value: "prom-team1"},
		{Type: client.EQUAL, Name: "__replica__", Value: "replica1"},
	}

	for name, tc := range map[string]struct {
		limits   RemoteReadLimits
		userID   string
		matchers []*client.LabelMatcher
		expected []string
	}{
		"HA enabled removes the replica matcher": {
			limits:   mockRemoteReadLimits{haEnabled: map[string]bool{"user-1": true}, label: "__replica__"},
			userID:   "user-1",
			matchers: queryMatchers,
			expected: []string{`__name__="up"`, `cluster="prom-team1"`},
		},
		"HA disabled keeps the replica matcher": {
			limits:   mockRemoteReadLimits{haEnabled: map[string]bool{}, label: "__replica__"},
			userID:   "user-1",
			matchers: queryMatchers,
			expected: []string{`__name__="up"`, `cluster="prom-team1"`, `__replica__="replica1"`},
		},
		"a different replica label name keeps the matcher": {
			limits:   mockRemoteReadLimits{haEnabled: map[string]bool{"user-1": true}, label: "other"},
			userID:   "user-1",
			matchers: queryMatchers,
			expected: []string{`__name__="up"`, `cluster="prom-team1"`, `__replica__="replica1"`},
		},
		"nil limits keeps the replica matcher": {
			limits:   nil,
			userID:   "user-1",
			matchers: queryMatchers,
			expected: []string{`__name__="up"`, `cluster="prom-team1"`, `__replica__="replica1"`},
		},
		"the replica matcher is kept when it is the only one": {
			limits:   mockRemoteReadLimits{haEnabled: map[string]bool{"user-1": true}, label: "__replica__"},
			userID:   "user-1",
			matchers: queryMatchers[2:],
			expected: []string{`__replica__="replica1"`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var got []*labels.Matcher
			q := storage.QueryableFunc(func(mint, maxt int64) (storage.Querier, error) {
				return matcherCapturingQuerier{mockQuerier: mockQuerier{}, got: &got}, nil
			})
			handler := RemoteReadHandler(q, tc.limits, log.NewNopLogger())

			requestBody, err := proto.Marshal(&client.ReadRequest{
				Queries: []*client.QueryRequest{
					{StartTimestampMs: 0, EndTimestampMs: 10, Matchers: tc.matchers},
				},
			})
			require.NoError(t, err)
			request, err := http.NewRequest("POST", "/query", bytes.NewReader(snappy.Encode(nil, requestBody)))
			require.NoError(t, err)
			request = request.WithContext(user.InjectOrgID(request.Context(), tc.userID))

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			require.Equal(t, 200, recorder.Result().StatusCode)

			actual := make([]string, 0, len(got))
			for _, m := range got {
				actual = append(actual, m.String())
			}
			require.Equal(t, tc.expected, actual)
		})
	}
}
