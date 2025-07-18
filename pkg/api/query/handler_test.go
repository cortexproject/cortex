package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/querier/stats"
)

type mockSampleAndChunkQueryable struct {
	queryableFn      func(mint, maxt int64) (storage.Querier, error)
	chunkQueryableFn func(mint, maxt int64) (storage.ChunkQuerier, error)
}

func (m mockSampleAndChunkQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return m.queryableFn(mint, maxt)
}

func (m mockSampleAndChunkQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return m.chunkQueryableFn(mint, maxt)
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

func Test_QueryAPI(t *testing.T) {
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples: 100,
		Timeout:    time.Second * 2,
	})
	mockQueryable := &mockSampleAndChunkQueryable{
		queryableFn: func(_, _ int64) (storage.Querier, error) {
			return mockQuerier{
				matrix: model.Matrix{
					{
						Metric: model.Metric{"__name__": "test", "foo": "bar"},
						Values: []model.SamplePair{
							{Timestamp: 1536673665000, Value: 0},
							{Timestamp: 1536673670000, Value: 1},
						},
					},
				},
			}, nil
		},
	}

	tests := []struct {
		name         string
		path         string
		expectedCode int
		expectedBody string
	}{
		{
			name:         "[Range Query] empty start",
			path:         "/api/v1/query_range?end=1536673680&query=test&step=5",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"start\\\": cannot parse \\\"\\\" to a valid timestamp\"}",
		},
		{
			name:         "[Range Query] empty end",
			path:         "/api/v1/query_range?query=test&start=1536673665&step=5",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"end\\\": cannot parse \\\"\\\" to a valid timestamp\"}",
		},
		{
			name:         "[Range Query] start is greater than end",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673681&step=5",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"end\\\": end timestamp must not be before start time\"}",
		},
		{
			name:         "[Range Query] negative step",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=-1",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"step\\\": zero or negative query resolution step widths are not accepted. Try a positive integer\"}",
		},
		{
			name:         "[Range Query] returned points are over 11000",
			path:         "/api/v1/query_range?end=1536700000&query=test&start=1536673665&step=1",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)\"}",
		},
		{
			name:         "[Range Query] empty query",
			path:         "/api/v1/query_range?end=1536673680&start=1536673665&step=5",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"query\\\": unknown position: parse error: no expression found in input\"}",
		},
		{
			name:         "[Range Query] invalid lookback delta",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5&lookback_delta=dummy",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"error parsing lookback delta duration: cannot parse \\\"dummy\\\" to a valid duration\"}",
		},
		{
			name:         "[Range Query] invalid timeout delta",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5&timeout=dummy",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"timeout\\\": cannot parse \\\"dummy\\\" to a valid duration\"}",
		},
		{
			name:         "[Range Query] normal case",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5",
			expectedCode: http.StatusOK,
			expectedBody: "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{\"__name__\":\"test\",\"foo\":\"bar\"},\"values\":[[1536673665,\"0\"],[1536673670,\"1\"],[1536673675,\"1\"],[1536673680,\"1\"]]}]}}",
		},
		{
			name:         "[Instant Query] empty query",
			path:         "/api/v1/query",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"query\\\": unknown position: parse error: no expression found in input\"}",
		},
		{
			name:         "[Instant Query] invalid lookback delta",
			path:         "/api/v1/query?lookback_delta=dummy",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"error parsing lookback delta duration: cannot parse \\\"dummy\\\" to a valid duration\"}",
		},
		{
			name:         "[Instant Query] invalid timeout",
			path:         "/api/v1/query?timeout=dummy",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"timeout\\\": cannot parse \\\"dummy\\\" to a valid duration\"}",
		},
		{
			name:         "[Instant Query] normal case",
			path:         "/api/v1/query?query=test&time=1536673670",
			expectedCode: http.StatusOK,
			expectedBody: "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"metric\":{\"__name__\":\"test\",\"foo\":\"bar\"},\"value\":[1536673670,\"1\"]}]}}",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := NewQueryAPI(engine, mockQueryable, querier.StatsRenderer, log.NewNopLogger(), []v1.Codec{v1.JSONCodec{}}, regexp.MustCompile(".*"))

			router := mux.NewRouter()
			router.Path("/api/v1/query").Methods("GET").Handler(c.Wrap(c.InstantQueryHandler))
			router.Path("/api/v1/query_range").Methods("GET").Handler(c.Wrap(c.RangeQueryHandler))

			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			ctx := context.Background()
			_, ctx = stats.ContextWithEmptyStats(ctx)
			req = req.WithContext(user.InjectOrgID(ctx, "user1"))

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			require.Equal(t, test.expectedCode, rec.Code)
			body, err := io.ReadAll(rec.Body)
			require.NoError(t, err)
			require.Equal(t, test.expectedBody, string(body))
		})
	}
}

type mockCodec struct{}

func (m *mockCodec) ContentType() v1.MIMEType {
	return v1.MIMEType{Type: "application", SubType: "mock"}
}

func (m *mockCodec) CanEncode(_ *v1.Response) bool {
	return false
}

func (m *mockCodec) Encode(_ *v1.Response) ([]byte, error) {
	return nil, errors.New("encode err")
}

func Test_InvalidCodec(t *testing.T) {
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples: 100,
		Timeout:    time.Second * 2,
	})
	mockQueryable := &mockSampleAndChunkQueryable{
		queryableFn: func(_, _ int64) (storage.Querier, error) {
			return mockQuerier{
				matrix: model.Matrix{
					{
						Metric: model.Metric{"__name__": "test", "foo": "bar"},
						Values: []model.SamplePair{
							{Timestamp: 1536673665000, Value: 0},
							{Timestamp: 1536673670000, Value: 1},
						},
					},
				},
			}, nil
		},
	}

	queryAPI := NewQueryAPI(engine, mockQueryable, querier.StatsRenderer, log.NewNopLogger(), []v1.Codec{&mockCodec{}}, regexp.MustCompile(".*"))
	router := mux.NewRouter()
	router.Path("/api/v1/query").Methods("GET").Handler(queryAPI.Wrap(queryAPI.InstantQueryHandler))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=test", nil)
	ctx := context.Background()
	_, ctx = stats.ContextWithEmptyStats(ctx)
	req = req.WithContext(user.InjectOrgID(ctx, "user1"))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotAcceptable, rec.Code)
}

func Test_QueryAPI_StatsRenderer(t *testing.T) {
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples: 100,
		Timeout:    time.Second * 2,
	})
	mockQueryable := &mockSampleAndChunkQueryable{
		queryableFn: func(_, _ int64) (storage.Querier, error) {
			return mockQuerier{
				matrix: model.Matrix{
					{
						Metric: model.Metric{"__name__": "test", "foo": "bar"},
						Values: []model.SamplePair{
							{Timestamp: 1536673665000, Value: 0},
							{Timestamp: 1536673670000, Value: 1},
							{Timestamp: 1536673675000, Value: 2},
							{Timestamp: 1536673680000, Value: 3},
						},
					},
				},
			}, nil
		},
	}

	queryAPI := NewQueryAPI(engine, mockQueryable, querier.StatsRenderer, log.NewNopLogger(), []v1.Codec{v1.JSONCodec{}}, regexp.MustCompile(".*"))

	router := mux.NewRouter()
	router.Path("/api/v1/query_range").Methods("GET").Handler(queryAPI.Wrap(queryAPI.RangeQueryHandler))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5", nil)
	ctx := context.Background()
	_, ctx = stats.ContextWithEmptyStats(ctx)
	req = req.WithContext(user.InjectOrgID(ctx, "user1"))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	queryStats := stats.FromContext(ctx)
	require.NotNil(t, queryStats)
	require.Equal(t, uint64(4), queryStats.LoadPeakSamples())
	require.Equal(t, uint64(4), queryStats.LoadScannedSamples())
}
