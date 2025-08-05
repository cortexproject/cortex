package queryapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/weaveworks/common/user"

	engine2 "github.com/cortexproject/cortex/pkg/engine"
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

func Test_CustomAPI(t *testing.T) {
	engine := engine2.New(
		promql.EngineOpts{
			MaxSamples: 100,
			Timeout:    time.Second * 2,
		},
		engine2.ThanosEngineConfig{Enabled: false},
		prometheus.NewRegistry())

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
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"start\\\"; cannot parse \\\"\\\" to a valid timestamp\"}",
		},
		{
			name:         "[Range Query] empty end",
			path:         "/api/v1/query_range?query=test&start=1536673665&step=5",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"end\\\"; cannot parse \\\"\\\" to a valid timestamp\"}",
		},
		{
			name:         "[Range Query] start is greater than end",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673681&step=5",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"end\\\"; end timestamp must not be before start time\"}",
		},
		{
			name:         "[Range Query] negative step",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=-1",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"step\\\"; zero or negative query resolution step widths are not accepted. Try a positive integer\"}",
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
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"query\\\"; unknown position: parse error: no expression found in input\"}",
		},
		{
			name:         "[Range Query] invalid lookback delta",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5&lookback_delta=dummy",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"error parsing lookback delta duration: rpc error: code = Code(400) desc = cannot parse \\\"dummy\\\" to a valid duration\"}",
		},
		{
			name:         "[Range Query] invalid timeout delta",
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5&timeout=dummy",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"timeout\\\"; cannot parse \\\"dummy\\\" to a valid duration\"}",
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
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"query\\\"; unknown position: parse error: no expression found in input\"}",
		},
		{
			name:         "[Instant Query] invalid lookback delta",
			path:         "/api/v1/query?lookback_delta=dummy",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"error parsing lookback delta duration: rpc error: code = Code(400) desc = cannot parse \\\"dummy\\\" to a valid duration\"}",
		},
		{
			name:         "[Instant Query] invalid timeout",
			path:         "/api/v1/query?timeout=dummy",
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"timeout\\\"; cannot parse \\\"dummy\\\" to a valid duration\"}",
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
			router.Path("/api/v1/query").Methods("POST").Handler(c.Wrap(c.InstantQueryHandler))
			router.Path("/api/v1/query_range").Methods("POST").Handler(c.Wrap(c.RangeQueryHandler))

			req := httptest.NewRequest(http.MethodPost, test.path, nil)
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
	engine := engine2.New(
		promql.EngineOpts{
			MaxSamples: 100,
			Timeout:    time.Second * 2,
		},
		engine2.ThanosEngineConfig{Enabled: false},
		prometheus.NewRegistry())

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
	router.Path("/api/v1/query").Methods("POST").Handler(queryAPI.Wrap(queryAPI.InstantQueryHandler))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/query?query=test", nil)
	ctx := context.Background()
	_, ctx = stats.ContextWithEmptyStats(ctx)
	req = req.WithContext(user.InjectOrgID(ctx, "user1"))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotAcceptable, rec.Code)
}

func Test_CustomAPI_StatsRenderer(t *testing.T) {
	engine := engine2.New(
		promql.EngineOpts{
			MaxSamples: 100,
			Timeout:    time.Second * 2,
		},
		engine2.ThanosEngineConfig{Enabled: false},
		prometheus.NewRegistry())

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
	router.Path("/api/v1/query_range").Methods("POST").Handler(queryAPI.Wrap(queryAPI.RangeQueryHandler))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5", nil)
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

func Test_Logicalplan_Requests(t *testing.T) {
	engine := engine2.New(
		promql.EngineOpts{
			MaxSamples: 100,
			Timeout:    time.Second * 2,
		},
		engine2.ThanosEngineConfig{Enabled: true},
		prometheus.NewRegistry(),
	)

	mockMatrix := model.Matrix{
		{
			Metric: model.Metric{"__name__": "test", "foo": "bar"},
			Values: []model.SamplePair{
				{Timestamp: 1536673665000, Value: 0},
				{Timestamp: 1536673670000, Value: 1},
			},
		},
	}

	mockQueryable := &mockSampleAndChunkQueryable{
		queryableFn: func(_, _ int64) (storage.Querier, error) {
			return mockQuerier{matrix: mockMatrix}, nil
		},
	}

	tests := []struct {
		name         string
		path         string
		start        int64
		end          int64
		stepDuration int64
		requestBody  func(t *testing.T) []byte
		expectedCode int
		expectedBody string
	}{
		{
			name:         "[Range Query] with valid logical plan and empty query string",
			path:         "/api/v1/query_range?end=1536673680&query=&start=1536673665&step=5",
			start:        1536673665,
			end:          1536673680,
			stepDuration: 5,
			requestBody: func(t *testing.T) []byte {
				return createTestLogicalPlan(t, 1536673665, 1536673680, 5)
			},
			expectedCode: http.StatusOK,
			expectedBody: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"test","foo":"bar"},"values":[[1536673665,"0"],[1536673670,"1"],[1536673675,"1"],[1536673680,"1"]]}]}}`,
		},
		{
			name:         "[Range Query] with corrupted logical plan", // will throw an error from unmarhsal step
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5",
			start:        1536673665,
			end:          1536673680,
			stepDuration: 5,
			requestBody: func(t *testing.T) []byte {
				return append(createTestLogicalPlan(t, 1536673665, 1536673680, 5), []byte("random data")...)
			},
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"status":"error","errorType":"server_error","error":"invalid logical plan: invalid character 'r' after top-level value"}`,
		},
		{
			name:         "[Range Query] with empty body and non-empty query string", // fall back to promql query execution
			path:         "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5",
			start:        1536673665,
			end:          1536673680,
			stepDuration: 5,
			requestBody: func(t *testing.T) []byte {
				return []byte{}
			},
			expectedCode: http.StatusOK,
			expectedBody: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"test","foo":"bar"},"values":[[1536673665,"0"],[1536673670,"1"],[1536673675,"1"],[1536673680,"1"]]}]}}`,
		},
		{
			name:         "[Range Query] with empty body and empty query string", // fall back to promql query execution, but will have error because of empty query string
			path:         "/api/v1/query_range?end=1536673680&query=&start=1536673665&step=5",
			start:        1536673665,
			end:          1536673680,
			stepDuration: 5,
			requestBody: func(t *testing.T) []byte {
				return []byte{}
			},
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"query\\\"; unknown position: parse error: no expression found in input\"}",
		},
		{
			name:         "[Instant Query] with valid logical plan and empty query string",
			path:         "/api/v1/query?query=test&time=1536673670",
			start:        1536673670,
			end:          1536673670,
			stepDuration: 0,
			requestBody: func(t *testing.T) []byte {
				return createTestLogicalPlan(t, 1536673670, 1536673670, 0)
			},
			expectedCode: http.StatusOK,
			expectedBody: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"test","foo":"bar"},"value":[1536673670,"1"]}]}}`,
		},
		{
			name:         "[Instant Query] with corrupted logical plan",
			path:         "/api/v1/query?query=test&time=1536673670",
			start:        1536673670,
			end:          1536673670,
			stepDuration: 0,
			requestBody: func(t *testing.T) []byte {
				return append(createTestLogicalPlan(t, 1536673670, 1536673670, 0), []byte("random data")...)
			},
			expectedCode: http.StatusInternalServerError,
			expectedBody: `{"status":"error","errorType":"server_error","error":"invalid logical plan: invalid character 'r' after top-level value"}`,
		},
		{
			name:         "[Instant Query] with empty body and non-empty query string",
			path:         "/api/v1/query?query=test&time=1536673670",
			start:        1536673670,
			end:          1536673670,
			stepDuration: 0,
			requestBody: func(t *testing.T) []byte {
				return []byte{}
			},
			expectedCode: http.StatusOK,
			expectedBody: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"test","foo":"bar"},"value":[1536673670,"1"]}]}}`,
		},
		{
			name:         "[Instant Query] with empty body and empty query string",
			path:         "/api/v1/query?query=&time=1536673670",
			start:        1536673670,
			end:          1536673670,
			stepDuration: 0,
			requestBody: func(t *testing.T) []byte {
				return []byte{}
			},
			expectedCode: http.StatusBadRequest,
			expectedBody: "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"query\\\"; unknown position: parse error: no expression found in input\"}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewQueryAPI(engine, mockQueryable, querier.StatsRenderer, log.NewNopLogger(), []v1.Codec{v1.JSONCodec{}}, regexp.MustCompile(".*"))
			router := mux.NewRouter()
			router.Path("/api/v1/query").Methods("POST").Handler(c.Wrap(c.InstantQueryHandler))
			router.Path("/api/v1/query_range").Methods("POST").Handler(c.Wrap(c.RangeQueryHandler))

			req := createTestRequest(tt.path, tt.requestBody(t))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			require.Equal(t, tt.expectedCode, rec.Code)
			body, err := io.ReadAll(rec.Body)
			require.NoError(t, err)
			require.Equal(t, tt.expectedBody, string(body))
		})
	}
}

func createTestRequest(path string, planBytes []byte) *http.Request {
	form := url.Values{}
	form.Set("plan", string(planBytes))
	req := httptest.NewRequest(http.MethodPost, path, io.NopCloser(strings.NewReader(form.Encode())))

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	ctx := context.Background()
	_, ctx = stats.ContextWithEmptyStats(ctx)
	return req.WithContext(user.InjectOrgID(ctx, "user1"))
}

func createTestLogicalPlan(t *testing.T, start, end int64, stepDuration int64) []byte {
	startTime, endTime := convertMsToTime(start), convertMsToTime(end)
	step := convertMsToDuration(stepDuration)

	qOpts := query.Options{
		Start:              startTime,
		End:                startTime,
		Step:               0,
		StepsBatch:         10,
		LookbackDelta:      0,
		EnablePerStepStats: false,
	}

	if step != 0 {
		qOpts.End = endTime
		qOpts.Step = step
	}

	// using a different metric name here so that we can check with debugger which query (from query string vs http request body)
	// is being executed by the queriers
	expr, err := parser.NewParser("up", parser.WithFunctions(parser.Functions)).ParseExpr()
	require.NoError(t, err)

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	logicalPlan, err := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	require.NoError(t, err)
	byteval, err := logicalplan.Marshal(logicalPlan.Root())
	require.NoError(t, err)

	return byteval
}
