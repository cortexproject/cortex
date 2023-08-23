package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/instantquery"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	thanos_api "github.com/thanos-io/thanos/pkg/api"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

var (
	testResponse = &queryrange.PrometheusResponse{
		Status: "success",
		Data: queryrange.PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []tripperware.SampleStream{
				{
					Labels: []cortexpb.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Samples: []cortexpb.Sample{
						{Value: 137, TimestampMs: 1536673680000},
						{Value: 137, TimestampMs: 1536673780000},
					},
				},
			},
		},
	}
)

func TestEndpoints(t *testing.T) {
	suite, err := promql.NewTest(t, ``)

	require.NoError(t, err)
	defer suite.Close()

	require.NoError(t, suite.Run())

	now := time.Now()
	t.Run("local", func(t *testing.T) {
		api := &API{
			Queryable:   suite.Storage(),
			QueryEngine: suite.QueryEngine(),
			Now:         func() time.Time { return now },
		}
		testEndpoints(t, api)
	})

	// Run all the API tests against a API that is wired to forward queries via
	// the remote read client to a test server, which in turn sends them to the
	// data from the test suite.
	t.Run("remote", func(t *testing.T) {
		server := setupRemote(suite.Storage())
		defer server.Close()

		u, err := url.Parse(server.URL)
		require.NoError(t, err)

		al := promlog.AllowedLevel{}
		require.NoError(t, al.Set("debug"))

		af := promlog.AllowedFormat{}
		require.NoError(t, af.Set("logfmt"))

		promlogConfig := promlog.Config{
			Level:  &al,
			Format: &af,
		}

		dbDir := t.TempDir()

		remote := remote.NewStorage(promlog.New(&promlogConfig), prometheus.DefaultRegisterer, func() (int64, error) {
			return 0, nil
		}, dbDir, 1*time.Second, nil)

		err = remote.ApplyConfig(&config.Config{
			RemoteReadConfigs: []*config.RemoteReadConfig{
				{
					URL:           &config_util.URL{URL: u},
					RemoteTimeout: model.Duration(1 * time.Second),
					ReadRecent:    true,
				},
			},
		})
		require.NoError(t, err)

		api := &API{
			Queryable:   remote,
			QueryEngine: suite.QueryEngine(),
			Now:         func() time.Time { return now },
		}
		testEndpoints(t, api)
	})
}

type testStats struct {
	Custom string `json:"custom"`
}

func (testStats) Builtin() (_ stats.BuiltinStats) {
	return
}

func TestStats(t *testing.T) {
	suite, err := promql.NewTest(t, ``)
	require.NoError(t, err)
	defer suite.Close()
	require.NoError(t, suite.Run())

	api := &API{
		Queryable:   suite.Storage(),
		QueryEngine: suite.QueryEngine(),
		Now: func() time.Time {
			return time.Unix(123, 0)
		},
	}
	request := func(method, param string) (*http.Request, error) {
		u, err := url.Parse("http://example.com")
		require.NoError(t, err)
		q := u.Query()
		q.Add("stats", param)
		q.Add("query", "up")
		q.Add("start", "0")
		q.Add("end", "100")
		q.Add("step", "10")
		u.RawQuery = q.Encode()

		r, err := http.NewRequest(method, u.String(), nil)
		if method == http.MethodPost {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		return r, err
	}

	for _, tc := range []struct {
		name     string
		renderer v1.StatsRenderer
		param    string
		expected func(*testing.T, interface{})
	}{
		{
			name:  "stats is blank",
			param: "",
			expected: func(t *testing.T, i interface{}) {
				require.IsType(t, i, &queryData{})
				qd := i.(*queryData)
				require.Nil(t, qd.Stats)
			},
		},
		{
			name:  "stats is true",
			param: "true",
			expected: func(t *testing.T, i interface{}) {
				require.IsType(t, i, &queryData{})
				qd := i.(*queryData)
				require.NotNil(t, qd.Stats)
				qs := qd.Stats.Builtin()
				require.NotNil(t, qs.Timings)
				require.Greater(t, qs.Timings.EvalTotalTime, float64(0))
				require.NotNil(t, qs.Samples)
				require.NotNil(t, qs.Samples.TotalQueryableSamples)
				require.Nil(t, qs.Samples.TotalQueryableSamplesPerStep)
			},
		},
		{
			name:  "stats is all",
			param: "all",
			expected: func(t *testing.T, i interface{}) {
				require.IsType(t, i, &queryData{})
				qd := i.(*queryData)
				require.NotNil(t, qd.Stats)
				qs := qd.Stats.Builtin()
				require.NotNil(t, qs.Timings)
				require.Greater(t, qs.Timings.EvalTotalTime, float64(0))
				require.NotNil(t, qs.Samples)
				require.NotNil(t, qs.Samples.TotalQueryableSamples)
				require.NotNil(t, qs.Samples.TotalQueryableSamplesPerStep)
			},
		},
		{
			name: "custom handler with known value",
			renderer: func(ctx context.Context, s *stats.Statistics, p string) stats.QueryStats {
				if p == "known" {
					return testStats{"Custom Value"}
				}
				return nil
			},
			param: "known",
			expected: func(t *testing.T, i interface{}) {
				require.IsType(t, i, &queryData{})
				qd := i.(*queryData)
				require.NotNil(t, qd.Stats)
				j, err := json.Marshal(qd.Stats)
				require.NoError(t, err)
				require.JSONEq(t, string(j), `{"custom":"Custom Value"}`)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := api.StatsRenderer
			defer func() { api.StatsRenderer = before }()
			api.StatsRenderer = tc.renderer

			for _, method := range []string{http.MethodGet, http.MethodPost} {
				ctx := context.Background()
				req, err := request(method, tc.param)
				require.NoError(t, err)
				data, _, error, _ := api.Query(req.WithContext(ctx))
				assertAPIError(t, error, "")
				tc.expected(t, data)

				data, _, error, _ = api.QueryRange(req.WithContext(ctx))
				assertAPIError(t, error, "")
				tc.expected(t, data)
			}
		})
	}
}

func setupRemote(s storage.Storage) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeReadRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := prompb.ReadResponse{
			Results: make([]*prompb.QueryResult, len(req.Queries)),
		}
		for i, query := range req.Queries {
			matchers, err := remote.FromLabelMatchers(query.Matchers)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var hints *storage.SelectHints
			if query.Hints != nil {
				hints = &storage.SelectHints{
					Start: query.Hints.StartMs,
					End:   query.Hints.EndMs,
					Step:  query.Hints.StepMs,
					Func:  query.Hints.Func,
				}
			}

			querier, err := s.Querier(r.Context(), query.StartTimestampMs, query.EndTimestampMs)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer querier.Close()

			set := querier.Select(false, hints, matchers...)
			resp.Results[i], _, err = remote.ToQueryResult(set, 1e6)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		if err := remote.EncodeReadResponse(&resp, w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	return httptest.NewServer(handler)
}

func testEndpoints(t *testing.T, api *API) {
	start := time.Unix(0, 0)

	type test struct {
		endpoint apiFunc
		query    url.Values
		response interface{}
		errType  thanos_api.ErrorType
	}

	tests := []test{
		{
			endpoint: api.Query,
			query: url.Values{
				"query": []string{"2"},
				"time":  []string{"123.4"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					V: 2,
					T: timestamp.FromTime(start.Add(123*time.Second + 400*time.Millisecond)),
				},
			},
		},
		{
			endpoint: api.Query,
			query: url.Values{
				"query": []string{"0.333"},
				"time":  []string{"1970-01-01T00:02:03Z"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					V: 0.333,
					T: timestamp.FromTime(start.Add(123 * time.Second)),
				},
			},
		},
		{
			endpoint: api.Query,
			query: url.Values{
				"query": []string{"0.333"},
				"time":  []string{"1970-01-01T01:02:03+01:00"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					V: 0.333,
					T: timestamp.FromTime(start.Add(123 * time.Second)),
				},
			},
		},
		{
			endpoint: api.Query,
			query: url.Values{
				"query": []string{"0.333"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					V: 0.333,
					T: timestamp.FromTime(api.Now()),
				},
			},
		},
		{
			endpoint: api.QueryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"0"},
				"end":   []string{"2"},
				"step":  []string{"1"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{
							{F: 0, T: timestamp.FromTime(start)},
							{F: 1, T: timestamp.FromTime(start.Add(1 * time.Second))},
							{F: 2, T: timestamp.FromTime(start.Add(2 * time.Second))},
						},
						// No Metric returned - use zero value for comparison.
					},
				},
			},
		},
		// Missing query params in range queries.
		{
			endpoint: api.QueryRange,
			query: url.Values{
				"query": []string{"time()"},
				"end":   []string{"2"},
				"step":  []string{"1"},
			},
			errType: thanos_api.ErrorBadData,
		},
		{
			endpoint: api.QueryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"0"},
				"step":  []string{"1"},
			},
			errType: thanos_api.ErrorBadData,
		},
		{
			endpoint: api.QueryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"0"},
				"end":   []string{"2"},
			},
			errType: thanos_api.ErrorBadData,
		},
		// Bad query expression.
		{
			endpoint: api.Query,
			query: url.Values{
				"query": []string{"invalid][query"},
				"time":  []string{"1970-01-01T01:02:03+01:00"},
			},
			errType: thanos_api.ErrorBadData,
		},
		{
			endpoint: api.QueryRange,
			query: url.Values{
				"query": []string{"invalid][query"},
				"start": []string{"0"},
				"end":   []string{"100"},
				"step":  []string{"1"},
			},
			errType: thanos_api.ErrorBadData,
		},
		// Invalid step.
		{
			endpoint: api.QueryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"1"},
				"end":   []string{"2"},
				"step":  []string{"0"},
			},
			errType: thanos_api.ErrorBadData,
		},
		// Start after end.
		{
			endpoint: api.QueryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"2"},
				"end":   []string{"1"},
				"step":  []string{"1"},
			},
			errType: thanos_api.ErrorBadData,
		},
		// Start overflows int64 internally.
		{
			endpoint: api.QueryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"148966367200.372"},
				"end":   []string{"1489667272.372"},
				"step":  []string{"1"},
			},
			errType: thanos_api.ErrorBadData,
		},
	}

	request := func(m string, q url.Values) (*http.Request, error) {
		if m == http.MethodPost {
			r, err := http.NewRequest(m, "http://example.com", strings.NewReader(q.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			r.RemoteAddr = "127.0.0.1:20201"
			return r, err
		}
		r, err := http.NewRequest(m, fmt.Sprintf("http://example.com?%s", q.Encode()), nil)
		r.RemoteAddr = "127.0.0.1:20201"
		return r, err
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("run %d %s %q", i, describeAPIFunc(test.endpoint), test.query.Encode()), func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				t.Run(method, func(t *testing.T) {
					// Build a context with the correct request params.
					ctx := context.Background()

					req, err := request(method, test.query)
					if err != nil {
						t.Fatal(err)
					}

					data, _, error, _ := test.endpoint(req.WithContext(ctx))
					assertAPIError(t, error, test.errType)
					assertAPIResponse(t, data, test.response)
				})
			}
		})
	}
}

func describeAPIFunc(f apiFunc) string {
	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	return strings.Split(name[strings.LastIndex(name, ".")+1:], "-")[0]
}

func assertAPIError(t *testing.T, got *thanos_api.ApiError, exp thanos_api.ErrorType) {
	t.Helper()

	if got != nil {
		if exp == thanos_api.ErrorNone {
			t.Fatalf("Unexpected error: %s", got)
		}
		if exp != got.Typ {
			t.Fatalf("Expected error of type %q but got type %q (%q)", exp, got.Typ, got)
		}
		return
	}
	if exp != thanos_api.ErrorNone {
		t.Fatalf("Expected error of type %q but got none", exp)
	}
}

func assertAPIResponse(t *testing.T, got, exp interface{}) {
	t.Helper()

	require.Equal(t, exp, got)
}

func TestRespondSuccess(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api := API{}
		api.Respond(w, testResponse, nil)
	}))
	defer s.Close()

	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatalf("Error on test request: %s", err)
	}
	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("Error reading response body: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Return code %d expected in success response but got %d", 200, resp.StatusCode)
	}
	if h := resp.Header.Get("Content-Type"); h != "application/x-protobuf" {
		t.Fatalf("Expected Content-Type %q but got %q", "application/x-protobuf", h)
	}

	var res queryrange.PrometheusResponse
	if err = proto.Unmarshal(body, &res); err != nil {
		t.Fatalf("Error unmarshaling response body: %s", err)
	}

	require.Equal(t, testResponse, &res)
}

func TestRespondError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api := API{}
		api.RespondError(w, &thanos_api.ApiError{thanos_api.ErrorTimeout, errors.New("message")}, "test")
	}))
	defer s.Close()

	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatalf("Error on test request: %s", err)
	}
	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("Error reading response body: %s", err)
	}

	if want, have := http.StatusServiceUnavailable, resp.StatusCode; want != have {
		t.Fatalf("Return code %d expected in error response but got %d", want, have)
	}
	if h := resp.Header.Get("Content-Type"); h != "application/json" {
		t.Fatalf("Expected Content-Type %q but got %q", "application/json", h)
	}

	var res response
	if err = json.Unmarshal(body, &res); err != nil {
		t.Fatalf("Error unmarshaling JSON body: %s", err)
	}

	exp := &response{
		Status:    statusError,
		Data:      "test",
		ErrorType: thanos_api.ErrorTimeout,
		Error:     "message",
	}
	require.Equal(t, exp, &res)
}

func TestParseTimeParam(t *testing.T) {
	type resultType struct {
		asTime  time.Time
		asError func() error
	}

	ts, err := parseTime("1582468023986")
	require.NoError(t, err)

	tests := []struct {
		paramName    string
		paramValue   string
		defaultValue time.Time
		result       resultType
	}{
		{ // When data is valid.
			paramName:    "start",
			paramValue:   "1582468023986",
			defaultValue: minTime,
			result: resultType{
				asTime:  ts,
				asError: nil,
			},
		},
		{ // When data is empty string.
			paramName:    "end",
			paramValue:   "",
			defaultValue: maxTime,
			result: resultType{
				asTime:  maxTime,
				asError: nil,
			},
		},
		{ // When data is not valid.
			paramName:    "foo",
			paramValue:   "baz",
			defaultValue: maxTime,
			result: resultType{
				asTime: time.Time{},
				asError: func() error {
					_, err := parseTime("baz")
					return errors.Wrapf(err, "Invalid time value for '%s'", "foo")
				},
			},
		},
	}

	for _, test := range tests {
		req, err := http.NewRequest("GET", "localhost:42/foo?"+test.paramName+"="+test.paramValue, nil)
		require.NoError(t, err)

		result := test.result
		asTime, err := parseTimeParam(req, test.paramName, test.defaultValue)

		if err != nil {
			require.EqualError(t, err, result.asError().Error())
		} else {
			require.True(t, asTime.Equal(result.asTime), "time as return value: %s not parsed correctly. Expected %s. Actual %s", test.paramValue, result.asTime, asTime)
		}
	}
}

func TestParseTime(t *testing.T) {
	ts, err := time.Parse(time.RFC3339Nano, "2015-06-03T13:21:58.555Z")
	if err != nil {
		panic(err)
	}

	tests := []struct {
		input  string
		fail   bool
		result time.Time
	}{
		{
			input: "",
			fail:  true,
		},
		{
			input: "abc",
			fail:  true,
		},
		{
			input: "30s",
			fail:  true,
		},
		{
			input:  "123",
			result: time.Unix(123, 0),
		},
		{
			input:  "123.123",
			result: time.Unix(123, 123000000),
		},
		{
			input:  "2015-06-03T13:21:58.555Z",
			result: ts,
		},
		{
			input:  "2015-06-03T14:21:58.555+01:00",
			result: ts,
		},
		{
			// Test float rounding.
			input:  "1543578564.705",
			result: time.Unix(1543578564, 705*1e6),
		},
		{
			input:  minTime.Format(time.RFC3339Nano),
			result: minTime,
		},
		{
			input:  maxTime.Format(time.RFC3339Nano),
			result: maxTime,
		},
	}

	for _, test := range tests {
		ts, err := parseTime(test.input)

		if err != nil && !test.fail {
			t.Errorf("Unexpected error for %q: %s", test.input, err)
			continue
		}
		if err == nil && test.fail {
			t.Errorf("Expected error for %q but got none", test.input)
			continue
		}
		if !test.fail && !ts.Equal(test.result) {
			t.Errorf("Expected time %v for input %q but got %v", test.result, test.input, ts)
		}
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input  string
		fail   bool
		result time.Duration
	}{
		{
			input: "",
			fail:  true,
		}, {
			input: "abc",
			fail:  true,
		}, {
			input: "2015-06-03T13:21:58.555Z",
			fail:  true,
		}, {
			// Internal int64 overflow.
			input: "-148966367200.372",
			fail:  true,
		}, {
			// Internal int64 overflow.
			input: "148966367200.372",
			fail:  true,
		}, {
			input:  "123",
			result: 123 * time.Second,
		}, {
			input:  "123.333",
			result: 123*time.Second + 333*time.Millisecond,
		}, {
			input:  "15s",
			result: 15 * time.Second,
		}, {
			input:  "5m",
			result: 5 * time.Minute,
		},
	}

	for _, test := range tests {
		d, err := parseDuration(test.input)
		if err != nil && !test.fail {
			t.Errorf("Unexpected error for %q: %s", test.input, err)
			continue
		}
		if err == nil && test.fail {
			t.Errorf("Expected error for %q but got none", test.input)
			continue
		}
		if !test.fail && d != test.result {
			t.Errorf("Expected duration %v for input %q but got %v", test.result, test.input, d)
		}
	}
}

func TestRespond(t *testing.T) {
	cases := []struct {
		response interface{}
	}{
		{
			response: &queryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1, T: 1000}},
						Metric: labels.FromStrings("__name__", "foo"),
					},
				},
			},
		},
		{
			response: &queryrange.PrometheusResponse{
				Status: "success",
				Data: queryrange.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []tripperware.SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{
								{Name: "foo", Value: "bar"},
							},
							Samples: []cortexpb.Sample{
								{Value: 137, TimestampMs: 1536673680000},
								{Value: 137, TimestampMs: 1536673780000},
							},
						},
					},
				},
			},
		},
		{
			response: &instantquery.PrometheusInstantQueryResponse{
				Status: "success",
				Data: instantquery.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: instantquery.PrometheusInstantQueryResult{
						Result: &instantquery.PrometheusInstantQueryResult_Vector{
							Vector: &instantquery.Vector{
								Samples: []*instantquery.Sample{
									{
										Labels: []cortexpb.LabelAdapter{
											{"__name__", "up"},
											{"job", "foo"},
										},
										Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, c := range cases {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			api := API{}
			api.Respond(w, c.response, nil)
		}))
		defer s.Close()

		resp, err := http.Get(s.URL)
		if err != nil {
			t.Fatalf("Error on test request: %s", err)
		}
		body, err := io.ReadAll(resp.Body)
		defer resp.Body.Close()
		if err != nil {
			t.Fatalf("Error reading response body: %s", err)
		}

		var res interface{}
		switch dest := c.response.(type) {
		case *queryrange.PrometheusResponse:
			err = proto.Unmarshal(body, dest)
			res = dest
		case *instantquery.PrometheusInstantQueryResponse:
			err = proto.Unmarshal(body, dest)
			res = dest
		case *queryData:
			err = json.Unmarshal(body, dest)
			res = dest
		}

		if err != nil {
			t.Fatalf("Error unmarshaling response body: %s", err)
		}

		require.Equal(t, c.response, res)
	}
}

func TestReturnAPIError(t *testing.T) {
	cases := []struct {
		err      error
		expected thanos_api.ErrorType
	}{
		{
			err:      promql.ErrStorage{Err: errors.New("storage error")},
			expected: thanos_api.ErrorInternal,
		}, {
			err:      fmt.Errorf("wrapped: %w", promql.ErrStorage{Err: errors.New("storage error")}),
			expected: thanos_api.ErrorInternal,
		}, {
			err:      promql.ErrQueryTimeout("timeout error"),
			expected: thanos_api.ErrorTimeout,
		}, {
			err:      fmt.Errorf("wrapped: %w", promql.ErrQueryTimeout("timeout error")),
			expected: thanos_api.ErrorTimeout,
		}, {
			err:      promql.ErrQueryCanceled("canceled error"),
			expected: thanos_api.ErrorCanceled,
		}, {
			err:      fmt.Errorf("wrapped: %w", promql.ErrQueryCanceled("canceled error")),
			expected: thanos_api.ErrorCanceled,
		}, {
			err:      errors.New("exec error"),
			expected: thanos_api.ErrorExec,
		},
	}

	for ix, c := range cases {
		actual := returnAPIError(c.err)
		require.Error(t, actual, ix)
		require.Equal(t, c.expected, actual.Typ, ix)
	}
}

// This is a global to avoid the benchmark being optimized away.
var testResponseWriter = httptest.ResponseRecorder{}

func BenchmarkRespondQueryData(b *testing.B) {
	b.ReportAllocs()
	points := []promql.FPoint{}
	for i := 0; i < 10000; i++ {
		points = append(points, promql.FPoint{F: float64(i * 1000000), T: int64(i)})
	}
	response := &queryData{
		ResultType: parser.ValueTypeMatrix,
		Result: promql.Matrix{
			promql.Series{
				Floats: points,
				Metric: labels.EmptyLabels(),
			},
		},
	}
	b.ResetTimer()
	api := API{}
	for n := 0; n < b.N; n++ {
		api.Respond(&testResponseWriter, response, nil)
	}
}

func BenchmarkRespondPrometheusResponse(b *testing.B) {
	b.ReportAllocs()
	points := []cortexpb.Sample{}
	for i := 0; i < 10000; i++ {
		points = append(points, cortexpb.Sample{Value: float64(i * 1000000), TimestampMs: int64(i)})
	}
	response := &queryrange.PrometheusResponse{
		Status: "success",
		Data: queryrange.PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []tripperware.SampleStream{
				{
					Labels:  []cortexpb.LabelAdapter{},
					Samples: points,
				},
			},
		},
	}
	b.ResetTimer()
	api := API{}
	for n := 0; n < b.N; n++ {
		api.Respond(&testResponseWriter, response, nil)
	}
}

func BenchmarkRespondPrometheusInstantQueryResponse(b *testing.B) {
	b.ReportAllocs()
	points := []cortexpb.Sample{}
	for i := 0; i < 10000; i++ {
		points = append(points, cortexpb.Sample{Value: float64(i * 1000000), TimestampMs: int64(i)})
	}
	response := &instantquery.PrometheusInstantQueryResponse{
		Status: "success",
		Data: instantquery.PrometheusInstantQueryData{
			ResultType: model.ValMatrix.String(),
			Result: instantquery.PrometheusInstantQueryResult{
				Result: &instantquery.PrometheusInstantQueryResult_Matrix{
					Matrix: &instantquery.Matrix{
						SampleStreams: []tripperware.SampleStream{
							{
								Labels:  []cortexpb.LabelAdapter{},
								Samples: points,
							},
						},
					},
				},
			},
		},
	}
	b.ResetTimer()
	api := API{}
	for n := 0; n < b.N; n++ {
		api.Respond(&testResponseWriter, response, nil)
	}
}

func TestExtractQueryOpts(t *testing.T) {
	tests := []struct {
		name   string
		form   url.Values
		expect *promql.QueryOpts
		err    error
	}{
		{
			name: "with stats all",
			form: url.Values{
				"stats": []string{"all"},
			},
			expect: &promql.QueryOpts{
				EnablePerStepStats: true,
			},
			err: nil,
		},
		{
			name: "with stats none",
			form: url.Values{
				"stats": []string{"none"},
			},
			expect: &promql.QueryOpts{
				EnablePerStepStats: false,
			},
			err: nil,
		},
		{
			name: "with lookback delta",
			form: url.Values{
				"stats":          []string{"all"},
				"lookback_delta": []string{"30s"},
			},
			expect: &promql.QueryOpts{
				EnablePerStepStats: true,
				LookbackDelta:      30 * time.Second,
			},
			err: nil,
		},
		{
			name: "with invalid lookback delta",
			form: url.Values{
				"lookback_delta": []string{"invalid"},
			},
			expect: nil,
			err:    errors.New(`error parsing lookback delta duration: cannot parse "invalid" to a valid duration`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := &http.Request{Form: test.form}
			opts, err := extractQueryOpts(req)
			require.Equal(t, test.expect, opts)
			if test.err == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, test.err.Error(), err.Error())
			}
		})
	}
}

func TestCreatePrometheusResponse(t *testing.T) {
	tests := []struct {
		data     *queryData
		response *queryrange.PrometheusResponse
		err      error
	}{
		{
			data: &queryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{
							{F: 1, T: 1000},
							{F: 2, T: 2000},
							{F: 3, T: 3000},
						},
					},
				},
			},
			response: &queryrange.PrometheusResponse{
				Status: "success",
				Data: queryrange.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []tripperware.SampleStream{
						{
							Samples: []cortexpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
							},
						},
					},
				},
			},
		},
		{
			data: &queryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Metric: labels.Labels{
							{Name: "__name__", Value: "foo"},
							{Name: "__job__", Value: "bar"},
						},
						Floats: []promql.FPoint{
							{F: 0.14, T: 18555000},
							{F: 2.9, T: 18556000},
							{F: 30, T: 18557000},
						},
					},
				},
			},
			response: &queryrange.PrometheusResponse{
				Status: "success",
				Data: queryrange.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []tripperware.SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{
								{Name: "__name__", Value: "foo"},
								{Name: "__job__", Value: "bar"},
							},
							Samples: []cortexpb.Sample{
								{Value: 0.14, TimestampMs: 18555000},
								{Value: 2.9, TimestampMs: 18556000},
								{Value: 30, TimestampMs: 18557000},
							},
						},
					},
				},
			},
		},
		{
			data: nil,
			err:  errors.New("no query response data"),
		},
	}
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			res, err := createPrometheusResponse(test.data)
			require.Equal(t, test.response, res)
			if test.err == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, test.err.Error(), err.Error())
			}
		})
	}
}

func TestCreatePrometheusInstantQueryResponse(t *testing.T) {
	tests := []struct {
		data     *queryData
		response *instantquery.PrometheusInstantQueryResponse
		err      error
	}{
		{
			data: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result:     promql.Scalar{T: 1000, V: 1},
			},
			response: &instantquery.PrometheusInstantQueryResponse{
				Status: "success",
				Data: instantquery.PrometheusInstantQueryData{
					ResultType: model.ValScalar.String(),
					Result: instantquery.PrometheusInstantQueryResult{
						Result: &instantquery.PrometheusInstantQueryResult_RawBytes{
							RawBytes: []byte(`{"resultType":"scalar","result":[1,"1"]}`),
						},
					},
				},
			},
		},
		{
			data: &queryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Metric: labels.Labels{
							{Name: "__name__", Value: "foo"},
							{Name: "__job__", Value: "bar"},
						},
						Floats: []promql.FPoint{
							{F: 0.14, T: 18555000},
							{F: 2.9, T: 18556000},
							{F: 30, T: 18557000},
						},
					},
				},
			},
			response: &instantquery.PrometheusInstantQueryResponse{
				Status: "success",
				Data: instantquery.PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: instantquery.PrometheusInstantQueryResult{
						Result: &instantquery.PrometheusInstantQueryResult_Matrix{
							Matrix: &instantquery.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{
											{"__name__", "foo"},
											{"__job__", "bar"},
										},
										Samples: []cortexpb.Sample{
											{Value: 0.14, TimestampMs: 18555000},
											{Value: 2.9, TimestampMs: 18556000},
											{Value: 30, TimestampMs: 18557000},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			data: &queryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						F: 1,
						T: 1000,
						Metric: labels.Labels{
							{"__name__", "up"},
							{"job", "foo"},
						},
					},
				},
			},
			response: &instantquery.PrometheusInstantQueryResponse{
				Status: "success",
				Data: instantquery.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: instantquery.PrometheusInstantQueryResult{
						Result: &instantquery.PrometheusInstantQueryResult_Vector{
							Vector: &instantquery.Vector{
								Samples: []*instantquery.Sample{
									{
										Labels: []cortexpb.LabelAdapter{
											{"__name__", "up"},
											{"job", "foo"},
										},
										Sample: cortexpb.Sample{Value: 1, TimestampMs: 1000},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			data: nil,
			err:  errors.New("no query response data"),
		},
	}
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			res, err := createPrometheusInstantQueryResponse(test.data)
			require.Equal(t, test.response, res)
			if test.err == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, test.err.Error(), err.Error())
			}
		})
	}
}
