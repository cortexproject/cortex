package ruler

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

type mockHTTPGRPCClient func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error)

func (c mockHTTPGRPCClient) Handle(ctx context.Context, req *httpgrpc.HTTPRequest, opts ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
	return c(ctx, req, opts...)
}

func TestTimeout(t *testing.T) {
	mockClientFn := func(ctx context.Context, _ *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, "userID")
	frontendClient := NewFrontendClient(mockHTTPGRPCClient(mockClientFn), time.Second*5, "/prometheus", "json")
	_, err := frontendClient.InstantQuery(ctx, "query", time.Now())
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestNoOrgId(t *testing.T) {
	mockClientFn := func(ctx context.Context, _ *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		return nil, nil
	}
	frontendClient := NewFrontendClient(mockHTTPGRPCClient(mockClientFn), time.Second*5, "/prometheus", "json")
	_, err := frontendClient.InstantQuery(context.Background(), "query", time.Now())
	require.Equal(t, user.ErrNoOrgID, err)
}

func TestMakeRequestRuleInfo(t *testing.T) {
	ts := time.Unix(1000, 0).UTC()
	ctx := user.InjectOrgID(context.Background(), "userID")
	ruleCtx := promql.NewOriginContext(ctx, map[string]any{
		"ruleGroup": map[string]string{
			"file": "namespace",
			"name": "group",
		},
	})
	ruleCtx = rules.NewOriginContext(ruleCtx, rules.RuleDetail{Name: "rule", Kind: "recording"})

	tests := map[string]struct {
		ctx      context.Context
		expected url.Values
	}{
		"without rule origin": {
			ctx: ctx,
			expected: url.Values{
				"query": []string{"up"},
				"time":  []string{ts.Format(time.RFC3339Nano)},
			},
		},
		"with rule origin": {
			ctx: ruleCtx,
			expected: url.Values{
				"query":          []string{"up"},
				"time":           []string{ts.Format(time.RFC3339Nano)},
				"rule_group":     []string{"group"},
				"rule_namespace": []string{"namespace"},
				"rule":           []string{"rule"},
				"rule_kind":      []string{"recording"},
			},
		},
		"unexpected query origin type": {
			ctx: promql.NewOriginContext(ctx, map[string]any{"ruleGroup": "invalid"}),
			expected: url.Values{
				"query": []string{"up"},
				"time":  []string{ts.Format(time.RFC3339Nano)},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			frontendClient := NewFrontendClient(nil, time.Second*5, "/prometheus", "json")
			req, err := frontendClient.makeRequest(tc.ctx, "up", ts)
			require.NoError(t, err)

			args, err := url.ParseQuery(string(req.Body))
			require.NoError(t, err)
			require.Equal(t, tc.expected, args)
		})
	}
}

func TestInstantQueryJsonCodec(t *testing.T) {
	tests := []struct {
		description  string
		responseBody string
		expected     promql.Vector
		expectedErr  error
	}{
		{
			description: "empty vector",
			responseBody: `{
					"status": "success",
					"data": {"resultType":"vector","result":[]}
				}`,
			expected:    promql.Vector{},
			expectedErr: nil,
		},
		{
			description: "vector with series",
			responseBody: `{
					"status": "success",
					"data": {
						"resultType": "vector",
						"result": [
							{
								"metric": {"foo":"bar"},
								"value": [1724146338.123,"1.234"]
							},
							{
								"metric": {"bar":"baz"},
								"value": [1724146338.456,"5.678"]
							}
						]
					}
				}`,
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1724146338123,
					F:      1.234,
				},
				{
					Metric: labels.FromStrings("bar", "baz"),
					T:      1724146338456,
					F:      5.678,
				},
			},
			expectedErr: nil,
		},
		{
			description: "get scalar",
			responseBody: `{
					"status": "success",
					"data": {"resultType":"scalar","result":[1724146338.123,"1.234"]}
				}`,
			expected: promql.Vector{
				{
					Metric: labels.EmptyLabels(),
					T:      1724146338123,
					F:      1.234,
				},
			},
			expectedErr: nil,
		},
		{
			description: "get matrix",
			responseBody: `{
					"status": "success",
					"data": {"resultType":"matrix","result":[]}
				}`,
			expected:    nil,
			expectedErr: errors.New("rule result is not a vector or scalar"),
		},
		{
			description: "get string",
			responseBody: `{
					"status": "success",
					"data": {"resultType":"string","result":[1724146338.123,"string"]}
				}`,
			expected:    nil,
			expectedErr: errors.New("rule result is not a vector or scalar"),
		},
		{
			description: "get error",
			responseBody: `{
					"status": "error",
					"errorType": "errorExec",
					"error": "something wrong"
				}`,
			expected:    nil,
			expectedErr: errors.New("something wrong"),
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			mockClientFn := func(ctx context.Context, _ *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				return &httpgrpc.HTTPResponse{
					Code: http.StatusOK,
					Headers: []*httpgrpc.Header{
						{Key: "Content-Type", Values: []string{"application/json"}},
					},
					Body: []byte(test.responseBody),
				}, nil
			}
			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, "userID")
			frontendClient := NewFrontendClient(mockHTTPGRPCClient(mockClientFn), time.Second*5, "/prometheus", "json")
			vector, err := frontendClient.InstantQuery(ctx, "query", time.Now())
			require.Equal(t, test.expected, vector)
			require.Equal(t, test.expectedErr, err)
		})
	}
}

func TestInstantQueryProtoCodec(t *testing.T) {
	var tests = []struct {
		description  string
		responseBody *tripperware.PrometheusResponse
		expected     promql.Vector
		expectedErr  error
	}{
		{
			description: "empty vector",
			responseBody: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "vector",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Vector{
							Vector: &tripperware.Vector{
								Samples: []tripperware.Sample{},
							},
						},
					},
				},
			},
			expected:    promql.Vector{},
			expectedErr: nil,
		},
		{
			description: "vector with series",
			responseBody: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "vector",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Vector{
							Vector: &tripperware.Vector{
								Samples: []tripperware.Sample{
									{
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("foo", "bar")),
										Sample: &cortexpb.Sample{
											Value:       1.234,
											TimestampMs: 1724146338123,
										},
									},
									{
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("bar", "baz")),
										Sample: &cortexpb.Sample{
											Value:       5.678,
											TimestampMs: 1724146338456,
										},
									},
								},
							},
						},
					},
				},
			},
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1724146338123,
					F:      1.234,
				},
				{
					Metric: labels.FromStrings("bar", "baz"),
					T:      1724146338456,
					F:      5.678,
				},
			},
			expectedErr: nil,
		},
		{
			description: "get scalar",
			responseBody: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "scalar",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_RawBytes{
							RawBytes: []byte(`{"resultType":"scalar","result":[1724146338.123,"1.234"]}`),
						},
					},
				},
			},
			expected: promql.Vector{
				{
					Metric: labels.EmptyLabels(),
					T:      1724146338123,
					F:      1.234,
				},
			},
			expectedErr: nil,
		},
		{
			description: "get matrix",
			responseBody: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "matrix",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{},
							},
						},
					},
				},
			},
			expectedErr: errors.New("rule result is not a vector or scalar"),
		},
		{
			description: "get string",
			responseBody: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "string",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_RawBytes{
							RawBytes: []byte(`{"resultType":"string","result":[1724146338.123,"string"]}`),
						},
					},
				},
			},
			expectedErr: errors.New("rule result is not a vector or scalar"),
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			mockClientFn := func(ctx context.Context, _ *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				d, err := test.responseBody.Marshal()
				if err != nil {
					return nil, err
				}
				return &httpgrpc.HTTPResponse{
					Code: http.StatusOK,
					Headers: []*httpgrpc.Header{
						{Key: "Content-Type", Values: []string{"application/x-cortex-query+proto"}},
					},
					Body: d,
				}, nil
			}
			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, "userID")
			frontendClient := NewFrontendClient(mockHTTPGRPCClient(mockClientFn), time.Second*5, "/prometheus", "protobuf")
			vector, err := frontendClient.InstantQuery(ctx, "query", time.Now())
			require.Equal(t, test.expected, vector)
			require.Equal(t, test.expectedErr, err)
		})
	}
}

func Test_extractHeader(t *testing.T) {
	tests := []struct {
		description    string
		headers        []*httpgrpc.Header
		expectedOutput string
	}{
		{
			description: "cortex query proto",
			headers: []*httpgrpc.Header{
				{
					Key:    "Content-Type",
					Values: []string{"application/x-cortex-query+proto"},
				},
			},
			expectedOutput: "application/x-cortex-query+proto",
		},
		{
			description: "json",
			headers: []*httpgrpc.Header{
				{
					Key:    "Content-Type",
					Values: []string{"application/json"},
				},
			},
			expectedOutput: "application/json",
		},
	}

	target := "Content-Type"
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require.Equal(t, test.expectedOutput, extractHeader(test.headers, target))
		})
	}
}
