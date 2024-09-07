package ruler

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
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
	frontendClient := NewFrontendClient(mockHTTPGRPCClient(mockClientFn), time.Second*5, "/prometheus")
	_, err := frontendClient.InstantQuery(ctx, "query", time.Now())
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestNoOrgId(t *testing.T) {
	mockClientFn := func(ctx context.Context, _ *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		return nil, nil
	}
	frontendClient := NewFrontendClient(mockHTTPGRPCClient(mockClientFn), time.Second*5, "/prometheus")
	_, err := frontendClient.InstantQuery(context.Background(), "query", time.Now())
	require.Equal(t, user.ErrNoOrgID, err)
}

func TestInstantQuery(t *testing.T) {
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
			expectedErr: errors.New("failed to execute query with error: something wrong"),
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
			frontendClient := NewFrontendClient(mockHTTPGRPCClient(mockClientFn), time.Second*5, "/prometheus")
			vector, err := frontendClient.InstantQuery(ctx, "query", time.Now())
			require.Equal(t, test.expected, vector)
			require.Equal(t, test.expectedErr, err)
		})
	}
}
