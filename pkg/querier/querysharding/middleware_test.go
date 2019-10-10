package querysharding

import (
	"context"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMiddleware(t *testing.T) {
	expired, _ := context.WithDeadline(context.Background(), time.Now())
	var testExpr = []struct {
		name     string
		next     queryrange.Handler
		input    *queryrange.Request
		ctx      context.Context
		expected *queryrange.APIResponse
		err      bool
	}{
		{
			name: "invalid query error",
			// if the query parses correctly force it to succeed
			next: mockHandler(&queryrange.APIResponse{
				Status: "",
				Data: queryrange.Response{
					ResultType: promql.ValueTypeVector,
					Result:     []queryrange.SampleStream{},
				},
				ErrorType: "",
				Error:     "",
			}, nil),
			input:    &queryrange.Request{Query: "^GARBAGE"},
			ctx:      context.Background(),
			expected: nil,
			err:      true,
		},
		{
			name:  "downstream err",
			next:  mockHandler(nil, errors.Errorf("some err")),
			input: defaultReq(),
			ctx:   context.Background(),
			expected: &queryrange.APIResponse{
				Status:    queryrange.StatusFailure,
				ErrorType: downStreamErrType,
				Error:     "some err",
			},
			err: false,
		},
		{
			name: "context expiry",
			next: mockHandler(&queryrange.APIResponse{
				Status: "",
				Data: queryrange.Response{
					ResultType: promql.ValueTypeVector,
					Result:     []queryrange.SampleStream{},
				},
				ErrorType: "",
				Error:     "",
			}, nil),
			input: defaultReq(),
			ctx:   expired,
			expected: &queryrange.APIResponse{
				Status:    queryrange.StatusFailure,
				ErrorType: downStreamErrType,
				Error:     "query timed out in query execution",
			},
			err: false,
		},
		{
			name:     "successful trip",
			next:     mockHandler(sampleMatrixResponse(), nil),
			input:    defaultReq(),
			ctx:      context.Background(),
			expected: sampleMatrixResponse(),
			err:      false,
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:        util.Logger,
				Reg:           nil,
				MaxConcurrent: 10,
				MaxSamples:    1000,
				Timeout:       time.Minute,
			})

			mware := QueryShardMiddleware(engine).Wrap(c.next)
			out, err := mware.Do(c.ctx, c.input)

			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.expected, out)
			}

		})
	}
}

func sampleMatrixResponse() *queryrange.APIResponse {
	return &queryrange.APIResponse{
		Status: queryrange.StatusSuccess,
		Data: queryrange.Response{
			ResultType: promql.ValueTypeMatrix,
			Result: []queryrange.SampleStream{
				{
					Labels: []client.LabelAdapter{
						{"a", "a1"},
						{"b", "b1"},
					},
					Samples: []client.Sample{
						client.Sample{
							Value:       1,
							TimestampMs: 1,
						},
						client.Sample{
							Value:       2,
							TimestampMs: 2,
						},
					},
				},
				{
					Labels: []client.LabelAdapter{
						{"a", "a2"},
						{"b", "b2"},
					},
					Samples: []client.Sample{
						client.Sample{
							Value:       8,
							TimestampMs: 1,
						},
						client.Sample{
							Value:       9,
							TimestampMs: 2,
						},
					},
				},
			},
		},
	}
}

func mockHandler(resp *queryrange.APIResponse, err error) queryrange.Handler {
	return queryrange.HandlerFunc(func(ctx context.Context, req *queryrange.Request) (*queryrange.APIResponse, error) {
		if expired := ctx.Err(); expired != nil {
			return nil, expired
		}

		return resp, err
	})
}

func defaultReq() *queryrange.Request {
	return &queryrange.Request{
		Path:    "/query_range",
		Start:   10,
		End:     20,
		Step:    5,
		Timeout: time.Minute,
		Query:   `__embedded_query__{__cortex_query__="687474705f72657175657374735f746f74616c7b636c75737465723d2270726f64227d"}`,
	}
}
