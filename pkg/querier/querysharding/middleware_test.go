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
	var testExpr = []struct {
		name     string
		next     queryrange.Handler
		input    *queryrange.Request
		ctx      context.Context
		expected *queryrange.APIResponse
		err      bool
		override func(*testing.T, queryrange.Handler)
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
			name: "expiration",
			next: mockHandler(sampleMatrixResponse(), nil),
			override: func(t *testing.T, handler queryrange.Handler) {
				expired, _ := context.WithDeadline(context.Background(), time.Unix(0, 0))
				res, err := handler.Do(expired, defaultReq())
				require.Nil(t, err)
				require.NotEqual(t, "", res.Error)
			},
		},
		{
			name: "successful trip",
			next: mockHandler(sampleMatrixResponse(), nil),
			override: func(t *testing.T, handler queryrange.Handler) {
				out, err := handler.Do(context.Background(), defaultReq())
				require.Nil(t, err)
				require.Equal(t, promql.ValueTypeMatrix, out.Data.ResultType)
				require.Equal(t, sampleMatrixResponse(), out)
			},
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

			handler := QueryShardMiddleware(engine).Wrap(c.next)

			// escape hatch for custom tests
			if c.override != nil {
				c.override(t, handler)
				return
			}

			out, err := handler.Do(c.ctx, c.input)

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
							TimestampMs: 5000,
							Value:       1,
						},
						client.Sample{
							TimestampMs: 10000,
							Value:       2,
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
							TimestampMs: 5000,
							Value:       8,
						},
						client.Sample{
							TimestampMs: 10000,
							Value:       9,
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
		Start:   00,
		End:     10,
		Step:    5,
		Timeout: time.Minute,
		// encoding of: `http_requests_total{cluster="prod"}`
		Query: `__embedded_query__{__cortex_query__="687474705f72657175657374735f746f74616c7b636c75737465723d2270726f64227d"}`,
	}
}
