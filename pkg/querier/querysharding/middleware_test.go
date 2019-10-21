package querysharding

import (
	"context"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
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

			handler := QueryShardMiddleware(
				engine,
				ShardingConfigs{
					{
						Shards: 3,
					},
				},
			).Wrap(c.next)

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
							TimestampMs: 5,
							Value:       1,
						},
						client.Sample{
							TimestampMs: 10,
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
							TimestampMs: 5,
							Value:       8,
						},
						client.Sample{
							TimestampMs: 10,
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

func TestShardingConfigs_ValidRange(t *testing.T) {
	reqWith := func(start, end string) *queryrange.Request {
		r := defaultReq()

		if start != "" {
			r.Start = int64(parseDate(start))
		}

		if end != "" {
			r.End = int64(parseDate(end))
		}

		return r
	}

	var testExpr = []struct {
		name     string
		confs    ShardingConfigs
		req      *queryrange.Request
		expected ShardingConfig
		err      error
	}{
		{
			name:  "0 ln configs fail",
			confs: ShardingConfigs{},
			req:   defaultReq(),
			err:   invalidShardingRange,
		},
		{
			name: "request starts before beginning config",
			confs: ShardingConfigs{
				{
					From:   chunk.DayTime{parseDate("2019-10-16")},
					Shards: 1,
				},
			},
			req: reqWith("2019-10-15", ""),
			err: invalidShardingRange,
		},
		{
			name: "request spans multiple configs",
			confs: ShardingConfigs{
				{
					From:   chunk.DayTime{parseDate("2019-10-16")},
					Shards: 1,
				},
				{
					From:   chunk.DayTime{parseDate("2019-11-16")},
					Shards: 2,
				},
			},
			req: reqWith("2019-10-15", "2019-11-17"),
			err: invalidShardingRange,
		},
		{
			name: "selects correct config ",
			confs: ShardingConfigs{
				{
					From:   chunk.DayTime{parseDate("2019-10-16")},
					Shards: 1,
				},
				{
					From:   chunk.DayTime{parseDate("2019-11-16")},
					Shards: 2,
				},
				{
					From:   chunk.DayTime{parseDate("2019-12-16")},
					Shards: 3,
				},
			},
			req: reqWith("2019-11-20", "2019-11-25"),
			expected: ShardingConfig{
				From:   chunk.DayTime{parseDate("2019-11-16")},
				Shards: 2,
			},
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			out, err := c.confs.ValidRange(c.req.Start, c.req.End)

			if c.err != nil {
				require.EqualError(t, err, c.err.Error())
			} else {
				require.Nil(t, err)
				require.Equal(t, c.expected, out)
			}
		})
	}
}

func TestTimeFromMillis(t *testing.T) {
	var testExpr = []struct {
		input    int64
		expected time.Time
	}{
		{1000, time.Unix(1, 0)},
		{1500, time.Unix(1, 500*nanosecondsInMillisecond)},
	}

	for i, c := range testExpr {
		t.Run(string(i), func(t *testing.T) {
			res := TimeFromMillis(c.input)
			require.Equal(t, c.expected, res)
		})
	}
}

func parseDate(in string) model.Time {
	t, err := time.Parse("2006-01-02", in)
	if err != nil {
		panic(err)
	}
	return model.Time(t.UnixNano())
}
