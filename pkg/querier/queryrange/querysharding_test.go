package queryrange

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestMiddleware(t *testing.T) {
	var testExpr = []struct {
		name     string
		next     Handler
		input    Request
		ctx      context.Context
		expected *PrometheusResponse
		err      bool
		override func(*testing.T, Handler)
	}{
		{
			name: "invalid query error",
			// if the query parses correctly force it to succeed
			next: mockHandler(&PrometheusResponse{
				Status: "",
				Data: PrometheusData{
					ResultType: promql.ValueTypeVector,
					Result:     []SampleStream{},
				},
				ErrorType: "",
				Error:     "",
			}, nil),
			input:    &PrometheusRequest{Query: "^GARBAGE"},
			ctx:      context.Background(),
			expected: nil,
			err:      true,
		},
		{
			name:     "downstream err",
			next:     mockHandler(nil, errors.Errorf("some err")),
			input:    defaultReq(),
			ctx:      context.Background(),
			expected: nil,
			err:      true,
		},
		{
			name: "successful trip",
			next: mockHandler(sampleMatrixResponse(), nil),
			override: func(t *testing.T, handler Handler) {

				// pre-encode the query so it doesn't try to re-split. We're just testing if it passes through correctly
				qry := defaultReq().WithQuery(
					`__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"prod\\\"}\"]}"}`,
				)
				out, err := handler.Do(context.Background(), qry)
				require.Nil(t, err)
				require.Equal(t, promql.ValueTypeMatrix, out.(*PrometheusResponse).Data.ResultType)
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

			handler := NewQueryShardMiddleware(
				log.NewNopLogger(),
				engine,
				ShardingConfigs{
					{
						RowShards: 3,
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

func sampleMatrixResponse() *PrometheusResponse {
	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: promql.ValueTypeMatrix,
			Result: []SampleStream{
				{
					Labels: []client.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []client.Sample{
						{
							TimestampMs: 5,
							Value:       1,
						},
						{
							TimestampMs: 10,
							Value:       2,
						},
					},
				},
				{
					Labels: []client.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []client.Sample{
						{
							TimestampMs: 5,
							Value:       8,
						},
						{
							TimestampMs: 10,
							Value:       9,
						},
					},
				},
			},
		},
	}
}

func mockHandler(resp *PrometheusResponse, err error) Handler {
	return HandlerFunc(func(ctx context.Context, req Request) (Response, error) {
		if expired := ctx.Err(); expired != nil {
			return nil, expired
		}

		return resp, err
	})
}

func defaultReq() *PrometheusRequest {
	return &PrometheusRequest{
		Path:    "/query_range",
		Start:   00,
		End:     10,
		Step:    5,
		Timeout: time.Minute,
		Query:   `sum(rate(http_requests_total{}[5m]))`,
	}
}

func TestShardingConfigs_ValidRange(t *testing.T) {
	reqWith := func(start, end string) *PrometheusRequest {
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
		req      *PrometheusRequest
		expected chunk.PeriodConfig
		err      error
	}{
		{
			name:  "0 ln configs fail",
			confs: ShardingConfigs{},
			req:   defaultReq(),
			err:   errInvalidShardingRange,
		},
		{
			name: "request starts before beginning config",
			confs: ShardingConfigs{
				{
					From:      chunk.DayTime{Time: parseDate("2019-10-16")},
					RowShards: 1,
				},
			},
			req: reqWith("2019-10-15", ""),
			err: errInvalidShardingRange,
		},
		{
			name: "request spans multiple configs",
			confs: ShardingConfigs{
				{
					From:      chunk.DayTime{Time: parseDate("2019-10-16")},
					RowShards: 1,
				},
				{
					From:      chunk.DayTime{Time: parseDate("2019-11-16")},
					RowShards: 2,
				},
			},
			req: reqWith("2019-10-15", "2019-11-17"),
			err: errInvalidShardingRange,
		},
		{
			name: "selects correct config ",
			confs: ShardingConfigs{
				{
					From:      chunk.DayTime{Time: parseDate("2019-10-16")},
					RowShards: 1,
				},
				{
					From:      chunk.DayTime{Time: parseDate("2019-11-16")},
					RowShards: 2,
				},
				{
					From:      chunk.DayTime{Time: parseDate("2019-12-16")},
					RowShards: 3,
				},
			},
			req: reqWith("2019-11-20", "2019-11-25"),
			expected: chunk.PeriodConfig{
				From:      chunk.DayTime{Time: parseDate("2019-11-16")},
				RowShards: 2,
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
		{input: 1000, expected: time.Unix(1, 0)},
		{input: 1500, expected: time.Unix(1, 500*nanosecondsInMillisecond)},
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

func BenchmarkQuerySharding(b *testing.B) {

	var shards []uint32

	for shard := 1; shard <= runtime.NumCPU(); shard = shard * 2 {
		shards = append(shards, uint32(shard))
	}

	for _, tc := range []struct {
		labelBuckets     int
		labels           []string
		samplesPerSeries int
		query            string
		desc             string
	}{
		// Ensure you have enough cores to run these tests without blocking.
		// We want to simulate parallel computations and waiting in queue doesn't help

		// no-group
		{
			labelBuckets:     10,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			query:            `sum(rate(http_requests_total[5m]))`,
			desc:             "sum nogroup",
		},
		// sum by
		{
			labelBuckets:     10,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			query:            `sum by(a) (rate(http_requests_total[5m]))`,
			desc:             "sum by",
		},
		// sum without
		{
			labelBuckets:     10,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			query:            `sum without (a) (rate(http_requests_total[5m]))`,
			desc:             "sum without",
		},
	} {
		engine := promql.NewEngine(promql.EngineOpts{
			Logger: util.Logger,
			Reg:    nil,
			// set high concurrency so we're not bottlenecked here
			MaxConcurrent: 100000,
			MaxSamples:    100000000,
			Timeout:       time.Minute,
		})

		queryable := astmapper.NewMockShardedQueryable(
			tc.samplesPerSeries,
			tc.labels,
			tc.labelBuckets,
		)
		downstream := &downstreamHandler{
			engine:    engine,
			queryable: queryable,
		}

		var (
			start int64 = 0
			end         = int64(1000 * tc.samplesPerSeries)
			step        = (end - start) / 1000
		)

		req := &PrometheusRequest{
			Path:    "/query_range",
			Start:   start,
			End:     end,
			Step:    step,
			Timeout: time.Minute,
			Query:   tc.query,
		}

		for _, shardFactor := range shards {
			mware := NewQueryShardMiddleware(
				log.NewNopLogger(),
				engine,
				// ensure that all requests are shard compatbile
				ShardingConfigs{
					chunk.PeriodConfig{
						Schema:    "v10",
						RowShards: shardFactor,
					},
				},
			).Wrap(downstream)

			b.Run(
				fmt.Sprintf(
					"desc:[%s]:shards:[%d]/series:[%.0f]/samplesPerSeries:[%d]",
					tc.desc,
					shardFactor,
					math.Pow(float64(tc.labelBuckets), float64(len(tc.labels))),
					tc.samplesPerSeries,
				),
				func(b *testing.B) {
					for n := 0; n < b.N; n++ {
						_, err := mware.Do(
							context.Background(),
							req,
						)
						if err != nil {
							b.Fatal(err.Error())
						}
					}
				},
			)
		}

	}
}

type downstreamHandler struct {
	engine    *promql.Engine
	queryable storage.Queryable
}

func (h *downstreamHandler) Do(ctx context.Context, r Request) (Response, error) {
	qry, err := h.engine.NewRangeQuery(
		h.queryable,
		r.GetQuery(),
		TimeFromMillis(r.GetStart()),
		TimeFromMillis(r.GetEnd()),
		time.Duration(r.GetStep())*time.Millisecond,
	)

	if err != nil {
		return nil, err
	}

	res := qry.Exec(ctx)
	extracted, err := FromResult(res)
	if err != nil {
		return nil, err

	}

	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
	}, nil
}
