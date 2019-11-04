package queryrange

import (
	"context"
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestSelect(t *testing.T) {
	var testExpr = []struct {
		name    string
		querier *DownstreamQuerier
		fn      func(*testing.T, *DownstreamQuerier)
	}{
		{
			name: "errors non embedded query",
			querier: mkQuerier(
				nil,
			),
			fn: func(t *testing.T, q *DownstreamQuerier) {
				set, _, err := q.Select(nil)
				require.Nil(t, set)
				require.EqualError(t, err, nonEmbeddedErrMsg)
			},
		},
		{
			name: "replaces query",
			querier: mkQuerier(mockHandler(
				&PrometheusResponse{},
				nil,
			)),
			fn: func(t *testing.T, q *DownstreamQuerier) {

				expected := &PrometheusResponse{
					Status: "success",
					Data: PrometheusData{
						ResultType: promql.ValueTypeVector,
					},
				}

				// override handler func to assert new query has been substituted
				q.Handler = HandlerFunc(
					func(ctx context.Context, req Request) (Response, error) {
						require.Equal(t, `http_requests_total{cluster="prod"}`, req.GetQuery())
						return expected, nil
					},
				)

				_, _, err := q.Select(
					nil,
					exactMatch("__name__", astmapper.EmbeddedQueryFlag),
					exactMatch(astmapper.QueryLabel, astmapper.HexCodec.Encode([]string{`http_requests_total{cluster="prod"}`})),
				)
				require.Nil(t, err)
			},
		},
		{
			name: "propagates response error",
			querier: mkQuerier(mockHandler(
				&PrometheusResponse{
					Error: "SomeErr",
				},
				nil,
			)),
			fn: func(t *testing.T, q *DownstreamQuerier) {
				set, _, err := q.Select(
					nil,
					exactMatch("__name__", astmapper.EmbeddedQueryFlag),
					exactMatch(astmapper.QueryLabel, astmapper.HexCodec.Encode([]string{`http_requests_total{cluster="prod"}`})),
				)
				require.Nil(t, set)
				require.EqualError(t, err, "SomeErr")
			},
		},
		{
			name: "returns SeriesSet",
			querier: mkQuerier(mockHandler(
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: promql.ValueTypeVector,
						Result: []SampleStream{
							{
								Labels: []client.LabelAdapter{
									{Name: "a", Value: "a1"},
									{Name: "b", Value: "b1"},
								},
								Samples: []client.Sample{
									{
										Value:       1,
										TimestampMs: 1,
									},
									{
										Value:       2,
										TimestampMs: 2,
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
										Value:       8,
										TimestampMs: 1,
									},
									{
										Value:       9,
										TimestampMs: 2,
									},
								},
							},
						},
					},
				},
				nil,
			)),
			fn: func(t *testing.T, q *DownstreamQuerier) {
				set, _, err := q.Select(
					nil,
					exactMatch("__name__", astmapper.EmbeddedQueryFlag),
					exactMatch(astmapper.QueryLabel, astmapper.HexCodec.Encode([]string{`http_requests_total{cluster="prod"}`})),
				)
				require.Nil(t, err)
				require.Equal(
					t,
					NewSeriesSet([]SampleStream{
						{
							Labels: []client.LabelAdapter{
								{Name: "a", Value: "a1"},
								{Name: "b", Value: "b1"},
							},
							Samples: []client.Sample{
								{
									Value:       1,
									TimestampMs: 1,
								},
								{
									Value:       2,
									TimestampMs: 2,
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
									Value:       8,
									TimestampMs: 1,
								},
								{
									Value:       9,
									TimestampMs: 2,
								},
							},
						},
					}),
					set,
				)
			},
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			c.fn(t, c.querier)
		})
	}
}

func TestSelectConcurrent(t *testing.T) {
	for _, c := range []struct {
		name    string
		queries []string
		err     error
	}{
		{
			name: "concats queries",
			queries: []string{
				`sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m]))`,
				`sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m]))`,
				`sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))`,
			},
			err: nil,
		},
		{
			name: "errors",
			queries: []string{
				`sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m]))`,
				`sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m]))`,
				`sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))`,
			},
			err: errors.Errorf("some-err"),
		},
	} {

		t.Run(c.name, func(t *testing.T) {
			// each request will return a single samplestream
			querier := mkQuerier(mockHandler(&PrometheusResponse{
				Data: PrometheusData{
					ResultType: promql.ValueTypeVector,
					Result: []SampleStream{
						{
							Labels: []client.LabelAdapter{
								{Name: "a", Value: "1"},
							},
							Samples: []client.Sample{
								{
									Value:       1,
									TimestampMs: 1,
								},
							},
						},
					},
				},
			}, c.err))

			set, _, err := querier.Select(
				nil,
				exactMatch("__name__", astmapper.EmbeddedQueryFlag),
				exactMatch(astmapper.QueryLabel, astmapper.HexCodec.Encode(c.queries)),
			)

			if c.err != nil {
				require.EqualError(t, err, c.err.Error())
				return
			}

			var ct int
			for set.Next() {
				ct++
			}
			require.Equal(t, len(c.queries), ct)

		})
	}
}

func exactMatch(k, v string) *labels.Matcher {
	m, err := labels.NewMatcher(labels.MatchEqual, k, v)
	if err != nil {
		panic(err)
	}
	return m

}

func mkQuerier(handler Handler) *DownstreamQuerier {
	return &DownstreamQuerier{context.Background(), &PrometheusRequest{}, handler}
}
