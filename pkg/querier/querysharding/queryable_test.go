package querysharding

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
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
				&queryrange.APIResponse{},
				nil,
			)),
			fn: func(t *testing.T, q *DownstreamQuerier) {

				expected := &queryrange.APIResponse{
					Status: "success",
					Data: queryrange.Response{
						ResultType: promql.ValueTypeVector,
					},
				}

				// override handler func to assert new query has been substituted
				q.Handler = queryrange.HandlerFunc(
					func(ctx context.Context, req *queryrange.Request) (*queryrange.APIResponse, error) {
						require.Equal(t, `http_requests_total{cluster="prod"}`, req.Query)
						return expected, nil
					},
				)

				_, _, err := q.Select(
					nil,
					exactMatch("__name__", astmapper.EmbeddedQueryFlag),
					exactMatch(astmapper.QueryLabel, hexEncode(`http_requests_total{cluster="prod"}`)),
				)
				require.Nil(t, err)
			},
		},
		{
			name: "propagates response error",
			querier: mkQuerier(mockHandler(
				&queryrange.APIResponse{
					Error: "SomeErr",
				},
				nil,
			)),
			fn: func(t *testing.T, q *DownstreamQuerier) {
				set, _, err := q.Select(
					nil,
					exactMatch("__name__", astmapper.EmbeddedQueryFlag),
					exactMatch(astmapper.QueryLabel, hexEncode(`http_requests_total{cluster="prod"}`)),
				)
				require.Nil(t, set)
				require.EqualError(t, err, "SomeErr")
			},
		},
		{
			name: "returns SeriesSet",
			querier: mkQuerier(mockHandler(
				&queryrange.APIResponse{
					Data: queryrange.Response{
						ResultType: promql.ValueTypeVector,
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
				},
				nil,
			)),
			fn: func(t *testing.T, q *DownstreamQuerier) {
				set, _, err := q.Select(
					nil,
					exactMatch("__name__", astmapper.EmbeddedQueryFlag),
					exactMatch(astmapper.QueryLabel, hexEncode(`http_requests_total{cluster="prod"}`)),
				)
				require.Nil(t, err)
				require.Equal(
					t,
					newSeriesSet([]queryrange.SampleStream{
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

func exactMatch(k, v string) *labels.Matcher {
	m, err := labels.NewMatcher(labels.MatchEqual, k, v)
	if err != nil {
		panic(err)
	}
	return m

}

func mkQuerier(handler queryrange.Handler) *DownstreamQuerier {
	return &DownstreamQuerier{context.Background(), &queryrange.Request{}, handler}
}

func hexEncode(str string) string {
	return hex.EncodeToString([]byte(str))
}
