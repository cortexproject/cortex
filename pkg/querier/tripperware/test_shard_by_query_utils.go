package tripperware

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querysharding"
)

func TestQueryShardQuery(t *testing.T, instantQueryCodec Codec, shardedPrometheusCodec Codec) {

	type queries struct {
		name           string
		expression     string
		shardingLabels []string
	}

	nonShardable := []queries{
		{
			name:       "aggregation",
			expression: "sum(http_requests_total)",
		},
		{
			name:       "outer aggregation with no grouping",
			expression: "count(sum by (pod) (http_requests_total))",
		},
		{
			name:       "outer aggregation with without grouping",
			expression: "count(sum without (pod) (http_requests_total))",
		},
		{
			name:       "aggregate expression with subquery",
			expression: `sum by (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
		},
		{
			name:       "aggregate without expression with subquery",
			expression: `sum without (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
		},
		{
			name:       "binary expression",
			expression: `http_requests_total{code="400"} / http_requests_total`,
		},
		{
			name:       "binary expression with constant",
			expression: `http_requests_total{code="400"} / 4`,
		},
		{
			name:       "binary expression with empty vector matching",
			expression: `http_requests_total{code="400"} / on () http_requests_total`,
		},
		{
			name:       "binary aggregation with different grouping labels",
			expression: `sum by (pod) (http_requests_total{code="400"}) / sum by (cluster) (http_requests_total)`,
		},
		{
			name:       "binary expression with vector matching and subquery",
			expression: `http_requests_total{code="400"} / on (pod) label_replace(metric, "dst_label", "$1", "src_label", "re")`,
		},
		{
			name:       "multiple binary expressions",
			expression: `(http_requests_total{code="400"} + http_requests_total{code="500"}) / http_requests_total`,
		},
		{
			name: "multiple binary expressions with empty vector matchers",
			expression: `
(http_requests_total{code="400"} + on (cluster, pod) http_requests_total{code="500"})
/ on ()
http_requests_total`,
		},
		{
			name:       "problematic query",
			expression: `sum(a by(lanel)`,
		},
	}

	shardableByLabels := []queries{
		{
			name:           "aggregation with grouping",
			expression:     "sum by (pod) (http_requests_total)",
			shardingLabels: []string{"pod"},
		},
		{
			name:           "aggregation with comparison",
			expression:     "avg by (Roles,type) (rss_service_message_handling) > 0.5",
			shardingLabels: []string{"Roles", "type"},
		},
		{
			name:           "multiple aggregations with grouping",
			expression:     "max by (pod) (sum by (pod, cluster) (http_requests_total))",
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary expression with vector matching",
			expression:     `http_requests_total{code="400"} / on (pod) http_requests_total`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary aggregation with same grouping labels",
			expression:     `sum by (pod) (http_requests_total{code="400"}) / sum by (pod) (http_requests_total)`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary expression with vector matching and grouping",
			expression:     `sum by (cluster, pod) (http_requests_total{code="400"}) / on (pod) sum by (cluster, pod) (http_requests_total)`,
			shardingLabels: []string{"pod"},
		},
		{
			name: "multiple binary expressions with vector matchers",
			expression: `
(http_requests_total{code="400"} + on (cluster, pod) http_requests_total{code="500"})
/ on (pod)
http_requests_total`,
			shardingLabels: []string{"pod"},
		},
		{
			name: "multiple binary expressions with grouping",
			expression: `
sum by (container) (
	(http_requests_total{code="400"} + on (cluster, pod, container) http_requests_total{code="500"})
	/ on (pod, container)
	http_requests_total
)`,
			shardingLabels: []string{"container"},
		},
		{
			name:           "multiple binary expressions with grouping",
			expression:     `(http_requests_total{code="400"} + on (pod) http_requests_total{code="500"}) / on (cluster, pod) http_requests_total`,
			shardingLabels: []string{"cluster", "pod"},
		},
		{
			name:           "histogram quantile",
			expression:     "histogram_quantile(0.95, sum(rate(metric[1m])) by (le, cluster))",
			shardingLabels: []string{"cluster"},
		},
	}

	shardableWithoutLabels := []queries{
		{
			name:           "aggregation without grouping",
			expression:     "sum without (pod) (http_requests_total)",
			shardingLabels: []string{"pod"},
		},
		{
			name:           "multiple aggregations with without grouping",
			expression:     "max without (pod) (sum without (pod, cluster) (http_requests_total))",
			shardingLabels: []string{"pod", "cluster"},
		},
		{
			name:           "binary expression with without vector matching and grouping",
			expression:     `sum without (cluster, pod) (http_requests_total{code="400"}) / ignoring (pod) sum without (cluster, pod) (http_requests_total)`,
			shardingLabels: []string{"pod", "cluster"},
		},
		{
			name:           "multiple binary expressions with without grouping",
			expression:     `(http_requests_total{code="400"} + ignoring (pod) http_requests_total{code="500"}) / ignoring (cluster, pod) http_requests_total`,
			shardingLabels: []string{"cluster", "pod"},
		},
		{
			name: "multiple binary expressions with without vector matchers",
			expression: `
(http_requests_total{code="400"} + ignoring (cluster, pod) http_requests_total{code="500"})
/ ignoring (pod)
http_requests_total`,
			shardingLabels: []string{"cluster", "pod"},
		},
	}

	type testCase struct {
		name           string
		path           string
		isShardable    bool
		shardSize      int
		codec          Codec
		responses      []string
		response       string
		shardingLabels []string
	}
	tests := []testCase{
		{
			name:           "should shard range query when query is shardable",
			path:           `/api/v1/query_range?end=1&start=0&step=120&query=sum(metric) by (pod,cluster_name)`,
			isShardable:    true,
			codec:          shardedPrometheusCodec,
			shardingLabels: []string{"pod", "cluster_name"},
			shardSize:      2,
			responses: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric","__job__":"a"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3]]}}}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric","__job__":"b"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3]]}}}}`,
			},
			response: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__job__":"a","__name__":"metric"},"values":[[1,"1"],[2,"2"],[3,"3"]]},{"metric":{"__job__":"b","__name__":"metric"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":12,"totalQueryableSamplesPerStep":[[1,2],[2,4],[3,6]]}}}}`,
		},
		{
			name:           "should shard instant query when query is shardable",
			path:           `/api/v1/query?time=120&query=sum(metric) by (pod,cluster_name)`,
			codec:          instantQueryCodec,
			shardSize:      2,
			shardingLabels: []string{"pod", "cluster_name"},
			isShardable:    true,
			responses: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			response: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		},
		{
			name:        "shold not shard if shard size is 1",
			path:        `/api/v1/query?time=120&query=sum(metric) by (pod,cluster_name)`,
			codec:       instantQueryCodec,
			shardSize:   1,
			isShardable: false,
			responses: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			response: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
		},
	}
	for _, query := range nonShardable {
		tests = append(tests, testCase{
			name:        fmt.Sprintf("non shardable query: %s", query.name),
			path:        fmt.Sprintf(`/api/v1/query?time=120&query=%s`, url.QueryEscape(query.expression)),
			codec:       instantQueryCodec,
			isShardable: false,
			responses: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			response: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
		})
		tests = append(tests, testCase{
			name:        fmt.Sprintf("non shardable query_range: %s", query.name),
			path:        fmt.Sprintf(`/api/v1/query_range?start=1&end=2&step=1&query=%s`, url.QueryEscape(query.expression)),
			codec:       shardedPrometheusCodec,
			isShardable: false,
			responses: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__job__":"a","__name__":"metric"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3]]}}}}`,
			},
			response: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__job__":"a","__name__":"metric"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3]]}}}}`,
		})
	}

	for _, query := range append(shardableWithoutLabels, shardableByLabels...) {
		tests = append(tests, testCase{
			name:           fmt.Sprintf("shardable query: %s", query.name),
			path:           fmt.Sprintf(`/api/v1/query?time=120&query=%s`, url.QueryEscape(query.expression)),
			codec:          instantQueryCodec,
			isShardable:    true,
			shardSize:      2,
			shardingLabels: query.shardingLabels,
			responses: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			response: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		})
		tests = append(tests, testCase{
			name:           fmt.Sprintf("shardable query_range: %s", query.name),
			path:           fmt.Sprintf(`/api/v1/query_range?start=1&end=2&step=1&query=%s`, url.QueryEscape(query.expression)),
			codec:          shardedPrometheusCodec,
			isShardable:    true,
			shardSize:      2,
			shardingLabels: query.shardingLabels,
			responses: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric","__job__":"a"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3]]}}}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric","__job__":"b"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3]]}}}}`,
			},
			response: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__job__":"a","__name__":"metric"},"values":[[1,"1"],[2,"2"],[3,"3"]]},{"metric":{"__job__":"b","__name__":"metric"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":12,"totalQueryableSamplesPerStep":[[1,2],[2,4],[3,6]]}}}}`,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sort.Strings(tt.shardingLabels)
			s := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					q := r.FormValue("query")
					expr, _ := parser.ParseExpr(q)
					shardIndex := int64(0)

					parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
						if selector, ok := n.(*parser.VectorSelector); ok {
							for _, matcher := range selector.LabelMatchers {
								if matcher.Name == querysharding.CortexShardByLabel {

									decoded, _ := base64.StdEncoding.DecodeString(matcher.Value)
									shardInfo := storepb.ShardInfo{}
									err := shardInfo.Unmarshal(decoded)
									require.NoError(t, err)
									sort.Strings(shardInfo.Labels)
									require.Equal(t, tt.shardingLabels, shardInfo.Labels)
									require.Equal(t, tt.isShardable, shardInfo.TotalShards > 0)
									shardIndex = shardInfo.ShardIndex
								}
							}
						}
						return nil
					})

					_, _ = w.Write([]byte(tt.responses[shardIndex]))
				}),
			)
			defer s.Close()

			u, err := url.Parse(s.URL)
			require.NoError(t, err)

			downstream := singleHostRoundTripper{
				host: u.Host,
				next: http.DefaultTransport,
			}

			roundtripper := NewRoundTripper(downstream, tt.codec, nil, ShardByMiddleware(log.NewNopLogger(), mockLimits{shardSize: tt.shardSize}, tt.codec))

			ctx := user.InjectOrgID(context.Background(), "1")

			req, err := http.NewRequest("GET", tt.path, http.NoBody)
			req = req.WithContext(ctx)

			require.NoError(t, err)
			resp, err := roundtripper.RoundTrip(req)

			require.NoError(t, err)
			require.NotNil(t, resp)

			contents, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tt.response, string(contents))
		})
	}
}

type mockLimits struct {
	maxQueryLookback  time.Duration
	maxQueryLength    time.Duration
	maxCacheFreshness time.Duration
	shardSize         int
}

func (m mockLimits) MaxQueryLookback(string) time.Duration {
	return m.maxQueryLookback
}

func (m mockLimits) MaxQueryLength(string) time.Duration {
	return m.maxQueryLength
}

func (mockLimits) MaxQueryParallelism(string) int {
	return 14 // Flag default.
}

func (m mockLimits) MaxCacheFreshness(string) time.Duration {
	return m.maxCacheFreshness
}

func (m mockLimits) QueryVerticalShardSize(userID string) int {
	return m.shardSize
}

type singleHostRoundTripper struct {
	host string
	next http.RoundTripper
}

func (s singleHostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = "http"
	r.URL.Host = s.host
	return s.next.RoundTrip(r)
}
