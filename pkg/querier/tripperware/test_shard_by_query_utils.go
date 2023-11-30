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
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	thanosquerysharding "github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querysharding"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestQueryShardQuery(t *testing.T, instantQueryCodec Codec, shardedPrometheusCodec Codec) {
	//parallel testing causes data race

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
		{
			name:       "aggregate by expression with label_replace, sharding label is dynamic",
			expression: `sum by (dst_label) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
		},
		{
			name:       "aggregate by expression with label_join, sharding label is dynamic",
			expression: `sum by (dst_label) (label_join(metric, "dst_label", ",", "src_label"))`,
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
			shardingLabels: []string{"pod"},
		},
		{
			name:           "histogram quantile",
			expression:     "histogram_quantile(0.95, sum(rate(metric[1m])) by (le, cluster))",
			shardingLabels: []string{"cluster"},
		},
		{
			name:           "aggregate by expression with label_replace, sharding label is not dynamic",
			expression:     `sum by (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "aggregate by expression with label_join, sharding label is not dynamic",
			expression:     `sum by (pod) (label_join(metric, "dst_label", ",", "src_label"))`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "label_join and aggregation on multiple labels. Can be sharded by the static one",
			expression:     `sum by (pod, dst_label) (label_join(metric, "dst_label", ",", "src_label"))`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "binary expression with vector matching and label_replace",
			expression:     `http_requests_total{code="400"} / on (pod) label_replace(metric, "dst_label", "$1", "src_label", "re")`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "nested label joins",
			expression:     `label_join(sum by (pod) (label_join(metric, "dst_label", ",", "src_label")), "dst_label1", ",", "dst_label")`,
			shardingLabels: []string{"pod"},
		},
		{
			name:           "complex query with label_replace, binary expr and aggregations on dynamic label",
			expression:     `sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace="kube-system"}[1d:5m])) by (instance, cluster) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes[1d:5m])) by (node, cluster), "instance", "$1", "node", "(.*)")) by (instance, cluster)`,
			shardingLabels: []string{"cluster"},
		},
		{
			name:           "complex query with label_replace and nested aggregations",
			expression:     `avg(label_replace(label_replace(avg(count_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="",container!="POD", node!="", }[1h] )*avg_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="",container!="POD", node!="", }[1h] )) by (namespace,container,pod,node,cluster_id) , "container_name","$1","container","(.+)"), "pod_name","$1","pod","(.+)")) by (namespace,container_name,pod_name,node,cluster_id)`,
			shardingLabels: []string{"namespace", "node", "cluster_id"},
		},
		{
			name:           "complex query with label_replace, nested aggregations and binary expressions",
			expression:     `sort_desc(avg(label_replace(label_replace(label_replace(count_over_time(container_memory_working_set_bytes{container!="", container!="POD", instance!="", }[1h] ), "node", "$1", "instance", "(.+)"), "container_name", "$1", "container", "(.+)"), "pod_name", "$1", "pod", "(.+)")*label_replace(label_replace(label_replace(avg_over_time(container_memory_working_set_bytes{container!="", container!="POD", instance!="", }[1h] ), "node", "$1", "instance", "(.+)"), "container_name", "$1", "container", "(.+)"), "pod_name", "$1", "pod", "(.+)")) by (namespace, container_name, pod_name, node, cluster_id))`,
			shardingLabels: []string{"namespace", "cluster_id"},
		},
		{
			name:           "aggregate expression with label_replace",
			expression:     `sum by (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
			shardingLabels: []string{"pod"},
		},
	}

	// Shardable by labels instant queries with matrix response
	shardableByLabelsMatrix := []queries{
		{
			name:           "subquery",
			expression:     "sum(http_requests_total) by (pod, cluster) [1h:1m]",
			shardingLabels: []string{"cluster", "pod"},
		},
		{
			name:           "subquery with function",
			expression:     "increase(sum(http_requests_total) by (pod, cluster) [1h:1m])",
			shardingLabels: []string{"cluster", "pod"},
		},
		{
			name:           "ignore vector matching with 2 aggregations",
			expression:     `sum(rate(node_cpu_seconds_total[3h])) by (cluster_id, mode) / ignoring(mode) group_left sum(rate(node_cpu_seconds_total[3h])) by (cluster_id)`,
			shardingLabels: []string{"cluster_id"},
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
			shardingLabels: []string{"pod", "cluster", model.MetricNameLabel},
		},
		{
			name:           "binary expression with outer without grouping",
			expression:     `sum(http_requests_total{code="400"} * http_requests_total) without (pod)`,
			shardingLabels: []string{model.MetricNameLabel, "pod"},
		},
		{
			name:           "binary expression with vector matching and outer without grouping",
			expression:     `sum(http_requests_total{code="400"} * ignoring(cluster) http_requests_total) without ()`,
			shardingLabels: []string{"__name__", "cluster"},
		},
		{
			name:           "multiple binary expressions with without grouping",
			expression:     `(http_requests_total{code="400"} + ignoring (pod) http_requests_total{code="500"}) / ignoring (cluster, pod) http_requests_total`,
			shardingLabels: []string{"cluster", "pod", model.MetricNameLabel},
		},
		{
			name: "multiple binary expressions with without vector matchers",
			expression: `
(http_requests_total{code="400"} + ignoring (cluster, pod) http_requests_total{code="500"})
/ ignoring (pod)
http_requests_total`,
			shardingLabels: []string{"cluster", "pod", model.MetricNameLabel},
		},
		{
			name:           "aggregate without expression with label_replace, sharding label is not dynamic",
			expression:     `sum without (dst_label) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
			shardingLabels: []string{"dst_label"},
		},
		{
			name:           "aggregate without expression with label_join, sharding label is not dynamic",
			expression:     `sum without (dst_label) (label_join(metric, "dst_label", ",", "src_label"))`,
			shardingLabels: []string{"dst_label"},
		},
		{
			name:           "aggregate without expression with label_replace",
			expression:     `sum without (pod) (label_replace(metric, "dst_label", "$1", "src_label", "re"))`,
			shardingLabels: []string{"pod", "dst_label"},
		},
		{
			name:           "binary expression",
			expression:     `http_requests_total{code="400"} / http_requests_total`,
			shardingLabels: []string{model.MetricNameLabel},
		},
		{
			name:           "binary expression among vector and scalar",
			expression:     `aaaa - bbb > 1000`,
			shardingLabels: []string{model.MetricNameLabel},
		},
		{
			name:           "binary expression with set operation",
			expression:     `aaaa and bbb`,
			shardingLabels: []string{model.MetricNameLabel},
		},
		{
			name:           "multiple binary expressions",
			expression:     `(http_requests_total{code="400"} + http_requests_total{code="500"}) / http_requests_total`,
			shardingLabels: []string{model.MetricNameLabel},
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
			name:        "should not shard if shard size is 1",
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
			shardSize:   2,
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
			shardSize:   2,
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

	for _, query := range shardableByLabelsMatrix {
		tests = append(tests, testCase{
			name:           fmt.Sprintf("shardable query: %s", query.name),
			path:           fmt.Sprintf(`/api/v1/query?time=120&query=%s`, url.QueryEscape(query.expression)),
			codec:          instantQueryCodec,
			isShardable:    true,
			shardSize:      2,
			shardingLabels: query.shardingLabels,
			responses: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","job":"foo"},"values":[[1,"1"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","job":"bar"},"values":[[2,"2"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			response: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","job":"bar"},"values":[[2,"2"]]},{"metric":{"__name__":"up","job":"foo"},"values":[[1,"1"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//parallel testing causes data race
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

			qa := thanosquerysharding.NewQueryAnalyzer()
			roundtripper := NewRoundTripper(downstream, tt.codec, nil, ShardByMiddleware(log.NewNopLogger(), mockLimits{shardSize: tt.shardSize}, tt.codec, qa))

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
	queryPriority     validation.QueryPriority
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

func (m mockLimits) QueryPriority(userID string) validation.QueryPriority {
	return m.queryPriority
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
