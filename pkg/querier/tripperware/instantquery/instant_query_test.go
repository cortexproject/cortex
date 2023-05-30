package instantquery

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func TestRequest(t *testing.T) {
	t.Parallel()
	now := time.Now()
	codec := InstantQueryCodec

	for _, tc := range []struct {
		url         string
		expectedURL string
		expected    tripperware.Request
		expectedErr error
	}{
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&stats=all&time=1536673680",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&stats=all&time=1536673680",
			expected: &PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  1536673680 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "all",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
			},
		},
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680",
			expected: &PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  1536673680 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
			},
		},
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29",
			expectedURL: fmt.Sprintf("%s%d", "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=", now.Unix()),
			expected: &PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  now.Unix() * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.url, func(t *testing.T) {
			t.Parallel()
			r, err := http.NewRequest("GET", tc.url, nil)
			require.NoError(t, err)
			r.Header.Add("Test-Header", "test")

			ctx := user.InjectOrgID(context.Background(), "1")

			// Get a deep copy of the request with Context changed to ctx
			r = r.Clone(ctx)

			req, err := codec.DecodeRequest(ctx, r, []string{"Test-Header"})
			if err != nil {
				require.EqualValues(t, tc.expectedErr, err)
				return
			}
			require.EqualValues(t, tc.expected, req)

			rdash, err := codec.EncodeRequest(context.Background(), req)
			require.NoError(t, err)
			require.EqualValues(t, tc.expectedURL, rdash.RequestURI)
		})
	}
}

func TestGzippedResponse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		body   string
		status int
		err    error
	}{
		{
			body:   `{"status":"success","data":{"resultType":"string","result":[1,"foo"]}}`,
			status: 200,
		},
		{
			body:   `error generic 400`,
			status: 400,
			err:    httpgrpc.Errorf(400, "error generic 400"),
		},
		{
			status: 400,
			err:    httpgrpc.Errorf(400, ""),
		},
	} {
		for _, c := range []bool{true, false} {
			c := c
			t.Run(fmt.Sprintf("compressed %t [%s]", c, tc.body), func(t *testing.T) {
				t.Parallel()

				h := http.Header{
					"Content-Type": []string{"application/json"},
				}

				responseBody := bytes.NewBuffer([]byte(tc.body))
				if c {
					h.Set("Content-Encoding", "gzip")
					var buf bytes.Buffer
					w := gzip.NewWriter(&buf)
					_, err := w.Write([]byte(tc.body))
					require.NoError(t, err)
					w.Close()
					responseBody = &buf
				}

				response := &http.Response{
					StatusCode: tc.status,
					Header:     h,
					Body:       io.NopCloser(responseBody),
				}
				r, err := InstantQueryCodec.DecodeResponse(context.Background(), response, nil)
				require.Equal(t, tc.err, err)

				if err == nil {
					resp, err := json.Marshal(r)
					require.NoError(t, err)

					require.Equal(t, tc.body, string(resp))
				}
			})
		}
	}
}

func TestResponse(t *testing.T) {
	t.Parallel()
	for i, tc := range []struct {
		body string
	}{
		{
			body: `{"status":"success","data":{"resultType":"string","result":[1,"foo"]}}`,
		},
		{
			body: `{"status":"success","data":{"resultType":"string","result":[1,"foo"],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]]}}}}`,
		},
		{
			body: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1,"137"],[2,"137"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]]}}}}`,
		},
		{
			body: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1,"137"],[2,"137"]]}]}}`,
		},
		{
			body: `{"status":"success","data":{"resultType":"scalar","result":[1,"13"]}}`,
		},
		{
			body: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1266464.0146205237"]}]}}`,
		},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			response := &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
			}
			resp, err := InstantQueryCodec.DecodeResponse(context.Background(), response, nil)
			require.NoError(t, err)

			// Reset response, as the above call will have consumed the body reader.
			response = &http.Response{
				StatusCode:    200,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
				ContentLength: int64(len(tc.body)),
			}
			resp2, err := InstantQueryCodec.EncodeResponse(context.Background(), resp)
			require.NoError(t, err)
			assert.Equal(t, response, resp2)
		})
	}
}

func TestMergeResponse(t *testing.T) {
	t.Parallel()
	defaultReq := &PrometheusRequest{
		Query: "sum(up)",
	}
	for _, tc := range []struct {
		name         string
		req          tripperware.Request
		resps        []string
		expectedResp string
		expectedErr  error
	}{
		{
			name:         "empty response",
			req:          defaultReq,
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[]}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[]}}`,
		},
		{
			name:         "empty response with stats",
			req:          defaultReq,
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[],"stats":{"samples":{"totalQueryableSamples":0,"totalQueryableSamplesPerStep":[]}}}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[],"stats":{"samples":{"totalQueryableSamples":0,"totalQueryableSamplesPerStep":[]}}}}`,
		},
		{
			name:         "single response",
			req:          defaultReq,
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
		},
		{
			name:         "single response with stats",
			req:          defaultReq,
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
		},
		{
			name: "duplicated response",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
		},
		{
			name: "duplicated response with stats",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		},
		{
			name: "merge two responses",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
		},
		{
			name: "merge two responses with sort",
			req:  &PrometheusRequest{Query: "sort(sum by (job) (up))"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]},{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
		},
		{
			name: "merge two responses with sort_desc",
			req:  &PrometheusRequest{Query: "sort_desc(sum by (job) (up))"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
		},
		{
			name: "merge two responses with topk",
			req:  &PrometheusRequest{Query: "topk(10, up) by(job)"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]},{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
		},
		{
			name: "merge two responses with stats",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		},
		{
			name: "responses don't contain vector, should return an error",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"string","result":[1662682521.409,"foo"]}}`,
				`{"status":"success","data":{"resultType":"string","result":[1662682521.409,"foo"]}}`,
			},
			expectedErr: fmt.Errorf("unexpected result type on instant query: %s", "string"),
		},
		{
			name: "single matrix response",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up"},"values":[[1,"1"],[2,"2"]]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up"},"values":[[1,"1"],[2,"2"]]}]}}`,
		},
		{
			name: "multiple matrix responses without duplicated series",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]}]}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"foo"},"values":[[3,"3"],[4,"4"]]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]},{"metric":{"__name__":"foo"},"values":[[3,"3"],[4,"4"]]}]}}`,
		},
		{
			name: "multiple matrix responses with duplicated series, but not same samples",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]}]}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[3,"3"]]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`,
		},
		{
			name: "multiple matrix responses with duplicated series and same samples",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]}]}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var resps []tripperware.Response
			for _, r := range tc.resps {
				hr := &http.Response{
					StatusCode: 200,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewBuffer([]byte(r))),
				}
				dr, err := InstantQueryCodec.DecodeResponse(context.Background(), hr, nil)
				require.NoError(t, err)
				resps = append(resps, dr)
			}
			resp, err := InstantQueryCodec.MergeResponse(context.Background(), tc.req, resps...)
			assert.Equal(t, err, tc.expectedErr)
			if err != nil {
				return
			}
			dr, err := InstantQueryCodec.EncodeResponse(context.Background(), resp)
			assert.Equal(t, err, tc.expectedErr)
			contents, err := io.ReadAll(dr.Body)
			assert.Equal(t, err, tc.expectedErr)
			assert.Equal(t, string(contents), tc.expectedResp)
		})
	}
}

func Test_sortPlanForQuery(t *testing.T) {
	tc := []struct {
		query        string
		expectedPlan sortPlan
		err          bool
	}{
		{
			query:        "invalid(10, up)",
			expectedPlan: mergeOnly,
			err:          true,
		},
		{
			query:        "topk(10, up)",
			expectedPlan: mergeOnly,
			err:          false,
		},
		{
			query:        "bottomk(10, up)",
			expectedPlan: mergeOnly,
			err:          false,
		},
		{
			query:        "1 + topk(10, up)",
			expectedPlan: sortByLabels,
			err:          false,
		},
		{
			query:        "1 + sort_desc(sum by (job) (up) )",
			expectedPlan: sortByValuesDesc,
			err:          false,
		},
		{
			query:        "sort(topk by (job) (10, up))",
			expectedPlan: sortByValuesAsc,
			err:          false,
		},
		{
			query:        "topk(5, up) by (job) + sort_desc(up)",
			expectedPlan: sortByValuesDesc,
			err:          false,
		},
		{
			query:        "sort(up) + topk(5, up) by (job)",
			expectedPlan: sortByValuesAsc,
			err:          false,
		},
		{
			query:        "sum(up) by (job)",
			expectedPlan: sortByLabels,
			err:          false,
		},
	}

	for _, tc := range tc {
		t.Run(tc.query, func(t *testing.T) {
			p, err := sortPlanForQuery(tc.query)
			if tc.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedPlan, p)
			}
		})
	}
}

func Benchmark_Decode(b *testing.B) {
	maxSamplesCount := 1000000
	samples := make([]tripperware.SampleStream, maxSamplesCount)

	for i := 0; i < maxSamplesCount; i++ {
		samples[i].Labels = append(samples[i].Labels, cortexpb.LabelAdapter{Name: fmt.Sprintf("Sample%v", i), Value: fmt.Sprintf("Value%v", i)})
		samples[i].Labels = append(samples[i].Labels, cortexpb.LabelAdapter{Name: fmt.Sprintf("Sample2%v", i), Value: fmt.Sprintf("Value2%v", i)})
		samples[i].Labels = append(samples[i].Labels, cortexpb.LabelAdapter{Name: fmt.Sprintf("Sample3%v", i), Value: fmt.Sprintf("Value3%v", i)})
		samples[i].Samples = append(samples[i].Samples, cortexpb.Sample{TimestampMs: int64(i), Value: float64(i)})
	}

	for name, tc := range map[string]struct {
		sampleStream []tripperware.SampleStream
	}{
		"100 samples": {
			sampleStream: samples[:100],
		},
		"1000 samples": {
			sampleStream: samples[:1000],
		},
		"10000 samples": {
			sampleStream: samples[:10000],
		},
		"100000 samples": {
			sampleStream: samples[:100000],
		},
		"1000000 samples": {
			sampleStream: samples[:1000000],
		},
	} {
		b.Run(name, func(b *testing.B) {
			r := PrometheusInstantQueryResponse{
				Data: PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: PrometheusInstantQueryResult{
						Result: &PrometheusInstantQueryResult_Matrix{
							Matrix: &Matrix{
								SampleStreams: tc.sampleStream,
							},
						},
					},
				},
			}

			body, err := json.Marshal(r)
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				response := &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBuffer(body)),
				}
				_, err := InstantQueryCodec.DecodeResponse(context.Background(), response, nil)
				require.NoError(b, err)
			}
		})
	}

}
