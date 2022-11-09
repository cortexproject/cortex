package instantquery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func TestRequest(t *testing.T) {
	now := time.Now()
	codec := instantQueryCodec{now: func() time.Time {
		return now
	}}

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
		t.Run(tc.url, func(t *testing.T) {
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

func TestResponse(t *testing.T) {
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
		t.Run(strconv.Itoa(i), func(t *testing.T) {
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
	for _, tc := range []struct {
		name         string
		resps        []string
		expectedResp string
		expectedErr  error
	}{
		{
			name:         "empty response",
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[]}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[]}}`,
		},
		{
			name:         "empty response with stats",
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[],"stats":{"samples":{"totalQueryableSamples":0,"totalQueryableSamplesPerStep":[]}}}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[],"stats":{"samples":{"totalQueryableSamples":0,"totalQueryableSamplesPerStep":[]}}}}`,
		},
		{
			name:         "single response",
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
		},
		{
			name:         "single response with stats",
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
		},
		{
			name: "duplicated response",
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}]}}`,
		},
		{
			name: "duplicated response with stats",
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		},
		{
			name: "merge two responses",
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
		},
		{
			name: "merge two responses with stats",
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]]}}}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]]}}}}`,
		},
		{
			name: "responses don't contain vector, should return an error",
			resps: []string{
				`{"status":"success","data":{"resultType":"string","result":[1662682521.409,"foo"]}}`,
				`{"status":"success","data":{"resultType":"string","result":[1662682521.409,"foo"]}}`,
			},
			expectedErr: fmt.Errorf("unexpected result type on instant query: %s", "string"),
		},
		{
			name: "single matrix response",
			resps: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up"},"values":[[1,"1"],[2,"2"]]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up"},"values":[[1,"1"],[2,"2"]]}]}}`,
		},
		{
			name: "multiple matrix responses without duplicated series",
			resps: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]}]}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"foo"},"values":[[3,"3"],[4,"4"]]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]},{"metric":{"__name__":"foo"},"values":[[3,"3"],[4,"4"]]}]}}`,
		},
		{
			name: "multiple matrix responses with duplicated series, but not same samples",
			resps: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]}]}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[3,"3"]]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`,
		},
		{
			name: "multiple matrix responses with duplicated series and same samples",
			resps: []string{
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"]]}]}}`,
				`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"bar"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			resp, err := InstantQueryCodec.MergeResponse(resps...)
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
