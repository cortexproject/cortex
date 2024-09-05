package instantquery

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
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

const testHistogramResponse = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"prometheus_http_request_duration_seconds","handler":"/metrics","instance":"localhost:9090","job":"prometheus"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"],[0,"0.0015060652591874421","0.001642375811042411","7"],[0,"0.001642375811042411","0.0017910235218841233","5"],[0,"0.0017910235218841233","0.001953125","13"],[0,"0.001953125","0.0021298979153618314","19"],[0,"0.0021298979153618314","0.0023226701464896895","13"],[0,"0.0023226701464896895","0.002532889755177753","13"],[0,"0.002532889755177753","0.002762135864009951","15"],[0,"0.002762135864009951","0.0030121305183748843","12"],[0,"0.0030121305183748843","0.003284751622084822","34"],[0,"0.003284751622084822","0.0035820470437682465","188"],[0,"0.0035820470437682465","0.00390625","372"],[0,"0.00390625","0.004259795830723663","400"],[0,"0.004259795830723663","0.004645340292979379","411"],[0,"0.004645340292979379","0.005065779510355506","425"],[0,"0.005065779510355506","0.005524271728019902","425"],[0,"0.005524271728019902","0.0060242610367497685","521"],[0,"0.0060242610367497685","0.006569503244169644","621"],[0,"0.006569503244169644","0.007164094087536493","593"],[0,"0.007164094087536493","0.0078125","506"],[0,"0.0078125","0.008519591661447326","458"],[0,"0.008519591661447326","0.009290680585958758","346"],[0,"0.009290680585958758","0.010131559020711013","285"],[0,"0.010131559020711013","0.011048543456039804","196"],[0,"0.011048543456039804","0.012048522073499537","129"],[0,"0.012048522073499537","0.013139006488339287","85"],[0,"0.013139006488339287","0.014328188175072986","65"],[0,"0.014328188175072986","0.015625","54"],[0,"0.015625","0.01703918332289465","53"],[0,"0.01703918332289465","0.018581361171917516","20"],[0,"0.018581361171917516","0.020263118041422026","21"],[0,"0.020263118041422026","0.022097086912079608","15"],[0,"0.022097086912079608","0.024097044146999074","11"],[0,"0.024097044146999074","0.026278012976678575","2"],[0,"0.026278012976678575","0.028656376350145972","3"],[0,"0.028656376350145972","0.03125","3"],[0,"0.04052623608284405","0.044194173824159216","2"]]}]}]}}`

func TestRequest(t *testing.T) {
	t.Parallel()
	codec := InstantQueryCodec

	for _, tc := range []struct {
		url         string
		expectedURL string
		expected    *tripperware.PrometheusRequest
		expectedErr error
	}{
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&stats=all&time=1536673680",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&stats=all&time=1536673680",
			expected: &tripperware.PrometheusRequest{
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
			expected: &tripperware.PrometheusRequest{
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
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=",
			expected: &tripperware.PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  0,
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

			if tc.expected.Time == 0 {
				now := time.Now()
				tc.expectedURL = fmt.Sprintf("%s%d", tc.expectedURL, now.Unix())
				tc.expected.Time = now.Unix() * 1e3
			}
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
			body: `{"status":"success","data":{"resultType":"string","result":[1,"foo"],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]],"peakSamples":10}}}}`,
		},
		{
			body: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1,"137"],[2,"137"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]],"peakSamples":10}}}}`,
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
		{
			body: testHistogramResponse,
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
	defaultReq := &tripperware.PrometheusRequest{
		Query: "sum(up)",
	}
	for _, tc := range []struct {
		name               string
		req                tripperware.Request
		resps              []string
		expectedResp       string
		expectedErr        error
		cancelBeforeDecode bool
		expectedDecodeErr  error
		cancelBeforeMerge  bool
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
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[],"stats":{"samples":{"totalQueryableSamples":0,"totalQueryableSamplesPerStep":[],"peakSamples":0}}}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[],"stats":{"samples":{"totalQueryableSamples":0,"totalQueryableSamplesPerStep":[],"peakSamples":0}}}}`,
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
			resps:        []string{`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]],"peakSamples":10}}}}`},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]],"peakSamples":10}}}}`,
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
			name:         "duplicated histogram responses",
			req:          defaultReq,
			resps:        []string{testHistogramResponse, testHistogramResponse},
			expectedResp: testHistogramResponse,
		},
		{
			name: "duplicated response with stats",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]],"peakSamples":10}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]],"peakSamples":10}}}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]],"peakSamples":10}}}}`,
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
			name: "merge two histogram responses",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"histogram":[1719528800,{"count":"1","sum":"0","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"histogram":[1719528800,{"count":"1","sum":"0","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]},{"metric":{"__name__":"up","job":"foo"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
		},
		{
			name: "merge two responses with sort",
			req:  &tripperware.PrometheusRequest{Query: "sort(sum by (job) (up))"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]},{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
		},
		{
			name: "merge two histogram responses with sort",
			req:  &tripperware.PrometheusRequest{Query: "sort(sum by (job) (up))"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"histogram":[1719528880,{"count":"1","sum":"0","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"histogram":[1719528880,{"count":"1","sum":"0","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]},{"metric":{"__name__":"up","job":"foo"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
		},
		{
			name: "merge two responses with sort_desc",
			req:  &tripperware.PrometheusRequest{Query: "sort_desc(sum by (job) (up))"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
		},
		{
			name: "merge two histogram responses with sort_desc",
			req:  &tripperware.PrometheusRequest{Query: "sort_desc(sum by (job) (up))"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"histogram":[1719528880,{"count":"1","sum":"0","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]},{"metric":{"__name__":"up","job":"bar"},"histogram":[1719528880,{"count":"1","sum":"0","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
		},
		{
			name: "merge two responses with topk",
			req:  &tripperware.PrometheusRequest{Query: "topk(10, up) by(job)"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]},{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
		},
		{
			name: "merge two histogram responses with topk",
			req:  &tripperware.PrometheusRequest{Query: "topk(10, up) by(job)"},
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"histogram":[1719528880,{"count":"1","sum":"0","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"histogram":[1719528871.898,{"count":"6342","sum":"43.31319875499995","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]},{"metric":{"__name__":"up","job":"bar"},"histogram":[1719528880,{"count":"1","sum":"0","buckets":[[0,"0.0013810679320049755","0.0015060652591874421","1"]]}]}]}}`,
		},
		{
			name: "merge with warnings.",
			req:  &tripperware.PrometheusRequest{Query: "topk(10, up) by(job)"},
			resps: []string{
				`{"status":"success","warnings":["warning1","warning2"],"data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","warnings":["warning1","warning3"],"data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]},{"metric":{"__name__":"up","job":"bar"},"value":[1,"2"]}]},"warnings":["warning1","warning2","warning3"]}`,
		},
		{
			name: "merge two responses with stats",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]],"peakSamples":10}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]],"peakSamples":10}}}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]],"peakSamples":10}}}}`,
		},
		{
			name: "merge two responses with stats, the peak samples should be larger one among the responses",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]],"peakSamples":10}}}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,10]],"peakSamples":15}}}}`,
			},
			expectedResp: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]},{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[1,20]],"peakSamples":15}}}}`,
		},
		{
			name: "responses don't contain vector, should return an error",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"string","result":[1662682521.409,"foo"]}}`,
				`{"status":"success","data":{"resultType":"string","result":[1662682521.409,"foo"]}}`,
			},
			expectedErr: errors.New("unexpected result type: string"),
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
		{
			name: "context cancelled before decoding response",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}]}}`,
			},
			expectedDecodeErr:  context.Canceled,
			cancelBeforeDecode: true,
		},
		{
			name: "context cancelled before merging response",
			req:  defaultReq,
			resps: []string{
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"foo"},"value":[1,"1"]}]}}`,
				`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"bar"},"value":[2,"2"]}]}}`,
			},
			expectedErr:       context.Canceled,
			cancelBeforeMerge: true,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancelCtx := context.WithCancel(context.Background())

			var resps []tripperware.Response
			for _, r := range tc.resps {
				hr := &http.Response{
					StatusCode: 200,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewBuffer([]byte(r))),
				}

				if tc.cancelBeforeDecode {
					cancelCtx()
				}
				dr, err := InstantQueryCodec.DecodeResponse(ctx, hr, nil)
				assert.Equal(t, tc.expectedDecodeErr, err)
				if err != nil {
					cancelCtx()
					return
				}
				resps = append(resps, dr)
			}

			if tc.cancelBeforeMerge {
				cancelCtx()
			}
			resp, err := InstantQueryCodec.MergeResponse(ctx, tc.req, resps...)
			assert.Equal(t, tc.expectedErr, err)
			if err != nil {
				cancelCtx()
				return
			}
			dr, err := InstantQueryCodec.EncodeResponse(ctx, resp)
			assert.Equal(t, tc.expectedErr, err)
			contents, err := io.ReadAll(dr.Body)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedResp, string(contents))
			cancelCtx()
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
			r := tripperware.PrometheusResponse{
				Data: tripperware.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
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
