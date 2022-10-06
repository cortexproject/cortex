package metadata

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
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
)

func TestRequest(t *testing.T) {
	now := time.Now()
	codec := seriesCodec{now: func() time.Time {
		return now
	}, lookback: time.Hour}

	for _, tc := range []struct {
		url         string
		expectedURL string
		expected    tripperware.Request
		expectedErr error
	}{
		{
			url:         "/api/v1/series?end=1536716898&match%5B%5D=%7B__name__%3D~%22metric_0.%2A%22%7D&match%5B%5D=%7B__name__%3D~%22metric_1.%2A%22%7D&start=1536710400",
			expectedURL: "/api/v1/series?end=1536716898&match%5B%5D=%7B__name__%3D~%22metric_0.%2A%22%7D&match%5B%5D=%7B__name__%3D~%22metric_1.%2A%22%7D&start=1536710400",
			expected: &PrometheusSeriesRequest{
				Path:     "/api/v1/series",
				Start:    1536710400 * 1e3,
				End:      1536716898 * 1e3,
				Matchers: []string{"{__name__=~\"metric_0.*\"}", "{__name__=~\"metric_1.*\"}"},
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
			},
		},
		{
			url:         "/api/v1/series?match%5B%5D=%7B__name__%3D~%22metric_0.%2A%22%7D&match%5B%5D=%7B__name__%3D~%22metric_1.%2A%22%7D",
			expectedURL: fmt.Sprintf("/api/v1/series?end=%d&%s&start=%d", now.Add(5*time.Minute).Unix(), "match%5B%5D=%7B__name__%3D~%22metric_0.%2A%22%7D&match%5B%5D=%7B__name__%3D~%22metric_1.%2A%22%7D", now.Add(-1*time.Hour).Unix()),
			expected: &PrometheusSeriesRequest{
				Path:     "/api/v1/series",
				Start:    now.Add(-1*time.Hour).Unix() * 1e3,
				End:      now.Add(5*time.Minute).Unix() * 1e3,
				Matchers: []string{"{__name__=~\"metric_0.*\"}", "{__name__=~\"metric_1.*\"}"},
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
			body: `{"status":"success","data":[{"__name__":"metric0","label1":"value"}]}`,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			response := &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
			}
			resp, err := NewSeriesCodec(queryrange.Config{}).DecodeResponse(context.Background(), response, nil)
			require.NoError(t, err)

			// Reset response, as the above call will have consumed the body reader.
			response = &http.Response{
				StatusCode:    200,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
				ContentLength: int64(len(tc.body)),
			}
			resp2, err := NewSeriesCodec(queryrange.Config{}).EncodeResponse(context.Background(), resp)
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
			resps:        []string{`{"status":"success","data":[]}`},
			expectedResp: `{"status":"success","data":[]}`,
		},
		{
			name:         "single response",
			resps:        []string{`{"status":"success","data":[{"__name__":"metric0","label1":"value"}]}`},
			expectedResp: `{"status":"success","data":[{"__name__":"metric0","label1":"value"}]}`,
		},
		{
			name:         "multiple distinct response",
			resps:        []string{`{"status":"success","data":[{"__name__":"metric0"}]}`, `{"status":"success","data":[{"__name__":"metric1"}]}`},
			expectedResp: `{"status":"success","data":[{"__name__":"metric0"},{"__name__":"metric1"}]}`,
		},
		{
			name:         "duplicated response",
			resps:        []string{`{"status":"success","data":[{"__name__":"metric0","label1":"value"}]}`, `{"status":"success","data":[{"__name__":"metric0","label1":"value"}]}`},
			expectedResp: `{"status":"success","data":[{"__name__":"metric0","label1":"value"}]}`,
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
				dr, err := NewSeriesCodec(queryrange.Config{}).DecodeResponse(context.Background(), hr, nil)
				require.NoError(t, err)
				resps = append(resps, dr)
			}
			resp, err := NewSeriesCodec(queryrange.Config{}).MergeResponse(resps...)
			assert.Equal(t, err, tc.expectedErr)
			if err != nil {
				return
			}
			dr, err := NewSeriesCodec(queryrange.Config{}).EncodeResponse(context.Background(), resp)
			assert.Equal(t, err, tc.expectedErr)
			contents, err := io.ReadAll(dr.Body)
			assert.Equal(t, err, tc.expectedErr)

			var expectedResp PrometheusSeriesResponse
			err = json.Unmarshal([]byte(tc.expectedResp), &expectedResp)
			assert.NoError(t, err, "Error decoding expected response")

			var actualResp PrometheusSeriesResponse
			err = json.Unmarshal(contents, &actualResp)
			assert.NoError(t, err, "Error decoding actual response")

			// We can't simply compare strings here because the order of the series in the JSON output is not guaranteed.
			assert.Equal(t, actualResp, expectedResp)
		})
	}
}
