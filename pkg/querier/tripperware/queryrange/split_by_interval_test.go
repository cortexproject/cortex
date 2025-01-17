package queryrange

import (
	"context"
	io "io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

const (
	seconds         = 1e3 // 1e3 milliseconds per second.
	queryStoreAfter = 24 * time.Hour
	lookbackDelta   = 5 * time.Minute
)

func TestNextIntervalBoundary(t *testing.T) {
	t.Parallel()
	for i, tc := range []struct {
		in, step, out int64
		interval      time.Duration
	}{
		// Smallest possible period is 1 millisecond
		{0, 1, toMs(day) - 1, day},
		{0, 1, toMs(time.Hour) - 1, time.Hour},
		// A more standard example
		{0, 15 * seconds, toMs(day) - 15*seconds, day},
		{0, 15 * seconds, toMs(time.Hour) - 15*seconds, time.Hour},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 15 * seconds, toMs(day) - (15-1)*seconds, day},
		{1 * seconds, 15 * seconds, toMs(time.Hour) - (15-1)*seconds, time.Hour},
		// Move start time forward 14 seconds; end time moves the same
		{14 * seconds, 15 * seconds, toMs(day) - (15-14)*seconds, day},
		{14 * seconds, 15 * seconds, toMs(time.Hour) - (15-14)*seconds, time.Hour},
		// Now some examples where the period does not divide evenly into a day:
		// 1 day modulus 35 seconds = 20 seconds
		{0, 35 * seconds, toMs(day) - 20*seconds, day},
		// 1 hour modulus 35 sec = 30  (3600 mod 35 = 30)
		{0, 35 * seconds, toMs(time.Hour) - 30*seconds, time.Hour},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 35 * seconds, toMs(day) - (20-1)*seconds, day},
		{1 * seconds, 35 * seconds, toMs(time.Hour) - (30-1)*seconds, time.Hour},
		// If the end time lands exactly on midnight we stop one period before that
		{20 * seconds, 35 * seconds, toMs(day) - 35*seconds, day},
		{30 * seconds, 35 * seconds, toMs(time.Hour) - 35*seconds, time.Hour},
		// This example starts 35 seconds after the 5th one ends
		{toMs(day) + 15*seconds, 35 * seconds, 2*toMs(day) - 5*seconds, day},
		{toMs(time.Hour) + 15*seconds, 35 * seconds, 2*toMs(time.Hour) - 15*seconds, time.Hour},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.out, nextIntervalBoundary(tc.in, tc.step, tc.interval))
		})
	}
}

func TestSplitQuery(t *testing.T) {
	t.Parallel()
	for i, tc := range []struct {
		input    tripperware.Request
		expected []tripperware.Request
		interval time.Duration
	}{
		{
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   60 * 60 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   60 * 60 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: day,
		},
		{
			input: &tripperware.PrometheusRequest{
				Start: 60 * 60 * seconds,
				End:   60 * 60 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 60 * 60 * seconds,
					End:   60 * 60 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: day,
		},
		{
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   60 * 60 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   60 * 60 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: day,
		},
		{
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   3 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   3 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   2 * 24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo @ start()",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   (24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo @ 0.000",
				},
				&tripperware.PrometheusRequest{
					Start: 24 * 3600 * seconds,
					End:   2 * 24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo @ 0.000",
				},
			},
			interval: day,
		},
		{
			input: &tripperware.PrometheusRequest{
				Start: 0,
				End:   2 * 3 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 0,
					End:   (3 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&tripperware.PrometheusRequest{
					Start: 3 * 3600 * seconds,
					End:   2 * 3 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &tripperware.PrometheusRequest{
				Start: 3 * 3600 * seconds,
				End:   3 * 24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 3 * 3600 * seconds,
					End:   (24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&tripperware.PrometheusRequest{
					Start: 24 * 3600 * seconds,
					End:   (2 * 24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&tripperware.PrometheusRequest{
					Start: 2 * 24 * 3600 * seconds,
					End:   3 * 24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: day,
		},
		{
			input: &tripperware.PrometheusRequest{
				Start: 2 * 3600 * seconds,
				End:   3 * 3 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []tripperware.Request{
				&tripperware.PrometheusRequest{
					Start: 2 * 3600 * seconds,
					End:   (3 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&tripperware.PrometheusRequest{
					Start: 3 * 3600 * seconds,
					End:   (2 * 3 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&tripperware.PrometheusRequest{
					Start: 2 * 3 * 3600 * seconds,
					End:   3 * 3 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: 3 * time.Hour,
		},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			days, err := splitQuery(tc.input, tc.interval)
			require.NoError(t, err)
			require.Equal(t, tc.expected, days)
		})
	}
}

func TestSplitByDay(t *testing.T) {
	t.Parallel()
	mergedResponse, err := PrometheusCodec.MergeResponse(context.Background(), nil, parsedResponse, parsedResponse)
	require.NoError(t, err)

	mergedHTTPResponse, err := PrometheusCodec.EncodeResponse(context.Background(), mergedResponse)
	require.NoError(t, err)

	mergedHTTPResponseBody, err := io.ReadAll(mergedHTTPResponse.Body)
	require.NoError(t, err)

	for i, tc := range []struct {
		path, expectedBody string
		expectedQueryCount int32
	}{
		{query, string(mergedHTTPResponseBody), 2},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			var actualCount atomic.Int32
			s := httptest.NewServer(
				middleware.AuthenticateUser.Wrap(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						actualCount.Inc()
						_, _ = w.Write([]byte(responseBody))
					}),
				),
			)
			defer s.Close()

			u, err := url.Parse(s.URL)
			require.NoError(t, err)

			interval := func(_ context.Context, _ tripperware.Request) (time.Duration, error) { return 24 * time.Hour, nil }
			roundtripper := tripperware.NewRoundTripper(singleHostRoundTripper{
				host: u.Host,
				next: http.DefaultTransport,
			}, PrometheusCodec, nil, NewLimitsMiddleware(mockLimits{}, 5*time.Minute), SplitByIntervalMiddleware(interval, mockLimits{}, PrometheusCodec, nil, queryStoreAfter, lookbackDelta))

			req, err := http.NewRequest("GET", tc.path, http.NoBody)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "1")
			req = req.WithContext(ctx)

			resp, err := roundtripper.RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			bs, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(bs))
			require.Equal(t, tc.expectedQueryCount, actualCount.Load())
		})
	}
}

func Test_evaluateAtModifier(t *testing.T) {
	const (
		start, end = int64(1546300800), int64(1646300800)
	)
	for _, tt := range []struct {
		in, expected      string
		expectedErrorCode int
	}{
		{
			in:       "topk(5, rate(http_requests_total[1h] @ start()))",
			expected: "topk(5, rate(http_requests_total[1h] @ 1546300.800))",
		},
		{
			in:       "topk(5, rate(http_requests_total[1h] @ 0))",
			expected: "topk(5, rate(http_requests_total[1h] @ 0.000))",
		},
		{
			in:       "http_requests_total[1h] @ 10.001",
			expected: "http_requests_total[1h] @ 10.001",
		},
		{
			in: `min_over_time(
				sum by(cluster) (
					rate(http_requests_total[5m] @ end())
				)[10m:]
			)
			or
			max_over_time(
				stddev_over_time(
					deriv(
						rate(http_requests_total[10m] @ start())
					[5m:1m])
				[2m:])
			[10m:])`,
			expected: `min_over_time(
				sum by(cluster) (
					rate(http_requests_total[5m] @ 1646300.800)
				)[10m:]
			)
			or
			max_over_time(
				stddev_over_time(
					deriv(
						rate(http_requests_total[10m] @ 1546300.800)
					[5m:1m])
				[2m:])
			[10m:])`,
		},
		{
			in:       `irate(kube_pod_info{namespace="test"}[1h:1m] @ start())`,
			expected: `irate(kube_pod_info{namespace="test"}[1h:1m] @ 1546300.800)`,
		},
		{
			in:       `irate(kube_pod_info{namespace="test"} @ end()[1h:1m] @ start())`,
			expected: `irate(kube_pod_info{namespace="test"} @ 1646300.800 [1h:1m] @ 1546300.800)`,
		},
		{
			// parse error: @ modifier must be preceded by an instant vector selector or range vector selector or a subquery
			in:                "sum(http_requests_total[5m]) @ 10.001",
			expectedErrorCode: http.StatusBadRequest,
		},
	} {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			out, err := evaluateAtModifierFunction(tt.in, start, end)
			if tt.expectedErrorCode != 0 {
				require.Error(t, err)
				httpResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "returned error is not an httpgrpc response")
				require.Equal(t, tt.expectedErrorCode, int(httpResp.Code))
			} else {
				require.NoError(t, err)
				expectedExpr, err := parser.ParseExpr(tt.expected)
				require.NoError(t, err)
				require.Equal(t, expectedExpr.String(), out)
			}
		})
	}
}

func TestDynamicIntervalFn(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		baseSplitInterval      time.Duration
		req                    tripperware.Request
		expectedInterval       time.Duration
		expectedError          bool
		maxQueryIntervalSplits int
		maxDaysOfDataFetched   int
	}{
		{
			baseSplitInterval: day,
			name:              "failed to parse request, return default interval",
			req: &tripperware.PrometheusRequest{
				Query: "up[aaa",
				Start: 0,
				End:   10 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
			},
			maxQueryIntervalSplits: 30,
			maxDaysOfDataFetched:   200,
			expectedInterval:       day,
			expectedError:          true,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range no limits, expect split by 1 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   30 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up",
			},
			expectedInterval: day,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range with 20 max splits, expect split by 2 day",
			req: &tripperware.PrometheusRequest{
				Start: 30 * 24 * 3600 * seconds,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up",
			},
			maxQueryIntervalSplits: 20,
			expectedInterval:       2 * day,
		},
		{
			baseSplitInterval: day,
			name:              "60 day range, expect split by 4 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up",
			},
			maxQueryIntervalSplits: 15,
			expectedInterval:       4 * day,
		},
		{
			baseSplitInterval: day,
			name:              "61 day range, expect split by 5 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   61 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up",
			},
			maxQueryIntervalSplits: 15,
			expectedInterval:       5 * day,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range short matrix selector with 200 days fetched limit, expect split by 1 day",
			req: &tripperware.PrometheusRequest{
				Start: 30 * 24 * 3600 * seconds,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "avg_over_time(up[1h])",
			},
			maxDaysOfDataFetched: 200,
			expectedInterval:     day,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range long matrix selector with 200 days fetched limit, expect split by 1 day",
			req: &tripperware.PrometheusRequest{
				Start: 30 * 24 * 3600 * seconds,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "avg_over_time(up[20d])",
			},
			maxDaysOfDataFetched: 200,
			expectedInterval:     4 * day,
		},
		{
			baseSplitInterval: day,
			name:              "60 day range, expect split by 7 day",
			req: &tripperware.PrometheusRequest{
				Start: (2 * 24 * 3600 * seconds) + (3600*seconds - 120),
				End:   (61 * 24 * 3600 * seconds) + (2*3600*seconds + 500),
				Step:  30 * 60 * seconds,
				Query: "rate(up[1d]) + rate(up[2d]) + rate(up[5d]) + rate(up[15d])",
			},
			maxDaysOfDataFetched: 200,
			expectedInterval:     7 * day,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range, expect split by 7 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100 * 24 * 3600 * seconds,
				Step:  60 * 60 * seconds,
				Query: "up[5d:10m]",
			},
			maxQueryIntervalSplits: 100,
			maxDaysOfDataFetched:   300,
			expectedInterval:       4 * day,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				SplitQueriesByInterval:          tc.baseSplitInterval,
				SplitQueriesByIntervalMaxSplits: tc.maxQueryIntervalSplits,
			}
			ctx := user.InjectOrgID(context.Background(), "1")
			interval, err := dynamicIntervalFn(cfg, mockLimits{}, querysharding.NewQueryAnalyzer(), queryStoreAfter, lookbackDelta, tc.maxDaysOfDataFetched)(ctx, tc.req)
			require.Equal(t, tc.expectedInterval, interval)
			if !tc.expectedError {
				require.Nil(t, err)
			}
		})
	}
}
