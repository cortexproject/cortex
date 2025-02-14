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
	longQuery       = "/api/v1/query_range?end=1539266098&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&stats=all&step=1200"
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

	mergedHTTPResponse, err := PrometheusCodec.EncodeResponse(context.Background(), nil, mergedResponse)
	require.NoError(t, err)

	mergedHTTPResponseBody, err := io.ReadAll(mergedHTTPResponse.Body)
	require.NoError(t, err)

	for i, tc := range []struct {
		path, expectedBody string
		expectedQueryCount int32
		intervalFn         IntervalFn
	}{
		{
			path:               query,
			expectedBody:       string(mergedHTTPResponseBody),
			expectedQueryCount: 2,
			intervalFn:         func(_ context.Context, _ tripperware.Request) (time.Duration, error) { return 24 * time.Hour, nil },
		},
		{
			path:               query,
			expectedBody:       string(mergedHTTPResponseBody),
			expectedQueryCount: 2,
			intervalFn:         dynamicIntervalFn(Config{SplitQueriesByInterval: day}, mockLimits{}, querysharding.NewQueryAnalyzer(), lookbackDelta),
		},
		{
			path:               longQuery,
			expectedBody:       string(mergedHTTPResponseBody),
			expectedQueryCount: 31,
			intervalFn:         func(_ context.Context, _ tripperware.Request) (time.Duration, error) { return day, nil },
		},
		{
			path:               longQuery,
			expectedBody:       string(mergedHTTPResponseBody),
			expectedQueryCount: 8,
			intervalFn:         dynamicIntervalFn(Config{SplitQueriesByInterval: day, DynamicQuerySplitsConfig: DynamicQuerySplitsConfig{MaxShardsPerQuery: 10}}, mockLimits{}, querysharding.NewQueryAnalyzer(), lookbackDelta),
		},
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

			roundtripper := tripperware.NewRoundTripper(singleHostRoundTripper{
				host: u.Host,
				next: http.DefaultTransport,
			}, PrometheusCodec, nil, NewLimitsMiddleware(mockLimits{}, 5*time.Minute), SplitByIntervalMiddleware(tc.intervalFn, mockLimits{}, PrometheusCodec, nil, lookbackDelta))

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

func Test_dynamicIntervalFn(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		baseSplitInterval        time.Duration
		req                      tripperware.Request
		expectedInterval         time.Duration
		expectedError            bool
		verticalShardSize        int
		maxIntervalSplits        int
		maxDurationOfDataFetched time.Duration
	}{
		{
			baseSplitInterval: day,
			name:              "failed to parse query, return default interval",
			req: &tripperware.PrometheusRequest{
				Query: "up[aaa",
				Start: 0,
				End:   10 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
			},
			maxIntervalSplits:        30,
			maxDurationOfDataFetched: 200 * day,
			expectedInterval:         day,
			expectedError:            true,
		},
		{
			baseSplitInterval: day,
			name:              "23 hour range with 10 max splits, expect split by 1 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   23 * 3600 * seconds,
				Step:  60 * seconds,
				Query: "up",
			},
			maxIntervalSplits: 10,
			expectedInterval:  day,
		},
		{
			baseSplitInterval: time.Hour,
			name:              "24 hour range with 100 max splits, expect split by 1 hour",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24 * 3600 * seconds,
				Step:  60 * seconds,
				Query: "up",
			},
			maxIntervalSplits: 100,
			expectedInterval:  time.Hour,
		},
		{
			baseSplitInterval: time.Hour,
			name:              "120 hour range with 15 max splits, expect split by 8 hours",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   5 * 24 * 3600 * seconds,
				Step:  60 * seconds,
				Query: "up",
			},
			maxIntervalSplits: 15,
			expectedInterval:  8 * time.Hour,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range with 30 max splits, expect split by 1 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   30 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up",
			},
			maxIntervalSplits: 30,
			expectedInterval:  day,
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
			maxIntervalSplits: 20,
			expectedInterval:  2 * day,
		},
		{
			baseSplitInterval: day,
			name:              "60 day range with 15 max splits, expect split by 4 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up",
			},
			maxIntervalSplits: 15,
			expectedInterval:  4 * day,
		},
		{
			baseSplitInterval: day,
			name:              "61 day range with 15 max splits, expect split by 5 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   61 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up",
			},
			maxIntervalSplits: 15,
			expectedInterval:  5 * day,
		},
		{
			baseSplitInterval: day,
			name:              "60 day range with 15 max splits and 3 vertical shard size, expect split by 12 days",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "sum by (pod) (up)",
			},
			verticalShardSize: 3,
			maxIntervalSplits: 15,
			expectedInterval:  12 * day,
		},
		{
			baseSplitInterval: time.Hour,
			name:              "101 hours with 200 hour max duration fetched and 5m matrix selector, expect split by 2 hour",
			req: &tripperware.PrometheusRequest{
				Start: (3 * 24 * 3600 * seconds) - (2 * 3600 * seconds),
				End:   (7 * 24 * 3600 * seconds) + (3 * 3600 * seconds) - 1,
				Step:  60 * seconds,
				Query: "up[5m]",
			},
			maxDurationOfDataFetched: 200 * time.Hour,
			expectedInterval:         2 * time.Hour,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range with 200 days max duration fetched and 1h matrix selector, expect split by 1 day",
			req: &tripperware.PrometheusRequest{
				Start: 30 * 24 * 3600 * seconds,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "avg_over_time(up[1h])",
			},
			maxDurationOfDataFetched: 200 * day,
			expectedInterval:         day,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range with 200 days max duration fetched and 20d matrix selector, expect split by 4 days",
			req: &tripperware.PrometheusRequest{
				Start: 30 * 24 * 3600 * seconds,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "avg_over_time(up[20d])",
			},
			maxDurationOfDataFetched: 200 * day,
			expectedInterval:         4 * day,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range with multiple matrix selectors, expect split by 3 days",
			req: &tripperware.PrometheusRequest{
				Start: (14 * 24 * 3600 * seconds) - (3600 * seconds),
				End:   (32 * 24 * 3600 * seconds) + (2 * 3600 * seconds),
				Step:  5 * 60 * seconds,
				Query: "rate(up[2d]) + rate(up[5d]) + rate(up[7d])",
			},
			maxDurationOfDataFetched: 200 * day,
			expectedInterval:         3 * day,
		},
		{
			baseSplitInterval: day,
			name:              "30 day range with multiple matrix selectors and 3 vertical shards, expect split by 6 days",
			req: &tripperware.PrometheusRequest{
				Start: (14 * 24 * 3600 * seconds) - (3600 * seconds),
				End:   (32 * 24 * 3600 * seconds) + (2 * 3600 * seconds),
				Step:  5 * 60 * seconds,
				Query: "rate(up[2d]) + rate(up[5d]) + rate(up[7d])",
			},
			verticalShardSize:        3,
			maxDurationOfDataFetched: 350 * day,
			expectedInterval:         6 * day,
		},
		{
			baseSplitInterval: day,
			name:              "100 day range with 5 day subquery, expect split by 13 days",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   100 * 24 * 3600 * seconds,
				Step:  60 * 60 * seconds,
				Query: "up[5d:10m]",
			},
			maxIntervalSplits:        100,
			maxDurationOfDataFetched: 150 * day,
			expectedInterval:         13 * day,
		},
		{
			baseSplitInterval: day,
			name:              "51 day range, longer than max duration fetched, expect split by 51 day",
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   (51 * 24 * 3600 * seconds),
				Step:  5 * 60 * seconds,
				Query: "up[5d]",
			},
			maxDurationOfDataFetched: 50 * day,
			expectedInterval:         51 * day,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				SplitQueriesByInterval: tc.baseSplitInterval,
				DynamicQuerySplitsConfig: DynamicQuerySplitsConfig{
					MaxShardsPerQuery:              tc.maxIntervalSplits,
					MaxFetchedDataDurationPerQuery: tc.maxDurationOfDataFetched,
				},
			}
			ctx := user.InjectOrgID(context.Background(), "1")
			interval, err := dynamicIntervalFn(cfg, mockLimits{queryVerticalShardSize: tc.verticalShardSize}, querysharding.NewQueryAnalyzer(), lookbackDelta)(ctx, tc.req)
			require.Equal(t, tc.expectedInterval, interval)
			if !tc.expectedError {
				require.Nil(t, err)
			}
		})
	}
}

func Test_getMaxSplitsByDurationFetched(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		baseSplitInterval        time.Duration
		req                      tripperware.Request
		verticalShardSize        int
		lookbackDelta            time.Duration
		maxDurationOfDataFetched time.Duration
		expectedMaxSplits        int
	}{
		{
			baseSplitInterval: day,
			name:              "20 day range with no lookback, expect default max splits",
			req: &tripperware.PrometheusRequest{
				Query: "up",
				Start: 0,
				End:   (20 * 24 * 3600 * seconds) - 1,
				Step:  5 * 60 * seconds,
			},
			verticalShardSize:        1,
			maxDurationOfDataFetched: 200 * day,
			expectedMaxSplits:        200,
		},
		{
			baseSplitInterval: day,
			name:              "20 days range with 5 min lookback, expect 180 max splits",
			req: &tripperware.PrometheusRequest{
				Query: "up",
				Start: 0,
				End:   (20 * 24 * 3600 * seconds) - 1,
				Step:  5 * 60 * seconds,
			},
			verticalShardSize:        1,
			lookbackDelta:            5 * time.Minute,
			maxDurationOfDataFetched: 200 * day,
			expectedMaxSplits:        180,
		},
		{
			baseSplitInterval: day,
			name:              "20 days range with 5 min lookback but first split starts at 30 min, expect 181 max splits",
			req: &tripperware.PrometheusRequest{
				Query: "up",
				Start: (30 * 60 * seconds),
				End:   (20 * 24 * 3600 * seconds) - 1,
				Step:  5 * 60 * seconds,
			},
			verticalShardSize:        1,
			lookbackDelta:            5 * time.Minute,
			maxDurationOfDataFetched: 200 * day,
			expectedMaxSplits:        181,
		},
		{
			baseSplitInterval: day,
			name:              "20 days range with 1 day subquery and 5 min lookback, expect 90 max splits",
			req: &tripperware.PrometheusRequest{
				Query: "up[1d:1h]",
				Start: 0,
				End:   (20 * 24 * 3600 * seconds) - 1,
				Step:  5 * 60 * seconds,
			},
			verticalShardSize:        1,
			lookbackDelta:            5 * time.Minute,
			maxDurationOfDataFetched: 200 * day,
			expectedMaxSplits:        90,
		},
		{
			baseSplitInterval: day,
			// Certain step sizes can cause the start time of every split except the first one to shift forward. In these cases,
			// lookbackDelta might not fetch additional days for all splits (except the first one if it starts at 00:00 UTC)
			name: "20 days range with 1 day subquery and 5 min lookback but splits are step aligned forward, expect 179 max splits",
			req: &tripperware.PrometheusRequest{
				Query: "up[1d:1h]",
				Start: 0,
				End:   (20 * 24 * 3600 * seconds) - 1,
				Step:  22 * 60 * seconds,
			},
			verticalShardSize:        1,
			lookbackDelta:            5 * time.Minute,
			maxDurationOfDataFetched: 200 * day,
			expectedMaxSplits:        179,
		},
		{
			baseSplitInterval: day,
			name:              "20 days range with 3 day matrix selector, expect 60 max splits",
			req: &tripperware.PrometheusRequest{
				Query: "up[3d]",
				Start: 0,
				End:   (20 * 24 * 3600 * seconds) - 1,
				Step:  5 * 60 * seconds,
			},
			verticalShardSize:        1,
			maxDurationOfDataFetched: 200 * day,
			expectedMaxSplits:        60,
		},
		{
			baseSplitInterval: day,
			name:              "30 days range with 20 day subquery, expect 8 max splits",
			req: &tripperware.PrometheusRequest{
				Start: 30 * 24 * 3600 * seconds,
				End:   (60 * 24 * 3600 * seconds) - 1,
				Step:  5 * 60 * seconds,
				Query: "avg_over_time(up[20d:1h])",
			},
			verticalShardSize:        1,
			lookbackDelta:            5 * time.Minute,
			maxDurationOfDataFetched: 200 * day,
			expectedMaxSplits:        8,
		},
		{
			baseSplitInterval: day,
			// Each term in the query fetches 20 days of range
			name: "20 days range with multiple matrix selectors, expect 20 max splits",
			req: &tripperware.PrometheusRequest{
				Start: (14 * 24 * 3600 * seconds) - (3600 * seconds),
				End:   (32 * 24 * 3600 * seconds) + (2 * 3600 * seconds),
				Step:  5 * 60 * seconds,
				Query: "rate(up[2d]) + rate(up[5d]) + rate(up[7d])",
			},
			verticalShardSize:        1,
			maxDurationOfDataFetched: 350 * day,
			expectedMaxSplits:        20,
		},
		{
			baseSplitInterval: day,
			name:              "20 days range with multiple matrix selectors and 3 vertical shard size, expect 4 max splits",
			req: &tripperware.PrometheusRequest{
				Start: (14 * 24 * 3600 * seconds) - (3600 * seconds),
				End:   (32 * 24 * 3600 * seconds) + (2 * 3600 * seconds),
				Step:  5 * 60 * seconds,
				Query: "rate(up[2d]) + rate(up[5d]) + rate(up[7d])",
			},
			verticalShardSize:        3,
			maxDurationOfDataFetched: 350 * day,
			expectedMaxSplits:        4,
		},
		{
			baseSplitInterval: day,
			name:              "around 37 days range with 5 day matrix selector and 3 vertical shards, expect 25 max splits",
			req: &tripperware.PrometheusRequest{
				Start: (3 * 24 * 3600 * seconds) - (4*3600*seconds + 240*seconds),
				End:   (38 * 24 * 3600 * seconds) + (1*3600*seconds + 60*seconds),
				Step:  5 * 60 * seconds,
				Query: "up[5d]",
			},
			verticalShardSize:        3,
			maxDurationOfDataFetched: 500 * day,
			expectedMaxSplits:        25,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queryExpr, err := parser.ParseExpr(tc.req.GetQuery())
			require.Nil(t, err)
			maxSplits := getMaxSplitsByDurationFetched(tc.maxDurationOfDataFetched, tc.verticalShardSize, queryExpr, tc.req.GetStart(), tc.req.GetEnd(), tc.req.GetStep(), tc.baseSplitInterval, tc.lookbackDelta)
			require.Equal(t, tc.expectedMaxSplits, maxSplits)
		})
	}
}

func Test_getMaxSplitsFromConfig(t *testing.T) {
	for i, tc := range []struct {
		maxIntervalSplits int
		verticalShardSize int
		expectedMaxSplits int
	}{
		{
			verticalShardSize: 1,
			maxIntervalSplits: 100,
			expectedMaxSplits: 100,
		},
		{
			verticalShardSize: 1,
			maxIntervalSplits: 15,
			expectedMaxSplits: 15,
		},
		{
			verticalShardSize: 1,
			maxIntervalSplits: 0,
			expectedMaxSplits: 1,
		},
		{
			verticalShardSize: 3,
			maxIntervalSplits: 15,
			expectedMaxSplits: 5,
		},
		{
			verticalShardSize: 5,
			maxIntervalSplits: 103,
			expectedMaxSplits: 20,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			maxSplits := getMaxSplitsFromConfig(tc.maxIntervalSplits, tc.verticalShardSize)
			require.Equal(t, tc.expectedMaxSplits, maxSplits)
		})
	}
}

func Test_getIntervalFromMaxSplits(t *testing.T) {
	for _, tc := range []struct {
		name              string
		baseSplitInterval time.Duration
		req               tripperware.Request
		maxSplits         int
		expectedInterval  time.Duration
	}{
		{
			name:              "24 hours with 30 max splits, expected to split by 1 hour",
			baseSplitInterval: time.Hour,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24 * 3600 * seconds,
				Step:  60 * seconds,
				Query: "foo",
			},
			maxSplits:        30,
			expectedInterval: time.Hour,
		},
		{
			name:              "24 hours with 10 max splits, expected to split by 3 hours",
			baseSplitInterval: time.Hour,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24 * 3600 * seconds,
				Step:  60 * seconds,
				Query: "foo",
			},
			maxSplits:        10,
			expectedInterval: 3 * time.Hour,
		},
		{
			name:              "120 hours with 20 max splits, expected to split by 6 hours",
			baseSplitInterval: time.Hour,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   5 * 24 * 3600 * seconds,
				Step:  60 * seconds,
				Query: "foo",
			},
			maxSplits:        20,
			expectedInterval: 6 * time.Hour,
		},
		{
			name:              "23h with 10 max splits, expected to split by 1 day",
			baseSplitInterval: day,
			req: &tripperware.PrometheusRequest{
				Start: 12 * 3600 * seconds,
				End:   35 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "foo",
			},
			maxSplits:        10,
			expectedInterval: 1 * day,
		},
		{
			name:              "30 days with 10 max splits, expected to split by 3 days",
			baseSplitInterval: day,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   30 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "foo",
			},
			maxSplits:        10,
			expectedInterval: 3 * day,
		},
		{
			name:              "60 days with 15 max splits, expected to split by 4 days",
			baseSplitInterval: day,
			req: &tripperware.PrometheusRequest{
				Start: 0 * 24 * 3600 * seconds,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "foo",
			},
			maxSplits:        15,
			expectedInterval: 4 * day,
		},
		{
			name:              "61 days with 15 max splits, expected to split by 5 days",
			baseSplitInterval: day,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   61 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "foo",
			},
			maxSplits:        15,
			expectedInterval: 5 * day,
		},
		{
			name:              "51 days with 5 max splits, expected to split by 9 days",
			baseSplitInterval: day,
			req: &tripperware.PrometheusRequest{
				Start: (13 * 24 * 3600 * seconds) + (7*3600*seconds - 1300*seconds),
				End:   (51 * 24 * 3600 * seconds) + (1*3600*seconds + 4900*seconds),
				Step:  5 * 60 * seconds,
				Query: "foo",
			},
			maxSplits:        5,
			expectedInterval: 9 * day,
		},
		{
			name:              "101 hours with 7 max splits, expected to split by 16 hours",
			baseSplitInterval: time.Hour,
			req: &tripperware.PrometheusRequest{
				Start: (3 * 24 * 3600 * seconds) - (4*3600*seconds + 240*seconds),
				End:   (7 * 24 * 3600 * seconds) + (1*3600*seconds + 60*seconds),
				Step:  5 * 60 * seconds,
				Query: "foo",
			},
			maxSplits:        7,
			expectedInterval: 16 * time.Hour,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			interval := getIntervalFromMaxSplits(tc.req, tc.baseSplitInterval, tc.maxSplits)
			require.Equal(t, tc.expectedInterval, interval)
		})
	}
}

func Test_analyzeDurationFetchedByQuery(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		baseSplitInterval                      time.Duration
		lookbackDelta                          time.Duration
		req                                    tripperware.Request
		expectedDurationFetchedByRange         time.Duration
		expectedDurationFetchedBySelectors     time.Duration
		expectedDurationFetchedByLookbackDelta time.Duration
	}{
		{
			name:              "query range 00:00 to 23:59",
			baseSplitInterval: time.Hour,
			lookbackDelta:     0,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24*3600*seconds - 1,
				Step:  60 * seconds,
				Query: "up",
			},
			expectedDurationFetchedByRange:         24 * time.Hour,
			expectedDurationFetchedBySelectors:     0 * time.Hour,
			expectedDurationFetchedByLookbackDelta: 0 * time.Hour,
		},
		{
			name:              "query range 00:00 to 00:00 next day",
			baseSplitInterval: time.Hour,
			lookbackDelta:     0,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24 * 3600 * seconds,
				Step:  60 * seconds,
				Query: "up",
			},
			expectedDurationFetchedByRange:         25 * time.Hour,
			expectedDurationFetchedBySelectors:     0 * time.Hour,
			expectedDurationFetchedByLookbackDelta: 0 * time.Hour,
		},
		{
			name:              "query range 00:00 to 23:59, with 5 min lookback",
			baseSplitInterval: time.Hour,
			lookbackDelta:     lookbackDelta,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24*3600*seconds - 1,
				Step:  60 * seconds,
				Query: "up",
			},
			expectedDurationFetchedByRange:         24 * time.Hour,
			expectedDurationFetchedBySelectors:     0 * time.Hour,
			expectedDurationFetchedByLookbackDelta: 1 * time.Hour,
		},
		{
			name:              "query range 00:00 to 23:59, with 5 hour subquery",
			baseSplitInterval: time.Hour,
			lookbackDelta:     lookbackDelta,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24*3600*seconds - 1,
				Step:  60 * seconds,
				Query: "up[5h:10m]",
			},
			expectedDurationFetchedByRange:         24 * time.Hour,
			expectedDurationFetchedBySelectors:     5 * time.Hour,
			expectedDurationFetchedByLookbackDelta: 1 * time.Hour,
		},
		{
			name:              "query range 00:00 to 23:59, with 2 hour matrix selector",
			baseSplitInterval: time.Hour,
			lookbackDelta:     lookbackDelta,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24*3600*seconds - 1,
				Step:  60 * seconds,
				Query: "rate(up[2h])",
			},
			expectedDurationFetchedByRange:         24 * time.Hour,
			expectedDurationFetchedBySelectors:     2 * time.Hour,
			expectedDurationFetchedByLookbackDelta: 0 * time.Hour,
		},
		{
			name:              "query range 00:00 to 23:59, with multiple matrix selectors",
			baseSplitInterval: time.Hour,
			lookbackDelta:     lookbackDelta,
			req: &tripperware.PrometheusRequest{
				Start: 0,
				End:   24*3600*seconds - 1,
				Step:  60 * seconds,
				Query: "rate(up[2h]) + rate(up[5h]) + rate(up[7h])",
			},
			expectedDurationFetchedByRange:         72 * time.Hour,
			expectedDurationFetchedBySelectors:     14 * time.Hour,
			expectedDurationFetchedByLookbackDelta: 0 * time.Hour,
		},
		{
			name:              "query range 60 day with 20 day subquery",
			baseSplitInterval: day,
			lookbackDelta:     lookbackDelta,
			req: &tripperware.PrometheusRequest{
				Start: 0 * 24 * 3600 * seconds,
				End:   60 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up[20d:1h]",
			},
			expectedDurationFetchedByRange:         61 * day,
			expectedDurationFetchedBySelectors:     20 * day,
			expectedDurationFetchedByLookbackDelta: 1 * day,
		},
		{
			name:              "query range 35 day, with 15 day subquery and 1 week offset",
			baseSplitInterval: day,
			lookbackDelta:     lookbackDelta,
			req: &tripperware.PrometheusRequest{
				Start: 0 * 24 * 3600 * seconds,
				End:   35 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "up[15d:1h] offset 1w",
			},
			expectedDurationFetchedByRange:         36 * day,
			expectedDurationFetchedBySelectors:     15 * day,
			expectedDurationFetchedByLookbackDelta: 1 * day,
		},
		{
			name:              "query range 10 days, with multiple subqueries and offsets",
			baseSplitInterval: day,
			lookbackDelta:     lookbackDelta,
			req: &tripperware.PrometheusRequest{
				Start: 10 * 24 * 3600 * seconds,
				End:   20 * 24 * 3600 * seconds,
				Step:  5 * 60 * seconds,
				Query: "rate(up[2d:1h] offset 1w) + rate(up[5d:1h] offset 2w) + rate(up[7d:1h] offset 3w)",
			},
			expectedDurationFetchedByRange:         33 * day,
			expectedDurationFetchedBySelectors:     14 * day,
			expectedDurationFetchedByLookbackDelta: 3 * day,
		},
		{
			name:              "query range spans 40 days with 4 day matrix selector",
			baseSplitInterval: day,
			lookbackDelta:     lookbackDelta,
			req: &tripperware.PrometheusRequest{
				Start: (13 * 24 * 3600 * seconds) - (7*3600*seconds - 1300*seconds),
				End:   (51 * 24 * 3600 * seconds) + (1*3600*seconds + 4900*seconds),
				Step:  5 * 60 * seconds,
				Query: "up[4d]",
			},
			expectedDurationFetchedByRange:         40 * day,
			expectedDurationFetchedBySelectors:     4 * day,
			expectedDurationFetchedByLookbackDelta: 0 * day,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.req.GetQuery())
			require.Nil(t, err)
			queryRangeIntervals, extraIntervalsPerSplit, lookbackDeltaIntervals := analyzeDurationFetchedByQueryExpr(expr, tc.req.GetStart(), tc.req.GetEnd(), tc.baseSplitInterval, tc.lookbackDelta)
			require.Equal(t, tc.expectedDurationFetchedByRange, queryRangeIntervals)
			require.Equal(t, tc.expectedDurationFetchedBySelectors, extraIntervalsPerSplit)
			require.Equal(t, tc.expectedDurationFetchedByLookbackDelta, lookbackDeltaIntervals)
		})
	}
}
