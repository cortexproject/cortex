package ingester

import (
	"context"
	"encoding/base64"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querysharding"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

const (
	queryStreamCancelMetricName = "query_stream_cancel_metric"
	queryStreamCancelShardLabel = "job"
	queryStreamCancelShardValue = "shared"
	queryStreamCancelTotalShard = 2
)

// queryStreamCancelSeriesCount is several times util.CheckContextEveryNIterations so that
// both the first and later periodic context checks are exercised.
var queryStreamCancelSeriesCount = 3*util.CheckContextEveryNIterations + 7

// setupIngesterForQueryStreamCancel starts an ingester and pushes queryStreamCancelSeriesCount
// series that all share the same value of the shard label, so they all hash to the same shard.
func setupIngesterForQueryStreamCancel(t testing.TB, enableMatcherOptimization bool) (*Ingester, []labels.Labels) {
	cfg := defaultIngesterTestConfig(t)
	cfg.EnableMatcherOptimization = enableMatcherOptimization

	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	test.Poll(t, 1*time.Second, ring.ACTIVE, func() any {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	pushed := make([]labels.Labels, 0, queryStreamCancelSeriesCount)
	for s := range queryStreamCancelSeriesCount {
		lbls := labels.FromStrings(
			labels.MetricName, queryStreamCancelMetricName,
			queryStreamCancelShardLabel, queryStreamCancelShardValue,
			"series", fmt.Sprintf("series-%d", s),
		)
		// Keep a copy: Push returns the request to a pool, which reuses the label memory.
		pushed = append(pushed, lbls.Copy())
		_, err := i.Push(ctx, writeRequestSingleSeries(lbls, []cortexpb.Sample{{Value: float64(s), TimestampMs: 1000}}))
		require.NoError(t, err)
	}

	return i, pushed
}

// shardIndexes returns the shard index that every pushed series falls into, and an index
// that none of them falls into, when sharding by queryStreamCancelShardLabel.
func shardIndexes(t testing.TB, series labels.Labels) (matching, nonMatching int64) {
	for idx := range int64(queryStreamCancelTotalShard) {
		sm := shardInfo(idx).Matcher(&querysharding.Buffers)
		matches := sm.MatchesLabels(series)
		sm.Close()
		if matches {
			matching = idx
		} else {
			nonMatching = idx
		}
	}
	require.NotEqual(t, matching, nonMatching)
	return matching, nonMatching
}

func shardInfo(shardIndex int64) *storepb.ShardInfo {
	return &storepb.ShardInfo{
		ShardIndex:  shardIndex,
		TotalShards: queryStreamCancelTotalShard,
		By:          true,
		Labels:      []string{queryStreamCancelShardLabel},
	}
}

// queryStreamCancelRequest builds a QueryStream request for the test metric. The second
// matcher is lazy when matcher optimization is enabled, so the lazy-matcher skip path is
// exercised as well. A non-nil shard is encoded the same way the query-frontend does it.
func queryStreamCancelRequest(t testing.TB, shard *storepb.ShardInfo) *client.QueryRequest {
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, queryStreamCancelMetricName),
		labels.MustNewMatcher(labels.MatchRegexp, queryStreamCancelShardLabel, ".+"),
	}
	if shard != nil {
		encoded, err := shard.Marshal()
		require.NoError(t, err)
		matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, querysharding.CortexShardByLabel, base64.StdEncoding.EncodeToString(encoded)))
	}
	req, err := client.ToQueryRequest(0, model.Time(10000), matchers)
	require.NoError(t, err)
	return req
}

// cancelAfterSelectContext reports the request as canceled to queryStreamChunks but not to
// the TSDB postings lookup. It models a query-frontend or querier that gives up after the
// ingester has resolved the postings and started walking the series, which is when the
// CPU was spent in the incident this guards against. A context that is canceled from the
// start is not enough, because tsdb.PostingsForMatchers already checks ctx.Err().
type cancelAfterSelectContext struct {
	context.Context
	// cancelOnCheck is the 1-based queryStreamChunks context check that first reports
	// the cancellation. Earlier checks see a live context.
	cancelOnCheck int64
	checksSeen    *atomic.Int64
}

func (c cancelAfterSelectContext) Err() error {
	pcs := make([]uintptr, 16)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".(*Ingester).queryStreamChunks") {
			if c.checksSeen.Inc() >= c.cancelOnCheck {
				return context.Canceled
			}
			return nil
		}
		if strings.Contains(frame.Function, "prometheus/prometheus/tsdb") || !more {
			return c.Context.Err()
		}
	}
}

func TestIngester_QueryStream_StopsWhenCanceledWhileWalkingSeries(t *testing.T) {
	for _, enableMatcherOptimization := range []bool{false, true} {
		t.Run(fmt.Sprintf("matcher optimization=%t", enableMatcherOptimization), func(t *testing.T) {
			i, pushed := setupIngesterForQueryStreamCancel(t, enableMatcherOptimization)
			_, nonMatching := shardIndexes(t, pushed[0])

			for name, cancelOnCheck := range map[string]int64{
				// The check before the first series stops the loop straight away.
				"canceled before the first series": 1,
				// The third check runs after 2*CheckContextEveryNIterations series have
				// been skipped, which proves the check repeats during the walk.
				"canceled part way through": 3,
			} {
				t.Run(name, func(t *testing.T) {
					ctx := cancelAfterSelectContext{
						Context:       user.InjectOrgID(context.Background(), userID),
						cancelOnCheck: cancelOnCheck,
						checksSeen:    atomic.NewInt64(0),
					}
					stream := &mockQueryStreamServer{ctx: ctx}
					err := i.QueryStream(queryStreamCancelRequest(t, shardInfo(nonMatching)), stream)

					// Every series fails the shard matcher, so nothing is ever sent and
					// SendQueryStream never gets the chance to notice the cancellation.
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, stream.series)
					require.Equal(t, cancelOnCheck, ctx.checksSeen.Load())
				})
			}
		})
	}
}

func TestIngester_QueryStream_StopsOnContextCancellation(t *testing.T) {
	for _, enableMatcherOptimization := range []bool{false, true} {
		t.Run(fmt.Sprintf("matcher optimization=%t", enableMatcherOptimization), func(t *testing.T) {
			i, pushed := setupIngesterForQueryStreamCancel(t, enableMatcherOptimization)
			_, nonMatching := shardIndexes(t, pushed[0])

			// Sanity check: with a live context, the non-matching shard runs to completion
			// without sending anything. Before this change, this was also what happened
			// after the context had been canceled.
			liveCtx := user.InjectOrgID(context.Background(), userID)
			liveStream := &mockQueryStreamServer{ctx: liveCtx}
			require.NoError(t, i.QueryStream(queryStreamCancelRequest(t, shardInfo(nonMatching)), liveStream))
			require.Empty(t, liveStream.series)

			t.Run("canceled", func(t *testing.T) {
				ctx, cancel := context.WithCancel(user.InjectOrgID(context.Background(), userID))
				cancel()

				stream := &mockQueryStreamServer{ctx: ctx}
				err := i.QueryStream(queryStreamCancelRequest(t, shardInfo(nonMatching)), stream)
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, stream.series)
			})

			t.Run("deadline exceeded", func(t *testing.T) {
				ctx, cancel := context.WithDeadline(user.InjectOrgID(context.Background(), userID), time.Now().Add(-time.Second))
				defer cancel()

				stream := &mockQueryStreamServer{ctx: ctx}
				err := i.QueryStream(queryStreamCancelRequest(t, shardInfo(nonMatching)), stream)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Empty(t, stream.series)
			})
		})
	}
}

func TestIngester_QueryStream_ContextCheckDoesNotChangeResults(t *testing.T) {
	for _, enableMatcherOptimization := range []bool{false, true} {
		t.Run(fmt.Sprintf("matcher optimization=%t", enableMatcherOptimization), func(t *testing.T) {
			i, pushed := setupIngesterForQueryStreamCancel(t, enableMatcherOptimization)
			matching, nonMatching := shardIndexes(t, pushed[0])

			expected := make([]string, 0, len(pushed))
			for _, l := range pushed {
				expected = append(expected, l.String())
			}
			slices.Sort(expected)

			for name, tc := range map[string]struct {
				shard    *storepb.ShardInfo
				expected []string
			}{
				"not sharded":        {shard: nil, expected: expected},
				"matching shard":     {shard: shardInfo(matching), expected: expected},
				"non matching shard": {shard: shardInfo(nonMatching), expected: []string{}},
			} {
				t.Run(name, func(t *testing.T) {
					ctx := user.InjectOrgID(context.Background(), userID)
					stream := &mockQueryStreamServer{ctx: ctx}
					require.NoError(t, i.QueryStream(queryStreamCancelRequest(t, tc.shard), stream))

					actual := make([]string, 0, len(stream.series))
					for _, s := range stream.series {
						actual = append(actual, cortexpb.FromLabelAdaptersToLabels(s.Labels).String())
						require.Len(t, s.Chunks, 1)
					}
					slices.Sort(actual)
					require.Equal(t, tc.expected, actual)
				})
			}
		})
	}
}

// BenchmarkIngester_QueryStream_ShardMatchesNothing measures the cost of a sharded query
// whose series all fail the shard matcher, which is the loop the context check runs in.
func BenchmarkIngester_QueryStream_ShardMatchesNothing(b *testing.B) {
	i, pushed := setupIngesterForQueryStreamCancel(b, false)
	_, nonMatching := shardIndexes(b, pushed[0])
	ctx := user.InjectOrgID(context.Background(), userID)
	stream := &mockQueryStreamServer{ctx: ctx}

	b.ReportAllocs()
	for b.Loop() {
		require.NoError(b, i.QueryStream(queryStreamCancelRequest(b, shardInfo(nonMatching)), stream))
	}
}
