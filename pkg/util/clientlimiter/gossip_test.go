package clientlimiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

type fixedStrategy struct {
	limit               float64
	burst               int
	trackedClientsLimit int
}

func (s fixedStrategy) Limit(string) float64           { return s.limit }
func (s fixedStrategy) Burst(string) int               { return s.burst }
func (s fixedStrategy) TrackedClientsLimit(string) int { return s.trackedClientsLimit }

const testWindow = 100 * time.Millisecond

func TestGossipLimiter_ConvergesAcrossReplicas(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetClientCounterDescCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	strategy := fixedStrategy{limit: 50, burst: 0, trackedClientsLimit: 100} // capacity = 50 * 100ms = 5 tokens/window

	replicaA := NewGossipLimiter(prometheus.NewRegistry(), "test_a", strategy, StaticKVClient(kvClient), "replica-a", testWindow, time.Minute, log.NewNopLogger())
	replicaB := NewGossipLimiter(prometheus.NewRegistry(), "test_b", strategy, StaticKVClient(kvClient), "replica-b", testWindow, time.Minute, log.NewNopLogger())

	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, replicaA))
	require.NoError(t, services.StartAndAwaitRunning(ctx, replicaB))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(ctx, replicaA)
		_ = services.StopAndAwaitTerminated(ctx, replicaB)
	})

	now := time.Now()

	// Both replicas have seen this client at least once -- applyPeerView
	// only updates peerSum for clients a replica already locally tracks, by
	// design (see GossipLimiter.applyPeerView): a replica that's never seen
	// a client has nothing to combine peer state with.
	require.True(t, replicaB.AllowN(now, "tenant-1", "client-1", 0))

	// replicaA alone consumes its full local view of the per-window budget.
	require.True(t, replicaA.AllowN(now, "tenant-1", "client-1", 5))

	// Wait for replicaA's count to gossip to replicaB.
	test.Poll(t, 2*time.Second, int64(5), func() any {
		ts := replicaB.getTenantState(time.Now(), "tenant-1")
		ts.mu.Lock()
		cs, ok := ts.cache.Peek("client-1")
		ts.mu.Unlock()
		if !ok {
			return int64(0)
		}
		cs.mu.Lock()
		defer cs.mu.Unlock()
		return cs.peerSum
	})

	// replicaB now sees replicaA's contribution and must combine it with its
	// own before admitting further requests against the shared budget.
	require.False(t, replicaB.AllowN(time.Now(), "tenant-1", "client-1", 1))
}

func TestGossipLimiter_WithoutPeers_EnforcesLocalWindow(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetClientCounterDescCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	strategy := fixedStrategy{limit: 50, burst: 0, trackedClientsLimit: 100} // capacity = 5 tokens/window
	limiter := NewGossipLimiter(prometheus.NewRegistry(), "test", strategy, StaticKVClient(kvClient), "solo-replica", testWindow, time.Minute, log.NewNopLogger())

	now := time.Now()
	require.True(t, limiter.AllowN(now, "tenant-1", "client-1", 5))
	require.False(t, limiter.AllowN(now, "tenant-1", "client-1", 1))
}

func TestGossipLimiter_WindowRollsOver(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetClientCounterDescCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	strategy := fixedStrategy{limit: 10, burst: 0, trackedClientsLimit: 100} // capacity = 10 * 100ms = 1 token/window
	limiter := NewGossipLimiter(prometheus.NewRegistry(), "test", strategy, StaticKVClient(kvClient), "solo-replica", testWindow, time.Minute, log.NewNopLogger())

	now := time.Now()
	require.True(t, limiter.AllowN(now, "tenant-1", "client-1", 1))
	require.False(t, limiter.AllowN(now, "tenant-1", "client-1", 1))

	later := now.Add(2 * testWindow)
	require.True(t, limiter.AllowN(later, "tenant-1", "client-1", 1))
}

func TestGossipLimiter_KVClientIsBuiltLazily(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetClientCounterDescCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	var builds atomic.Int32
	factory := func() (kv.Client, error) {
		builds.Inc()
		return kvClient, nil
	}

	strategy := fixedStrategy{limit: 50, burst: 0, trackedClientsLimit: 100}
	limiter := NewGossipLimiter(prometheus.NewRegistry(), "test", strategy, factory, "solo-replica", testWindow, time.Minute, log.NewNopLogger())

	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, limiter))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, limiter) })

	// No tenant has used the limiter, so no KV client may be built however
	// many gossip ticks elapse.
	time.Sleep(5 * testWindow)
	require.Equal(t, int32(0), builds.Load())

	require.True(t, limiter.AllowN(time.Now(), "tenant-1", "client-1", 1))
	test.Poll(t, 2*time.Second, int32(1), func() any { return builds.Load() })
}

func TestGossipLimiter_KVClientFailureFailsOpen(t *testing.T) {
	var builds atomic.Int32
	factory := func() (kv.Client, error) {
		builds.Inc()
		return nil, errors.New("memberlist not configured")
	}

	strategy := fixedStrategy{limit: 50, burst: 0, trackedClientsLimit: 100} // capacity = 5 tokens/window
	limiter := NewGossipLimiter(prometheus.NewRegistry(), "test", strategy, factory, "solo-replica", testWindow, time.Minute, log.NewNopLogger())

	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, limiter))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, limiter) })

	now := time.Now()
	require.True(t, limiter.AllowN(now, "tenant-1", "client-1", 5))
	test.Poll(t, 2*time.Second, true, func() any { return builds.Load() >= 1 })

	// Enforcement continues on local state, and the failed construction is
	// retried on a backoff rather than every gossip tick.
	require.False(t, limiter.AllowN(now, "tenant-1", "client-1", 1))
	time.Sleep(5 * testWindow)
	require.Equal(t, int32(1), builds.Load())
}
