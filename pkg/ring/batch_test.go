package ring

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var _ ReadRing = (*mockReadRing)(nil)

// recoveringExecutor wraps each submitted function in a goroutine that
// recovers from panics. This prevents a panic inside a DoBatch callback
// from crashing the test process, while still allowing us to observe
// whether cleanup is called (which depends on defer wg.Done()).
type recoveringExecutor struct{}

func (e *recoveringExecutor) Submit(f func()) {
	go func() {
		defer func() { _ = recover() }()
		f()
	}()
}

func (e *recoveringExecutor) Stop() {}

// mockReadRing is a minimal ReadRing implementation for testing DoBatch.
// It returns a single healthy instance for any Get call.
type mockReadRing struct {
	inst InstanceDesc
}

func (m *mockReadRing) Get(_ uint32, _ Operation, _ []InstanceDesc, _ []string, _ map[string]int) (ReplicationSet, error) {
	return ReplicationSet{
		Instances: []InstanceDesc{m.inst},
		MaxErrors: 0,
	}, nil
}

func (m *mockReadRing) GetAllHealthy(_ Operation) (ReplicationSet, error) {
	return ReplicationSet{}, nil
}
func (m *mockReadRing) GetAllInstanceDescs(_ Operation) ([]InstanceDesc, []InstanceDesc, error) {
	return nil, nil, nil
}
func (m *mockReadRing) GetInstanceDescsForOperation(_ Operation) (map[string]InstanceDesc, error) {
	return nil, nil
}
func (m *mockReadRing) GetReplicationSetForOperation(_ Operation) (ReplicationSet, error) {
	return ReplicationSet{}, nil
}
func (m *mockReadRing) ReplicationFactor() int { return 1 }
func (m *mockReadRing) InstancesCount() int    { return 1 }
func (m *mockReadRing) ShuffleShard(_ string, _ int) ReadRing {
	return m
}
func (m *mockReadRing) ShuffleShardWithZoneStability(_ string, _ int) ReadRing {
	return m
}
func (m *mockReadRing) GetInstanceState(_ string) (InstanceState, error) {
	return ACTIVE, nil
}
func (m *mockReadRing) GetInstanceIdByAddr(_ string) (string, error) {
	return "", nil
}
func (m *mockReadRing) ShuffleShardWithLookback(_ string, _ int, _ time.Duration, _ time.Time) ReadRing {
	return m
}
func (m *mockReadRing) HasInstance(_ string) bool         { return true }
func (m *mockReadRing) CleanupShuffleShardCache(_ string) {}

func TestDoBatchCleanupCalledOnCallbackPanic(t *testing.T) {
	ring := &mockReadRing{
		inst: InstanceDesc{
			Addr:      "addr-0",
			Timestamp: time.Now().Unix(),
			State:     ACTIVE,
			Tokens:    []uint32{0},
		},
	}

	var cleanupCalled atomic.Bool
	cleanup := func() {
		cleanupCalled.Store(true)
	}

	panicCallback := func(_ InstanceDesc, _ []int) error {
		panic("test panic in callback")
	}

	// Use a context with timeout so DoBatch can return. When the callback
	// panics, tracker.record is never called, so neither tracker.done nor
	// tracker.err is signaled. DoBatch exits via ctx.Done(). The key
	// assertion is that cleanup still runs: with defer wg.Done(), the
	// WaitGroup completes despite the panic, unblocking the cleanup goroutine.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = DoBatch(ctx, Write, ring, &recoveringExecutor{}, []uint32{0}, panicCallback, cleanup)

	assert.Eventually(t, func() bool {
		return cleanupCalled.Load()
	}, 5*time.Second, 10*time.Millisecond, "cleanup must be called even when callback panics")
}

// batchRing routes each key to its own replication set, so a test controls
// exactly which instances, and how many failures, each key gets.
type batchRing struct {
	mockReadRing
	sets map[uint32]ReplicationSet
}

func (r *batchRing) Get(key uint32, _ Operation, _ []InstanceDesc, _ []string, _ map[string]int) (ReplicationSet, error) {
	set, ok := r.sets[key]
	if !ok {
		return ReplicationSet{}, fmt.Errorf("no replication set for key %d", key)
	}
	return set, nil
}

func (r *batchRing) ReplicationFactor() int { return 3 }

func (r *batchRing) InstancesCount() int { return len(r.addrs()) }

func (r *batchRing) addrs() map[string]struct{} {
	addrs := map[string]struct{}{}
	for _, set := range r.sets {
		for _, inst := range set.Instances {
			addrs[inst.Addr] = struct{}{}
		}
	}
	return addrs
}

func (r *batchRing) keys() []uint32 {
	keys := make([]uint32, 0, len(r.sets))
	for key := range r.sets {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func testReplicationSet(maxErrors int, addrs ...string) ReplicationSet {
	instances := make([]InstanceDesc, 0, len(addrs))
	for _, addr := range addrs {
		instances = append(instances, InstanceDesc{Addr: addr, State: ACTIVE})
	}
	return ReplicationSet{Instances: instances, MaxErrors: maxErrors}
}

// recordingExecutor runs each submitted function in its own goroutine and
// signals once it has returned, i.e. once DoBatch has recorded the outcome of
// that instance's RPC.
type recordingExecutor struct {
	recorded chan struct{}
}

func (e *recordingExecutor) Submit(f func()) {
	go func() {
		f()
		e.recorded <- struct{}{}
	}()
}

func (e *recordingExecutor) Stop() {}

const scriptedBatchTimeout = 5 * time.Second

// scriptedBatch runs DoBatch with every RPC blocked until the test completes
// it, so the order in which DoBatch records outcomes is fixed by the test.
type scriptedBatch struct {
	t        *testing.T
	gates    map[string]chan error
	executor *recordingExecutor
	result   chan error
}

func startScriptedBatch(t *testing.T, r *batchRing) *scriptedBatch {
	t.Helper()
	addrs := r.addrs()
	s := &scriptedBatch{
		t:        t,
		gates:    make(map[string]chan error, len(addrs)),
		executor: &recordingExecutor{recorded: make(chan struct{}, len(addrs))},
		result:   make(chan error, 1),
	}
	for addr := range addrs {
		s.gates[addr] = make(chan error, 1)
	}
	// Release any RPC the test did not complete, so a failed test does not
	// leave goroutines blocked.
	t.Cleanup(func() {
		for _, gate := range s.gates {
			select {
			case gate <- nil:
			default:
			}
		}
	})
	callback := func(inst InstanceDesc, _ []int) error {
		return <-s.gates[inst.Addr]
	}
	ctx, cancel := context.WithTimeout(context.Background(), scriptedBatchTimeout)
	t.Cleanup(cancel)
	go func() {
		s.result <- DoBatch(ctx, Write, r, s.executor, r.keys(), callback, func() {})
	}()
	return s
}

// complete makes the RPC to addr return err, and waits until DoBatch has
// recorded that outcome.
func (s *scriptedBatch) complete(addr string, err error) {
	s.t.Helper()
	gate, ok := s.gates[addr]
	require.True(s.t, ok, "unknown instance %s", addr)
	gate <- err
	select {
	case <-s.executor.recorded:
	case <-time.After(scriptedBatchTimeout):
		require.FailNow(s.t, "outcome was not recorded", "instance %s", addr)
	}
}

func (s *scriptedBatch) wait() error {
	s.t.Helper()
	select {
	case err := <-s.result:
		return err
	case <-time.After(2 * scriptedBatchTimeout):
		require.FailNow(s.t, "DoBatch did not return")
		return nil
	}
}

type rpcOutcome struct {
	addr string
	err  error
}

var (
	errRejected    = httpgrpc.Errorf(http.StatusBadRequest, "rejected")
	errUnavailable = httpgrpc.Errorf(http.StatusInternalServerError, "unavailable")
)

func succeed(addr string) rpcOutcome        { return rpcOutcome{addr: addr} }
func fail4xx(addr string) rpcOutcome        { return rpcOutcome{addr: addr, err: errRejected} }
func fail5xx(addr string) rpcOutcome        { return rpcOutcome{addr: addr, err: errUnavailable} }
func outcomes(o ...rpcOutcome) []rpcOutcome { return o }

func requireHTTPCode(t *testing.T, err error, code int32) {
	t.Helper()
	require.Error(t, err)
	resp, isHTTP := httpgrpc.HTTPResponseFromError(err)
	require.True(t, isHTTP, "not an HTTP error: %v", err)
	require.Equal(t, code, resp.Code, "unexpected error: %v", err)
}

func TestDoBatch_Quorum(t *testing.T) {
	const (
		keyFull uint32 = iota + 1
		keyBad
		keyOther
	)
	rf3 := func(prefix string) ReplicationSet {
		return testReplicationSet(1, prefix+"1", prefix+"2", prefix+"3")
	}

	tests := map[string]struct {
		sets map[uint32]ReplicationSet
		// order in which the RPC to each instance completes. Every instance
		// of the ring must appear exactly once.
		order []rpcOutcome
		// wantCode is the HTTP code of the expected error, or 0 for success.
		wantCode int32
	}{
		"fully replicated key and key rejected everywhere, 4xx, successes first": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b")},
			order:    outcomes(succeed("a1"), succeed("a2"), succeed("a3"), fail4xx("b1"), fail4xx("b2"), fail4xx("b3")),
			wantCode: http.StatusBadRequest,
		},
		"fully replicated key and key rejected everywhere, 4xx, rejections first": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b")},
			order:    outcomes(fail4xx("b1"), fail4xx("b2"), fail4xx("b3"), succeed("a1"), succeed("a2"), succeed("a3")),
			wantCode: http.StatusBadRequest,
		},
		"fully replicated key and key rejected everywhere, 4xx, interleaved": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b")},
			order:    outcomes(succeed("a1"), fail4xx("b1"), succeed("a2"), succeed("a3"), fail4xx("b2"), fail4xx("b3")),
			wantCode: http.StatusBadRequest,
		},
		"fully replicated key and key failing everywhere, 5xx, successes first": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b")},
			order:    outcomes(succeed("a1"), succeed("a2"), succeed("a3"), fail5xx("b1"), fail5xx("b2"), fail5xx("b3")),
			wantCode: http.StatusInternalServerError,
		},
		"fully replicated key and key failing everywhere, 5xx, failures first": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b")},
			order:    outcomes(fail5xx("b1"), fail5xx("b2"), fail5xx("b3"), succeed("a1"), succeed("a2"), succeed("a3")),
			wantCode: http.StatusInternalServerError,
		},
		"fully replicated key and key failing everywhere, 5xx, interleaved": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b")},
			order:    outcomes(succeed("a1"), fail5xx("b1"), succeed("a2"), succeed("a3"), fail5xx("b2"), fail5xx("b3")),
			wantCode: http.StatusInternalServerError,
		},
		"key at exactly quorum and key failing 2 of 3": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b")},
			order:    outcomes(succeed("a1"), succeed("a2"), fail5xx("a3"), succeed("b1"), fail5xx("b2"), fail5xx("b3")),
			wantCode: http.StatusInternalServerError,
		},
		"fully replicated key, key at exactly quorum and key failing 2 of 3": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyOther: rf3("c"), keyBad: rf3("b")},
			order:    outcomes(succeed("a1"), succeed("a2"), succeed("a3"), succeed("c1"), succeed("c2"), fail4xx("c3"), succeed("b1"), fail4xx("b2"), fail4xx("b3")),
			wantCode: http.StatusBadRequest,
		},
		"every key at quorum with one failed replica": {
			sets:  map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b"), keyOther: rf3("c")},
			order: outcomes(succeed("a1"), fail5xx("a2"), succeed("a3"), fail4xx("b1"), succeed("b2"), succeed("b3"), succeed("c1"), succeed("c2"), fail5xx("c3")),
		},
		"every key fully replicated": {
			sets:  map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b"), keyOther: rf3("c")},
			order: outcomes(succeed("a1"), succeed("a2"), succeed("a3"), succeed("b1"), succeed("b2"), succeed("b3"), succeed("c1"), succeed("c2"), succeed("c3")),
		},

		// Error family of a single key, as documented in batchTracker.record.
		"error family: 2xx, 4xx, 5xx returns 4xx": {
			sets:     map[uint32]ReplicationSet{keyBad: rf3("b")},
			order:    outcomes(succeed("b1"), fail4xx("b2"), fail5xx("b3")),
			wantCode: http.StatusBadRequest,
		},
		"error family: 2xx, 5xx, 4xx returns 4xx": {
			sets:     map[uint32]ReplicationSet{keyBad: rf3("b")},
			order:    outcomes(succeed("b1"), fail5xx("b2"), fail4xx("b3")),
			wantCode: http.StatusBadRequest,
		},
		"error family: 4xx, 5xx, 2xx returns 4xx": {
			sets:     map[uint32]ReplicationSet{keyBad: rf3("b")},
			order:    outcomes(fail4xx("b1"), fail5xx("b2"), succeed("b3")),
			wantCode: http.StatusBadRequest,
		},
		"error family: 4xx, 4xx, _ returns 4xx": {
			sets:     map[uint32]ReplicationSet{keyBad: rf3("b")},
			order:    outcomes(fail4xx("b1"), fail4xx("b2"), succeed("b3")),
			wantCode: http.StatusBadRequest,
		},
		"error family: 5xx, _, 5xx returns 5xx": {
			sets:     map[uint32]ReplicationSet{keyBad: rf3("b")},
			order:    outcomes(fail5xx("b1"), succeed("b2"), fail5xx("b3")),
			wantCode: http.StatusInternalServerError,
		},
		"error family: 4xx, 5xx, 5xx returns 5xx": {
			sets:     map[uint32]ReplicationSet{keyBad: rf3("b")},
			order:    outcomes(fail4xx("b1"), fail5xx("b2"), fail5xx("b3")),
			wantCode: http.StatusInternalServerError,
		},
		"error family: 2xx, 4xx, 5xx returns 4xx next to a fully replicated key": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: rf3("b")},
			order:    outcomes(succeed("a1"), succeed("a2"), succeed("a3"), succeed("b1"), fail4xx("b2"), fail5xx("b3")),
			wantCode: http.StatusBadRequest,
		},

		// Synthetic 4-instance sets: Ring.Get never returns one for RF 3, because
		// Filter drops the non-ACTIVE instance that extends the set. Quorum is 3 of 4.
		"synthetic 4-instance set: 3 of 4 is quorum": {
			sets:  map[uint32]ReplicationSet{keyFull: testReplicationSet(1, "e1", "e2", "e3", "e4")},
			order: outcomes(succeed("e1"), fail4xx("e2"), succeed("e3"), succeed("e4")),
		},
		"synthetic 4-instance set: 2 of 4 is not quorum": {
			sets:     map[uint32]ReplicationSet{keyBad: testReplicationSet(1, "f1", "f2", "f3", "f4")},
			order:    outcomes(succeed("f1"), succeed("f2"), fail5xx("f3"), fail5xx("f4")),
			wantCode: http.StatusInternalServerError,
		},
		"synthetic 4-instance set: fully replicated key and key at 2 of 4": {
			sets: map[uint32]ReplicationSet{
				keyFull: testReplicationSet(1, "e1", "e2", "e3", "e4"),
				keyBad:  testReplicationSet(1, "f1", "f2", "f3", "f4"),
			},
			order:    outcomes(succeed("e1"), succeed("e2"), succeed("e3"), succeed("e4"), succeed("f1"), succeed("f2"), fail5xx("f3"), fail5xx("f4")),
			wantCode: http.StatusInternalServerError,
		},

		// Two instances, MaxErrors 0: what Ring.Get returns for RF 3 when one
		// replica is filtered out, e.g. for a stale heartbeat. Both must succeed.
		"one replica filtered out: both remaining succeed": {
			sets:  map[uint32]ReplicationSet{keyFull: testReplicationSet(0, "d1", "d2")},
			order: outcomes(succeed("d1"), succeed("d2")),
		},
		"one replica filtered out: one failure fails the key": {
			sets:     map[uint32]ReplicationSet{keyBad: testReplicationSet(0, "d1", "d2")},
			order:    outcomes(succeed("d1"), fail4xx("d2")),
			wantCode: http.StatusBadRequest,
		},
		"one replica filtered out, next to a fully replicated key": {
			sets:     map[uint32]ReplicationSet{keyFull: rf3("a"), keyBad: testReplicationSet(0, "d1", "d2")},
			order:    outcomes(succeed("a1"), succeed("a2"), succeed("a3"), succeed("d1"), fail5xx("d2")),
			wantCode: http.StatusInternalServerError,
		},

		// MaxErrors = 0 requires every instance, as UserStats does.
		"no errors tolerated: one failure fails the key": {
			sets:     map[uint32]ReplicationSet{keyBad: testReplicationSet(0, "m1", "m2", "m3")},
			order:    outcomes(succeed("m1"), succeed("m2"), fail5xx("m3")),
			wantCode: http.StatusInternalServerError,
		},
		"no errors tolerated: every instance succeeds": {
			sets:  map[uint32]ReplicationSet{keyFull: testReplicationSet(0, "m1", "m2", "m3")},
			order: outcomes(succeed("m1"), succeed("m2"), succeed("m3")),
		},

		// MaxErrors >= len(Instances). No ReplicationStrategy in Cortex returns
		// this, but ReadRing is an interface. Such a key needs one success and
		// fails once every instance has failed, as before the quorum fix.
		"every error tolerated: one success is enough": {
			sets:  map[uint32]ReplicationSet{keyFull: testReplicationSet(3, "z1", "z2", "z3")},
			order: outcomes(fail5xx("z1"), succeed("z2"), fail5xx("z3")),
		},
		"more errors tolerated than instances: one success is enough": {
			sets:  map[uint32]ReplicationSet{keyFull: testReplicationSet(5, "z1", "z2", "z3")},
			order: outcomes(succeed("z1"), succeed("z2"), succeed("z3")),
		},
		"every error tolerated: every instance fails": {
			sets:     map[uint32]ReplicationSet{keyBad: testReplicationSet(3, "z1", "z2", "z3")},
			order:    outcomes(fail4xx("z1"), fail4xx("z2"), fail4xx("z3")),
			wantCode: http.StatusBadRequest,
		},
		"every error tolerated, next to a key rejected everywhere": {
			sets:     map[uint32]ReplicationSet{keyFull: testReplicationSet(3, "z1", "z2", "z3"), keyBad: rf3("b")},
			order:    outcomes(succeed("z1"), succeed("z2"), succeed("z3"), fail4xx("b1"), fail4xx("b2"), fail4xx("b3")),
			wantCode: http.StatusBadRequest,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := &batchRing{sets: tc.sets}
			require.Len(t, tc.order, len(r.addrs()), "every instance must complete exactly once")

			batch := startScriptedBatch(t, r)
			for _, o := range tc.order {
				batch.complete(o.addr, o.err)
			}
			err := batch.wait()

			if tc.wantCode == 0 {
				require.NoError(t, err)
				return
			}
			requireHTTPCode(t, err, tc.wantCode)
		})
	}
}

// Shared instances: an RPC carries several keys, and one outcome counts for
// each of them.
func TestDoBatch_QuorumOverSharedInstances(t *testing.T) {
	r := &batchRing{sets: map[uint32]ReplicationSet{
		1: testReplicationSet(1, "i1", "i2", "i3"),
		2: testReplicationSet(1, "i2", "i3", "i4"),
		3: testReplicationSet(1, "i4", "i5", "i6"),
	}}
	// Keys 1 and 2 are fully replicated, key 3 is rejected on i5 and i6 and so
	// misses quorum.
	batch := startScriptedBatch(t, r)
	for _, o := range outcomes(succeed("i1"), succeed("i2"), succeed("i3"), succeed("i4"), fail4xx("i5"), fail4xx("i6")) {
		batch.complete(o.addr, o.err)
	}
	requireHTTPCode(t, batch.wait(), http.StatusBadRequest)
}

func TestDoBatch_DoesNotReturnBeforeEveryKeyReachesQuorum(t *testing.T) {
	r := &batchRing{sets: map[uint32]ReplicationSet{
		1: testReplicationSet(1, "a1", "a2", "a3"),
		2: testReplicationSet(1, "b1", "b2", "b3"),
	}}
	batch := startScriptedBatch(t, r)

	// Key 1 is fully replicated, key 2 has one success: one key still lacks quorum.
	for _, addr := range []string{"a1", "a2", "a3", "b1"} {
		batch.complete(addr, nil)
	}
	// Any early return was already decided while recording the outcomes above,
	// so this only waits for DoBatch to deliver it. A slow machine can only make
	// it miss a buggy early return; it cannot fail a correct DoBatch.
	select {
	case err := <-batch.result:
		require.FailNow(t, "DoBatch returned before every key reached quorum", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	batch.complete("b2", nil)
	require.NoError(t, batch.wait())
	batch.complete("b3", nil)
}

// Many keys over shared instances, with RPCs completing concurrently in a
// random order. DoBatch must agree with a sequential quorum calculation over
// the same per-instance outcomes, and must not return success before every key
// has quorum among the RPCs that have returned.
func TestDoBatch_RandomizedOutcomesMatchSequentialQuorum(t *testing.T) {
	const iterations = 50

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Logf("seed: %d", seed)

	for it := range iterations {
		numInstances := 6 + rnd.Intn(4)
		addrs := make([]string, numInstances)
		for i := range addrs {
			addrs[i] = fmt.Sprintf("instance-%d", i)
		}

		outcome := make(map[string]error, numInstances)
		delays := make(map[string]time.Duration, numInstances)
		for _, addr := range addrs {
			delays[addr] = time.Duration(rnd.Intn(500)) * time.Microsecond
			switch p := rnd.Float64(); {
			case p < 0.15:
				outcome[addr] = errRejected
			case p < 0.3:
				outcome[addr] = errUnavailable
			default:
				outcome[addr] = nil
			}
		}

		sets := map[uint32]ReplicationSet{}
		expectSuccess := true
		failureCodes := map[int32]struct{}{}
		for key := range uint32(1 + rnd.Intn(30)) {
			var size, maxErrors int
			switch p := rnd.Float64(); {
			case p < 0.1:
				size, maxErrors = 4, 1 // synthetic, see TestDoBatch_Quorum
			case p < 0.2:
				size, maxErrors = 3, 0 // every instance required
			case p < 0.3:
				size, maxErrors = 2, 0 // one replica filtered out
			default:
				size, maxErrors = 3, 1
			}
			perm := rnd.Perm(numInstances)[:size]
			instanceAddrs := make([]string, 0, size)
			successes := 0
			for _, i := range perm {
				instanceAddrs = append(instanceAddrs, addrs[i])
				if outcome[addrs[i]] == nil {
					successes++
				}
			}
			sets[key] = testReplicationSet(maxErrors, instanceAddrs...)

			if successes < size-maxErrors {
				expectSuccess = false
				for _, addr := range instanceAddrs {
					if err := outcome[addr]; err != nil {
						resp, _ := httpgrpc.HTTPResponseFromError(err)
						failureCodes[resp.Code] = struct{}{}
					}
				}
			}
		}

		r := &batchRing{sets: sets}
		var returnedMtx sync.Mutex
		returned := make(map[string]bool, numInstances)
		ctx, cancel := context.WithTimeout(context.Background(), scriptedBatchTimeout)
		err := DoBatch(ctx, Write, r, nil, r.keys(), func(inst InstanceDesc, _ []int) error {
			time.Sleep(delays[inst.Addr])
			returnedMtx.Lock()
			returned[inst.Addr] = true
			returnedMtx.Unlock()
			return outcome[inst.Addr]
		}, func() {})
		cancel()

		if expectSuccess {
			require.NoError(t, err, "iteration %d: every key reached quorum", it)
			// An RPC is marked before its outcome is recorded, so this snapshot
			// can only over-count successes: it may miss an early return, never
			// report one that did not happen.
			returnedMtx.Lock()
			for key, set := range sets {
				successes := 0
				for _, inst := range set.Instances {
					if returned[inst.Addr] && outcome[inst.Addr] == nil {
						successes++
					}
				}
				require.GreaterOrEqual(t, successes, len(set.Instances)-set.MaxErrors, "iteration %d: returned before key %d reached quorum", it, key)
			}
			returnedMtx.Unlock()
			continue
		}
		require.Error(t, err, "iteration %d: a key missed quorum", it)
		require.NotErrorIs(t, err, context.DeadlineExceeded, "iteration %d", it)
		resp, isHTTP := httpgrpc.HTTPResponseFromError(err)
		require.True(t, isHTTP, "iteration %d: %v", it, err)
		if len(failureCodes) == 1 {
			require.Contains(t, failureCodes, resp.Code, "iteration %d: %v", it, err)
		}
	}
}

// sequentialBatch builds the trackers as DoBatch does and records outcomes in
// the order the test chooses, noting the step at which done or err is sent.
type sequentialBatch struct {
	trackers  []itemTracker
	instances map[string]instance
	tracker   batchTracker
	doneStep  int
	errStep   int
	err       error
	signals   int
}

func newSequentialBatch(sets []ReplicationSet) *sequentialBatch {
	b := &sequentialBatch{
		trackers:  make([]itemTracker, len(sets)),
		instances: map[string]instance{},
		tracker:   batchTracker{done: make(chan struct{}, 1), err: make(chan error, 1)},
		doneStep:  -1,
		errStep:   -1,
	}
	for i, set := range sets {
		b.trackers[i].minSuccess = max(1, len(set.Instances)-set.MaxErrors)
		b.trackers[i].maxFailures = set.MaxErrors
		b.trackers[i].remaining.Store(int32(len(set.Instances)))
		for _, desc := range set.Instances {
			curr := b.instances[desc.Addr]
			b.instances[desc.Addr] = instance{desc: desc, itemTrackers: append(curr.itemTrackers, &b.trackers[i]), indexes: append(curr.indexes, i)}
		}
	}
	b.tracker.rpcsPending.Store(int32(len(sets)))
	return b
}

func (b *sequentialBatch) record(step int, addr string, err error) {
	b.tracker.record(b.instances[addr], err)
	select {
	case <-b.tracker.done:
		b.signals++
		if b.doneStep < 0 {
			b.doneStep = step
		}
	default:
	}
	select {
	case e := <-b.tracker.err:
		b.signals++
		if b.errStep < 0 {
			b.errStep = step
			b.err = e
		}
	default:
	}
}

type rpcResult int

const (
	rpcOK rpcResult = iota
	rpc4xx
	rpc5xx
	numRPCResults
)

func (o rpcResult) err() error {
	switch o {
	case rpc4xx:
		return errRejected
	case rpc5xx:
		return errUnavailable
	}
	return nil
}

func (o rpcResult) code() int32 {
	if o == rpc4xx {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func sortedAddrs(sets []ReplicationSet) []string {
	seen := map[string]struct{}{}
	for _, set := range sets {
		for _, desc := range set.Instances {
			seen[desc.Addr] = struct{}{}
		}
	}
	addrs := make([]string, 0, len(seen))
	for addr := range seen {
		addrs = append(addrs, addr)
	}
	slices.Sort(addrs)
	return addrs
}

// permutations calls f with every permutation of 0..n-1 (Heap's algorithm).
func permutations(n int, f func([]int)) {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	c := make([]int, n)
	f(p)
	for i := 0; i < n; {
		if c[i] < i {
			if i%2 == 0 {
				p[0], p[i] = p[i], p[0]
			} else {
				p[c[i]], p[i] = p[i], p[c[i]]
			}
			f(p)
			c[i]++
			i = 0
		} else {
			c[i] = 0
			i++
		}
	}
}

// checkEveryOrder records every assignment of results to instances, in every
// completion order, and returns the first order DoBatch answers wrongly.
func checkEveryOrder(sets []ReplicationSet) string {
	addrs := sortedAddrs(sets)
	n := len(addrs)
	addrIdx := make(map[string]int, n)
	for i, addr := range addrs {
		addrIdx[addr] = i
	}

	results := make([]rpcResult, n)
	combinations := 1
	for range n {
		combinations *= int(numRPCResults)
	}
	for c := range combinations {
		for i := range results {
			results[i] = rpcResult(c % int(numRPCResults))
			c /= int(numRPCResults)
		}

		var violation string
		permutations(n, func(order []int) {
			if violation != "" {
				return
			}
			step := make([]int, n)
			for s, a := range order {
				step[a] = s
			}

			// Expected answer: success at the step the last key reaches quorum,
			// or an error no later than the last result of the first key to fail.
			allOK, lastQuorumStep, firstFailedStep := true, -1, n
			for _, set := range sets {
				minSuccess := max(1, len(set.Instances)-set.MaxErrors)
				var successSteps, allSteps []int
				for _, desc := range set.Instances {
					a := addrIdx[desc.Addr]
					allSteps = append(allSteps, step[a])
					if results[a] == rpcOK {
						successSteps = append(successSteps, step[a])
					}
				}
				slices.Sort(successSteps)
				if len(successSteps) >= minSuccess {
					lastQuorumStep = max(lastQuorumStep, successSteps[minSuccess-1])
				} else {
					allOK = false
					firstFailedStep = min(firstFailedStep, slices.Max(allSteps))
				}
			}
			// failedAt reports which error codes could explain a failure at step s:
			// those of the keys that had more failures than they tolerate by then.
			failedAt := func(s int) map[int32]bool {
				codes := map[int32]bool{}
				for _, set := range sets {
					var keyCodes []int32
					for _, desc := range set.Instances {
						if a := addrIdx[desc.Addr]; results[a] != rpcOK && step[a] <= s {
							keyCodes = append(keyCodes, results[a].code())
						}
					}
					if len(keyCodes) > len(set.Instances)-max(1, len(set.Instances)-set.MaxErrors) {
						for _, code := range keyCodes {
							codes[code] = true
						}
					}
				}
				return codes
			}

			b := newSequentialBatch(sets)
			for s, a := range order {
				b.record(s, addrs[a], results[a].err())
			}
			switch {
			case b.signals != 1:
				violation = fmt.Sprintf("%d signals", b.signals)
			case allOK && b.doneStep != lastQuorumStep:
				violation = fmt.Sprintf("done at step %d, last key reached quorum at step %d", b.doneStep, lastQuorumStep)
			case !allOK && b.errStep < 0:
				violation = "success although a key missed quorum"
			case !allOK && b.errStep > firstFailedStep:
				violation = fmt.Sprintf("error at step %d, a key had failed by step %d", b.errStep, firstFailedStep)
			case !allOK:
				resp, isHTTP := httpgrpc.HTTPResponseFromError(b.err)
				if codes := failedAt(b.errStep); !isHTTP || !codes[resp.Code] {
					violation = fmt.Sprintf("error %v at step %d, codes of the failed keys %v", b.err, b.errStep, codes)
				}
			}
			if violation != "" {
				var desc strings.Builder
				for _, a := range order {
					fmt.Fprintf(&desc, " %s:%v", addrs[a], results[a].err())
				}
				violation += ", order:" + desc.String()
			}
		})
		if violation != "" {
			return violation
		}
	}
	return ""
}

// Every result and completion order over small key layouts, so the quorum
// accounting does not depend on which RPC returns first.
func TestDoBatch_EveryCompletionOrder(t *testing.T) {
	layouts := map[string][]ReplicationSet{
		"two keys sharing one instance": {
			testReplicationSet(1, "a1", "a2", "s"), testReplicationSet(1, "s", "b2", "b3"),
		},
		"three overlapping keys over 5 instances": {
			testReplicationSet(1, "i1", "i2", "i3"), testReplicationSet(1, "i2", "i3", "i4"), testReplicationSet(1, "i3", "i4", "i5"),
		},
		"five keys over a ring of 5 instances": {
			testReplicationSet(1, "r1", "r2", "r3"), testReplicationSet(1, "r2", "r3", "r4"), testReplicationSet(1, "r3", "r4", "r5"),
			testReplicationSet(1, "r4", "r5", "r1"), testReplicationSet(1, "r5", "r1", "r2"),
		},
		"three keys on the same 3 instances": {
			testReplicationSet(1, "s1", "s2", "s3"), testReplicationSet(1, "s1", "s2", "s3"), testReplicationSet(1, "s1", "s2", "s3"),
		},
		"one replica filtered out next to RF 3 keys": {
			testReplicationSet(0, "u1", "u2"), testReplicationSet(1, "u2", "u3", "u4"), testReplicationSet(1, "u4", "u5", "u1"),
		},
		"every error tolerated next to an RF 3 key": {
			testReplicationSet(3, "z1", "z2", "z3"), testReplicationSet(1, "z3", "b1", "b2"),
		},
		"single key over 4 instances, MaxErrors 3": {
			testReplicationSet(3, "am1", "am2", "am3", "am4"),
		},
	}
	for name, sets := range layouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Empty(t, checkEveryOrder(sets))
		})
	}
}

// sequentialBatch must match what DoBatch answers for the same order, so that
// TestDoBatch_EveryCompletionOrder checks the real code path.
func TestDoBatch_SequentialBatchMatchesDoBatch(t *testing.T) {
	layouts := [][]ReplicationSet{
		{testReplicationSet(1, "a1", "a2", "a3"), testReplicationSet(1, "b1", "b2", "b3")},
		{testReplicationSet(1, "i1", "i2", "i3"), testReplicationSet(1, "i2", "i3", "i4"), testReplicationSet(1, "i4", "i5", "i6")},
		{testReplicationSet(0, "u1", "u2"), testReplicationSet(1, "u2", "u3", "u4"), testReplicationSet(1, "u4", "u5", "u1")},
	}
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Logf("seed: %d", seed)

	for it := range 100 {
		sets := layouts[rnd.Intn(len(layouts))]
		addrs := sortedAddrs(sets)
		results := make(map[string]rpcResult, len(addrs))
		for _, addr := range addrs {
			results[addr] = rpcResult(rnd.Intn(int(numRPCResults)))
		}
		order := rnd.Perm(len(addrs))

		b := newSequentialBatch(sets)
		for s, a := range order {
			b.record(s, addrs[a], results[addrs[a]].err())
		}

		ringSets := make(map[uint32]ReplicationSet, len(sets))
		for i, set := range sets {
			ringSets[uint32(i)] = set
		}
		batch := startScriptedBatch(t, &batchRing{sets: ringSets})
		for _, a := range order {
			batch.complete(addrs[a], results[addrs[a]].err())
		}
		err := batch.wait()
		require.Equal(t, 1, b.signals, "iteration %d", it)
		require.Equal(t, b.errStep >= 0, err != nil, "iteration %d: sequential err %v, DoBatch err %v", it, b.err, err)
	}
}

func newTestRing(t testing.TB, numInstances, numZones int, zoneAware bool) *Ring {
	desc := NewDesc()
	gen := NewRandomTokenGenerator()
	for i := range numInstances {
		id := fmt.Sprintf("instance-%02d", i)
		zone := fmt.Sprintf("zone-%d", i%numZones)
		desc.AddIngester(id, fmt.Sprintf("10.0.0.%d", i), zone, gen.GenerateTokens(desc, id, zone, 32, true), ACTIVE, time.Now())
	}
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.ReplicationFactor = 3
	cfg.ZoneAwarenessEnabled = zoneAware
	// Heartbeats are never refreshed, so they must not go stale during a slow run.
	cfg.HeartbeatTimeout = time.Hour
	return &Ring{
		cfg:                 cfg,
		ringDesc:            desc,
		strategy:            NewDefaultReplicationStrategy(),
		ringTokens:          desc.GetTokens(),
		ringZones:           getZones(desc.getTokensByZone()),
		ringTokensByZone:    desc.getTokensByZone(),
		ringInstanceByToken: desc.getTokensInfo(),
		KVClient:            &MockClient{},
	}
}

// DoBatch over a real ring, with random keys, results and latencies, on a
// worker pool. The answer must match the quorum of each key's replication set,
// and success must not be answered before every key has quorum among the RPCs
// that have returned.
func TestDoBatch_ConcurrentOverRealRing(t *testing.T) {
	iterations := 100
	if testing.Short() {
		iterations = 10
	}
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Logf("seed: %d", seed)
	pool := util.NewWorkerPool("test", 50, nil)
	defer pool.Stop()

	for it := range iterations {
		r := newTestRing(t, 6+rnd.Intn(10), 3, rnd.Intn(2) == 0)
		keys := make([]uint32, 1+rnd.Intn(300))
		for i := range keys {
			keys[i] = rnd.Uint32()
		}
		results := map[string]rpcResult{}
		delays := map[string]time.Duration{}
		pFail := []float64{0, 0.05, 0.2, 0.4}[rnd.Intn(4)]
		for _, inst := range r.ringDesc.Ingesters {
			results[inst.Addr] = rpcOK
			if rnd.Float64() < pFail {
				results[inst.Addr] = rpcResult(1 + rnd.Intn(2))
			}
			delays[inst.Addr] = time.Duration(rnd.Intn(3000)) * time.Microsecond
		}

		wantOK := true
		sets := make([]ReplicationSet, len(keys))
		for i, key := range keys {
			set, err := r.Get(key, Write, nil, nil, nil)
			require.NoError(t, err)
			sets[i] = ReplicationSet{Instances: slices.Clone(set.Instances), MaxErrors: set.MaxErrors}
			successes := 0
			for _, inst := range set.Instances {
				if results[inst.Addr] == rpcOK {
					successes++
				}
			}
			wantOK = wantOK && successes >= len(set.Instances)-set.MaxErrors
		}

		var returnedMtx sync.Mutex
		returned := map[string]bool{}
		var cleanups atomic.Int32
		ctx, cancel := context.WithTimeout(context.Background(), scriptedBatchTimeout)
		err := DoBatch(ctx, Write, r, pool, keys, func(inst InstanceDesc, _ []int) error {
			time.Sleep(delays[inst.Addr])
			returnedMtx.Lock()
			returned[inst.Addr] = true
			returnedMtx.Unlock()
			return results[inst.Addr].err()
		}, func() { cleanups.Inc() })
		cancel()

		require.Equal(t, wantOK, err == nil, "iteration %d: err %v", it, err)
		if err == nil {
			// Marked before the outcome is recorded: can miss an early return, never invent one.
			returnedMtx.Lock()
			for i, set := range sets {
				successes := 0
				for _, inst := range set.Instances {
					if returned[inst.Addr] && results[inst.Addr] == rpcOK {
						successes++
					}
				}
				require.GreaterOrEqual(t, successes, len(set.Instances)-set.MaxErrors, "iteration %d: returned before key %d reached quorum", it, i)
			}
			returnedMtx.Unlock()
		} else {
			require.NotErrorIs(t, err, context.DeadlineExceeded, "iteration %d", it)
		}
		require.Eventually(t, func() bool { return cleanups.Load() == 1 }, scriptedBatchTimeout, time.Millisecond)
	}
}

// An RPC failure counts against every key in the RPC, as when an ingester
// rejects one series and fails its whole Push. A key that shares one such RPC
// still reaches quorum, one that shares two does not.
func TestDoBatch_FailedRPCCountsForEveryKeyInIt(t *testing.T) {
	r := &batchRing{sets: map[uint32]ReplicationSet{
		1: testReplicationSet(1, "i1", "i2", "i3"), // rejected everywhere
		2: testReplicationSet(1, "i3", "i4", "i5"), // shares i3: 2 of 3
		3: testReplicationSet(1, "i4", "i5", "i6"), // clean
	}}
	batch := startScriptedBatch(t, r)
	for _, o := range outcomes(succeed("i4"), succeed("i5"), succeed("i6"), fail4xx("i3")) {
		batch.complete(o.addr, o.err)
	}
	select {
	case err := <-batch.result:
		require.FailNow(t, "key 1 has one failure and two RPCs pending", "err: %v", err)
	default:
	}
	batch.complete("i1", errRejected)
	requireHTTPCode(t, batch.wait(), http.StatusBadRequest)
	batch.complete("i2", errRejected)

	r = &batchRing{sets: map[uint32]ReplicationSet{
		1: testReplicationSet(1, "i1", "i2", "i3"), // rejected everywhere
		2: testReplicationSet(1, "i2", "i3", "i4"), // shares i2 and i3: misses quorum
	}}
	// Only key 2's failures are recorded before DoBatch answers.
	batch = startScriptedBatch(t, r)
	for _, o := range outcomes(succeed("i4"), fail4xx("i2"), fail4xx("i3")) {
		batch.complete(o.addr, o.err)
	}
	requireHTTPCode(t, batch.wait(), http.StatusBadRequest)
	batch.complete("i1", errRejected)
}

func TestDoBatch_OutcomesRecordedAfterContextCanceled(t *testing.T) {
	r := &batchRing{sets: map[uint32]ReplicationSet{
		1: testReplicationSet(1, "a1", "a2", "a3"),
		2: testReplicationSet(1, "b1", "b2", "b3"),
	}}
	var cleanups atomic.Int32
	gates := map[string]chan error{}
	for addr := range r.addrs() {
		gates[addr] = make(chan error, 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- DoBatch(ctx, Write, r, nil, r.keys(), func(inst InstanceDesc, _ []int) error {
			return <-gates[inst.Addr]
		}, func() { cleanups.Inc() })
	}()
	gates["a1"] <- nil
	gates["b1"] <- errUnavailable
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)

	// Outcomes that would send done and err, after nobody waits for them.
	gates["a2"] <- nil
	gates["a3"] <- nil
	gates["b2"] <- errUnavailable
	gates["b3"] <- errUnavailable
	require.Eventually(t, func() bool { return cleanups.Load() == 1 }, scriptedBatchTimeout, time.Millisecond)
}

func TestDoBatch_SuccessesAfterAKeyFailedDoNotReportSuccess(t *testing.T) {
	r := &batchRing{sets: map[uint32]ReplicationSet{
		1: testReplicationSet(1, "a1", "a2", "a3"),
		2: testReplicationSet(1, "b1", "b2", "b3"),
	}}
	batch := startScriptedBatch(t, r)
	batch.complete("b1", errRejected)
	batch.complete("b2", errRejected)
	for _, addr := range []string{"a1", "a2", "a3", "b3"} {
		batch.complete(addr, nil)
	}
	requireHTTPCode(t, batch.wait(), http.StatusBadRequest)
}
