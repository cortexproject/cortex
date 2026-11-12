package ingester

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAppender records which tasks appended to it and reports a configurable
// commit outcome. storage.Appender is embedded as a nil interface: the tests
// that use it never call the sample-appending methods, so only the few methods
// the coalescer itself uses are implemented.
type fakeAppender struct {
	storage.Appender

	id        int
	commitErr error

	// gate blocks Commit until it is closed, which lets a test hold one commit
	// open while it queues further requests behind it.
	gate     chan struct{}
	gateHit  chan struct{}
	gateOnce sync.Once

	mtx       sync.Mutex
	taskIDs   []int
	committed bool
	rolled    bool
}

func (a *fakeAppender) GetRef(labels.Labels, uint64) (storage.SeriesRef, labels.Labels) {
	return 0, labels.EmptyLabels()
}

func (a *fakeAppender) SetOptions(*storage.AppendOptions) {}

func (a *fakeAppender) recordTask(id int) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	a.taskIDs = append(a.taskIDs, id)
}

func (a *fakeAppender) tasks() []int {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	return append([]int(nil), a.taskIDs...)
}

func (a *fakeAppender) Commit() error {
	if a.gate != nil {
		a.gateOnce.Do(func() { close(a.gateHit) })
		<-a.gate
	}
	a.mtx.Lock()
	a.committed = true
	a.mtx.Unlock()
	return a.commitErr
}

func (a *fakeAppender) Rollback() error {
	a.mtx.Lock()
	a.rolled = true
	a.mtx.Unlock()
	return nil
}

// coalesceHarness drives a pushCoalescer with fake appenders and makes batch
// composition deterministic: the first commit is held open, so every request
// started after it is guaranteed to be queued and to end up in the batch that
// runs once the gate is released.
type coalesceHarness struct {
	t *testing.T
	c *pushCoalescer

	commitErr error

	gate    chan struct{}
	gateHit chan struct{}

	mtx       sync.Mutex
	appenders []*fakeAppender
}

func newCoalesceHarness(t *testing.T, cfg PushCoalesceConfig, commitErr error) *coalesceHarness {
	t.Helper()

	cfg.Enabled = true
	h := &coalesceHarness{
		t:         t,
		commitErr: commitErr,
		gate:      make(chan struct{}),
		gateHit:   make(chan struct{}),
	}

	h.c = newPushCoalescer(
		cfg,
		newPushCoalescerMetrics(prometheus.NewPedanticRegistry()),
		log.NewNopLogger(),
		func() extendedAppender {
			h.mtx.Lock()
			defer h.mtx.Unlock()
			app := &fakeAppender{id: len(h.appenders), commitErr: h.commitErr}
			if len(h.appenders) == 0 {
				// Only the first appender gates, so that the batch under test
				// commits without further interference.
				app.gate, app.gateHit = h.gate, h.gateHit
			}
			h.appenders = append(h.appenders, app)
			return app
		},
		func() int64 { return 0 },
	)
	return h
}

// task builds a task whose append records its id on the appender it is given.
func (h *coalesceHarness) task(ctx context.Context, id int, opts ...func(*pushTask)) (*pushTask, *int32) {
	var appends int32
	t := &pushTask{
		ctx:     ctx,
		samples: 1,
		appendFn: func(app extendedAppender, _ int64) error {
			app.(*fakeAppender).recordTask(id)
			appends++
			return nil
		},
	}
	for _, o := range opts {
		o(t)
	}
	return t, &appends
}

func (h *coalesceHarness) pushAsync(t *pushTask) <-chan error {
	res := make(chan error, 1)
	go func() { res <- h.c.push(t) }()
	return res
}

// waitForFirstCommit blocks until the gated first commit has been entered, at
// which point the coalescer is busy and anything pushed next will queue.
func (h *coalesceHarness) waitForFirstCommit() {
	h.t.Helper()
	select {
	case <-h.gateHit:
	case <-time.After(10 * time.Second):
		h.t.Fatal("first commit was never entered")
	}
}

// waitForPending blocks until exactly n tasks are queued.
func (h *coalesceHarness) waitForPending(n int) {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := h.c.pendingLen()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("expected %d queued requests, got %d", n, got)
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *coalesceHarness) release() { close(h.gate) }

func (h *coalesceHarness) appenderList() []*fakeAppender {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	return append([]*fakeAppender(nil), h.appenders...)
}

// batches returns, per appender after the gated one, the task ids it carried.
func (h *coalesceHarness) batches() [][]int {
	var out [][]int
	for _, app := range h.appenderList()[1:] {
		out = append(out, app.tasks())
	}
	return out
}

func mustRecv(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("push did not return")
		return nil
	}
}

// errTestFatalAppend stands in for an append failure that requires the
// appender to be discarded.
var errTestFatalAppend = errors.New("test: append failed fatally")

func defaultCoalesceConfig() PushCoalesceConfig {
	return PushCoalesceConfig{
		Enabled:          true,
		MaxBatchRequests: defaultPushCoalesceMaxBatchRequests,
		MaxBatchSamples:  defaultPushCoalesceMaxBatchSamples,
		MaxBatchBytes:    0,
	}
}

// A request that arrives while nothing is committing must commit on its own
// and not wait for anybody: that is what keeps coalescing free of added
// latency.
func TestPushCoalescer_LoneRequestCommitsImmediately(t *testing.T) {
	t.Parallel()

	h := newCoalesceHarness(t, defaultCoalesceConfig(), nil)
	h.release() // nothing to hold open

	task, appends := h.task(context.Background(), 1)
	require.NoError(t, h.c.push(task))

	require.Equal(t, int32(1), *appends)
	apps := h.appenderList()
	require.Len(t, apps, 1, "a lone request must not create more than one appender")
	require.Equal(t, []int{1}, apps[0].tasks())
	require.True(t, apps[0].committed)
}

// Requests that arrive while a commit is in flight must be folded into one
// commit behind it.
func TestPushCoalescer_QueuedRequestsShareOneCommit(t *testing.T) {
	t.Parallel()

	h := newCoalesceHarness(t, defaultCoalesceConfig(), nil)

	seed, _ := h.task(context.Background(), 0)
	seedDone := h.pushAsync(seed)
	h.waitForFirstCommit()

	const n = 5
	dones := make([]<-chan error, 0, n)
	for id := 1; id <= n; id++ {
		task, _ := h.task(context.Background(), id)
		dones = append(dones, h.pushAsync(task))
		// Enqueue one at a time so that the recorded order is the arrival
		// order; which requests share the commit is what matters here, but an
		// exact assertion reads better than a set comparison.
		h.waitForPending(id)
	}

	h.release()
	require.NoError(t, mustRecv(t, seedDone))
	for _, d := range dones {
		require.NoError(t, mustRecv(t, d))
	}

	require.Equal(t, [][]int{{1, 2, 3, 4, 5}}, h.batches(),
		"all five queued requests should have shared a single commit")
}

// A commit failure is the one case where every request in the batch has to be
// told, because none of their samples reached the WAL.
func TestPushCoalescer_CommitFailureFailsWholeBatch(t *testing.T) {
	t.Parallel()

	commitErr := errors.New("wal is full")
	h := newCoalesceHarness(t, defaultCoalesceConfig(), commitErr)

	seed, _ := h.task(context.Background(), 0)
	seedDone := h.pushAsync(seed)
	h.waitForFirstCommit()

	const n = 4
	dones := make([]<-chan error, 0, n)
	for id := 1; id <= n; id++ {
		task, _ := h.task(context.Background(), id)
		dones = append(dones, h.pushAsync(task))
	}
	h.waitForPending(n)
	h.release()

	require.ErrorIs(t, mustRecv(t, seedDone), commitErr)
	for i, d := range dones {
		require.ErrorIsf(t, mustRecv(t, d), commitErr, "request %d", i+1)
	}
}

// A request that fails in a way that requires discarding the appender takes the
// whole batch down with it, because an appender cannot discard one request's
// samples alone. The offender gets its own error; everybody else gets a
// retryable one, since their samples were never committed.
func TestPushCoalescer_FatalAppendRollsBackWholeBatch(t *testing.T) {
	t.Parallel()

	h := newCoalesceHarness(t, defaultCoalesceConfig(), nil)

	seed, _ := h.task(context.Background(), 0)
	seedDone := h.pushAsync(seed)
	h.waitForFirstCommit()

	fatalErr := errors.New("label set is out of order")

	okTask, _ := h.task(context.Background(), 1)
	okDone := h.pushAsync(okTask)
	h.waitForPending(1)

	badTask, _ := h.task(context.Background(), 2)
	badTask.appendFn = func(extendedAppender, int64) error { return fatalErr }
	badDone := h.pushAsync(badTask)
	h.waitForPending(2)

	lateTask, _ := h.task(context.Background(), 3)
	lateDone := h.pushAsync(lateTask)
	h.waitForPending(3)

	h.release()

	require.NoError(t, mustRecv(t, seedDone))
	require.ErrorIs(t, mustRecv(t, badDone), fatalErr)
	require.ErrorIs(t, mustRecv(t, okDone), errPushCoalesceBatchRolledBack)
	require.ErrorIs(t, mustRecv(t, lateDone), errPushCoalesceBatchRolledBack)

	apps := h.appenderList()
	require.Len(t, apps, 2)
	require.True(t, apps[1].rolled, "the batch appender should have been rolled back")
	require.False(t, apps[1].committed)
}

// A caller that has already given up is told so, and its samples are not
// committed on its behalf.
func TestPushCoalescer_ContextCanceledBeforeJoining(t *testing.T) {
	t.Parallel()

	h := newCoalesceHarness(t, defaultCoalesceConfig(), nil)
	h.release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	task, appends := h.task(ctx, 1)
	require.ErrorIs(t, h.c.push(task), context.Canceled)
	require.Equal(t, int32(0), *appends, "a canceled request must not be appended")
	require.Empty(t, h.appenderList(), "a canceled request must not create an appender")
}

// A request abandoned while it sat in the queue is dropped when the batch runs,
// without affecting the rest of the batch.
func TestPushCoalescer_ContextCanceledWhileQueued(t *testing.T) {
	t.Parallel()

	h := newCoalesceHarness(t, defaultCoalesceConfig(), nil)

	seed, _ := h.task(context.Background(), 0)
	seedDone := h.pushAsync(seed)
	h.waitForFirstCommit()

	keepCtx := context.Background()
	dropCtx, cancel := context.WithCancel(context.Background())

	keep, keepAppends := h.task(keepCtx, 1)
	keepDone := h.pushAsync(keep)
	h.waitForPending(1)

	drop, dropAppends := h.task(dropCtx, 2)
	dropDone := h.pushAsync(drop)
	h.waitForPending(2)

	// Give up only after the request is safely queued.
	cancel()
	h.release()

	require.NoError(t, mustRecv(t, seedDone))
	require.NoError(t, mustRecv(t, keepDone))
	require.ErrorIs(t, mustRecv(t, dropDone), context.Canceled)

	require.Equal(t, int32(1), *keepAppends)
	require.Equal(t, int32(0), *dropAppends, "an abandoned request must not be appended")
	require.Equal(t, [][]int{{1}}, h.batches())
}

func TestPushCoalescer_BatchLimits(t *testing.T) {
	t.Parallel()

	withSamples := func(n int) func(*pushTask) {
		return func(t *pushTask) { t.samples = n }
	}
	withBytes := func(n int) func(*pushTask) {
		return func(t *pushTask) { t.bytes = n }
	}
	withDiscardOOO := func(v bool) func(*pushTask) {
		return func(t *pushTask) { t.discardOutOfOrder = v }
	}

	for _, tc := range []struct {
		name    string
		cfg     func(*PushCoalesceConfig)
		opts    [][]func(*pushTask)
		batches [][]int
	}{
		{
			name: "max requests truncates and the remainder forms the next batch",
			cfg:  func(c *PushCoalesceConfig) { c.MaxBatchRequests = 2 },
			opts: [][]func(*pushTask){nil, nil, nil, nil, nil},
			// Requests 1 and 2 share a commit; 3 and 4 the next; 5 the last.
			batches: [][]int{{1, 2}, {3, 4}, {5}},
		},
		{
			name: "max samples truncates",
			cfg:  func(c *PushCoalesceConfig) { c.MaxBatchSamples = 10 },
			opts: [][]func(*pushTask){
				{withSamples(6)},
				{withSamples(4)},
				{withSamples(5)},
			},
			batches: [][]int{{1, 2}, {3}},
		},
		{
			name: "a request larger than the sample limit still leads its own batch",
			cfg:  func(c *PushCoalesceConfig) { c.MaxBatchSamples = 10 },
			opts: [][]func(*pushTask){
				{withSamples(4)},
				{withSamples(100)},
				{withSamples(4)},
			},
			batches: [][]int{{1}, {2}, {3}},
		},
		{
			name: "max bytes truncates",
			cfg:  func(c *PushCoalesceConfig) { c.MaxBatchBytes = 1000 },
			opts: [][]func(*pushTask){
				{withBytes(600)},
				{withBytes(300)},
				{withBytes(400)},
			},
			batches: [][]int{{1, 2}, {3}},
		},
		{
			name: "requests disagreeing on discarding out of order samples do not share a commit",
			cfg:  func(*PushCoalesceConfig) {},
			opts: [][]func(*pushTask){
				{withDiscardOOO(false)},
				{withDiscardOOO(false)},
				{withDiscardOOO(true)},
				{withDiscardOOO(true)},
			},
			batches: [][]int{{1, 2}, {3, 4}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultCoalesceConfig()
			tc.cfg(&cfg)

			h := newCoalesceHarness(t, cfg, nil)
			seed, _ := h.task(context.Background(), 0)
			seedDone := h.pushAsync(seed)
			h.waitForFirstCommit()

			dones := make([]<-chan error, 0, len(tc.opts))
			for idx, opts := range tc.opts {
				task, _ := h.task(context.Background(), idx+1, opts...)
				dones = append(dones, h.pushAsync(task))
				// Enqueue one at a time so that arrival order is the order the
				// batch slicer sees.
				h.waitForPending(idx + 1)
			}

			h.release()
			require.NoError(t, mustRecv(t, seedDone))
			for _, d := range dones {
				require.NoError(t, mustRecv(t, d))
			}

			assert.Equal(t, tc.batches, h.batches())
		})
	}
}

// Under concurrency the coalescer must commit every request exactly once and
// leave nobody blocked. Run with -race.
func TestPushCoalescer_Concurrent(t *testing.T) {
	t.Parallel()

	h := newCoalesceHarness(t, defaultCoalesceConfig(), nil)
	h.release()

	const (
		goroutines = 64
		perRoutine = 50
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for n := 0; n < perRoutine; n++ {
				task, appends := h.task(context.Background(), g*perRoutine+n)
				require.NoError(t, h.c.push(task))
				require.Equal(t, int32(1), *appends)
			}
		}(g)
	}
	wg.Wait()

	// Every request appended exactly once, across however many commits the
	// coalescer decided to use.
	seen := map[int]int{}
	total := 0
	for _, app := range h.appenderList() {
		for _, id := range app.tasks() {
			seen[id]++
			total++
		}
	}
	require.Equal(t, goroutines*perRoutine, total)
	require.Len(t, seen, goroutines*perRoutine)
	for id, count := range seen {
		require.Equalf(t, 1, count, "task %d appended %d times", id, count)
	}

	// The coalescer must be idle again, with nothing queued.
	require.True(t, h.c.isIdle())

	// It must also have done real batching, otherwise the test proves nothing
	// about the mechanism it is meant to exercise.
	require.Less(t, len(h.appenderList()), goroutines*perRoutine,
		"expected fewer commits than requests")
	t.Logf("%d requests committed in %d commits", total, len(h.appenderList()))
}

func TestPushCoalesceConfig_Validate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  PushCoalesceConfig
		err  string
	}{
		{name: "disabled config is never rejected", cfg: PushCoalesceConfig{}},
		{
			name: "max batch requests must be positive",
			cfg:  PushCoalesceConfig{Enabled: true, MaxBatchRequests: 0},
			err:  "ingester.push-coalesce.max-batch-requests must be at least 1",
		},
		{
			name: "max batch samples must not be negative",
			cfg:  PushCoalesceConfig{Enabled: true, MaxBatchRequests: 1, MaxBatchSamples: -1},
			err:  "ingester.push-coalesce.max-batch-samples must not be negative",
		},
		{
			name: "max batch bytes must not be negative",
			cfg:  PushCoalesceConfig{Enabled: true, MaxBatchRequests: 1, MaxBatchBytes: -1},
			err:  "ingester.push-coalesce.max-batch-bytes must not be negative",
		},
		{name: "valid config", cfg: defaultCoalesceConfig()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.cfg.Validate()
			if tc.err == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.err)
		})
	}
}

func TestPushCoalescer_SliceBatchReportsTruncationReason(t *testing.T) {
	t.Parallel()

	cfg := defaultCoalesceConfig()
	cfg.MaxBatchRequests = 2
	reg := prometheus.NewPedanticRegistry()
	c := newPushCoalescer(cfg, newPushCoalescerMetrics(reg), log.NewNopLogger(),
		func() extendedAppender { return &fakeAppender{} },
		func() int64 { return 0 })

	pending := []*pushTask{{samples: 1}, {samples: 1}, {samples: 1}}
	leader, followers, rest := c.sliceBatch(pending)

	require.Same(t, pending[0], leader)
	require.Len(t, followers, 1)
	require.Len(t, rest, 1)
	require.Equal(t, 1.0, promCounterValue(t, reg,
		"cortex_ingester_push_coalesce_batch_truncated_total", truncateReasonMaxRequests))
}

func promCounterValue(t *testing.T, reg *prometheus.Registry, name, label string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetValue() == label {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("metric %s with label %s not found", name, label)
	return 0
}
