package ingester

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// Tests for push coalescing, the merging of concurrent Push requests for the
// same tenant into one TSDB commit (push_coalesce.go).
//
// The file is in two halves. The first drives pushCoalescer directly with fake
// appenders, so that batch composition, error delivery and cancellation can be
// controlled exactly. The second pushes real write requests through
// Ingester.Push into a real TSDB, and compares the outcome of a coalesced batch
// with the outcome of committing the same requests one at a time.

// ----------------------------------------------------------------------------
// pushCoalescer, driven directly with fake appenders.
// ----------------------------------------------------------------------------

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

// A request abandoned while queued keeps its own context error even when the
// batch it was queued in is rolled back because a different request failed.
// Without the explicit dropped flag, re-reading the context during the
// rollback could reclassify it.
func TestPushCoalescer_CanceledRequestKeepsItsErrorThroughRollback(t *testing.T) {
	t.Parallel()

	h := newCoalesceHarness(t, defaultCoalesceConfig(), nil)

	seed, _ := h.task(context.Background(), 0)
	seedDone := h.pushAsync(seed)
	h.waitForFirstCommit()

	dropCtx, cancel := context.WithCancel(context.Background())
	drop, dropAppends := h.task(dropCtx, 1)
	dropDone := h.pushAsync(drop)
	h.waitForPending(1)

	fatalErr := errTestFatalAppend
	bad, _ := h.task(context.Background(), 2)
	bad.appendFn = func(extendedAppender, int64) error { return fatalErr }
	badDone := h.pushAsync(bad)
	h.waitForPending(2)

	other, _ := h.task(context.Background(), 3)
	otherDone := h.pushAsync(other)
	h.waitForPending(3)

	// Abandon the first queued request only after it is safely in the queue,
	// so the leader meets it already canceled.
	cancel()
	h.release()

	require.NoError(t, mustRecv(t, seedDone))
	require.ErrorIs(t, mustRecv(t, dropDone), context.Canceled,
		"a dropped request must keep its context error, not the rollback error")
	require.ErrorIs(t, mustRecv(t, badDone), fatalErr)
	require.ErrorIs(t, mustRecv(t, otherDone), errPushCoalesceBatchRolledBack)
	require.Equal(t, int32(0), *dropAppends)
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
	for g := range goroutines {
		wg.Go(func() {
			for n := range perRoutine {
				task, appends := h.task(context.Background(), g*perRoutine+n)
				require.NoError(t, h.c.push(task))
				require.Equal(t, int32(1), *appends)
			}
		})
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

// ----------------------------------------------------------------------------
// Ingester.Push into a real TSDB: helpers.
// ----------------------------------------------------------------------------

const (
	coalesceTestUser = "coalesce-user"

	// The out-of-order window the tests run with, in milliseconds.
	coalesceTestOOOWindowMs = int64(30 * 60 * 1000)

	// Base timestamp the scenarios build on. Far enough from zero that samples
	// can be placed before it without becoming out of bounds.
	coalesceTestBaseMs = int64(10 * 60 * 60 * 1000)
)

// writeRequestSpec describes one write request. Requests are rebuilt from the
// spec for every run, because Push returns the request's time series to a pool.
type writeRequestSpec struct {
	series []labels.Labels
	// samples[i] belongs to series[i].
	samples []cortexpb.Sample
	// histograms[i] belongs to series[i].
	histograms []cortexpb.WrappedHistogram
	// exemplars are all attached to the first series.
	exemplars []cortexpb.Exemplar
}

func (s writeRequestSpec) build() *cortexpb.WriteRequest {
	req := cortexpb.ToWriteRequest(s.series, s.samples, nil, s.histograms, cortexpb.API)
	for _, ex := range s.exemplars {
		// The exemplar's label adapters must be copied per build. Push returns
		// the time series to a pool, and cortexpb.ReuseTimeseries clears
		// exemplar label names and values in place, which would write through
		// to the spec's own slice and leave a later build with empty labels.
		copied := cortexpb.Exemplar{TimestampMs: ex.TimestampMs, Value: ex.Value}
		copied.Labels = append(copied.Labels, ex.Labels...)
		req.Timeseries[0].Exemplars = append(req.Timeseries[0].Exemplars, copied)
	}
	return req
}

func coalesceTestSeries(name string) labels.Labels {
	return labels.FromStrings(labels.MetricName, name)
}

// oneSample is a request carrying a single sample for a single series.
func oneSample(seriesName string, tMs int64, v float64) writeRequestSpec {
	return writeRequestSpec{
		series:  []labels.Labels{coalesceTestSeries(seriesName)},
		samples: []cortexpb.Sample{{TimestampMs: tMs, Value: v}},
	}
}

// histogramRequest builds a request carrying one native histogram.
func histogramRequest(seriesName string, tMs int64, seed int) writeRequestSpec {
	return writeRequestSpec{
		series: []labels.Labels{coalesceTestSeries(seriesName)},
		histograms: []cortexpb.WrappedHistogram{
			cortexpb.WrapHistogram(cortexpb.HistogramToHistogramProto(tMs, tsdbutil.GenerateTestHistogram(int64(seed)))),
		},
	}
}

// exemplarRequest builds a request carrying one float sample and one exemplar
// attached to it.
func exemplarRequest(seriesName string, tMs int64, v float64, traceID string) writeRequestSpec {
	return writeRequestSpec{
		series:  []labels.Labels{coalesceTestSeries(seriesName)},
		samples: []cortexpb.Sample{{TimestampMs: tMs, Value: v}},
		exemplars: []cortexpb.Exemplar{{
			Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: traceID}},
			TimestampMs: tMs,
			Value:       v,
		}},
	}
}

// startCoalesceTestIngester starts an ingester with the given push coalescing
// configuration, an out-of-order window, native histograms and exemplars
// enabled, and waits until it is ACTIVE in the ring.
func startCoalesceTestIngester(t *testing.T, coalesce PushCoalesceConfig) (*Ingester, *prometheus.Registry) {
	t.Helper()

	cfg := defaultIngesterTestConfig(t)
	cfg.PushCoalesce = coalesce

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = model.Duration(time.Duration(coalesceTestOOOWindowMs) * time.Millisecond)
	limits.EnableNativeHistograms = true
	limits.MaxExemplars = 10

	reg := prometheus.NewPedanticRegistry()
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ing) })

	test.Poll(t, time.Second, ring.ACTIVE, func() any {
		return ing.lifecycler.GetState()
	})
	return ing, reg
}

// pushSequentially pushes each request on its own and returns the error each
// one produced. This is the behaviour coalescing has to reproduce.
func pushSequentially(t *testing.T, ing *Ingester, specs []writeRequestSpec) []error {
	t.Helper()

	ctx := user.InjectOrgID(context.Background(), coalesceTestUser)
	errs := make([]error, 0, len(specs))
	for _, spec := range specs {
		_, err := ing.Push(ctx, spec.build())
		errs = append(errs, err)
	}
	return errs
}

// mirrorBatchHarnessPushes replays, on an ingester driven sequentially, the two
// requests pushAsOneBatch issues of its own accord: one to create the tenant's
// TSDB and one to hold a commit open. Without this the two sides would have
// pushed a different number of requests and their counters could not be
// compared.
func mirrorBatchHarnessPushes(t *testing.T, ing *Ingester) {
	t.Helper()
	for _, err := range pushSequentially(t, ing, []writeRequestSpec{
		oneSample("coalesce_gate_seed", coalesceTestBaseMs, 1),
		oneSample("coalesce_gate_holder", coalesceTestBaseMs, 1),
	}) {
		require.NoError(t, err)
	}
}

// pushAsOneBatch pushes every request into a single coalesced commit, and
// returns the error each one produced.
//
// Batch composition is made deterministic rather than left to chance: a gate
// holds one commit open, every request under test is queued behind it, and only
// then is the gate released, so the queued requests necessarily form one batch.
func pushAsOneBatch(t *testing.T, ing *Ingester, specs []writeRequestSpec) []error {
	t.Helper()

	ctx := user.InjectOrgID(context.Background(), coalesceTestUser)

	// The tenant's TSDB, and with it the coalescer, is created by the first
	// push. Seed it with a series none of the scenarios touch.
	_, err := ing.Push(ctx, oneSample("coalesce_gate_seed", coalesceTestBaseMs, 1).build())
	require.NoError(t, err)

	db, err := ing.getTSDB(coalesceTestUser)
	require.NoError(t, err)
	require.NotNil(t, db.pushCoalescer, "push coalescing must be enabled for this test")

	// Replace the coalescer with one whose first appender blocks in Commit.
	// Everything else about it, including the real TSDB appender, is unchanged.
	gate := make(chan struct{})
	gateHit := make(chan struct{})
	var (
		mtx       sync.Mutex
		appenders int
	)
	db.pushCoalescer = newPushCoalescer(
		ing.cfg.PushCoalesce,
		ing.metrics.pushCoalesce,
		log.NewNopLogger(),
		func() extendedAppender {
			real := db.Appender(context.Background()).(extendedAppender)
			mtx.Lock()
			first := appenders == 0
			appenders++
			mtx.Unlock()
			if !first {
				return real
			}
			return &gatedAppender{extendedAppender: real, gate: gate, gateHit: gateHit}
		},
		func() int64 { return db.Head().MaxTime() },
	)

	// Hold a commit open on a series no scenario uses.
	gateDone := make(chan error, 1)
	go func() {
		_, err := ing.Push(ctx, oneSample("coalesce_gate_holder", coalesceTestBaseMs, 1).build())
		gateDone <- err
	}()
	select {
	case <-gateHit:
	case <-time.After(30 * time.Second):
		t.Fatal("gate commit was never entered")
	}

	// Queue every request under test behind the held commit, one at a time so
	// that they reach the batch in the order the scenario declares them.
	results := make([]chan error, len(specs))
	for idx, spec := range specs {
		results[idx] = make(chan error, 1)
		go func(spec writeRequestSpec, out chan error) {
			_, err := ing.Push(ctx, spec.build())
			out <- err
		}(spec, results[idx])

		deadline := time.Now().Add(30 * time.Second)
		for {
			queued := db.pushCoalescer.pendingLen()
			if queued == idx+1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("expected %d queued requests, got %d", idx+1, queued)
			}
			time.Sleep(time.Millisecond)
		}
	}

	close(gate)
	require.NoError(t, <-gateDone)

	errs := make([]error, len(specs))
	for idx, res := range results {
		select {
		case errs[idx] = <-res:
		case <-time.After(30 * time.Second):
			t.Fatalf("request %d did not return", idx)
		}
	}
	return errs
}

// gatedAppender blocks in Commit until its gate is closed.
type gatedAppender struct {
	extendedAppender

	gate    chan struct{}
	gateHit chan struct{}
	once    sync.Once
}

func (a *gatedAppender) Commit() error {
	a.once.Do(func() { close(a.gateHit) })
	<-a.gate
	return a.extendedAppender.Commit()
}

var _ storage.Appender = (*gatedAppender)(nil)

// headSample is one sample as stored in the head, in-order or out-of-order.
type headSample struct {
	T int64
	V float64
}

// dumpHead returns every sample the tenant's head holds, keyed by series. The
// querier covers the out-of-order head as well, so a sample that landed there
// shows up here. Histogram samples are reported by their count plus sum, so
// that a comparison does not depend on bucket layout details.
func dumpHead(t *testing.T, ing *Ingester) map[string][]headSample {
	t.Helper()

	db, err := ing.getTSDB(coalesceTestUser)
	require.NoError(t, err)

	q, err := db.Querier(0, coalesceTestBaseMs*10)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	matcher, err := labels.NewMatcher(labels.MatchRegexp, labels.MetricName, ".+")
	require.NoError(t, err)

	out := map[string][]headSample{}
	set := q.Select(context.Background(), true, nil, matcher)
	for set.Next() {
		s := set.At()
		key := s.Labels().String()
		it := s.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				ts, v := it.At()
				out[key] = append(out[key], headSample{T: ts, V: v})
			case chunkenc.ValHistogram:
				ts, h := it.AtHistogram(nil)
				out[key] = append(out[key], headSample{T: ts, V: float64(h.Count) + h.Sum})
			case chunkenc.ValFloatHistogram:
				ts, fh := it.AtFloatHistogram(nil)
				out[key] = append(out[key], headSample{T: ts, V: fh.Count + fh.Sum})
			}
		}
		require.NoError(t, it.Err())
	}
	require.NoError(t, set.Err())
	require.Empty(t, set.Warnings().AsErrors())

	for key := range out {
		slices.SortFunc(out[key], func(a, b headSample) int { return cmp.Compare(a.T, b.T) })
	}
	return out
}

// dumpExemplars returns every exemplar stored for the named series, sorted so
// the comparison does not depend on query order.
func dumpExemplars(t *testing.T, ing *Ingester, seriesNames ...string) []string {
	t.Helper()

	db, err := ing.getTSDB(coalesceTestUser)
	require.NoError(t, err)

	q, err := db.ExemplarQuerier(context.Background())
	require.NoError(t, err)

	var matchers [][]*labels.Matcher
	for _, name := range seriesNames {
		m, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, name)
		require.NoError(t, err)
		matchers = append(matchers, []*labels.Matcher{m})
	}

	res, err := q.Select(0, coalesceTestBaseMs*10, matchers...)
	require.NoError(t, err)

	var out []string
	for _, r := range res {
		for _, e := range r.Exemplars {
			out = append(out, r.SeriesLabels.String()+" "+exemplarString(e))
		}
	}
	slices.Sort(out)
	return out
}

func exemplarString(e exemplar.Exemplar) string {
	return e.Labels.String() + "@" + model.Time(e.Ts).String()
}

func errStrings(errs []error) []string {
	out := make([]string, len(errs))
	for i, err := range errs {
		if err == nil {
			out[i] = "<nil>"
			continue
		}
		out[i] = err.Error()
	}
	return out
}

// ----------------------------------------------------------------------------
// Ingester.Push: what a coalesced batch changes compared with separate commits.
// ----------------------------------------------------------------------------

// coalesceScenario is a set of write requests whose interaction with each other
// is what makes merging their commits interesting, together with what merging
// them is expected to change.
//
// A batch shares one appender, and Append only sees committed state while
// Commit re-checks against the state earlier samples in the same batch left
// behind, counting rather than returning what it then rejects. So a batch can
// lose an error a separate commit would have returned, and in one case can
// accept a sample a separate commit would have rejected. Every scenario below
// declares which, so that any change in that set fails here rather than being
// discovered in production.
type coalesceScenario struct {
	name  string
	specs []writeRequestSpec

	// errorsDiffer records that at least one request gets a different error
	// than it would from a separate commit.
	errorsDiffer bool
	// dataDiffers records that the head ends up holding different samples.
	dataDiffers bool
}

func coalesceScenarios() []coalesceScenario {
	const (
		base = coalesceTestBaseMs
		win  = coalesceTestOOOWindowMs
	)

	return []coalesceScenario{
		{
			// The control: nothing shares a series, so merging changes nothing.
			name: "disjoint series",
			specs: []writeRequestSpec{
				oneSample("disjoint_a", base+1000, 1),
				oneSample("disjoint_b", base+1000, 2),
				oneSample("disjoint_c", base+1000, 3),
			},
		},
		{
			// Separate commits reject the second sample at Append time with a
			// duplicate error. A shared appender only finds out at Commit
			// time, where the error is counted and dropped rather than
			// returned, so the client is told its write succeeded. The sample
			// is dropped either way.
			//
			// Note that Cortex already behaves this way when the duplicate
			// arrives inside one request: see
			// TestIngesterPush_DuplicateIsAlreadySilentWithinOneRequest.
			name: "same series, same timestamp, different value",
			specs: []writeRequestSpec{
				oneSample("dup", base+1000, 1),
				oneSample("dup", base+1000, 2),
			},
			errorsDiffer: true,
		},
		{
			// An exact duplicate is accepted by Prometheus on purpose, so both
			// paths must report success.
			name: "same series, same timestamp, same value",
			specs: []writeRequestSpec{
				oneSample("exact_dup", base+1000, 7),
				oneSample("exact_dup", base+1000, 7),
			},
		},
		{
			// In-order then out-of-order, inside the window: accepted into the
			// out-of-order head either way.
			name: "same series, out of order within the window",
			specs: []writeRequestSpec{
				oneSample("ooo_in_window", base+1000, 1),
				oneSample("ooo_in_window", base+1000-win/2, 2),
			},
		},
		{
			// Out-of-order beyond the window. Separate commits return a
			// too-old error on the second request; the batch drops the sample
			// without reporting it. Same data either way.
			name: "same series, out of order beyond the window",
			specs: []writeRequestSpec{
				oneSample("ooo_too_old", base+1000, 1),
				oneSample("ooo_too_old", base+1000-2*win, 2),
			},
			errorsDiffer: true,
		},
		{
			// The only scenario where the stored data differs: a sample that
			// Append accepts against committed state, and that a larger sample
			// earlier in the same batch would have pushed outside the window
			// had it been committed first. The batch accepts it into the
			// out-of-order head; separate commits reject it as too old.
			//
			// The timestamps here exaggerate the effect to make it reachable at
			// all. TestIngesterPushCoalescing_OutOfOrderEscapeIsBoundedByInBatchHeadAdvance
			// bounds how far it can actually reach.
			name: "same series, in-batch sample pushes a later one out of the window",
			specs: []writeRequestSpec{
				oneSample("ooo_pushed_out", base+1000, 1),
				oneSample("ooo_pushed_out", base+1000+4*win, 2),
				oneSample("ooo_pushed_out", base+1000+win, 3),
			},
			errorsDiffer: true,
			dataDiffers:  true,
		},
		{
			// Several samples for one series inside a single request behave
			// identically in both modes, because a single request has always
			// shared one appender.
			name: "several samples for one series inside one request",
			specs: []writeRequestSpec{
				{
					series: []labels.Labels{
						coalesceTestSeries("intra"),
						coalesceTestSeries("intra"),
					},
					samples: []cortexpb.Sample{
						{TimestampMs: base + 1000, Value: 1},
						{TimestampMs: base + 1000, Value: 2},
					},
				},
				oneSample("intra_other", base+1000, 3),
			},
		},
		{
			// A new series created by one request and written again by another
			// in the same batch: the second request finds the series through
			// GetRef even though it has not been committed yet.
			name: "new series written by two requests",
			specs: []writeRequestSpec{
				oneSample("fresh", base+1000, 1),
				oneSample("fresh", base+2000, 2),
				oneSample("fresh", base+1000, 3),
			},
		},
	}
}

// A coalesced commit must change exactly what each scenario says it changes,
// and nothing else: the same error for every other request, and the same data
// in the head afterwards, including the out-of-order head.
func TestIngesterPushCoalescing_DivergenceFromSeparateCommits(t *testing.T) {
	for _, sc := range coalesceScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			sequential, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
			coalesced, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())

			// The sequential run gets the same seed and gate-holder requests
			// the coalesced run needs, so the two heads start out identical.
			seedErrs := pushSequentially(t, sequential, []writeRequestSpec{
				oneSample("coalesce_gate_seed", coalesceTestBaseMs, 1),
				oneSample("coalesce_gate_holder", coalesceTestBaseMs, 1),
			})
			for _, err := range seedErrs {
				require.NoError(t, err)
			}

			wantErrs := errStrings(pushSequentially(t, sequential, sc.specs))
			gotErrs := errStrings(pushAsOneBatch(t, coalesced, sc.specs))
			wantHead := dumpHead(t, sequential)
			gotHead := dumpHead(t, coalesced)

			if sc.errorsDiffer {
				require.NotEqual(t, wantErrs, gotErrs,
					"this scenario is expected to lose an error; if it no longer does, "+
						"the upstream TSDB has changed and this expectation should be dropped")
				t.Logf("separate: %v", wantErrs)
				t.Logf("coalesced: %v", gotErrs)
			} else {
				require.Equal(t, wantErrs, gotErrs,
					"per-request errors must match separate commits")
			}

			if sc.dataDiffers {
				require.NotEqual(t, wantHead, gotHead,
					"this scenario is expected to store different data; if it no longer does, "+
						"the upstream TSDB has changed and this expectation should be dropped")
				t.Logf("separate head: %v", wantHead)
				t.Logf("coalesced head: %v", gotHead)
			} else {
				require.Equal(t, wantHead, gotHead,
					"head contents must match separate commits")
			}
		})
	}
}

// How far outside the out-of-order window can a sample get in, when two
// requests for the same series share one batch?
//
// The divergence window is derivable: a batch's appender keeps the head max
// time H it snapshotted before the batch, while separate commits would let the
// first request advance it to H' = max(H, t_X). A sample at t_Y is therefore
// accepted by the batch but rejected by separate commits exactly when
//
//	H - W <= t_Y < t_X - W
//
// so the band of timestamps that behave differently is (t_X - H) wide: the
// amount the batch's own newest sample advances the head max. In steady state
// that is one scrape interval, not a window's worth.
//
// This test pins that bound by placing t_Y just inside and just outside the
// band and showing where the stored data starts to differ.
func TestIngesterPushCoalescing_OutOfOrderEscapeIsBoundedByInBatchHeadAdvance(t *testing.T) {
	const (
		base = coalesceTestBaseMs
		win  = coalesceTestOOOWindowMs

		// How far beyond the pre-batch head max the batch's newest sample sits.
		// A scrape interval, which is what it is in steady state.
		advance = int64(15000)
	)

	for _, tc := range []struct {
		name string
		// Offset of t_Y relative to the band's lower edge, base-win.
		offsetIntoBand int64
		inBand         bool
	}{
		{
			name:           "t_Y inside the band: the batch stores a sample separate commits reject",
			offsetIntoBand: advance / 2,
			inBand:         true,
		},
		{
			name:           "t_Y below the band: both reject it, only the error differs",
			offsetIntoBand: -5000,
			inBand:         false,
		},
		{
			name:           "t_Y above the band: both accept it",
			offsetIntoBand: advance + 5000,
			inBand:         false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			series := fmt.Sprintf("escape_%d", tc.offsetIntoBand)
			tX := base + advance
			tY := base - win + tc.offsetIntoBand

			specs := []writeRequestSpec{
				oneSample(series, tX, 1),
				oneSample(series, tY, 2),
			}

			sequential, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
			for _, err := range pushSequentially(t, sequential, []writeRequestSpec{
				oneSample("coalesce_gate_seed", base, 1),
				oneSample("coalesce_gate_holder", base, 1),
			}) {
				require.NoError(t, err)
			}
			wantErrs := pushSequentially(t, sequential, specs)

			coalesced, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())
			gotErrs := pushAsOneBatch(t, coalesced, specs)

			wantSamples := dumpHead(t, sequential)[coalesceTestSeries(series).String()]
			gotSamples := dumpHead(t, coalesced)[coalesceTestSeries(series).String()]

			t.Logf("t_X - H = %d ms, t_Y - (H - W) = %d ms", advance, tc.offsetIntoBand)
			t.Logf("separate: errs=%v samples=%v", errStrings(wantErrs), wantSamples)
			t.Logf("coalesced: errs=%v samples=%v", errStrings(gotErrs), gotSamples)

			if tc.inBand {
				require.Len(t, wantSamples, 1, "separate commits reject the out-of-window sample")
				require.Len(t, gotSamples, 2, "the batch accepts it")
				return
			}
			require.Equal(t, wantSamples, gotSamples,
				"outside the band the stored data must be identical")
		})
	}
}

// Cortex already reports a duplicate differently depending on whether it
// arrives in one request or two, with coalescing nowhere in the picture: one
// request sharing one appender hides it, two requests each committing on their
// own surface it as a 400. Coalescing widens the first case from "one request"
// to "one batch", so this is the baseline the lost duplicate error has to be
// judged against.
func TestIngesterPush_DuplicateIsAlreadySilentWithinOneRequest(t *testing.T) {
	ing, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})

	withinOneRequest := pushSequentially(t, ing, []writeRequestSpec{{
		series: []labels.Labels{
			coalesceTestSeries("intra_dup"),
			coalesceTestSeries("intra_dup"),
		},
		samples: []cortexpb.Sample{
			{TimestampMs: coalesceTestBaseMs + 1000, Value: 1},
			{TimestampMs: coalesceTestBaseMs + 1000, Value: 2},
		},
	}})
	require.NoError(t, withinOneRequest[0],
		"a duplicate inside one request is silently dropped and reported as success")
	require.Equal(t,
		[]headSample{{T: coalesceTestBaseMs + 1000, V: 1}},
		dumpHead(t, ing)[coalesceTestSeries("intra_dup").String()],
		"only the first value is stored")

	other, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
	acrossTwoRequests := pushSequentially(t, other, []writeRequestSpec{
		oneSample("split_dup", coalesceTestBaseMs+1000, 1),
		oneSample("split_dup", coalesceTestBaseMs+1000, 2),
	})
	require.NoError(t, acrossTwoRequests[0])
	require.ErrorContains(t, acrossTwoRequests[1], "duplicate sample for timestamp",
		"the same duplicate split across two requests is a 400")
}

// ----------------------------------------------------------------------------
// Ingester.Push: per-request attribution of errors and metrics.
// ----------------------------------------------------------------------------

// A leader runs the append phase on behalf of every request in its batch, so
// everything that phase records has to stay attributed to the request it
// belongs to. The implementation relies on the append phase being a closure
// over each request's own locals, with the metrics emitted afterwards by the
// request's own goroutine. These tests check that it actually works out that
// way, rather than taking the closure scoping on trust.
//
// The requests below write distinct series on purpose: a request whose verdict
// depends on another request in the same batch would be testing the divergence
// covered by TestIngesterPushCoalescing_DivergenceFromSeparateCommits instead
// of testing attribution.

// attributionSpecs builds a batch where several requests fail for different
// reasons, interleaved with requests that succeed. Every failure is decided
// against committed state, so a batch must produce the same per-request result
// as separate commits.
func attributionSpecs() []writeRequestSpec {
	const (
		base = coalesceTestBaseMs
		win  = coalesceTestOOOWindowMs
	)
	return []writeRequestSpec{
		oneSample("attr_ok_0", base+1000, 1),
		// Far enough back that the out-of-order window cannot take it.
		oneSample("attr_too_old", base-4*win, 2),
		oneSample("attr_ok_1", base+1000, 3),
		// attr_dup is seeded with value 1 before the batch, so this is a
		// duplicate against committed state and is rejected at Append time.
		oneSample("attr_dup", base+1000, 99),
		oneSample("attr_ok_2", base+1000, 4),
		oneSample("attr_too_old_2", base-5*win, 5),
		oneSample("attr_ok_3", base+1000, 6),
	}
}

// seedAttributionState gives attr_dup a committed value, so that the batch's
// duplicate is decided against committed state rather than against another
// request in the same batch.
func seedAttributionState(t *testing.T, ing *Ingester) {
	t.Helper()
	for _, err := range pushSequentially(t, ing, []writeRequestSpec{
		oneSample("attr_dup", coalesceTestBaseMs+1000, 1),
	}) {
		require.NoError(t, err)
	}
}

// Each request in a batch must receive its own error, and only its own.
func TestIngesterPushCoalescing_ErrorsStayAttributedToTheirRequest(t *testing.T) {
	specs := attributionSpecs()

	coalesced, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())
	seedAttributionState(t, coalesced)
	got := pushAsOneBatch(t, coalesced, specs)

	require.Len(t, got, 7)

	// The successful requests must not have picked up a neighbour's error.
	for _, idx := range []int{0, 2, 4, 6} {
		require.NoErrorf(t, got[idx], "request %d writes only a good sample", idx)
	}

	// Each failing request must carry the error for its own series and reason.
	require.ErrorContains(t, got[1], "too old sample")
	require.ErrorContains(t, got[1], `series={__name__="attr_too_old"}`)

	require.ErrorContains(t, got[3], "duplicate sample for timestamp")
	require.ErrorContains(t, got[3], `series={__name__="attr_dup"}`)

	require.ErrorContains(t, got[5], "too old sample")
	require.ErrorContains(t, got[5], `series={__name__="attr_too_old_2"}`)

	// And separate commits must agree, so the attribution is not merely
	// self-consistent but identical to the uncoalesced path.
	sequential, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
	seedAttributionState(t, sequential)
	mirrorBatchHarnessPushes(t, sequential)
	want := pushSequentially(t, sequential, specs)
	require.Equal(t, errStrings(want), errStrings(got))
}

// The per-user counters the append phase feeds must total the same whether one
// leader ran seven append phases or seven goroutines ran their own. A shared
// counter in the wrong scope would show up here as double counting or as
// counts that went missing.
func TestIngesterPushCoalescing_PerUserMetricTotalsMatchSeparateCommits(t *testing.T) {
	specs := attributionSpecs()

	// Metrics that must agree. Deliberately not a whole-registry comparison:
	// the commit duration histogram counts commits, not requests, and the
	// coalescer's own metrics exist on one side only.
	names := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_discarded_samples_total",
	}

	sequential, seqReg := startCoalesceTestIngester(t, PushCoalesceConfig{})
	seedAttributionState(t, sequential)
	mirrorBatchHarnessPushes(t, sequential)
	for _, err := range pushSequentially(t, sequential, specs) {
		_ = err // errors are asserted by the test above
	}

	coalesced, coalReg := startCoalesceTestIngester(t, defaultCoalesceConfig())
	seedAttributionState(t, coalesced)
	pushAsOneBatch(t, coalesced, specs)

	want := gatherValues(t, seqReg, names)
	got := gatherValues(t, coalReg, names)

	require.NotEmpty(t, want, "the scenario must move the metrics it claims to compare")
	require.Equal(t, want, got)

	// Spell out what the scenario is expected to have produced, so that a
	// change which happens to keep the two sides equal but wrong still fails.
	require.Equal(t, 7.0, want["cortex_ingester_ingested_samples_total{}"],
		"4 good samples in the batch, plus attr_dup and the two harness pushes")
	require.Equal(t, 3.0, want["cortex_ingester_ingested_samples_failures_total{}"])
	require.Equal(t, 2.0, want[`cortex_discarded_samples_total{reason="sample-too-old",user="`+coalesceTestUser+`"}`])
	require.Equal(t, 1.0, want[`cortex_discarded_samples_total{reason="new-value-for-timestamp",user="`+coalesceTestUser+`"}`])
}

// gatherValues flattens the named metric families into label-keyed values.
func gatherValues(t *testing.T, reg *prometheus.Registry, names []string) map[string]float64 {
	t.Helper()

	wanted := make(map[string]struct{}, len(names))
	for _, n := range names {
		wanted[n] = struct{}{}
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	out := map[string]float64{}
	for _, f := range families {
		if _, ok := wanted[f.GetName()]; !ok {
			continue
		}
		for _, m := range f.GetMetric() {
			key := f.GetName() + "{"
			for i, l := range m.GetLabel() {
				if i > 0 {
					key += ","
				}
				key += l.GetName() + `="` + l.GetValue() + `"`
			}
			key += "}"
			out[key] = metricValue(m)
		}
	}
	return out
}

func metricValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	default:
		return 0
	}
}

// ----------------------------------------------------------------------------
// Ingester.Push: native histograms, exemplars and concurrency.
// ----------------------------------------------------------------------------

// Push appends native histograms and exemplars as well as float samples, and a
// batch leader runs all of it on behalf of other requests. The scenarios above
// carry only float samples, so these two branches are covered here.

// Native histograms appended by a leader on another request's behalf must end
// up where separate commits would have put them.
func TestIngesterPushCoalescing_NativeHistogramsMatchSeparateCommits(t *testing.T) {
	const base = coalesceTestBaseMs

	specs := []writeRequestSpec{
		histogramRequest("hist_a", base+1000, 1),
		histogramRequest("hist_b", base+1000, 2),
		// Same series as the first request, later timestamp: in order, so both
		// paths must store both.
		histogramRequest("hist_a", base+2000, 3),
		// A float sample in the same batch, to exercise both branches together.
		oneSample("hist_float", base+1000, 7),
	}

	sequential, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
	mirrorBatchHarnessPushes(t, sequential)
	wantErrs := pushSequentially(t, sequential, specs)

	coalesced, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())
	gotErrs := pushAsOneBatch(t, coalesced, specs)

	require.Equal(t, errStrings(wantErrs), errStrings(gotErrs))
	require.Equal(t, dumpHead(t, sequential), dumpHead(t, coalesced))

	// The scenario has to have actually stored histograms, or it proves nothing.
	stored := dumpHead(t, coalesced)
	require.Len(t, stored[coalesceTestSeries("hist_a").String()], 2)
	require.Len(t, stored[coalesceTestSeries("hist_b").String()], 1)
}

// Exemplars appended by a leader on another request's behalf must end up where
// separate commits would have put them.
func TestIngesterPushCoalescing_ExemplarsMatchSeparateCommits(t *testing.T) {
	const base = coalesceTestBaseMs

	specs := []writeRequestSpec{
		exemplarRequest("ex_a", base+1000, 1, "trace-a"),
		exemplarRequest("ex_b", base+1000, 2, "trace-b"),
		exemplarRequest("ex_a", base+2000, 3, "trace-c"),
	}

	sequential, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
	mirrorBatchHarnessPushes(t, sequential)
	wantErrs := pushSequentially(t, sequential, specs)

	coalesced, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())
	gotErrs := pushAsOneBatch(t, coalesced, specs)

	require.Equal(t, errStrings(wantErrs), errStrings(gotErrs))

	want := dumpExemplars(t, sequential, "ex_a", "ex_b")
	got := dumpExemplars(t, coalesced, "ex_a", "ex_b")
	require.Equal(t, want, got)
	require.Len(t, got, 3, "the scenario must have stored exemplars")
}

// Concurrent pushes for the same tenant must not lose, duplicate or corrupt
// samples. Run with -race.
func TestIngesterPushCoalescing_ConcurrentPushesAreConsistent(t *testing.T) {
	ing, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())
	ctx := user.InjectOrgID(context.Background(), coalesceTestUser)

	const (
		goroutines      = 32
		samplesPerSerie = 20
	)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			name := fmt.Sprintf("concurrent_%d", g)
			for n := range samplesPerSerie {
				// Ascending timestamps per series, so nothing is out of order
				// and every sample must be stored.
				tMs := coalesceTestBaseMs + int64(n)*1000
				_, err := ing.Push(ctx, oneSample(name, tMs, float64(n)).build())
				require.NoError(t, err)
			}
		})
	}
	wg.Wait()

	head := dumpHead(t, ing)
	for g := range goroutines {
		key := coalesceTestSeries(fmt.Sprintf("concurrent_%d", g)).String()
		got := head[key]
		require.Lenf(t, got, samplesPerSerie, "series %s", key)
		for n, s := range got {
			require.Equal(t, coalesceTestBaseMs+int64(n)*1000, s.T)
			require.Equal(t, float64(n), s.V)
		}
	}
}
