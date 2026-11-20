package ingester

import (
	"context"
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/storage"
	"go.uber.org/atomic"
)

// Push coalescing folds the TSDB commits of several concurrent write requests
// for the same tenant into a single commit.
//
// Every Push takes its own appender and calls Commit on it. Commit writes the
// samples to the WAL through wlog.(*WL).Log, which holds the per-tenant WAL
// mutex for the whole record loop. When the distributor shards one large
// tenant's writes by series across many ingesters, each ingester receives a
// very high rate of requests carrying only a handful of samples each, and
// takes that one mutex once per request. Once any waiter has waited longer
// than a millisecond, Go's sync.Mutex switches to starvation mode and hands
// the lock over in strict FIFO order; if a woken waiter is then slow to get
// back on a CPU, each handoff costs far more than the work done under the
// lock, throughput falls below the arrival rate and the queue of pushes grows
// without bound.
//
// Coalescing attacks the request-per-commit ratio rather than the lock itself.
// A request that arrives while no commit is in flight commits on its own and
// pays no extra latency. A request that arrives while a commit is in flight
// joins the next batch; when the in-flight commit finishes, the first queued
// request becomes the leader of that batch, appends every request in it into
// one appender, calls Commit once, and hands each request its own result. There
// is no timer and no "wait for X milliseconds" anywhere: batching only ever
// uses time a request was going to spend waiting regardless.

const (
	// Defaults chosen so that a batch stays small enough that its WAL record
	// still fits comfortably in the WAL page buffer, while cutting the commit
	// rate by an order of magnitude under heavy concurrent load.
	defaultPushCoalesceMaxBatchRequests = 32
	defaultPushCoalesceMaxBatchSamples  = 2000
)

// PushCoalesceConfig configures opportunistic merging of per-request TSDB
// commits. It is disabled by default.
type PushCoalesceConfig struct {
	Enabled bool `yaml:"enabled"`

	// MaxBatchRequests bounds how many write requests one commit may carry.
	MaxBatchRequests int `yaml:"max_batch_requests"`

	// MaxBatchSamples bounds how many samples (floats plus histograms) one
	// commit may carry. 0 disables the bound.
	MaxBatchSamples int `yaml:"max_batch_samples"`

	// MaxBatchBytes bounds the encoded size of the requests one commit may
	// carry. 0 disables the bound.
	MaxBatchBytes int `yaml:"max_batch_bytes"`
}

// RegisterFlagsWithPrefix registers the push coalescing flags.
func (cfg *PushCoalesceConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.Enabled, prefix+"push-coalesce.enabled", false,
		"Experimental: merge the TSDB commits of concurrent write requests for the same tenant into a single commit. A request only ever waits for a commit that was already in flight, so this adds no latency of its own.")
	f.IntVar(&cfg.MaxBatchRequests, prefix+"push-coalesce.max-batch-requests", defaultPushCoalesceMaxBatchRequests,
		"Maximum number of write requests a single coalesced commit may carry.")
	f.IntVar(&cfg.MaxBatchSamples, prefix+"push-coalesce.max-batch-samples", defaultPushCoalesceMaxBatchSamples,
		"Maximum number of samples a single coalesced commit may carry. 0 to disable the limit.")
	f.IntVar(&cfg.MaxBatchBytes, prefix+"push-coalesce.max-batch-bytes", 0,
		"Maximum encoded size, in bytes, of the write requests a single coalesced commit may carry. 0 to disable the limit.")
}

// Validate returns an error if the configuration cannot be used.
func (cfg *PushCoalesceConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxBatchRequests < 1 {
		return errors.New("ingester.push-coalesce.max-batch-requests must be at least 1")
	}
	if cfg.MaxBatchSamples < 0 {
		return errors.New("ingester.push-coalesce.max-batch-samples must not be negative")
	}
	if cfg.MaxBatchBytes < 0 {
		return errors.New("ingester.push-coalesce.max-batch-bytes must not be negative")
	}
	return nil
}

// Reasons a batch stopped taking further queued requests. Reported on
// cortex_ingester_push_coalesce_batch_truncated_total.
const (
	truncateReasonMaxRequests = "max_requests"
	truncateReasonMaxSamples  = "max_samples"
	truncateReasonMaxBytes    = "max_bytes"
	truncateReasonDiscardOOO  = "discard_out_of_order"
)

// How a request's samples reached the TSDB. Reported on
// cortex_ingester_push_coalesce_requests_total.
const (
	coalesceRoleLeaderImmediate = "leader_immediate"
	coalesceRoleLeaderElected   = "leader_elected"
	coalesceRoleFollower        = "follower"
	coalesceRoleCanceled        = "canceled"
)

// errPushCoalesceBatchRolledBack is returned to the requests of a batch that
// was rolled back because a different request in it failed in a way that
// requires the appender to be discarded. It is deliberately retryable: the
// samples of these requests were never committed.
var errPushCoalesceBatchRolledBack = errors.New("write request was not committed: another request in the same coalesced commit failed, please retry")

// errPushCoalesceLeaderAborted is the result a batch member is left with if the
// leader stops running the batch without setting a result, which can only
// happen if appending panics. It is retryable for the same reason.
var errPushCoalesceLeaderAborted = errors.New("write request was not committed: the coalesced commit it joined did not complete, please retry")

type pushCoalescerMetrics struct {
	requests       *prometheus.CounterVec
	batchRequests  prometheus.Histogram
	batchSamples   prometheus.Histogram
	waitSeconds    prometheus.Histogram
	commitSeconds  prometheus.Histogram
	commits        prometheus.Counter
	commitFailures prometheus.Counter
	rollbacks      prometheus.Counter
	truncated      *prometheus.CounterVec
}

func newPushCoalescerMetrics(r prometheus.Registerer) *pushCoalescerMetrics {
	return &pushCoalescerMetrics{
		requests: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_push_coalesce_requests_total",
			Help: "Number of write requests that went through the push coalescer, by the role the request played in its batch.",
		}, []string{"role"}),
		batchRequests: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_push_coalesce_batch_requests",
			Help:    "Number of write requests carried by a single coalesced commit.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
		}),
		batchSamples: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_push_coalesce_batch_samples",
			Help:    "Number of samples carried by a single coalesced commit.",
			Buckets: prometheus.ExponentialBuckets(4, 2, 12),
		}),
		waitSeconds: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_push_coalesce_wait_seconds",
			Help:    "Time a write request spent queued waiting for the in-flight commit to finish before its own batch started.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
		}),
		commitSeconds: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_push_coalesce_commit_seconds",
			Help:    "Duration of a coalesced TSDB commit.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
		}),
		commits: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_push_coalesce_commits_total",
			Help: "Number of TSDB commits issued by the push coalescer.",
		}),
		commitFailures: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_push_coalesce_commit_failures_total",
			Help: "Number of coalesced TSDB commits that failed. Every request in such a batch is returned the commit error.",
		}),
		rollbacks: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_push_coalesce_batch_rollbacks_total",
			Help: "Number of coalesced batches rolled back because one of their requests failed in a way that requires discarding the appender.",
		}),
		truncated: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_push_coalesce_batch_truncated_total",
			Help: "Number of times a batch stopped taking further queued requests, by reason. The remaining requests form the next batch.",
		}, []string{"reason"}),
	}
}

// pushTask is one logical write request waiting to be folded into a batch.
//
// Everything a leader needs in order to act on behalf of the request lives
// here, because the appender is not safe for concurrent use and so only the
// leader may touch it.
type pushTask struct {
	ctx context.Context

	samples int
	bytes   int

	// discardOutOfOrder mirrors the request's flag. It is set on the appender,
	// not per sample, so requests that disagree on it cannot share a batch.
	discardOutOfOrder bool

	// appendFn writes the request's samples into the shared appender. It
	// returns a non-nil error only for failures that require the appender to be
	// discarded; per-sample soft errors are recorded on the request itself and
	// reported by its own goroutine after the commit.
	appendFn func(app extendedAppender, headMaxTime int64) error

	// result is the batch outcome, read by the waiting goroutine once wake is
	// closed. Written only by the leader of the batch this task joined.
	result error

	// dropped records that the leader skipped this task because its caller had
	// already given up, so later passes over the batch must leave its result
	// alone.
	dropped bool

	// next links queued tasks. Only the task's own goroutine writes it, before
	// publishing the task, and only the leader that claimed the queue reads it
	// afterwards.
	next *pushTask

	// elected is set when this task has been made leader of followers, rather
	// than served by somebody else's batch.
	elected   bool
	followers []*pushTask

	// wake is closed once result is final. nil for a request that never queued.
	wake     chan struct{}
	signaled bool

	enqueuedAt time.Time
}

func (t *pushTask) signal() {
	if t.signaled || t.wake == nil {
		return
	}
	t.signaled = true
	close(t.wake)
}

// pushCoalescer is the per-tenant merge point. It is attached to the tenant's
// userTSDB, so a batch never spans tenants and the WAL mutex a batch reduces
// contention on is always the one belonging to that tenant.
type pushCoalescer struct {
	cfg     PushCoalesceConfig
	metrics *pushCoalescerMetrics
	logger  log.Logger

	// newAppender and headMaxTime are injected so the coalescer can be tested
	// without a full Ingester.
	newAppender func() extendedAppender
	headMaxTime func() int64

	// head encodes both whether a batch is in flight and the queue of requests
	// waiting for the next one, in one word:
	//   nil               idle: the next request commits immediately.
	//   coalesceBusy      a batch is in flight and nothing is queued.
	//   any other task    a batch is in flight and this is the most recently
	//                     queued task; next links back to coalesceBusy.
	// A request therefore joins with a single compare-and-swap and never waits
	// on a lock to do so. Go's sync.Mutex switches to strict FIFO handoff once
	// any waiter has waited more than 1ms, and when the scheduler is saturated
	// a woken waiter routinely waits longer than that just to get a P, however
	// short the critical section; a queue nobody blocks on to join cannot get
	// into that state. head is also non-nil across a handoff, so that requests
	// arriving between the election of the next leader and that leader actually
	// starting queue for the batch after it rather than racing it with a second
	// appender.
	head atomic.Pointer[pushTask]
	// carry holds tasks the previous leader claimed from head but could not
	// fit into its batch, in arrival order. It is owned by whichever goroutine
	// is currently leading; ownership passes with the wake channel close.
	carry []*pushTask
	// queued counts tasks that joined the queue and are not in a batch yet.
	queued atomic.Int64
}

// coalesceBusy is the sentinel stored in pushCoalescer.head while a batch is in
// flight. It is never run or signaled.
var coalesceBusy = &pushTask{}

func newPushCoalescer(cfg PushCoalesceConfig, metrics *pushCoalescerMetrics, logger log.Logger, newAppender func() extendedAppender, headMaxTime func() int64) *pushCoalescer {
	return &pushCoalescer{
		cfg:         cfg,
		metrics:     metrics,
		logger:      logger,
		newAppender: newAppender,
		headMaxTime: headMaxTime,
	}
}

// push runs the task's append and commit, either on its own or folded into a
// batch, and returns the result for that task alone.
//
// The caller must not release the write request until push returns: a follower
// blocks until the leader has finished appending its samples precisely so that
// Push's deferred ReuseSlice cannot run while the leader still reads them.
func (c *pushCoalescer) push(t *pushTask) error {
	// A caller that has already given up is told so rather than having its
	// samples committed on its behalf.
	if err := t.ctx.Err(); err != nil {
		c.metrics.requests.WithLabelValues(coalesceRoleCanceled).Inc()
		return err
	}

	for {
		old := c.head.Load()
		if old == nil {
			if !c.head.CompareAndSwap(nil, coalesceBusy) {
				continue
			}
			// Nothing is committing, so commit immediately and alone. This
			// is the path that keeps coalescing free of added latency.
			c.metrics.requests.WithLabelValues(coalesceRoleLeaderImmediate).Inc()
			c.lead(t, nil)
			return t.result
		}

		// A batch is in flight, so join the one that runs after it. Prepare,
		// once, before publishing the task: after the compare-and-swap below
		// the leader may read it.
		if t.wake == nil {
			t.wake = make(chan struct{})
			t.enqueuedAt = time.Now()
			t.result = errPushCoalesceLeaderAborted
		}
		t.next = old
		// Count the task before publishing it: once the compare-and-swap
		// succeeds a leader may claim it and decrement the count, which would
		// otherwise go briefly negative.
		c.queued.Add(1)
		if c.head.CompareAndSwap(old, t) {
			return c.await(t)
		}
		c.queued.Add(-1)
	}
}

// await blocks a queued task until it has been served by a batch or elected to
// lead one, and runs that batch if so.
func (c *pushCoalescer) await(t *pushTask) error {
	<-t.wake

	if t.elected {
		c.metrics.requests.WithLabelValues(coalesceRoleLeaderElected).Inc()
		c.lead(t, t.followers)
	} else {
		c.metrics.requests.WithLabelValues(coalesceRoleFollower).Inc()
	}
	return t.result
}

// isIdle reports that no batch is in flight and nothing is queued.
//
// It reads carry, which only the current leader owns, so it is only meaningful
// when no push is in progress. Tests use it once every pusher has returned.
func (c *pushCoalescer) isIdle() bool {
	return c.head.Load() == nil && len(c.carry) == 0 && c.queued.Load() == 0
}

// pendingLen reports how many requests are queued and not yet in a batch.
func (c *pushCoalescer) pendingLen() int {
	return int(c.queued.Load())
}

// lead runs one batch and then hands leadership to the next queued request.
func (c *pushCoalescer) lead(leader *pushTask, followers []*pushTask) {
	// Release every follower exactly once, whatever happens in runBatch, so
	// that no request is left blocked. Followers start out holding a retryable
	// error, so a panic cannot report an uncommitted request as succeeded.
	defer func() {
		for _, t := range followers {
			t.signal()
		}
		c.handoff()
	}()

	c.runBatch(leader, followers)
}

// runBatch appends every request in the batch into one appender, commits once,
// and records each request's own result.
func (c *pushCoalescer) runBatch(leader *pushTask, followers []*pushTask) {
	batch := make([]*pushTask, 0, len(followers)+1)
	batch = append(batch, leader)
	batch = append(batch, followers...)

	now := time.Now()
	for _, t := range batch {
		if !t.enqueuedAt.IsZero() {
			c.metrics.waitSeconds.Observe(now.Sub(t.enqueuedAt).Seconds())
		}
	}

	// The appender carries samples for every request in the batch, so it is not
	// tied to any one request's context. Prometheus' head appender ignores the
	// context it is given, so nothing is lost by saying so explicitly.
	app := c.newAppender()
	headMaxTime := c.headMaxTime()
	if leader.discardOutOfOrder {
		app.SetOptions(&storage.AppendOptions{DiscardOutOfOrder: true})
	}

	var (
		appended  = make([]*pushTask, 0, len(batch))
		samples   int
		fatal     error
		fatalTask *pushTask
	)

	for _, t := range batch {
		// Re-check: a request may have been abandoned while it sat in the
		// queue. Dropping it here is safe because it is still blocked on its
		// wake channel and so has not released its write request. It is also a
		// deliberate difference from the uncoalesced path, which commits the
		// samples of a request whose caller has already gone away; a client
		// that timed out retries either way, and shedding the work is what we
		// want when the ingester is behind.
		if err := t.ctx.Err(); err != nil {
			t.result = err
			t.dropped = true
			continue
		}
		if err := t.appendFn(app, headMaxTime); err != nil {
			fatal, fatalTask = err, t
			break
		}
		appended = append(appended, t)
		samples += t.samples
	}

	if fatal != nil {
		// One request failed in a way that requires discarding the appender,
		// and an appender cannot discard one request's samples alone. Roll the
		// whole batch back and let everyone retry. This is the unexpected-error
		// path; soft per-sample errors never reach it.
		if err := app.Rollback(); err != nil {
			level.Warn(c.logger).Log("msg", "failed to roll back coalesced push batch", "err", err)
		}
		c.metrics.rollbacks.Inc()
		for _, t := range batch {
			if t.dropped {
				continue
			}
			if t == fatalTask {
				t.result = fatal
				continue
			}
			t.result = errPushCoalesceBatchRolledBack
		}
		return
	}

	start := time.Now()
	err := app.Commit()
	c.metrics.commits.Inc()
	if err != nil {
		c.metrics.commitFailures.Inc()
		for _, t := range appended {
			t.result = err
		}
		return
	}

	c.metrics.commitSeconds.Observe(time.Since(start).Seconds())
	c.metrics.batchRequests.Observe(float64(len(appended)))
	c.metrics.batchSamples.Observe(float64(samples))
	for _, t := range appended {
		t.result = nil
	}
}

// handoff elects the leader of the next batch, or marks the coalescer idle.
//
// Only the goroutine that currently leads calls it, so it alone touches carry.
func (c *pushCoalescer) handoff() {
	for {
		old := c.head.Load()
		if old == coalesceBusy {
			if len(c.carry) > 0 {
				// Nothing new arrived, but the previous claim left work over.
				break
			}
			if c.head.CompareAndSwap(coalesceBusy, nil) {
				return
			}
			continue
		}

		// Claim everything queued so far in one swap. head goes back to
		// coalesceBusy, not nil, so that arrivals keep queueing while the next
		// batch is elected and run.
		if !c.head.CompareAndSwap(old, coalesceBusy) {
			continue
		}
		// The claimed list is newest first. Append it to carry oldest first so
		// that batches keep arrival order.
		n := 0
		for p := old; p != coalesceBusy; p = p.next {
			n++
		}
		base := len(c.carry)
		c.carry = append(c.carry, make([]*pushTask, n)...)
		i := base + n - 1
		for p := old; p != coalesceBusy; {
			next := p.next
			p.next = nil
			c.carry[i] = p
			i--
			p = next
		}
		break
	}

	next, followers, rest := c.sliceBatch(c.carry)
	c.carry = rest
	c.queued.Add(-int64(1 + len(followers)))
	next.elected = true
	next.followers = followers

	// Closing wake hands carry, and leadership, to next.
	next.signal()
}

// sliceBatch takes the longest prefix of the queue that may share one commit.
//
// It stops at the first request that does not fit rather than skipping it, so
// requests keep their arrival order and a request that can never join somebody
// else's batch still reaches the head of the queue and leads one of its own.
// Called only by the current leader.
func (c *pushCoalescer) sliceBatch(pending []*pushTask) (leader *pushTask, followers, rest []*pushTask) {
	leader = pending[0]

	samples := leader.samples
	bytes := leader.bytes

	n := 1
	for ; n < len(pending); n++ {
		t := pending[n]

		reason := ""
		switch {
		case n >= c.cfg.MaxBatchRequests:
			reason = truncateReasonMaxRequests
		case c.cfg.MaxBatchSamples > 0 && samples+t.samples > c.cfg.MaxBatchSamples:
			reason = truncateReasonMaxSamples
		case c.cfg.MaxBatchBytes > 0 && bytes+t.bytes > c.cfg.MaxBatchBytes:
			reason = truncateReasonMaxBytes
		case t.discardOutOfOrder != leader.discardOutOfOrder:
			reason = truncateReasonDiscardOOO
		}
		if reason != "" {
			c.metrics.truncated.WithLabelValues(reason).Inc()
			break
		}

		followers = append(followers, t)
		samples += t.samples
		bytes += t.bytes
	}

	// Copy the remainder so that the batch does not keep the queue's backing
	// array, and the tasks it held, alive for as long as the queue lives.
	if n < len(pending) {
		rest = make([]*pushTask, len(pending)-n)
		copy(rest, pending[n:])
	}
	return leader, followers, rest
}
