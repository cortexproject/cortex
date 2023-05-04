package limiter

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	errCheckIntervalOutOfRange = errors.New(
		"checkInterval must be greater than zero")
	errThresholdOutOfRange = errors.New(
		"yellowThresholdBytes and redThresholdBytes should be greater than zero")
	errInvlaidThreshold = errors.New(
		"redThresholdBytes should be greather than yellowThresholdBytes")
	errMemPressure = errors.New(
		"undergoing memory pressure")
)

type MemoryLimiterConfig struct {
	Enabled              bool          `yaml:"enabled"`
	FailFast             bool          `yaml:"fail_fast"`
	RedThresholdBytes    uint64        `yaml:"red_threshold_bytes"`
	YellowThresholdBytes uint64        `yaml:"yellow_threshold_bytes"`
	CheckInterval        time.Duration `yaml:"check_interval"`
	MaxWaitDuration      time.Duration `yaml:"max_wait_duration"`
}

// RegisterFlags registers flags.
func (cfg *MemoryLimiterConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"memory-limiter.enabled", false, "Enable memory-limiter.")
	f.BoolVar(&cfg.FailFast, prefix+"memory-limiter.fail-fast", false, "Specifies whether to fail when the heap is above the limit. If fail fast is false, the requests will wait for the heap to go down or until the max-wait-duration.")
	f.DurationVar(&cfg.CheckInterval, prefix+"memory-limiter.check-interval", time.Second, "Configures how often to watch the heap to detect memory pressure.")
	f.Uint64Var(&cfg.RedThresholdBytes, prefix+"memory-limiter.red-threshold-bytes", 0, "Specifies the limit above which system is considered to in red state. In red state, all except one request will be throttled.")
	f.Uint64Var(&cfg.YellowThresholdBytes, prefix+"memory-limiter.yellow-threshold-bytes", 0, "Specifies the limit above which the system is considered to be in yellow state. In yellow state, new requests cannot be pulled in.")
	f.DurationVar(&cfg.MaxWaitDuration, prefix+"memory-limiter.max-wait-duration", 5*time.Second, "Configures the maximum wait duration. If a goroutine were blocked for more than the max-wait-duration, it'll continue execution even under memory pressure.")
}

// MemoryLimiter provides a way for the service to detect memory pressure and slow itself down.
type MemoryLimiter interface {
	// NewJob creates a new job and adds to the job queue.
	NewJob() Job

	// Wait checks if the state of the system and blocks () until the desired state is achieved or context is done.
	// If allowFirst is true, the first request in the queue is always allowed to succeed. This can be used to prevent deadlocks.
	Wait(ctx context.Context, id JobID, desiredState State, allowFirst bool, failFast bool) error

	// enqueue adds a job to the queue.
	enqueue(j Job)

	// dequeue removes a job from the queue.
	dequeue(j Job)
}

type JobID int64

// Job is a piece of work that the service is assigned to do.
type Job interface {
	// ID returns the id of the job
	ID() JobID

	// Complete ends the job and removes it from the queue.
	Complete()

	// Continue waits until the state is yellow or green.
	Continue(ctx context.Context) error
}

type job struct {
	failFast bool
	id       JobID
	limiter  MemoryLimiter
}

func (j *job) ID() JobID {
	return j.id
}

func (j *job) Complete() {
	j.limiter.dequeue(j)
}

func (j *job) Continue(ctx context.Context) error {
	return j.limiter.Wait(ctx, j.id, Yellow, true, j.failFast)
}

// AddJobToContext is the helper function to add the job to context.
func AddJobToContext(ctx context.Context, job Job) context.Context {
	return context.WithValue(ctx, mlJobCtxkey, job)
}

// JobFromContext is the helper function to get the job from context.
func JobFromContext(ctx context.Context) Job {
	j, ok := ctx.Value(mlJobCtxkey).(Job)
	if !ok {
		return &job{
			id:      0,
			limiter: &NoOpMemoryLimiter{},
		}
	}
	return j
}

type jobCtxKey struct{}

var (
	mlJobCtxkey = &jobCtxKey{}
)

// NewMemoryLimiter returns a new memorylimiter.
func NewMemoryLimiter(cfg *MemoryLimiterConfig, r prometheus.Registerer, logger log.Logger) (MemoryLimiter, error) {
	if !cfg.Enabled {
		return &NoOpMemoryLimiter{}, nil
	}

	if cfg.CheckInterval <= 0 {
		return nil, errCheckIntervalOutOfRange
	}
	if cfg.RedThresholdBytes == 0 || cfg.YellowThresholdBytes == 0 {
		return nil, errThresholdOutOfRange
	}
	if cfg.YellowThresholdBytes >= cfg.RedThresholdBytes {
		return nil, errInvlaidThreshold
	}

	ml := &HeapMemoryLimiter{
		failFast:         cfg.FailFast,
		memCheckInterval: cfg.CheckInterval,
		redThreshold:     cfg.RedThresholdBytes,
		yellowThreshold:  cfg.YellowThresholdBytes,
		ticker:           time.NewTicker(cfg.CheckInterval),
		readMemStatsFn:   runtime.ReadMemStats,
		yellowCond:       newCond(cfg.MaxWaitDuration, logger),
		greenCond:        newCond(5*time.Minute, logger),
		logger:           logger,
		stateGuage: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "memory_limiter_state",
			Help:      "The state of the memory limiter (Green is 0, Yellow is 1, Red is 2).",
		}),
	}

	ml.start()
	return ml, nil
}

// State is the state of the service
//
// Green - The service operates normally (below yellowThreshold).
// Yellow - The service will not pull in any new work. All the current work will continue (above the yellowThreshold and below redThreshold).
// Red - The service is undergoing severe memory pressure. All except the first job will be paused (above the redThreshold).
type State int

const (
	Green  State = 0
	Yellow State = 1
	Red    State = 2
)

// HeapMemoryLimiter is the implementation of MemoryLimiter that watches Heap allocation to detect memory pressure.
type HeapMemoryLimiter struct {
	failFast bool // failFast specifies whether the jobs should fail immediately instead of waiting for the desired state.

	redThreshold    uint64 // threshold in bytes above which the the system enters red state.
	yellowThreshold uint64 // threshold in bytes above which the system enters yellow state. below this the system will be in green state.

	yellowCond *cond // used for broadcasting when the state has changed to yellow.
	greenCond  *cond // used for broadcasting when the state has changed to green.

	jobsQueue []JobID    // FIFO queue for jobs.
	jobsMutex sync.Mutex // mutex for accessing the queue.

	state      State      // keeps track of the current status.
	stateMutex sync.Mutex // mutex for accessing state

	// memCheckInterval is the interval to read MemStats.
	memCheckInterval time.Duration
	ticker           *time.Ticker

	// readMemStatsFn is the function to read the mem values is set as a reference to help with
	// testing different values.
	readMemStatsFn func(m *runtime.MemStats)

	logger     log.Logger
	stateGuage prometheus.Gauge
}

func (ml *HeapMemoryLimiter) NewJob() Job {
	id := time.Now().UnixNano()
	j := &job{
		failFast: ml.failFast,
		id:       JobID(id),
		limiter:  ml,
	}
	ml.enqueue(j)
	return j
}

// enqueue adds a job to the queue.
func (ml *HeapMemoryLimiter) enqueue(j Job) {
	ml.jobsMutex.Lock()
	defer ml.jobsMutex.Unlock()
	ml.jobsQueue = append(ml.jobsQueue, j.ID())
}

// dequeue removes a job from the queue.
func (ml *HeapMemoryLimiter) dequeue(j Job) {
	ml.jobsMutex.Lock()
	defer ml.jobsMutex.Unlock()
	for i, v := range ml.jobsQueue {
		if v == j.ID() {
			ml.jobsQueue = append(ml.jobsQueue[:i], ml.jobsQueue[i+1:]...)
			break
		}
	}
}

// Wait provides a way for goroutines to slow down if there is memory pressure.
// If memory pressure is detected, wait will block until the memory pressure is mitigated or the context is done.
// If allowFirst is true, the first request in the queue will be always allowed.
func (ml *HeapMemoryLimiter) Wait(ctx context.Context, id JobID, desiredState State, allowFirst bool, failFast bool) error {
	if allowFirst && ml.isFirstJob(id) {
		level.Debug(ml.logger).Log("msg", "Allowing the first job in the queue.")
		// Allow the first job in the queue to continue.
		return nil
	}

	ml.stateMutex.Lock()
	currentState := ml.state
	ml.stateMutex.Unlock()

	if currentState <= desiredState {
		// Allow if the desired state is already achieved
		return nil
	}

	if failFast {
		// If running in failFast mode, fail instead of waiting.
		return errMemPressure
	}

	if desiredState == Yellow {
		// If under memory pressure, wait for a signal before continuing
		level.Warn(ml.logger).Log("msg", "Under memory pressure, waiting")
		ml.yellowCond.Wait(ctx)
	} else {
		// If under memory pressure, wait for a signal before continuing
		level.Warn(ml.logger).Log("msg", "Under memory pressure, waiting")
		ml.greenCond.Wait(ctx)
	}

	return nil
}

func (ml *HeapMemoryLimiter) start() {
	ml.updateState(Green) // start in green state.
	go func() {
		for range ml.ticker.C {
			ml.checkMemLimits()
		}
	}()
}

// checkMemLimits checks the current heap alloc. If previously the system got out of memory pressure, it'll broadcast to goroutines to continue.
func (ml *HeapMemoryLimiter) checkMemLimits() {
	ms := ml.readMemStats()

	ml.stateMutex.Lock()
	prevState := ml.state
	ml.stateMutex.Unlock()

	if ms.Alloc > ml.redThreshold {
		if prevState != Red {
			level.Debug(ml.logger).Log("msg", "Entering Red state.")
			ml.updateState(Red)
		}
	} else if ms.Alloc <= ml.redThreshold && ms.Alloc > ml.yellowThreshold {
		if prevState != Yellow {
			level.Debug(ml.logger).Log("msg", "Entering Yellow state.")
			ml.updateState(Yellow)
			ml.yellowCond.Broadcast()
		}
	} else {
		// Green state
		if prevState != Green {
			level.Debug(ml.logger).Log("msg", "Entering Green state.")
			ml.updateState(Green)
			ml.yellowCond.Broadcast()
			ml.greenCond.Broadcast()
		}
	}
}

func (ml *HeapMemoryLimiter) updateState(state State) {
	ml.stateMutex.Lock()
	defer ml.stateMutex.Unlock()
	level.Debug(ml.logger).Log("msg", fmt.Sprintf("Updating state to %d", state))
	ml.state = state
	ml.stateGuage.Set(float64(state))
}

func (ml *HeapMemoryLimiter) readMemStats() *runtime.MemStats {
	ms := &runtime.MemStats{}
	ml.readMemStatsFn(ms)
	return ms
}

func (ml *HeapMemoryLimiter) isFirstJob(id JobID) bool {
	ml.jobsMutex.Lock()
	defer ml.jobsMutex.Unlock()
	return len(ml.jobsQueue) == 0 || ml.jobsQueue[0] == id
}

// NoOpMemoryLimiter can be used when memory limiter is disabled.
type NoOpMemoryLimiter struct{}

func (ml *NoOpMemoryLimiter) NewJob() Job {
	return &job{
		id:      JobID(0),
		limiter: ml,
	}
}
func (ml *NoOpMemoryLimiter) Wait(ctx context.Context, id JobID, status State, allowFirst bool, failFast bool) error {
	return nil
}
func (ml *NoOpMemoryLimiter) enqueue(j Job) {}
func (ml *NoOpMemoryLimiter) dequeue(j Job) {}

func newCond(maxWait time.Duration, l log.Logger) *cond {
	return &cond{
		ch:      make(chan struct{}),
		logger:  l,
		maxWait: maxWait,
	}
}

// cond is a wrapper around a channel that provides the functionality sync.Cond
type cond struct {
	mu      sync.Mutex // guards ch
	ch      chan struct{}
	maxWait time.Duration
	logger  log.Logger
}

func (c *cond) Wait(ctx context.Context) {
	c.mu.Lock()
	ch := c.ch
	c.mu.Unlock()
	timer := time.NewTimer(c.maxWait)
	defer timer.Stop()

	select {
	case <-ch:
		level.Debug(c.logger).Log("msg", "Received signal, continuing")
		return
	case <-ctx.Done():
		level.Debug(c.logger).Log("msg", "Context cancelled, continuing")
		return
	case <-timer.C:
		level.Warn(c.logger).Log("msg", "Maximum wait duration elapsed, continuing anyways")
		return
	}
}

func (c *cond) Broadcast() {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(c.ch)
	c.ch = make(chan struct{})
}
