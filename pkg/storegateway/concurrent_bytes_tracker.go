package storegateway

import (
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var ErrMaxConcurrentBytesLimitExceeded = errors.New("max concurrent bytes limit exceeded")

const peakResetInterval = 30 * time.Second

type ConcurrentBytesTracker interface {
	Add(bytes uint64) error
	Release(bytes uint64)
	Current() uint64
	Stop()
}

type concurrentBytesTracker struct {
	mu                 sync.Mutex
	maxConcurrentBytes uint64
	currentBytes       uint64
	peakBytes          uint64
	stop               chan struct{}

	peakBytesGauge        prometheus.Gauge
	maxBytesGauge         prometheus.Gauge
	rejectedRequestsTotal prometheus.Counter
}

func NewConcurrentBytesTracker(maxConcurrentBytes uint64, reg prometheus.Registerer) ConcurrentBytesTracker {
	tracker := &concurrentBytesTracker{
		maxConcurrentBytes: maxConcurrentBytes,
		stop:               make(chan struct{}),
		peakBytesGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_storegateway_concurrent_bytes_peak",
			Help: "Peak concurrent bytes observed in the last 30s window.",
		}),
		maxBytesGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_storegateway_concurrent_bytes_max",
			Help: "Configured maximum concurrent bytes limit.",
		}),
		rejectedRequestsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_storegateway_bytes_limiter_rejected_requests_total",
			Help: "Total requests rejected due to concurrent bytes limit.",
		}),
	}

	tracker.maxBytesGauge.Set(float64(maxConcurrentBytes))
	if reg != nil {
		reg.MustRegister(tracker.peakBytesGauge)
		reg.MustRegister(tracker.maxBytesGauge)
		reg.MustRegister(tracker.rejectedRequestsTotal)
	}

	go tracker.publishPeakLoop()

	return tracker
}

func (t *concurrentBytesTracker) Add(bytes uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	newValue := t.currentBytes + bytes
	if t.maxConcurrentBytes > 0 && newValue > t.maxConcurrentBytes {
		t.rejectedRequestsTotal.Inc()
		return ErrMaxConcurrentBytesLimitExceeded
	}

	t.currentBytes = newValue
	if newValue > t.peakBytes {
		t.peakBytes = newValue
	}

	return nil
}

func (t *concurrentBytesTracker) Release(bytes uint64) {
	t.mu.Lock()
	t.currentBytes -= bytes
	t.mu.Unlock()
}

func (t *concurrentBytesTracker) Current() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.currentBytes
}

func (t *concurrentBytesTracker) publishPeakLoop() {
	ticker := time.NewTicker(peakResetInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.mu.Lock()
			peak := t.peakBytes
			t.peakBytes = t.currentBytes
			t.mu.Unlock()

			t.peakBytesGauge.Set(float64(peak))
		case <-t.stop:
			return
		}
	}
}

func (t *concurrentBytesTracker) Stop() {
	select {
	case <-t.stop:
		// Already stopped.
	default:
		close(t.stop)
	}
}
