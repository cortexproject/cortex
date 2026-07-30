package storegateway

import (
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var ErrMaxConcurrentDataBytesLimitExceeded = errors.New("max concurrent data bytes limit exceeded")

const peakResetInterval = 30 * time.Second

type ConcurrentDataBytesTracker interface {
	Add(bytes uint64) error
	Release(bytes uint64)
	Current() uint64
	Stop()
}

type concurrentDataBytesTracker struct {
	mu                     sync.Mutex
	maxConcurrentDataBytes uint64
	currentBytes           uint64
	peakBytes              uint64
	stop                   chan struct{}

	peakBytesGauge        prometheus.Gauge
	maxBytesGauge         prometheus.Gauge
	rejectedRequestsTotal prometheus.Counter
}

func NewConcurrentDataBytesTracker(maxConcurrentDataBytes uint64, reg prometheus.Registerer) ConcurrentDataBytesTracker {
	tracker := &concurrentDataBytesTracker{
		maxConcurrentDataBytes: maxConcurrentDataBytes,
		stop:                   make(chan struct{}),
		peakBytesGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_storegateway_concurrent_data_bytes_peak",
			Help: "Peak concurrent data bytes observed in the last 30s window.",
		}),
		maxBytesGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_storegateway_concurrent_data_bytes_max",
			Help: "Configured maximum concurrent data bytes limit.",
		}),
		rejectedRequestsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_storegateway_concurrent_data_bytes_rejected_requests_total",
			Help: "Total requests rejected due to concurrent data bytes limit.",
		}),
	}

	tracker.maxBytesGauge.Set(float64(maxConcurrentDataBytes))
	if reg != nil {
		reg.MustRegister(tracker.peakBytesGauge)
		reg.MustRegister(tracker.maxBytesGauge)
		reg.MustRegister(tracker.rejectedRequestsTotal)
	}

	go tracker.publishPeakLoop()

	return tracker
}

func (t *concurrentDataBytesTracker) Add(bytes uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	newValue := t.currentBytes + bytes
	if t.maxConcurrentDataBytes > 0 && newValue > t.maxConcurrentDataBytes {
		t.rejectedRequestsTotal.Inc()
		return ErrMaxConcurrentDataBytesLimitExceeded
	}

	t.currentBytes = newValue
	if newValue > t.peakBytes {
		t.peakBytes = newValue
	}

	return nil
}

func (t *concurrentDataBytesTracker) Release(bytes uint64) {
	t.mu.Lock()
	t.currentBytes -= bytes
	t.mu.Unlock()
}

func (t *concurrentDataBytesTracker) Current() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.currentBytes
}

func (t *concurrentDataBytesTracker) publishPeakLoop() {
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

func (t *concurrentDataBytesTracker) Stop() {
	select {
	case <-t.stop:
		// Already stopped.
	default:
		close(t.stop)
	}
}
