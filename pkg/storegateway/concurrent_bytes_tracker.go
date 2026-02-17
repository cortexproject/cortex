package storegateway

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cortexproject/cortex/pkg/util/requestmeta"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
)

const peakResetInterval = 30 * time.Second

type ConcurrentBytesTracker interface {
	TryAccept(ctx context.Context) error
	Add(bytes uint64) func()
	Release(bytes uint64)
	Current() uint64
	Stop()
}

type concurrentBytesTracker struct {
	maxConcurrentBytes uint64
	currentBytes       atomic.Uint64
	peakBytes          atomic.Uint64
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

func (t *concurrentBytesTracker) TryAccept(ctx context.Context) error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Context is not cancelled, continue
		}
	}

	if t.maxConcurrentBytes == 0 {
		return nil
	}

	current := t.currentBytes.Load()
	if current >= t.maxConcurrentBytes {
		t.rejectedRequestsTotal.Inc()
		reqID := requestmeta.RequestIdFromContext(ctx)
		return httpgrpc.Errorf(http.StatusServiceUnavailable,
			"concurrent bytes limit reached: current=%d, max=%d, request_id=%s", current, t.maxConcurrentBytes, reqID)
	}
	return nil
}

func (t *concurrentBytesTracker) Add(bytes uint64) func() {
	newValue := t.currentBytes.Add(bytes)
	for {
		peak := t.peakBytes.Load()
		if newValue <= peak {
			break
		}
		if t.peakBytes.CompareAndSwap(peak, newValue) {
			break
		}
		// CAS failed, retry
	}

	return func() {
		t.Release(bytes)
	}
}

func (t *concurrentBytesTracker) Release(bytes uint64) {
	for {
		current := t.currentBytes.Load()
		newValue := current - bytes
		if t.currentBytes.CompareAndSwap(current, newValue) {
			return
		}
		// CAS failed, retry
	}
}

func (t *concurrentBytesTracker) Current() uint64 {
	return t.currentBytes.Load()
}

func (t *concurrentBytesTracker) publishPeakLoop() {
	ticker := time.NewTicker(peakResetInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			current := t.currentBytes.Load()
			peak := t.peakBytes.Swap(current)
			if current > peak {
				peak = current
			}
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
