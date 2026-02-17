package storegateway

import (
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/thanos-io/thanos/pkg/store"
)

type trackingBytesLimiter struct {
	inner    store.BytesLimiter
	tracker  ConcurrentBytesTracker
	tracked  atomic.Uint64
	released atomic.Bool
}

func newTrackingBytesLimiter(inner store.BytesLimiter, tracker ConcurrentBytesTracker) *trackingBytesLimiter {
	return &trackingBytesLimiter{
		inner:   inner,
		tracker: tracker,
	}
}

func (t *trackingBytesLimiter) ReserveWithType(num uint64, dataType store.StoreDataType) error {
	if err := t.inner.ReserveWithType(num, dataType); err != nil {
		return err
	}

	_ = t.tracker.Add(num)
	_ = t.tracked.Add(num)

	return nil
}

func (t *trackingBytesLimiter) Release() {
	if !t.released.CompareAndSwap(false, true) {
		return
	}

	bytes := t.tracked.Load()
	if bytes > 0 {
		t.tracker.Release(bytes)
		t.tracked.Store(0)
	}
}

func (t *trackingBytesLimiter) TrackedBytes() uint64 {
	return t.tracked.Load()
}

type trackingBytesLimiterRegistry struct {
	mu       sync.Mutex
	limiters []*trackingBytesLimiter
}

func newTrackingBytesLimiterRegistry() *trackingBytesLimiterRegistry {
	return &trackingBytesLimiterRegistry{}
}

func (r *trackingBytesLimiterRegistry) Register(limiter *trackingBytesLimiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.limiters = append(r.limiters, limiter)
}

func (r *trackingBytesLimiterRegistry) ReleaseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, limiter := range r.limiters {
		limiter.Release()
	}
	r.limiters = nil
}

type trackingLimiterRegistryHolder struct {
	registries sync.Map
}

func (h *trackingLimiterRegistryHolder) SetRegistry(registry *trackingBytesLimiterRegistry) {
	h.registries.Store(getGoroutineID(), registry)
}

func (h *trackingLimiterRegistryHolder) GetRegistry() *trackingBytesLimiterRegistry {
	val, ok := h.registries.Load(getGoroutineID())
	if !ok {
		return nil
	}
	return val.(*trackingBytesLimiterRegistry)
}

func (h *trackingLimiterRegistryHolder) ClearRegistry() {
	h.registries.Delete(getGoroutineID())
}

func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Stack output starts with "goroutine <id> ["
	s := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	if idx := strings.IndexByte(s, ' '); idx >= 0 {
		s = s[:idx]
	}
	id, _ := strconv.ParseInt(s, 10, 64)
	return id
}
