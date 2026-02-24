package storegateway

import (
	"runtime"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/store"
)

type requestBytesTracker struct {
	tracker  ConcurrentBytesTracker
	total    atomic.Uint64
	released atomic.Bool
}

func newRequestBytesTracker(tracker ConcurrentBytesTracker) *requestBytesTracker {
	return &requestBytesTracker{
		tracker: tracker,
	}
}

func (r *requestBytesTracker) Add(bytes uint64) error {
	if err := r.tracker.Add(bytes); err != nil {
		return err
	}
	r.total.Add(bytes)
	return nil
}

func (r *requestBytesTracker) ReleaseAll() {
	if !r.released.CompareAndSwap(false, true) {
		return
	}
	bytes := r.total.Load()
	if bytes > 0 {
		r.tracker.Release(bytes)
	}
}

func (r *requestBytesTracker) Total() uint64 {
	return r.total.Load()
}

type trackingBytesLimiter struct {
	inner          store.BytesLimiter
	requestTracker *requestBytesTracker
}

func newTrackingBytesLimiter(inner store.BytesLimiter, requestTracker *requestBytesTracker) *trackingBytesLimiter {
	return &trackingBytesLimiter{
		inner:          inner,
		requestTracker: requestTracker,
	}
}

func (t *trackingBytesLimiter) ReserveWithType(num uint64, dataType store.StoreDataType) error {
	if err := t.inner.ReserveWithType(num, dataType); err != nil {
		return err
	}
	return t.requestTracker.Add(num)
}

type requestBytesTrackerHolder struct {
	trackers sync.Map
}

func (h *requestBytesTrackerHolder) Set(tracker *requestBytesTracker) {
	h.trackers.Store(getGoroutineID(), tracker)
}

func (h *requestBytesTrackerHolder) Get() *requestBytesTracker {
	val, ok := h.trackers.Load(getGoroutineID())
	if !ok {
		return nil
	}
	return val.(*requestBytesTracker)
}

func (h *requestBytesTrackerHolder) Clear() {
	h.trackers.Delete(getGoroutineID())
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
