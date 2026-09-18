package storegateway

import (
	"runtime"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/store"
)

type requestDataBytesTracker struct {
	tracker  ConcurrentDataBytesTracker
	total    atomic.Uint64
	released atomic.Bool
}

func newRequestDataBytesTracker(tracker ConcurrentDataBytesTracker) *requestDataBytesTracker {
	return &requestDataBytesTracker{
		tracker: tracker,
	}
}

func (r *requestDataBytesTracker) Add(bytes uint64) error {
	if err := r.tracker.Add(bytes); err != nil {
		return err
	}
	r.total.Add(bytes)
	return nil
}

func (r *requestDataBytesTracker) ReleaseAll() {
	if !r.released.CompareAndSwap(false, true) {
		return
	}
	bytes := r.total.Load()
	if bytes > 0 {
		r.tracker.Release(bytes)
	}
}

func (r *requestDataBytesTracker) Total() uint64 {
	return r.total.Load()
}

type trackingDataBytesLimiter struct {
	inner          store.BytesLimiter
	requestTracker *requestDataBytesTracker
}

func newTrackingDataBytesLimiter(inner store.BytesLimiter, requestTracker *requestDataBytesTracker) *trackingDataBytesLimiter {
	return &trackingDataBytesLimiter{
		inner:          inner,
		requestTracker: requestTracker,
	}
}

func (t *trackingDataBytesLimiter) ReserveWithType(num uint64, dataType store.StoreDataType) error {
	if err := t.inner.ReserveWithType(num, dataType); err != nil {
		return err
	}
	return t.requestTracker.Add(num)
}

type requestDataBytesTrackerHolder struct {
	trackers sync.Map
}

func (h *requestDataBytesTrackerHolder) Set(tracker *requestDataBytesTracker) {
	h.trackers.Store(getGoroutineID(), tracker)
}

func (h *requestDataBytesTrackerHolder) Get() *requestDataBytesTracker {
	val, ok := h.trackers.Load(getGoroutineID())
	if !ok {
		return nil
	}
	return val.(*requestDataBytesTracker)
}

func (h *requestDataBytesTrackerHolder) Clear() {
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
