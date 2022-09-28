package limiter

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/go-kit/log/level"
)

type memLimiterCtxKey struct{}
type requestIDCtxKey struct{}

type MemLimiter interface {
	RemoveRequest(requestID string)
	AddRequest(requestID string)
	LoadBytes(dataBytes int, requestID string) error
}

var (
	mlCtxKey             = &memLimiterCtxKey{}
	mlRidCtxKey          = &requestIDCtxKey{}
	ErrMaxMemoryLimitHit = "continuing to execute the query will potentially cause OOM"
)

func AddMemLimiterToContext(ctx context.Context, limiter MemLimiter) context.Context {
	return context.WithValue(ctx, mlCtxKey, limiter)
}

// MemLimiterFromContextWithFallback returns a MemLimiter from the current context.
// If there is not a MemLimiter on the context it will return a new no-op limiter.
func MemLimiterFromContextWithFallback(ctx context.Context) MemLimiter {
	ml, ok := ctx.Value(mlCtxKey).(MemLimiter)
	if !ok {
		ml = NewNoOpMemLimiter()
	}
	return ml
}

func RequestIDFromContextWithFallback(ctx context.Context) string {
	requestID, ok := ctx.Value(mlRidCtxKey).(string)
	if !ok {
		// If there's no limiter return empty requestID
		return ""
	}
	return requestID
}

func AddRequestIDToContext(ctx context.Context, requestID string) context.Context {
	level.Warn(util_log.Logger).Log("msg", "Adding requestID to context", "request", requestID)
	return context.WithValue(ctx, mlRidCtxKey, requestID)
}

// No-Op limiter
type NoOpMemLimiter struct{}

func (gl *NoOpMemLimiter) AddRequest(requestID string)                     {}
func (gl *NoOpMemLimiter) RemoveRequest(requestID string)                  {}
func (gl *NoOpMemLimiter) LoadBytes(dataBytes int, requestID string) error { return nil }

func NewNoOpMemLimiter() MemLimiter {
	return &NoOpMemLimiter{}
}

// Heap based memory limiter.
type HeapMemLimiter struct {
	mutex            sync.RWMutex // mutex for accessing the requests queue
	queue            []string     // All the active requests
	heapLimitInBytes uint64
	memStats         *runtime.MemStats // Only used for mocking
}

// NewHeapMemLimiter makes a new heap based memory limiter.
func NewHeapMemLimiter(heapLimitInBytes uint64) MemLimiter {
	return &HeapMemLimiter{
		heapLimitInBytes: heapLimitInBytes,
		memStats:         nil,
	}
}

func (l *HeapMemLimiter) AddRequest(requestID string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.queue = append(l.queue, requestID)
	level.Debug(util_log.Logger).Log("msg", "Added requestID to queue", "request", requestID, "active requests", fmt.Sprintf("%v", l.queue))
}

func (l *HeapMemLimiter) RemoveRequest(requestID string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for i, v := range l.queue {
		if v == requestID {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			break
		}
	}
	level.Warn(util_log.Logger).Log("msg", "Removed requestID from queue", "request", requestID, "active requests", fmt.Sprintf("%v", l.queue))
}

// AddDataBytes adds the input data size in bytes and returns an error if the limit is reached.
func (l *HeapMemLimiter) LoadBytes(dataBytes int, requestID string) error {
	level.Debug(util_log.Logger).Log("msg", "Adding bytes for Request", "request", requestID, "bytes", fmt.Sprintf("(adding: %d)", dataBytes))
	if l.heapLimitInBytes == 0 {
		return nil
	}

	heapAlloc := l.getHeapAlloc()

	if heapAlloc >= uint64(l.heapLimitInBytes) {
		level.Warn(util_log.Logger).Log("msg", "Current heap alloc is higher than limit", "heapalloc", heapAlloc, "limit", l.heapLimitInBytes)
		if l.isFirstRequest(requestID) {
			// Always allow the first request to succeed to avoid any deadlock.
			level.Warn(util_log.Logger).Log("msg", "Loading bytes above limit for the first request in queue", "request", requestID)
			return nil
		}
		level.Warn(util_log.Logger).Log("msg", "Failing request because it could potentially cause OOM", "request", requestID, "heap", heapAlloc, "limit", l.heapLimitInBytes)
		return fmt.Errorf(ErrMaxMemoryLimitHit)
	}
	return nil
}

func (l *HeapMemLimiter) getHeapAlloc() uint64 {
	if l.memStats != nil {
		return l.memStats.HeapAlloc
	}

	var rm runtime.MemStats
	runtime.ReadMemStats(&rm)
	return rm.HeapAlloc
}

func (l *HeapMemLimiter) isFirstRequest(requestID string) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return len(l.queue) == 0 || l.queue[0] == requestID
}
