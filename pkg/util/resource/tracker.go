package resource

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/util/services"
)

type memoryBuckets struct {
	buckets    [3]uint64 // 3 buckets for 3 seconds
	lastUpdate time.Time
	currentIdx int
}

const maxActiveRequests = 100000

type ResourceTracker struct {
	services.Service

	memoryData map[string]*memoryBuckets
	lastUpdate map[string]time.Time // Track last update per requestID

	mu sync.RWMutex
}

type IResourceTracker interface {
	AddBytes(requestID string, bytes uint64)
	GetHeaviestQuery() (requestID string, bytes uint64)
}

func NewResourceTracker(registerer prometheus.Registerer) *ResourceTracker {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_resource_tracker_active_requests",
	}, rt.activeRequestCount)

	rt.Service = services.NewBasicService(nil, rt.running, nil)
	return rt
}

func (rt *ResourceTracker) AddBytes(requestID string, bytes uint64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now().Truncate(time.Second)

	buckets, exists := rt.memoryData[requestID]
	if !exists {
		// Check if we're at capacity
		if len(rt.memoryData) >= maxActiveRequests {
			// Evict oldest request
			rt.evictOldest()
		}

		buckets = &memoryBuckets{
			lastUpdate: now,
			currentIdx: 0,
		}
		rt.memoryData[requestID] = buckets
	}

	// Calculate seconds drift and rotate buckets if needed
	secondsDrift := int(now.Sub(buckets.lastUpdate).Seconds())
	if secondsDrift > 0 {
		// Clear old buckets
		for i := 0; i < min(secondsDrift, 3); i++ {
			nextIdx := (buckets.currentIdx + 1 + i) % 3
			buckets.buckets[nextIdx] = 0
		}
		// Update current index
		buckets.currentIdx = (buckets.currentIdx + secondsDrift) % 3
		buckets.lastUpdate = now
	}

	// Add bytes to current bucket
	buckets.buckets[buckets.currentIdx] += bytes
	rt.lastUpdate[requestID] = time.Now()
}

func (rt *ResourceTracker) GetHeaviestQuery() (string, uint64) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	var maxID string
	var maxBytes uint64

	for id, buckets := range rt.memoryData {
		// Sum all buckets (represents last 3 seconds)
		var totalBytes uint64
		for _, bytes := range buckets.buckets {
			totalBytes += bytes
		}
		if totalBytes > maxBytes {
			maxBytes = totalBytes
			maxID = id
		}
	}

	return maxID, maxBytes
}

func (rt *ResourceTracker) running(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			rt.cleanup()
		}
	}
}

func (rt *ResourceTracker) activeRequestCount() float64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	return float64(len(rt.lastUpdate))
}

func (rt *ResourceTracker) cleanup() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-3 * time.Second)

	// Remove stale requestIDs
	for requestID, lastUpdate := range rt.lastUpdate {
		if lastUpdate.Before(cutoff) {
			delete(rt.memoryData, requestID)
			delete(rt.lastUpdate, requestID)
		}
	}
}

func (rt *ResourceTracker) evictOldest() {
	var oldestID string
	var oldestTime time.Time

	// Find oldest request
	for requestID, lastUpdate := range rt.lastUpdate {
		if oldestID == "" || lastUpdate.Before(oldestTime) {
			oldestID = requestID
			oldestTime = lastUpdate
		}
	}

	// Remove oldest request
	if oldestID != "" {
		delete(rt.memoryData, oldestID)
		delete(rt.lastUpdate, oldestID)
	}
}
