package resource

import (
	"sync"
	"time"
)

type memoryBuckets struct {
	buckets    [3]uint64 // 3 buckets for 3 seconds
	lastUpdate time.Time
	currentIdx int
}

type ResourceTracker struct {
	cpuData    map[string]time.Duration
	memoryData map[string]*memoryBuckets
	lastUpdate map[string]time.Time // Track last update per requestID

	mu sync.RWMutex
}

type IResourceTracker interface {
	AddDuration(requestID string, duration time.Duration)
	AddBytes(requestID string, bytes uint64)
	GetSlowestQuery() (requestID string, duration time.Duration)
	GetHeaviestQuery() (requestID string, bytes uint64)
}

func NewResourceTracker() *ResourceTracker {
	rt := &ResourceTracker{
		cpuData:    make(map[string]time.Duration),
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	// Start cleanup goroutine
	go rt.cleanupLoop()

	return rt
}

func (rt *ResourceTracker) AddDuration(requestID string, duration time.Duration) {
	rt.mu.Lock()
	now := time.Now()
	rt.cpuData[requestID] += duration
	rt.lastUpdate[requestID] = now
	rt.mu.Unlock()
}

func (rt *ResourceTracker) AddBytes(requestID string, bytes uint64) {
	rt.mu.Lock()
	now := time.Now().Truncate(time.Second)
	
	buckets, exists := rt.memoryData[requestID]
	if !exists {
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
	rt.mu.Unlock()
}

func (rt *ResourceTracker) GetSlowestQuery() (string, time.Duration) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	var maxID string
	var maxDuration time.Duration

	for id, duration := range rt.cpuData {
		if duration > maxDuration {
			maxDuration = duration
			maxID = id
		}
	}

	return maxID, maxDuration
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

func (rt *ResourceTracker) cleanupLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rt.cleanup()
	}
}

func (rt *ResourceTracker) cleanup() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-5 * time.Second)

	// Remove stale requestIDs
	for requestID, lastUpdate := range rt.lastUpdate {
		if lastUpdate.Before(cutoff) {
			delete(rt.cpuData, requestID)
			delete(rt.memoryData, requestID)
			delete(rt.lastUpdate, requestID)
		}
	}

	// Memory buckets are self-cleaning via rotation, no additional cleanup needed
}
