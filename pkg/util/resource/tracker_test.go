package resource

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResourceTracker_AddBytes(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	rt.AddBytes("req1", 1000)

	assert.Len(t, rt.memoryData, 1)
	assert.Contains(t, rt.memoryData, "req1")
	assert.Equal(t, uint64(1000), rt.memoryData["req1"].buckets[0])
}

func TestResourceTracker_GetHeaviestQuery(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	rt.AddBytes("req1", 1000)
	rt.AddBytes("req2", 2000)
	rt.AddBytes("req3", 500)

	requestID, bytes := rt.GetHeaviestQuery()
	assert.Equal(t, "req2", requestID)
	assert.Equal(t, uint64(2000), bytes)
}

func TestResourceTracker_EmptyTracker(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	requestID, bytes := rt.GetHeaviestQuery()
	assert.Equal(t, "", requestID)
	assert.Equal(t, uint64(0), bytes)
}

func TestResourceTracker_SlidingWindow(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	// Add bytes at different times
	rt.AddBytes("req1", 1000)
	
	// Simulate 1 second later
	rt.mu.Lock()
	rt.memoryData["req1"].lastUpdate = rt.memoryData["req1"].lastUpdate.Add(-1 * time.Second)
	rt.mu.Unlock()
	
	rt.AddBytes("req1", 2000)
	
	// Should have both values in different buckets
	_, bytes := rt.GetHeaviestQuery()
	assert.Equal(t, uint64(3000), bytes) // 1000 + 2000
}

func TestResourceTracker_BucketRotation(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	rt.AddBytes("req1", 1000)
	
	// Simulate 4 seconds later (should clear old buckets)
	rt.mu.Lock()
	rt.memoryData["req1"].lastUpdate = rt.memoryData["req1"].lastUpdate.Add(-4 * time.Second)
	rt.mu.Unlock()
	
	rt.AddBytes("req1", 2000)
	
	// Should only have the new value (old bucket cleared)
	_, bytes := rt.GetHeaviestQuery()
	assert.Equal(t, uint64(2000), bytes)
}

func TestResourceTracker_Cleanup(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	rt.AddBytes("req1", 1000)
	rt.AddBytes("req2", 2000)

	// Simulate old lastUpdate time
	rt.mu.Lock()
	rt.lastUpdate["req1"] = time.Now().Add(-5 * time.Second)
	rt.mu.Unlock()

	rt.cleanup()

	assert.Len(t, rt.memoryData, 1)
	assert.Contains(t, rt.memoryData, "req2")
	assert.NotContains(t, rt.memoryData, "req1")
}

func TestResourceTracker_MaxActiveRequests(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	// Manually set to limit for faster test
	rt.mu.Lock()
	for i := 0; i < maxActiveRequests; i++ {
		rt.memoryData[fmt.Sprintf("req%d", i)] = &memoryBuckets{lastUpdate: time.Now()}
		rt.lastUpdate[fmt.Sprintf("req%d", i)] = time.Now()
	}
	rt.mu.Unlock()

	// Add one more request (should trigger eviction)
	rt.AddBytes("new_req", 9999)

	assert.Len(t, rt.memoryData, maxActiveRequests)
	assert.Contains(t, rt.memoryData, "new_req")
}

func TestResourceTracker_EvictOldest(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	now := time.Now()
	
	// Add requests with different timestamps
	rt.memoryData["req1"] = &memoryBuckets{}
	rt.lastUpdate["req1"] = now.Add(-10 * time.Second) // Oldest
	
	rt.memoryData["req2"] = &memoryBuckets{}
	rt.lastUpdate["req2"] = now.Add(-5 * time.Second)
	
	rt.memoryData["req3"] = &memoryBuckets{}
	rt.lastUpdate["req3"] = now

	rt.evictOldest()

	assert.Len(t, rt.memoryData, 2)
	assert.NotContains(t, rt.memoryData, "req1") // Oldest should be evicted
	assert.Contains(t, rt.memoryData, "req2")
	assert.Contains(t, rt.memoryData, "req3")
}

func TestResourceTracker_ConcurrentAccess(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	// Test concurrent writes
	done := make(chan bool, 20)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				rt.AddBytes(fmt.Sprintf("req%d", id), uint64(j))
			}
			done <- true
		}(i)
	}

	// Test concurrent reads
	for i := 0; i < 10; i++ {
		go func() {
			rt.GetHeaviestQuery()
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Should have 10 requests
	assert.Len(t, rt.memoryData, 10)
}

func TestResourceTracker_AccumulateBytes(t *testing.T) {
	rt := &ResourceTracker{
		memoryData: make(map[string]*memoryBuckets),
		lastUpdate: make(map[string]time.Time),
	}

	// Add bytes multiple times to same request
	rt.AddBytes("req1", 1000)
	rt.AddBytes("req1", 2000)
	rt.AddBytes("req1", 3000)

	_, bytes := rt.GetHeaviestQuery()
	assert.Equal(t, uint64(6000), bytes) // Should accumulate
}