package tracker

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryTracker(t *testing.T) {
	queryTracker := NewQueryTracker()
	queryTracker.Add("r-1", 3000)
	queryTracker.Add("r-2", 300)
	requestID, rate := queryTracker.GetWorstQuery()

	assert.Equal(t, "r-1", requestID)
	assert.Equal(t, float64(1000), rate)
}

func TestQueryTracker_MaxQueries(t *testing.T) {
	queryTracker := NewQueryTracker()

	// Add more than maxTrackedQueries with low rates
	for i := 0; i < 200; i++ {
		queryTracker.Add(fmt.Sprintf("low-%d", i), 100)
	}

	// Add high-rate query
	queryTracker.Add("high-rate", 5000)

	// Should track the high-rate query and evict low-rate ones
	requestID, rate := queryTracker.GetWorstQuery()
	assert.Equal(t, "high-rate", requestID)
	assert.True(t, rate > 1000)

	// Verify heap size is bounded
	queryTracker.mu.Lock()
	heapSize := queryTracker.heap.Len()
	queryTracker.mu.Unlock()
	assert.LessOrEqual(t, heapSize, maxTrackedQueries)
}

func TestQueryTracker_TTL(t *testing.T) {
	queryTracker := NewQueryTracker()
	for i := 0; i < 200; i++ {
		queryTracker.Add(fmt.Sprintf("low-%d", i), 300)
	}
	requestID, rate := queryTracker.GetWorstQuery()
	assert.Equal(t, float64(100), rate)

	time.Sleep(4 * time.Second) // expire all items
	requestID, rate = queryTracker.GetWorstQuery()

	assert.Equal(t, "", requestID)
	assert.Equal(t, float64(0), rate)
}

func TestQueryTracker_EmptyHeap(t *testing.T) {
	queryTracker := NewQueryTracker()
	requestID, rate := queryTracker.GetWorstQuery()
	assert.Equal(t, "", requestID)
	assert.Equal(t, float64(0), rate)
}

func TestQueryTracker_UpdateExistingQuery(t *testing.T) {
	queryTracker := NewQueryTracker()
	queryTracker.Add("r-1", 1000)
	initialRate := queryTracker.lookup["r-1"].bytesRate.rate()
	
	// Add more bytes to same query
	queryTracker.Add("r-1", 2000)
	updatedRate := queryTracker.lookup["r-1"].bytesRate.rate()
	
	assert.True(t, updatedRate > initialRate)
	requestID, rate := queryTracker.GetWorstQuery()
	assert.Equal(t, "r-1", requestID)
	assert.Equal(t, float64(1000), rate) // 3000 bytes / 3 seconds
}

func TestQueryTracker_HeapEviction(t *testing.T) {
	queryTracker := NewQueryTracker()
	
	// Fill heap to capacity with low-rate queries
	for i := 0; i < maxTrackedQueries; i++ {
		queryTracker.Add(fmt.Sprintf("low-%d", i), 100)
	}
	
	// Verify heap is at capacity
	assert.Equal(t, maxTrackedQueries, queryTracker.heap.Len())
	
	// Add a high-rate query that should evict the lowest
	queryTracker.Add("high-rate", 10000)
	
	// Heap should still be at capacity
	assert.Equal(t, maxTrackedQueries, queryTracker.heap.Len())
	
	// High-rate query should be tracked
	_, exists := queryTracker.lookup["high-rate"]
	assert.True(t, exists)
	
	// Should be the worst query
	requestID, rate := queryTracker.GetWorstQuery()
	assert.Equal(t, "high-rate", requestID)
	assert.True(t, rate > 3000)
}

func TestQueryTracker_NoEvictionForLowRate(t *testing.T) {
	queryTracker := NewQueryTracker()
	
	// Fill heap with medium-rate queries
	for i := 0; i < maxTrackedQueries; i++ {
		queryTracker.Add(fmt.Sprintf("med-%d", i), 1000)
	}
	
	// Try to add a lower-rate query
	queryTracker.Add("low-rate", 50)
	
	// Low-rate query should not be tracked
	_, exists := queryTracker.lookup["low-rate"]
	assert.False(t, exists)
	
	// Heap should still be at capacity
	assert.Equal(t, maxTrackedQueries, queryTracker.heap.Len())
}
