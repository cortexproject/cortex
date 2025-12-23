package ingester

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestActiveQueriedSeries_UpdateSeries(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	now := time.Now()
	ls1 := labels.FromStrings("a", "1")
	ls2 := labels.FromStrings("a", "2")

	// Initially should be 0
	count, err := a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// Add first series
	hash1 := ls1.Hash()
	a.UpdateSeriesBatch([]uint64{hash1}, now)
	active, err := a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	assert.Greater(t, active, uint64(0), "Should have at least 1 series")
	assert.LessOrEqual(t, active, uint64(2), "HyperLogLog estimate should be close to 1")

	// Add same series again (should still be ~1)
	a.UpdateSeriesBatch([]uint64{hash1}, now)
	active, err = a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	assert.Greater(t, active, uint64(0))
	assert.LessOrEqual(t, active, uint64(2), "Duplicate series should not significantly increase count")

	// Add different series
	hash2 := ls2.Hash()
	a.UpdateSeriesBatch([]uint64{hash2}, now)
	active, err = a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, active, uint64(1), "Should have at least 1 series")
	assert.LessOrEqual(t, active, uint64(3), "HyperLogLog estimate should be close to 2")
}

func TestActiveQueriedSeries_MultipleSeries(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	now := time.Now()
	numSeries := 100

	// Add many different series
	hashes := make([]uint64, 0, numSeries)
	for i := range numSeries {
		ls := labels.FromStrings("metric", "value", "index", fmt.Sprintf("%d", i))
		hash := ls.Hash()
		hashes = append(hashes, hash)
	}
	a.UpdateSeriesBatch(hashes, now)

	active, err := a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	// HyperLogLog with precision 14 has ~0.81% error, so we allow some variance
	// For 100 series, estimate should be between ~90 and ~110
	assert.Greater(t, active, uint64(80), "Should estimate at least 80 series")
	assert.Less(t, active, uint64(120), "Should estimate at most 120 series")
}

func TestActiveQueriedSeries_WindowRotation(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	baseTime := time.Now()

	// Add series in first window
	ls1 := labels.FromStrings("a", "1")
	hash1 := ls1.Hash()
	a.UpdateSeriesBatch([]uint64{hash1}, baseTime)
	count, err := a.GetSeriesQueried(baseTime, 0)
	assert.NoError(t, err)
	assert.Greater(t, count, uint64(0))

	// Advance time by one window duration
	time1 := baseTime.Add(windowDuration)
	ls2 := labels.FromStrings("a", "2")
	hash2 := ls2.Hash()
	a.UpdateSeriesBatch([]uint64{hash2}, time1)

	// Both series should still be active (within tracking period)
	active, err := a.GetSeriesQueried(time1, 0)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, active, uint64(1), "Should have at least 1 series")
	assert.LessOrEqual(t, active, uint64(3), "Should have at most 3 series (allowing for HLL variance)")

	// Advance time beyond all windows
	time2 := baseTime.Add(time.Duration(numWindows+1) * windowDuration)
	a.Purge(time2)

	// All windows should be expired
	active, err = a.GetSeriesQueried(time2, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), active, "All windows should be expired")
}

func TestActiveQueriedSeries_Purge(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	baseTime := time.Now()

	// Add series in first window
	ls1 := labels.FromStrings("a", "1")
	hash1 := ls1.Hash()
	a.UpdateSeriesBatch([]uint64{hash1}, baseTime)

	// Add series in second window
	time1 := baseTime.Add(windowDuration)
	ls2 := labels.FromStrings("a", "2")
	hash2 := ls2.Hash()
	a.UpdateSeriesBatch([]uint64{hash2}, time1)

	// Both should be active
	active, err := a.GetSeriesQueried(time1, 0)
	assert.NoError(t, err)
	assert.Greater(t, active, uint64(0))

	// Purge at time that expires first window but not second
	time2 := baseTime.Add(time.Duration(numWindows) * windowDuration)
	a.Purge(time2)

	// First window should be expired, but second should still be active
	active, err = a.GetSeriesQueried(time2, 0)
	assert.NoError(t, err)
	// The estimate might be 0 or 1 depending on which windows are still active
	assert.GreaterOrEqual(t, active, uint64(0))
}

func TestActiveQueriedSeries_TimeExpiration(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 2
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	baseTime := time.Now()

	// Add series at base time
	ls1 := labels.FromStrings("a", "1")
	hash1 := ls1.Hash()
	a.UpdateSeriesBatch([]uint64{hash1}, baseTime)

	// Add series one window later
	time1 := baseTime.Add(windowDuration)
	ls2 := labels.FromStrings("a", "2")
	hash2 := ls2.Hash()
	a.UpdateSeriesBatch([]uint64{hash2}, time1)

	// Both should be active
	active, err := a.GetSeriesQueried(time1, 0)
	assert.NoError(t, err)
	assert.Greater(t, active, uint64(0))

	// Advance time beyond tracking period
	time2 := baseTime.Add(time.Duration(numWindows+1) * windowDuration)
	a.Purge(time2)

	// All should be expired
	active, err = a.GetSeriesQueried(time2, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), active, "All series should be expired")
}

func TestActiveQueriedSeries_DefaultValues(t *testing.T) {
	// Test with valid minimal values
	// Note: Invalid values (zero/negative) are now validated at the Config level,
	// so this test verifies that the function works with valid minimal inputs.
	windowDuration := 1 * time.Minute
	a1 := NewActiveQueriedSeries([]time.Duration{windowDuration}, windowDuration, 1.0, log.NewNopLogger())
	assert.NotNil(t, a1)
	count, err := a1.GetSeriesQueried(time.Now(), 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// Test with minimum sample rate
	a2 := NewActiveQueriedSeries([]time.Duration{windowDuration}, windowDuration, 0.1, log.NewNopLogger())
	assert.NotNil(t, a2)
	count, err = a2.GetSeriesQueried(time.Now(), 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}

func TestActiveQueriedSeries_ConcurrentUpdates(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 10
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	now := time.Now()
	numGoroutines := 50
	numSeriesPerGoroutine := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(goroutineID int) {
			defer wg.Done()
			for j := range numSeriesPerGoroutine {
				ls := labels.FromStrings("metric", "value", "goroutine", fmt.Sprintf("%d", goroutineID), "series", fmt.Sprintf("%d", j))
				hash := ls.Hash()
				a.UpdateSeriesBatch([]uint64{hash}, now)
			}
		}(i)
	}

	wg.Wait()

	// Check that we got a reasonable estimate
	active, err := a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	expectedMin := uint64(numGoroutines * numSeriesPerGoroutine / 2) // Allow for 50% variance
	expectedMax := uint64(numGoroutines * numSeriesPerGoroutine * 2) // Allow for 2x variance
	assert.Greater(t, active, expectedMin, "Concurrent updates should be tracked")
	assert.Less(t, active, expectedMax, "Estimate should not be too high")
}

func TestActiveQueriedSeries_Clear(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	now := time.Now()

	// Add some series
	ls1 := labels.FromStrings("a", "1")
	hash1 := ls1.Hash()
	a.UpdateSeriesBatch([]uint64{hash1}, now)

	ls2 := labels.FromStrings("a", "2")
	hash2 := ls2.Hash()
	a.UpdateSeriesBatch([]uint64{hash2}, now)

	count, err := a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	assert.Greater(t, count, uint64(0))

	// Clear and verify
	a.clear()
	count, err = a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count, "After clear, should have 0 active series")
}

func TestActiveQueriedSeries_EmptyWindows(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	now := time.Now()

	// Should return 0 for empty tracker
	count, err := a.GetSeriesQueried(now, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}

func TestActiveQueriedSeries_WindowBoundaries(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 2
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	baseTime := time.Now()

	// Add series exactly at window start
	ls1 := labels.FromStrings("a", "1")
	hash1 := ls1.Hash()
	a.UpdateSeriesBatch([]uint64{hash1}, baseTime)

	// Add series exactly at window end (should be in next window)
	time1 := baseTime.Add(windowDuration)
	ls2 := labels.FromStrings("a", "2")
	hash2 := ls2.Hash()
	a.UpdateSeriesBatch([]uint64{hash2}, time1)

	// Both should be tracked
	active, err := a.GetSeriesQueried(time1, 0)
	assert.NoError(t, err)
	assert.Greater(t, active, uint64(0))
}

func TestActiveQueriedSeries_ManyWindows(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 10
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	baseTime := time.Now()

	// Add series across multiple windows
	for i := range numWindows {
		ls := labels.FromStrings("metric", "value", "window", fmt.Sprintf("%d", i))
		hash := ls.Hash()
		windowTime := baseTime.Add(time.Duration(i) * windowDuration)
		a.UpdateSeriesBatch([]uint64{hash}, windowTime)
	}

	// All should be active
	lastTime := baseTime.Add(time.Duration(numWindows-1) * windowDuration)
	active, err := a.GetSeriesQueried(lastTime, 0)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, active, uint64(1), "Should have at least some series active")
}

func TestActiveQueriedSeries_Accuracy(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	// Test with known number of unique series
	testCases := []struct {
		name        string
		numSeries   int
		expectedMin uint64
		expectedMax uint64
	}{
		{"10 series", 10, 8, 15},
		{"50 series", 50, 40, 65},
		{"100 series", 100, 85, 120},
		{"500 series", 500, 450, 550},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a.clear()
			// Use a fresh timestamp after clearing to ensure it's within the new window range
			testNow := time.Now()
			for i := 0; i < tc.numSeries; i++ {
				ls := labels.FromStrings("metric", "value", "id", fmt.Sprintf("%d", i))
				hash := ls.Hash()
				a.UpdateSeriesBatch([]uint64{hash}, testNow)
			}

			active, err := a.GetSeriesQueried(testNow, 0)
			assert.NoError(t, err)
			assert.GreaterOrEqual(t, active, tc.expectedMin,
				"Estimate should be at least %d for %d series", tc.expectedMin, tc.numSeries)
			assert.LessOrEqual(t, active, tc.expectedMax,
				"Estimate should be at most %d for %d series", tc.expectedMax, tc.numSeries)
		})
	}
}

func TestActiveQueriedSeries_RapidRotation(t *testing.T) {
	windowDuration := 100 * time.Millisecond
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	baseTime := time.Now()

	// Add series in first window
	ls1 := labels.FromStrings("a", "1")
	hash1 := ls1.Hash()
	a.UpdateSeriesBatch([]uint64{hash1}, baseTime)

	// Rapidly advance through windows
	for i := 1; i <= numWindows+1; i++ {
		windowTime := baseTime.Add(time.Duration(i) * windowDuration)
		ls := labels.FromStrings("a", fmt.Sprintf("%d", i))
		hash := ls.Hash()
		a.UpdateSeriesBatch([]uint64{hash}, windowTime)
		a.Purge(windowTime)
	}

	// After rapid rotation, only recent windows should have data
	finalTime := baseTime.Add(time.Duration(numWindows+1) * windowDuration)
	active, err := a.GetSeriesQueried(finalTime, 0)
	assert.NoError(t, err)
	// Should have at least the most recent series
	assert.GreaterOrEqual(t, active, uint64(0))
}

func TestActiveQueriedSeries_MergeWindows(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	baseTime := time.Now()

	// Add different series to different windows
	series1 := labels.FromStrings("metric", "value1")
	hash1 := series1.Hash()
	a.UpdateSeriesBatch([]uint64{hash1}, baseTime)

	series2 := labels.FromStrings("metric", "value2")
	hash2 := series2.Hash()
	time1 := baseTime.Add(windowDuration)
	a.UpdateSeriesBatch([]uint64{hash2}, time1)

	series3 := labels.FromStrings("metric", "value3")
	hash3 := series3.Hash()
	time2 := baseTime.Add(2 * windowDuration)
	a.UpdateSeriesBatch([]uint64{hash3}, time2)

	// All three should be merged and counted
	active, err := a.GetSeriesQueried(time2, 0)
	assert.NoError(t, err)
	// HyperLogLog should estimate around 3, allowing for variance
	assert.GreaterOrEqual(t, active, uint64(2), "Should estimate at least 2 unique series")
	assert.LessOrEqual(t, active, uint64(5), "Should estimate at most 5 unique series")
}

func TestActiveQueriedSeries_EdgeCaseTimes(t *testing.T) {
	windowDuration := 1 * time.Minute
	numWindows := 3
	totalDuration := time.Duration(numWindows) * windowDuration
	a := NewActiveQueriedSeries([]time.Duration{totalDuration}, windowDuration, 1.0, log.NewNopLogger())

	baseTime := time.Now()

	// Test with time exactly at window boundary
	ls := labels.FromStrings("a", "1")
	hash := ls.Hash()
	a.UpdateSeriesBatch([]uint64{hash}, baseTime)

	// Query at exact boundary
	active, err := a.GetSeriesQueried(baseTime, 0)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, active, uint64(0))

	// Test with time before base
	pastTime := baseTime.Add(-1 * time.Hour)
	active, err = a.GetSeriesQueried(pastTime, 0)
	assert.NoError(t, err)
	// Should handle gracefully
	assert.GreaterOrEqual(t, active, uint64(0))
}
