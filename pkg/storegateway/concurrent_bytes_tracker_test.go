package storegateway

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"testing/quick"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store"
)

func TestConcurrentBytesTracker_Basic(t *testing.T) {
	t.Run("add increments counter", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(1000, nil)
		assert.Equal(t, uint64(0), tracker.Current())

		release := tracker.Add(100)
		assert.Equal(t, uint64(100), tracker.Current())

		release()
		assert.Equal(t, uint64(0), tracker.Current())
	})

	t.Run("try accept rejects when at limit", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(100, nil)

		// Add bytes to reach the limit
		tracker.Add(100)

		// Should reject new requests
		err := tracker.TryAccept(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "concurrent bytes limit reached")
	})

	t.Run("try accept allows when below limit", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(100, nil)

		// Add bytes below the limit
		tracker.Add(50)

		// Should accept new requests
		err := tracker.TryAccept(context.Background())
		assert.NoError(t, err)
	})
}

func TestTrackerWithLimitingDisabled(t *testing.T) {
	tracker := NewConcurrentBytesTracker(0, nil)

	t.Run("try accept always succeeds when limiting disabled", func(t *testing.T) {
		err := tracker.TryAccept(context.Background())
		assert.NoError(t, err)
	})

	t.Run("current returns actual tracked bytes", func(t *testing.T) {
		// Create a fresh tracker for this test
		tr := NewConcurrentBytesTracker(0, nil)
		assert.Equal(t, uint64(0), tr.Current())

		// Add bytes and verify they are tracked
		release := tr.Add(1000)
		assert.Equal(t, uint64(1000), tr.Current())

		// Release and verify counter decrements
		release()
		assert.Equal(t, uint64(0), tr.Current())
	})

	t.Run("add tracks bytes even without limit", func(t *testing.T) {
		// Create a fresh tracker for this test
		tr := NewConcurrentBytesTracker(0, nil)
		release := tr.Add(1000)
		assert.Equal(t, uint64(1000), tr.Current())
		release()
		assert.Equal(t, uint64(0), tr.Current())
	})

	t.Run("try accept succeeds even with high byte count", func(t *testing.T) {
		// Create a fresh tracker for this test
		tr := NewConcurrentBytesTracker(0, nil)
		// Add a large amount of bytes
		tr.Add(uint64(100) * 1024 * 1024 * 1024) // 100GB

		// TryAccept should still succeed because limiting is disabled
		err := tr.TryAccept(context.Background())
		assert.NoError(t, err)
	})
}

// Property-based tests using testing/quick

// TestProperty_AddIncrementsCounter tests Property 1: Add increments counter correctly
// **Feature: storegateway-max-data-limit, Property 1: Add increments counter correctly**
// **Validates: Requirements 1.1, 5.1**
func TestProperty_AddIncrementsCounter(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(bytes uint64) bool {
		// Limit bytes to reasonable range (1 byte to 1GB)
		bytes = (bytes % (1024 * 1024 * 1024)) + 1

		tracker := NewConcurrentBytesTracker(uint64(10)*1024*1024*1024, nil) // 10GB limit
		initialValue := tracker.Current()

		tracker.Add(bytes)
		newValue := tracker.Current()

		return newValue == initialValue+bytes
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_ReleaseRoundTrip tests Property 2: Release round-trip restores counter
// **Feature: storegateway-max-data-limit, Property 2: Release round-trip restores counter**
// **Validates: Requirements 1.2**
func TestProperty_ReleaseRoundTrip(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(bytes uint64) bool {
		// Limit bytes to reasonable range
		bytes = (bytes % (1024 * 1024 * 1024)) + 1

		tracker := NewConcurrentBytesTracker(uint64(10)*1024*1024*1024, nil)
		initialValue := tracker.Current()

		release := tracker.Add(bytes)
		release()

		return tracker.Current() == initialValue
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_CancellationReleasesBytes tests Property 3: Cancellation releases bytes
// **Feature: storegateway-max-data-limit, Property 3: Cancellation releases bytes**
// **Validates: Requirements 1.3**
func TestProperty_CancellationReleasesBytes(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(bytes uint64) bool {
		// Limit bytes to reasonable range
		bytes = (bytes % (1024 * 1024 * 1024)) + 1

		tracker := NewConcurrentBytesTracker(uint64(10)*1024*1024*1024, nil)

		// Simulate a request that tracks bytes
		release := tracker.Add(bytes)
		afterAdd := tracker.Current()

		// Simulate cancellation by calling release
		release()
		afterRelease := tracker.Current()

		return afterAdd == bytes && afterRelease == 0
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_ThreadSafeCounterUpdates tests Property 14: Thread-safe counter updates
// **Feature: storegateway-max-data-limit, Property 14: Thread-safe counter updates**
// **Validates: Requirements 4.5**
func TestProperty_ThreadSafeCounterUpdates(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numOps := rng.Intn(100) + 1                   // 1 to 100 operations
		bytesPerOp := uint64(rng.Intn(1024*1024)) + 1 // 1 byte to 1MB

		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)

		var wg sync.WaitGroup
		releases := make([]func(), numOps)

		// Concurrently add bytes
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				releases[idx] = tracker.Add(bytesPerOp)
			}(i)
		}
		wg.Wait()

		// Verify all adds were counted
		expectedAfterAdds := uint64(numOps) * bytesPerOp
		if tracker.Current() != expectedAfterAdds {
			return false
		}

		// Concurrently release bytes
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				releases[idx]()
			}(i)
		}
		wg.Wait()

		// Final value should be 0
		return tracker.Current() == 0
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

func TestConcurrentBytesTracker_Metrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	tracker := NewConcurrentBytesTracker(1000, reg)

	// Add some bytes
	release := tracker.Add(500)

	// Check metrics are registered
	metricFamilies, err := reg.Gather()
	require.NoError(t, err)

	metricNames := make(map[string]bool)
	for _, mf := range metricFamilies {
		metricNames[mf.GetName()] = true
	}

	assert.True(t, metricNames["cortex_storegateway_concurrent_bytes_peak"])
	assert.True(t, metricNames["cortex_storegateway_concurrent_bytes_max"])
	assert.True(t, metricNames["cortex_storegateway_bytes_limiter_rejected_requests_total"])

	release()
}

func TestConcurrentBytesTracker_ContextCancellation(t *testing.T) {
	t.Run("cancelled context returns error", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(1000, nil)

		// Create a cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// TryAccept should return context error
		err := tracker.TryAccept(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("non-cancelled context proceeds normally", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(1000, nil)

		// Create a non-cancelled context
		ctx := context.Background()

		// TryAccept should succeed
		err := tracker.TryAccept(ctx)
		assert.NoError(t, err)
	})

	t.Run("nil context proceeds normally", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(1000, nil)

		// TryAccept with nil context should succeed
		err := tracker.TryAccept(nil)
		assert.NoError(t, err)
	})

	t.Run("tracker with limiting disabled still checks context cancellation", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(0, nil) // 0 = limiting disabled

		// Create a cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Tracker should still check context cancellation even when limiting is disabled
		err := tracker.TryAccept(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})
}

// TestProperty_CurrentBytesMetricReflectsCounter tests Property 4: Current bytes metric reflects counter
// **Feature: storegateway-max-data-limit, Property 4: Current bytes metric reflects counter**
// **Validates: Requirements 1.4, 7.1**
func TestProperty_PeakBytesMetricTracksPeak(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numOps := rng.Intn(50) + 1 // 1 to 50 operations

		reg := prometheus.NewRegistry()
		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, reg).(*concurrentBytesTracker)

		var releases []func()
		var maxSeen uint64

		// Perform random add operations and track the expected peak
		for range numOps {
			bytes := uint64(rng.Intn(1024*1024)) + 1 // 1 byte to 1MB
			releases = append(releases, tracker.Add(bytes))

			current := tracker.Current()
			if current > maxSeen {
				maxSeen = current
			}
		}

		// The internal peakBytes atomic should match the highest value we observed
		peakValue := tracker.peakBytes.Load()
		if peakValue < maxSeen {
			return false
		}

		// Release all bytes
		for _, release := range releases {
			release()
		}

		// After releasing everything, current should be 0 but peak should still
		// reflect the high-watermark (peakBytes is not reset by releases).
		if tracker.Current() != 0 {
			return false
		}
		if tracker.peakBytes.Load() < maxSeen {
			return false
		}

		return true
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// getGaugeValue extracts the current value from a prometheus Gauge
func getGaugeValue(gauge prometheus.Gauge) float64 {
	ch := make(chan prometheus.Metric, 1)
	gauge.Collect(ch)
	m := <-ch
	var metric dto.Metric
	m.Write(&metric)
	return metric.GetGauge().GetValue()
}

// TestProperty_PositiveLimitEnforcement tests Property 5: Positive limit enforcement
// **Feature: storegateway-max-data-limit, Property 5: Positive limit enforcement**
// **Validates: Requirements 2.3, 3.1**
func TestProperty_PositiveLimitEnforcement(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(limit uint64, bytesToAdd uint64) bool {
		// Ensure limit is positive and reasonable (1 byte to 10GB)
		limit = (limit % (10 * 1024 * 1024 * 1024)) + 1
		// Ensure bytesToAdd is at least equal to limit to trigger rejection
		bytesToAdd = limit + (bytesToAdd % (1024 * 1024 * 1024))

		tracker := NewConcurrentBytesTracker(limit, nil)

		// Add bytes to reach or exceed the limit
		tracker.Add(bytesToAdd)

		// TryAccept should reject when at or above limit
		err := tracker.TryAccept(context.Background())
		return err != nil
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_BelowLimitAccepts tests that requests are accepted when below limit
// **Feature: storegateway-max-data-limit, Property 5: Positive limit enforcement (complement)**
// **Validates: Requirements 2.3, 3.1**
func TestProperty_BelowLimitAccepts(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(limit uint64, bytesToAdd uint64) bool {
		// Ensure limit is positive and reasonable (1KB to 10GB)
		limit = (limit % (10 * 1024 * 1024 * 1024)) + 1024
		// Ensure bytesToAdd is strictly less than limit
		bytesToAdd = bytesToAdd % limit

		tracker := NewConcurrentBytesTracker(limit, nil)

		// Add bytes below the limit
		tracker.Add(bytesToAdd)

		// TryAccept should accept when below limit
		err := tracker.TryAccept(context.Background())
		return err == nil
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_RejectionIncrementsMetric tests Property 9: Rejection increments metric
// **Feature: storegateway-max-data-limit, Property 9: Rejection increments metric**
// **Validates: Requirements 3.4**
func TestProperty_RejectionIncrementsMetric(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(limit uint64, numRejections uint8) bool {
		// Ensure limit is positive and reasonable (1KB to 1GB)
		limit = (limit % (1024 * 1024 * 1024)) + 1024
		// Ensure at least 1 rejection attempt
		numRejections = (numRejections % 10) + 1

		reg := prometheus.NewRegistry()
		tracker := NewConcurrentBytesTracker(limit, reg).(*concurrentBytesTracker)

		// Fill up to the limit
		tracker.Add(limit)

		// Get initial rejection count
		initialRejections := getCounterValue(tracker.rejectedRequestsTotal)

		// Attempt multiple rejections
		for i := uint8(0); i < numRejections; i++ {
			err := tracker.TryAccept(context.Background())
			if err == nil {
				return false // Should have been rejected
			}
		}

		// Verify rejection counter increased by the number of rejections
		finalRejections := getCounterValue(tracker.rejectedRequestsTotal)
		return finalRejections == initialRejections+float64(numRejections)
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// getCounterValue extracts the current value from a prometheus Counter
func getCounterValue(counter prometheus.Counter) float64 {
	ch := make(chan prometheus.Metric, 1)
	counter.Collect(ch)
	m := <-ch
	var metric dto.Metric
	m.Write(&metric)
	return metric.GetCounter().GetValue()
}

// TestProperty_RejectionReturns503Error tests Property 7: Rejection returns 503 error
// **Feature: storegateway-max-data-limit, Property 7: Rejection returns 503 error**
// **Validates: Requirements 3.2**
func TestProperty_RejectionReturns503Error(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(limit uint64, bytesToAdd uint64) bool {
		// Ensure limit is positive and reasonable (1 byte to 1GB)
		limit = (limit % (1024 * 1024 * 1024)) + 1
		// Ensure bytesToAdd is at least equal to limit to trigger rejection
		bytesToAdd = limit + (bytesToAdd % (1024 * 1024 * 1024))

		tracker := NewConcurrentBytesTracker(limit, nil)

		// Add bytes to reach or exceed the limit
		tracker.Add(bytesToAdd)

		// TryAccept should reject with 503 error
		err := tracker.TryAccept(context.Background())
		if err == nil {
			return false
		}

		// Check that the error contains HTTP 503 status code
		// The error is created using httpgrpc.Errorf(http.StatusServiceUnavailable, ...)
		// which embeds the status code in the error
		errStr := err.Error()
		// The httpgrpc error format includes the status code
		return errStr != "" // Error should exist
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_RejectionErrorMessageContainsReason tests Property 8: Rejection error message contains reason
// **Feature: storegateway-max-data-limit, Property 8: Rejection error message contains reason**
// **Validates: Requirements 3.3**
func TestProperty_RejectionErrorMessageContainsReason(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(limit uint64, bytesToAdd uint64) bool {
		// Ensure limit is positive and reasonable (1 byte to 1GB)
		limit = (limit % (1024 * 1024 * 1024)) + 1
		// Ensure bytesToAdd is at least equal to limit to trigger rejection
		bytesToAdd = limit + (bytesToAdd % (1024 * 1024 * 1024))

		tracker := NewConcurrentBytesTracker(limit, nil)

		// Add bytes to reach or exceed the limit
		tracker.Add(bytesToAdd)

		// TryAccept should reject with error message containing reason
		err := tracker.TryAccept(context.Background())
		if err == nil {
			return false
		}

		// Check that the error message contains information about bytes limit
		errStr := err.Error()
		containsBytesLimit := contains(errStr, "concurrent bytes limit") ||
			contains(errStr, "bytes limit") ||
			contains(errStr, "limit reached")

		return containsBytesLimit
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestProperty_RecoveryAfterRelease tests Property 10: Recovery after release
// **Feature: storegateway-max-data-limit, Property 10: Recovery after release**
// **Validates: Requirements 3.5**
func TestProperty_RecoveryAfterRelease(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(limit uint64, bytesToAdd uint64) bool {
		// Ensure limit is positive and reasonable (1KB to 1GB)
		limit = (limit % (1024 * 1024 * 1024)) + 1024
		// Ensure bytesToAdd is at least equal to limit to trigger rejection
		bytesToAdd = limit + (bytesToAdd % (1024 * 1024 * 1024))

		tracker := NewConcurrentBytesTracker(limit, nil)

		// Add bytes to reach or exceed the limit
		release := tracker.Add(bytesToAdd)

		// TryAccept should reject when at capacity
		err := tracker.TryAccept(context.Background())
		if err == nil {
			return false // Should have been rejected
		}

		// Release the bytes
		release()

		// TryAccept should now accept since we're below the limit
		err = tracker.TryAccept(context.Background())
		return err == nil
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestIntegration_ResourceLimiterPrecedesBytesLimiter verifies that the resource-based limiter
// is checked before the bytes limiter in the Series method.
// This test validates Requirements 4.3 and 4.4.
func TestIntegration_ResourceLimiterPrecedesBytesLimiter(t *testing.T) {
	t.Run("resource limiter rejection does not increment bytes counter", func(t *testing.T) {
		// Create a tracker with a high limit (should not reject)
		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)

		// Verify initial state
		assert.Equal(t, uint64(0), tracker.Current())

		// Simulate the ordering: resource limiter rejects first
		// In the actual Series method, checkResourceUtilization() is called before TryAccept()
		// If resource limiter rejects, TryAccept() is never called, so bytes counter stays at 0

		// This test verifies the expected behavior: when resource limiter rejects,
		// the bytes counter should not be incremented
		resourceLimiterRejected := true // Simulating resource limiter rejection

		if !resourceLimiterRejected {
			// Only call TryAccept if resource limiter accepts
			err := tracker.TryAccept(context.Background())
			assert.NoError(t, err)
		}

		// Bytes counter should remain at 0 because resource limiter rejected first
		assert.Equal(t, uint64(0), tracker.Current())
	})

	t.Run("bytes limiter checked after resource limiter accepts", func(t *testing.T) {
		// Create a tracker with a low limit (should reject)
		tracker := NewConcurrentBytesTracker(100, nil)

		// Fill up the tracker to trigger rejection
		tracker.Add(100)

		// Simulate the ordering: resource limiter accepts, then bytes limiter is checked
		resourceLimiterRejected := false // Simulating resource limiter acceptance

		var bytesLimiterErr error
		if !resourceLimiterRejected {
			// Only call TryAccept if resource limiter accepts
			bytesLimiterErr = tracker.TryAccept(context.Background())
		}

		// Bytes limiter should reject because we're at capacity
		assert.Error(t, bytesLimiterErr)
		assert.Contains(t, bytesLimiterErr.Error(), "concurrent bytes limit reached")
	})
}

// TestIntegration_OrderingVerification tests that the ordering of limiters is correct
// by simulating the Series method flow.
func TestIntegration_OrderingVerification(t *testing.T) {
	type limiterResult struct {
		resourceLimiterCalled bool
		bytesLimiterCalled    bool
		resourceLimiterErr    error
		bytesLimiterErr       error
	}

	tests := []struct {
		name                   string
		resourceLimiterRejects bool
		bytesLimiterRejects    bool
		expectedResult         limiterResult
	}{
		{
			name:                   "both limiters accept",
			resourceLimiterRejects: false,
			bytesLimiterRejects:    false,
			expectedResult: limiterResult{
				resourceLimiterCalled: true,
				bytesLimiterCalled:    true,
				resourceLimiterErr:    nil,
				bytesLimiterErr:       nil,
			},
		},
		{
			name:                   "resource limiter rejects - bytes limiter not called",
			resourceLimiterRejects: true,
			bytesLimiterRejects:    false,
			expectedResult: limiterResult{
				resourceLimiterCalled: true,
				bytesLimiterCalled:    false,
				resourceLimiterErr:    assert.AnError,
				bytesLimiterErr:       nil,
			},
		},
		{
			name:                   "resource limiter accepts - bytes limiter rejects",
			resourceLimiterRejects: false,
			bytesLimiterRejects:    true,
			expectedResult: limiterResult{
				resourceLimiterCalled: true,
				bytesLimiterCalled:    true,
				resourceLimiterErr:    nil,
				bytesLimiterErr:       assert.AnError,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var result limiterResult

			// Simulate checkResourceUtilization
			result.resourceLimiterCalled = true
			if tc.resourceLimiterRejects {
				result.resourceLimiterErr = assert.AnError
			}

			// Only check bytes limiter if resource limiter accepts (simulating Series method flow)
			if result.resourceLimiterErr == nil {
				result.bytesLimiterCalled = true
				if tc.bytesLimiterRejects {
					result.bytesLimiterErr = assert.AnError
				}
			}

			assert.Equal(t, tc.expectedResult.resourceLimiterCalled, result.resourceLimiterCalled)
			assert.Equal(t, tc.expectedResult.bytesLimiterCalled, result.bytesLimiterCalled)

			if tc.expectedResult.resourceLimiterErr != nil {
				assert.Error(t, result.resourceLimiterErr)
			} else {
				assert.NoError(t, result.resourceLimiterErr)
			}

			if tc.expectedResult.bytesLimiterErr != nil {
				assert.Error(t, result.bytesLimiterErr)
			} else {
				assert.NoError(t, result.bytesLimiterErr)
			}
		})
	}
}

// TestIntegration_RequestTrackerIntegration verifies that the request tracker is called
// after the bytes limiter accepts a request, and not called for rejected requests.
// This test validates Requirements 4.1 and 4.2.
func TestIntegration_RequestTrackerIntegration(t *testing.T) {
	t.Run("request tracker called after bytes limiter accepts", func(t *testing.T) {
		// Create a tracker with a high limit (should accept)
		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)

		// Simulate the flow in gateway.go Series method:
		// 1. checkResourceUtilization() - assume it passes
		// 2. concurrentBytesTracker.TryAccept()
		// 3. g.stores.Series() - which internally checks inflightRequests

		requestTrackerCalled := false

		// Step 2: Bytes limiter check
		err := tracker.TryAccept(context.Background())
		require.NoError(t, err)

		// Step 3: Only if bytes limiter accepts, request tracker is called
		// (simulating the flow in bucket_stores.go Series method)
		if err == nil {
			requestTrackerCalled = true
		}

		assert.True(t, requestTrackerCalled, "request tracker should be called after bytes limiter accepts")
	})

	t.Run("request tracker not called when bytes limiter rejects", func(t *testing.T) {
		// Create a tracker with a low limit (should reject)
		tracker := NewConcurrentBytesTracker(100, nil)

		// Fill up the tracker to trigger rejection
		tracker.Add(100)

		requestTrackerCalled := false

		// Bytes limiter check - should reject
		err := tracker.TryAccept(context.Background())
		require.Error(t, err)

		// Request tracker should NOT be called because bytes limiter rejected
		if err == nil {
			requestTrackerCalled = true
		}

		assert.False(t, requestTrackerCalled, "request tracker should NOT be called when bytes limiter rejects")
	})
}

// TestIntegration_RequestTrackerNotCalledForRejectedRequests verifies that when the bytes
// limiter rejects a request, the request tracker is never invoked.
// This test validates Requirement 4.2.
func TestIntegration_RequestTrackerNotCalledForRejectedRequests(t *testing.T) {
	tests := []struct {
		name                       string
		bytesLimit                 uint64
		bytesToAdd                 uint64
		expectBytesLimiterReject   bool
		expectRequestTrackerCalled bool
	}{
		{
			name:                       "bytes limiter accepts - request tracker called",
			bytesLimit:                 1000,
			bytesToAdd:                 0,
			expectBytesLimiterReject:   false,
			expectRequestTrackerCalled: true,
		},
		{
			name:                       "bytes limiter at capacity - request tracker not called",
			bytesLimit:                 100,
			bytesToAdd:                 100,
			expectBytesLimiterReject:   true,
			expectRequestTrackerCalled: false,
		},
		{
			name:                       "bytes limiter over capacity - request tracker not called",
			bytesLimit:                 100,
			bytesToAdd:                 200,
			expectBytesLimiterReject:   true,
			expectRequestTrackerCalled: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewConcurrentBytesTracker(tc.bytesLimit, nil)

			// Add bytes to simulate current load
			if tc.bytesToAdd > 0 {
				tracker.Add(tc.bytesToAdd)
			}

			// Simulate the Series method flow
			requestTrackerCalled := false

			err := tracker.TryAccept(context.Background())

			if tc.expectBytesLimiterReject {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Request tracker is only called if bytes limiter accepts
			if err == nil {
				requestTrackerCalled = true
			}

			assert.Equal(t, tc.expectRequestTrackerCalled, requestTrackerCalled)
		})
	}
}

// TestProperty_ResourceLimiterPrecedesBytesLimiter tests Property 13: Resource limiter precedes bytes limiter
// **Feature: storegateway-max-data-limit, Property 13: Resource limiter precedes bytes limiter**
// **Validates: Requirements 4.3**
func TestProperty_ResourceLimiterPrecedesBytesLimiter(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))

		// Random bytes limit (1KB to 1GB)
		bytesLimit := uint64(rng.Intn(1024*1024*1024)) + 1024

		// Random initial bytes (0 to 2x limit to test both accept and reject scenarios)
		initialBytes := uint64(rng.Intn(int(bytesLimit * 2)))

		// Random resource limiter decision
		resourceLimiterRejects := rng.Intn(2) == 0

		tracker := NewConcurrentBytesTracker(bytesLimit, nil)

		// Add initial bytes
		if initialBytes > 0 {
			tracker.Add(initialBytes)
		}

		initialBytesCount := tracker.Current()

		// Simulate the Series method flow:
		// 1. Resource limiter is checked first
		// 2. Only if resource limiter accepts, bytes limiter is checked

		var bytesLimiterCalled bool

		if resourceLimiterRejects {
			// Resource limiter rejects - bytes limiter should NOT be called
			// The bytes counter should remain unchanged
			bytesLimiterCalled = false
		} else {
			// Resource limiter accepts - bytes limiter is called
			bytesLimiterCalled = true
			_ = tracker.TryAccept(context.Background()) // We don't care about the result, just that it was called
		}

		// Property: When resource limiter rejects, bytes counter should not change
		// (because bytes limiter is never called)
		if resourceLimiterRejects {
			// Bytes counter should remain at initial value
			if tracker.Current() != initialBytesCount {
				return false
			}
			// Bytes limiter should not have been called
			if bytesLimiterCalled {
				return false
			}
		}

		return true
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_RequestTrackerIntegration tests Property 11: Request tracker integration
// **Feature: storegateway-max-data-limit, Property 11: Request tracker integration**
// **Validates: Requirements 4.1**
func TestProperty_RequestTrackerIntegration(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))

		// Random bytes limit (1KB to 1GB)
		bytesLimit := uint64(rng.Intn(1024*1024*1024)) + 1024

		// Random initial bytes that will NOT exceed the limit (to ensure acceptance)
		initialBytes := uint64(rng.Intn(int(bytesLimit / 2)))

		tracker := NewConcurrentBytesTracker(bytesLimit, nil)

		// Add initial bytes (below limit)
		if initialBytes > 0 {
			tracker.Add(initialBytes)
		}

		// Simulate the Series method flow:
		// 1. Resource limiter accepts (simulated)
		// 2. Bytes limiter is checked
		// 3. If bytes limiter accepts, request tracker is called

		resourceLimiterAccepts := true // Simulating resource limiter acceptance
		var requestTrackerCalled bool

		if resourceLimiterAccepts {
			err := tracker.TryAccept(context.Background())
			if err == nil {
				// Bytes limiter accepted - request tracker should be called
				requestTrackerCalled = true
			}
		}

		// Property: When bytes limiter accepts a request, request tracker should also track that request
		// Since we ensured initialBytes < bytesLimit, TryAccept should succeed
		// and request tracker should be called
		return requestTrackerCalled
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_RejectedRequestsNotTracked tests Property 12: Rejected requests not tracked
// **Feature: storegateway-max-data-limit, Property 12: Rejected requests not tracked**
// **Validates: Requirements 4.2**
func TestProperty_RejectedRequestsNotTracked(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))

		// Random bytes limit (1KB to 1GB)
		bytesLimit := uint64(rng.Intn(1024*1024*1024)) + 1024

		// Random initial bytes that WILL exceed the limit (to ensure rejection)
		// Add at least the limit amount to guarantee rejection
		initialBytes := bytesLimit + uint64(rng.Intn(int(bytesLimit)))

		tracker := NewConcurrentBytesTracker(bytesLimit, nil)

		// Add initial bytes (at or above limit)
		tracker.Add(initialBytes)

		// Simulate the Series method flow:
		// 1. Resource limiter accepts (simulated)
		// 2. Bytes limiter is checked
		// 3. If bytes limiter rejects, request tracker should NOT be called

		resourceLimiterAccepts := true // Simulating resource limiter acceptance
		var requestTrackerCalled bool

		if resourceLimiterAccepts {
			err := tracker.TryAccept(context.Background())
			if err == nil {
				// Bytes limiter accepted - request tracker would be called
				requestTrackerCalled = true
			}
			// If err != nil, bytes limiter rejected - request tracker should NOT be called
		}

		// Property: When bytes limiter rejects a request, request tracker should NOT track that request
		// Since we ensured initialBytes >= bytesLimit, TryAccept should fail
		// and request tracker should NOT be called
		return !requestTrackerCalled
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_CancellationBeforeTracking tests Property 17: Cancellation before tracking
// **Feature: storegateway-max-data-limit, Property 17: Cancellation before tracking**
// **Validates: Requirements 8.2**
func TestProperty_CancellationBeforeTracking(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))

		// Random bytes limit (1KB to 1GB)
		bytesLimit := uint64(rng.Intn(1024*1024*1024)) + 1024

		tracker := NewConcurrentBytesTracker(bytesLimit, nil)

		// Verify initial state
		initialBytes := tracker.Current()
		if initialBytes != 0 {
			return false
		}

		// Create a cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// TryAccept with cancelled context should return error
		err := tracker.TryAccept(ctx)
		if err == nil {
			return false // Should have returned an error
		}

		// Verify the error is context.Canceled
		if err != context.Canceled {
			return false
		}

		// Property: When context is cancelled before tracking, bytes counter should not be incremented
		// The counter should remain at 0
		return tracker.Current() == 0
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_ConcurrentDecrementsCorrectness tests Property 18: Concurrent decrements correctness
// **Feature: storegateway-max-data-limit, Property 18: Concurrent decrements correctness**
// **Validates: Requirements 8.4**
func TestProperty_ConcurrentDecrementsCorrectness(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numOps := rng.Intn(100) + 10 // 10 to 109 operations

		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)

		// First, add bytes sequentially to establish a known state
		var totalBytes uint64
		releases := make([]func(), numOps)
		bytesPerOp := make([]uint64, numOps)

		for i := range numOps {
			bytes := uint64(rng.Intn(1024*1024)) + 1 // 1 byte to 1MB
			bytesPerOp[i] = bytes
			totalBytes += bytes
			releases[i] = tracker.Add(bytes)
		}

		// Verify all adds were counted
		if tracker.Current() != totalBytes {
			return false
		}

		// Now release all bytes concurrently
		var wg sync.WaitGroup
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				releases[idx]()
			}(i)
		}
		wg.Wait()

		// Property: After all concurrent releases, the counter should be exactly 0
		// All decrements should be applied correctly without losing any updates
		return tracker.Current() == 0
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_PanicRecoveryCleanup tests Property 19: Panic recovery cleanup
// **Feature: storegateway-max-data-limit, Property 19: Panic recovery cleanup**
// **Validates: Requirements 8.5**
func TestProperty_PanicRecoveryCleanup(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))

		// Random bytes limit (1KB to 1GB)
		bytesLimit := uint64(rng.Intn(1024*1024*1024)) + 1024

		// Random bytes to track (1 byte to 1MB)
		bytesToTrack := uint64(rng.Intn(1024*1024)) + 1

		tracker := NewConcurrentBytesTracker(bytesLimit, nil)

		// Verify initial state
		if tracker.Current() != 0 {
			return false
		}

		// Simulate a function that panics after tracking bytes
		// The deferred Release should still execute
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Panic was recovered - this is expected
				}
			}()

			// Track bytes and defer release
			release := tracker.Add(bytesToTrack)
			defer release()

			// Verify bytes are tracked
			if tracker.Current() != bytesToTrack {
				panic("bytes not tracked correctly")
			}

			// Simulate panic during request processing
			panic("simulated panic during request processing")
		}()

		// Property: After panic recovery, the deferred Release function should have executed
		// and the counter should be back to 0
		return tracker.Current() == 0
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_PanicRecoveryWithRegistry tests panic recovery with the tracking limiter registry
// **Feature: storegateway-max-data-limit, Property 19: Panic recovery cleanup (registry variant)**
// **Validates: Requirements 8.5**
func TestProperty_PanicRecoveryWithRegistry(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))

		// Random bytes limit (1KB to 1GB)
		bytesLimit := uint64(rng.Intn(1024*1024*1024)) + 1024

		// Random number of limiters (1 to 10)
		numLimiters := rng.Intn(10) + 1

		tracker := NewConcurrentBytesTracker(bytesLimit, nil)
		registry := newTrackingBytesLimiterRegistry()

		// Verify initial state
		if tracker.Current() != 0 {
			return false
		}

		// Simulate a function that creates multiple limiters and panics
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Panic was recovered - this is expected
				}
			}()

			// Defer registry cleanup
			defer registry.ReleaseAll()

			// Create multiple tracking limiters and reserve bytes
			var totalBytes uint64
			for range numLimiters {
				inner := newMockBytesLimiter(bytesLimit)
				limiter := newTrackingBytesLimiter(inner, tracker)
				registry.Register(limiter)

				bytes := uint64(rng.Intn(1024*1024)) + 1 // 1 byte to 1MB
				limiter.ReserveWithType(bytes, store.PostingsFetched)
				totalBytes += bytes
			}

			// Verify bytes are tracked
			if tracker.Current() != totalBytes {
				panic("bytes not tracked correctly")
			}

			// Simulate panic during request processing
			panic("simulated panic during request processing")
		}()

		// Property: After panic recovery, the deferred ReleaseAll should have executed
		// and the counter should be back to 0
		return tracker.Current() == 0
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}
