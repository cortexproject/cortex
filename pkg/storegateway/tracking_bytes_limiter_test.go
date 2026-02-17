package storegateway

import (
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store"
)

// mockBytesLimiter is a mock implementation of store.BytesLimiter for testing
type mockBytesLimiter struct {
	reservedBytes uint64
	limit         uint64
}

func newMockBytesLimiter(limit uint64) *mockBytesLimiter {
	return &mockBytesLimiter{limit: limit}
}

func (m *mockBytesLimiter) ReserveWithType(num uint64, _ store.StoreDataType) error {
	m.reservedBytes += num
	return nil
}

func (m *mockBytesLimiter) Reserved() uint64 {
	return m.reservedBytes
}

func TestTrackingBytesLimiter_Basic(t *testing.T) {
	t.Run("reserves bytes through inner limiter", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		err := limiter.ReserveWithType(100, store.PostingsFetched)
		require.NoError(t, err)

		assert.Equal(t, uint64(100), inner.Reserved())
	})

	t.Run("tracks bytes in concurrent tracker", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		err := limiter.ReserveWithType(100, store.PostingsFetched)
		require.NoError(t, err)

		assert.Equal(t, uint64(100), tracker.Current())
		assert.Equal(t, uint64(100), limiter.TrackedBytes())
	})

	t.Run("release decrements tracker", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		err := limiter.ReserveWithType(100, store.PostingsFetched)
		require.NoError(t, err)

		limiter.Release()

		assert.Equal(t, uint64(0), tracker.Current())
		assert.Equal(t, uint64(0), limiter.TrackedBytes())
	})

	t.Run("multiple reserves accumulate", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		err := limiter.ReserveWithType(100, store.PostingsFetched)
		require.NoError(t, err)
		err = limiter.ReserveWithType(200, store.SeriesFetched)
		require.NoError(t, err)
		err = limiter.ReserveWithType(300, store.ChunksFetched)
		require.NoError(t, err)

		assert.Equal(t, uint64(600), tracker.Current())
		assert.Equal(t, uint64(600), limiter.TrackedBytes())
	})
}

// TestProperty_BytesLimiterIntegration tests Property 15: BytesLimiter integration
// **Feature: storegateway-max-data-limit, Property 15: BytesLimiter integration**
// **Validates: Requirements 5.2**
func TestProperty_BytesLimiterIntegration(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numReserves := rng.Intn(50) + 1 // 1 to 50 reserves

		inner := newMockBytesLimiter(uint64(100) * 1024 * 1024 * 1024) // 100GB limit
		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		var totalBytes uint64

		// Reserve random amounts of bytes
		for range numReserves {
			bytes := uint64(rng.Intn(1024*1024)) + 1 // 1 byte to 1MB
			err := limiter.ReserveWithType(bytes, store.StoreDataType(rng.Intn(6)))
			if err != nil {
				return false
			}
			totalBytes += bytes
		}

		// Verify concurrent tracker was updated with the same total amount
		if tracker.Current() != totalBytes {
			return false
		}

		// Verify inner limiter received the same total amount
		if inner.Reserved() != totalBytes {
			return false
		}

		// Verify tracked bytes matches
		if limiter.TrackedBytes() != totalBytes {
			return false
		}

		// Release and verify cleanup
		limiter.Release()
		return tracker.Current() == 0 && limiter.TrackedBytes() == 0
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

// TestProperty_AllDataTypesTracked tests Property 16: All data types tracked
// **Feature: storegateway-max-data-limit, Property 16: All data types tracked**
// **Validates: Requirements 5.3**
func TestProperty_AllDataTypesTracked(t *testing.T) {
	config := &quick.Config{
		MaxCount: 100,
	}

	// All StoreDataType values from Thanos
	dataTypes := []store.StoreDataType{
		store.PostingsFetched,
		store.PostingsTouched,
		store.SeriesFetched,
		store.SeriesTouched,
		store.ChunksFetched,
		store.ChunksTouched,
	}

	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))

		inner := newMockBytesLimiter(uint64(100) * 1024 * 1024 * 1024)
		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		var totalBytes uint64

		// Reserve bytes for each data type
		for _, dataType := range dataTypes {
			bytes := uint64(rng.Intn(1024*1024)) + 1 // 1 byte to 1MB
			err := limiter.ReserveWithType(bytes, dataType)
			if err != nil {
				return false
			}
			totalBytes += bytes
		}

		// Verify all data types were tracked consistently
		if tracker.Current() != totalBytes {
			return false
		}

		if limiter.TrackedBytes() != totalBytes {
			return false
		}

		// Release and verify cleanup
		limiter.Release()
		return tracker.Current() == 0
	}

	if err := quick.Check(f, config); err != nil {
		t.Error(err)
	}
}

func TestTrackingBytesLimiter_ReleaseIdempotent(t *testing.T) {
	inner := newMockBytesLimiter(1000)
	tracker := NewConcurrentBytesTracker(10000, nil)
	limiter := newTrackingBytesLimiter(inner, tracker)

	err := limiter.ReserveWithType(100, store.PostingsFetched)
	require.NoError(t, err)

	// First release
	limiter.Release()
	assert.Equal(t, uint64(0), tracker.Current())

	// Second release should be safe (idempotent)
	limiter.Release()
	assert.Equal(t, uint64(0), tracker.Current())
}

func TestTrackingBytesLimiter_WithLimitingDisabled(t *testing.T) {
	inner := newMockBytesLimiter(1000)
	tracker := NewConcurrentBytesTracker(0, nil) // 0 limit = limiting disabled but tracking enabled
	limiter := newTrackingBytesLimiter(inner, tracker)

	err := limiter.ReserveWithType(100, store.PostingsFetched)
	require.NoError(t, err)

	// Inner limiter should still work
	assert.Equal(t, uint64(100), inner.Reserved())

	// Tracker should track bytes even when limiting is disabled
	assert.Equal(t, uint64(100), tracker.Current())

	// Release should work correctly
	limiter.Release()
	assert.Equal(t, uint64(0), tracker.Current())
}

func TestTrackingBytesLimiter_PanicRecovery(t *testing.T) {
	t.Run("defer release cleans up on panic with recover", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		// Use a wrapper function to recover from panic
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Panic was recovered, verify cleanup happened
					assert.Equal(t, uint64(0), tracker.Current(), "bytes should be released after panic recovery")
				}
			}()

			defer limiter.Release()

			err := limiter.ReserveWithType(100, store.PostingsFetched)
			require.NoError(t, err)

			// Verify bytes are tracked before panic
			assert.Equal(t, uint64(100), tracker.Current())

			// Simulate panic
			panic("simulated panic")
		}()

		// Verify cleanup happened
		assert.Equal(t, uint64(0), tracker.Current())
	})
}

func TestTrackingBytesLimiterRegistry(t *testing.T) {
	t.Run("registry releases all limiters", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		registry := newTrackingBytesLimiterRegistry()

		// Create multiple limiters and register them
		limiter1 := newTrackingBytesLimiter(newMockBytesLimiter(1000), tracker)
		limiter2 := newTrackingBytesLimiter(newMockBytesLimiter(1000), tracker)
		limiter3 := newTrackingBytesLimiter(newMockBytesLimiter(1000), tracker)

		registry.Register(limiter1)
		registry.Register(limiter2)
		registry.Register(limiter3)

		// Reserve bytes through each limiter
		limiter1.ReserveWithType(100, store.PostingsFetched)
		limiter2.ReserveWithType(200, store.SeriesFetched)
		limiter3.ReserveWithType(300, store.ChunksFetched)

		// Verify total bytes tracked
		assert.Equal(t, uint64(600), tracker.Current())

		// Release all through registry
		registry.ReleaseAll()

		// Verify all bytes released
		assert.Equal(t, uint64(0), tracker.Current())
	})

	t.Run("registry handles panic recovery", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		registry := newTrackingBytesLimiterRegistry()

		func() {
			defer func() {
				if r := recover(); r != nil {
					// Panic was recovered
				}
			}()

			defer registry.ReleaseAll()

			limiter := newTrackingBytesLimiter(newMockBytesLimiter(1000), tracker)
			registry.Register(limiter)

			limiter.ReserveWithType(100, store.PostingsFetched)
			assert.Equal(t, uint64(100), tracker.Current())

			panic("simulated panic")
		}()

		// Verify cleanup happened
		assert.Equal(t, uint64(0), tracker.Current())
	})
}
