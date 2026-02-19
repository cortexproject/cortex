package storegateway

import (
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store"
)

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

		require.NoError(t, limiter.ReserveWithType(100, store.PostingsFetched))
		assert.Equal(t, uint64(100), inner.Reserved())
	})

	t.Run("tracks bytes in concurrent tracker", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		require.NoError(t, limiter.ReserveWithType(100, store.PostingsFetched))
		assert.Equal(t, uint64(100), tracker.Current())
		assert.Equal(t, uint64(100), limiter.TrackedBytes())
	})

	t.Run("release decrements tracker", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		require.NoError(t, limiter.ReserveWithType(100, store.PostingsFetched))
		limiter.Release()
		assert.Equal(t, uint64(0), tracker.Current())
		assert.Equal(t, uint64(0), limiter.TrackedBytes())
	})

	t.Run("multiple reserves accumulate", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		require.NoError(t, limiter.ReserveWithType(100, store.PostingsFetched))
		require.NoError(t, limiter.ReserveWithType(200, store.SeriesFetched))
		require.NoError(t, limiter.ReserveWithType(300, store.ChunksFetched))
		assert.Equal(t, uint64(600), tracker.Current())
		assert.Equal(t, uint64(600), limiter.TrackedBytes())
	})
}

func TestTrackingBytesLimiter_ReleaseIdempotent(t *testing.T) {
	inner := newMockBytesLimiter(1000)
	tracker := NewConcurrentBytesTracker(10000, nil)
	limiter := newTrackingBytesLimiter(inner, tracker)

	require.NoError(t, limiter.ReserveWithType(100, store.PostingsFetched))
	limiter.Release()
	assert.Equal(t, uint64(0), tracker.Current())

	limiter.Release()
	assert.Equal(t, uint64(0), tracker.Current())
}

func TestTrackingBytesLimiterRegistry(t *testing.T) {
	t.Run("releases all limiters", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		registry := newTrackingBytesLimiterRegistry()

		limiter1 := newTrackingBytesLimiter(newMockBytesLimiter(1000), tracker)
		limiter2 := newTrackingBytesLimiter(newMockBytesLimiter(1000), tracker)
		limiter3 := newTrackingBytesLimiter(newMockBytesLimiter(1000), tracker)
		registry.Register(limiter1)
		registry.Register(limiter2)
		registry.Register(limiter3)

		limiter1.ReserveWithType(100, store.PostingsFetched)
		limiter2.ReserveWithType(200, store.SeriesFetched)
		limiter3.ReserveWithType(300, store.ChunksFetched)
		assert.Equal(t, uint64(600), tracker.Current())

		registry.ReleaseAll()
		assert.Equal(t, uint64(0), tracker.Current())
	})

	t.Run("handles panic recovery", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		registry := newTrackingBytesLimiterRegistry()

		func() {
			defer func() { recover() }()
			defer registry.ReleaseAll()

			limiter := newTrackingBytesLimiter(newMockBytesLimiter(1000), tracker)
			registry.Register(limiter)
			limiter.ReserveWithType(100, store.PostingsFetched)

			panic("simulated panic")
		}()

		assert.Equal(t, uint64(0), tracker.Current())
	})
}

func TestProperty_BytesLimiterIntegration(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numReserves := rng.Intn(50) + 1

		inner := newMockBytesLimiter(uint64(100) * 1024 * 1024 * 1024)
		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)
		limiter := newTrackingBytesLimiter(inner, tracker)

		var totalBytes uint64
		for range numReserves {
			bytes := uint64(rng.Intn(1024*1024)) + 1
			if err := limiter.ReserveWithType(bytes, store.StoreDataType(rng.Intn(6))); err != nil {
				return false
			}
			totalBytes += bytes
		}

		if tracker.Current() != totalBytes || inner.Reserved() != totalBytes || limiter.TrackedBytes() != totalBytes {
			return false
		}

		limiter.Release()
		return tracker.Current() == 0 && limiter.TrackedBytes() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestPropertyReserveWithTypePropagatesAddError(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		limit := uint64(rng.Intn(10*1024*1024)) + 1

		tracker := NewConcurrentBytesTracker(limit, nil)
		inner := newMockBytesLimiter(^uint64(0))
		limiter := newTrackingBytesLimiter(inner, tracker)

		numReserves := rng.Intn(20) + 1
		var trackedBytes uint64

		for range numReserves {
			bytes := uint64(rng.Intn(2*1024*1024)) + 1
			wouldExceed := trackedBytes+bytes > limit

			err := limiter.ReserveWithType(bytes, store.StoreDataType(rng.Intn(6)))

			if wouldExceed && err == nil {
				return false
			}
			if !wouldExceed && err != nil {
				return false
			}
			if err == nil {
				trackedBytes += bytes
			}
		}
		return true
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestPropertyReleaseWorksCorrectlyAfterAddError(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		limit := uint64(rng.Intn(1024)) + 1

		tracker := NewConcurrentBytesTracker(limit, nil)
		inner := newMockBytesLimiter(^uint64(0))
		limiter := newTrackingBytesLimiter(inner, tracker)

		numReserves := rng.Intn(10) + 1
		sawError := false

		for range numReserves {
			bytes := uint64(rng.Intn(int(limit))) + 1
			if limiter.ReserveWithType(bytes, store.StoreDataType(rng.Intn(6))) != nil {
				sawError = true
			}
		}

		if !sawError {
			if limiter.ReserveWithType(limit+1, store.PostingsFetched) == nil {
				return false
			}
		}

		limiter.Release()
		return tracker.Current() == 0 && limiter.TrackedBytes() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}
