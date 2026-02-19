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

func TestRequestBytesTracker_Basic(t *testing.T) {
	t.Run("add delegates to underlying tracker", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		reqTracker := newRequestBytesTracker(tracker)

		require.NoError(t, reqTracker.Add(100))
		assert.Equal(t, uint64(100), tracker.Current())
		assert.Equal(t, uint64(100), reqTracker.Total())
	})

	t.Run("multiple adds accumulate", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		reqTracker := newRequestBytesTracker(tracker)

		require.NoError(t, reqTracker.Add(100))
		require.NoError(t, reqTracker.Add(200))
		require.NoError(t, reqTracker.Add(300))
		assert.Equal(t, uint64(600), tracker.Current())
		assert.Equal(t, uint64(600), reqTracker.Total())
	})

	t.Run("release all decrements tracker", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		reqTracker := newRequestBytesTracker(tracker)

		require.NoError(t, reqTracker.Add(100))
		require.NoError(t, reqTracker.Add(200))
		reqTracker.ReleaseAll()
		assert.Equal(t, uint64(0), tracker.Current())
	})

	t.Run("release all is idempotent", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		reqTracker := newRequestBytesTracker(tracker)

		require.NoError(t, reqTracker.Add(100))
		reqTracker.ReleaseAll()
		assert.Equal(t, uint64(0), tracker.Current())

		// Second call should be a no-op.
		reqTracker.ReleaseAll()
		assert.Equal(t, uint64(0), tracker.Current())
	})

	t.Run("propagates add error from tracker", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(50, nil)
		reqTracker := newRequestBytesTracker(tracker)

		require.NoError(t, reqTracker.Add(30))
		require.Error(t, reqTracker.Add(30)) // would exceed 50
		assert.Equal(t, uint64(30), tracker.Current())
		assert.Equal(t, uint64(30), reqTracker.Total())
	})
}

func TestTrackingBytesLimiter_Basic(t *testing.T) {
	t.Run("reserves bytes through inner limiter", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		reqTracker := newRequestBytesTracker(tracker)
		limiter := newTrackingBytesLimiter(inner, reqTracker)

		require.NoError(t, limiter.ReserveWithType(100, store.PostingsFetched))
		assert.Equal(t, uint64(100), inner.Reserved())
	})

	t.Run("tracks bytes in request tracker", func(t *testing.T) {
		inner := newMockBytesLimiter(1000)
		tracker := NewConcurrentBytesTracker(10000, nil)
		reqTracker := newRequestBytesTracker(tracker)
		limiter := newTrackingBytesLimiter(inner, reqTracker)

		require.NoError(t, limiter.ReserveWithType(100, store.PostingsFetched))
		assert.Equal(t, uint64(100), tracker.Current())
		assert.Equal(t, uint64(100), reqTracker.Total())
	})

	t.Run("multiple limiters share request tracker", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(10000, nil)
		reqTracker := newRequestBytesTracker(tracker)

		limiter1 := newTrackingBytesLimiter(newMockBytesLimiter(1000), reqTracker)
		limiter2 := newTrackingBytesLimiter(newMockBytesLimiter(1000), reqTracker)
		limiter3 := newTrackingBytesLimiter(newMockBytesLimiter(1000), reqTracker)

		require.NoError(t, limiter1.ReserveWithType(100, store.PostingsFetched))
		require.NoError(t, limiter2.ReserveWithType(200, store.SeriesFetched))
		require.NoError(t, limiter3.ReserveWithType(300, store.ChunksFetched))
		assert.Equal(t, uint64(600), tracker.Current())
		assert.Equal(t, uint64(600), reqTracker.Total())

		reqTracker.ReleaseAll()
		assert.Equal(t, uint64(0), tracker.Current())
	})
}

func TestRequestBytesTracker_PanicRecovery(t *testing.T) {
	tracker := NewConcurrentBytesTracker(10000, nil)

	func() {
		reqTracker := newRequestBytesTracker(tracker)
		defer func() { recover() }()
		defer reqTracker.ReleaseAll()

		require.NoError(t, reqTracker.Add(100))
		panic("simulated panic")
	}()

	assert.Equal(t, uint64(0), tracker.Current())
}

func TestProperty_BytesLimiterIntegration(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numReserves := rng.Intn(50) + 1

		inner := newMockBytesLimiter(uint64(100) * 1024 * 1024 * 1024)
		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)
		reqTracker := newRequestBytesTracker(tracker)
		limiter := newTrackingBytesLimiter(inner, reqTracker)

		var totalBytes uint64
		for range numReserves {
			bytes := uint64(rng.Intn(1024*1024)) + 1
			if err := limiter.ReserveWithType(bytes, store.StoreDataType(rng.Intn(6))); err != nil {
				return false
			}
			totalBytes += bytes
		}

		if tracker.Current() != totalBytes || inner.Reserved() != totalBytes || reqTracker.Total() != totalBytes {
			return false
		}

		reqTracker.ReleaseAll()
		return tracker.Current() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestPropertyReserveWithTypePropagatesAddError(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		limit := uint64(rng.Intn(10*1024*1024)) + 1

		tracker := NewConcurrentBytesTracker(limit, nil)
		reqTracker := newRequestBytesTracker(tracker)
		inner := newMockBytesLimiter(^uint64(0))
		limiter := newTrackingBytesLimiter(inner, reqTracker)

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
		reqTracker := newRequestBytesTracker(tracker)
		inner := newMockBytesLimiter(^uint64(0))
		limiter := newTrackingBytesLimiter(inner, reqTracker)

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

		reqTracker.ReleaseAll()
		return tracker.Current() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}
