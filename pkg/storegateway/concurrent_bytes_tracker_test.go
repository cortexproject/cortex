package storegateway

import (
	"math/rand"
	"sync"
	"testing"
	"testing/quick"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/store"
)

func TestConcurrentBytesTracker_Basic(t *testing.T) {
	t.Run("add increments counter", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(1000, nil)
		assert.Equal(t, uint64(0), tracker.Current())

		require.NoError(t, tracker.Add(100))
		assert.Equal(t, uint64(100), tracker.Current())

		tracker.Release(100)
		assert.Equal(t, uint64(0), tracker.Current())
	})

	t.Run("add rejects when would exceed limit", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(100, nil)
		require.NoError(t, tracker.Add(100))

		err := tracker.Add(1)
		assert.ErrorIs(t, err, pool.ErrPoolExhausted)
		assert.Equal(t, uint64(100), tracker.Current())
	})

	t.Run("add allows when at exactly limit", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(100, nil)
		require.NoError(t, tracker.Add(50))
		require.NoError(t, tracker.Add(50))
		assert.Equal(t, uint64(100), tracker.Current())
	})
}

func TestTrackerWithLimitingDisabled(t *testing.T) {
	t.Run("add always succeeds", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(0, nil)
		require.NoError(t, tracker.Add(1000))
		assert.Equal(t, uint64(1000), tracker.Current())
		tracker.Release(1000)
		assert.Equal(t, uint64(0), tracker.Current())
	})

	t.Run("add succeeds even with very high byte count", func(t *testing.T) {
		tracker := NewConcurrentBytesTracker(0, nil)
		assert.NoError(t, tracker.Add(uint64(100)*1024*1024*1024))
	})
}

func TestConcurrentBytesTracker_Metrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	tracker := NewConcurrentBytesTracker(1000, reg)
	require.NoError(t, tracker.Add(500))

	metricFamilies, err := reg.Gather()
	require.NoError(t, err)

	metricNames := make(map[string]bool)
	for _, mf := range metricFamilies {
		metricNames[mf.GetName()] = true
	}

	assert.True(t, metricNames["cortex_storegateway_concurrent_bytes_peak"])
	assert.True(t, metricNames["cortex_storegateway_concurrent_bytes_max"])
	assert.True(t, metricNames["cortex_storegateway_bytes_limiter_rejected_requests_total"])

	tracker.Release(500)
}

func TestProperty_AddIncrementsCounter(t *testing.T) {
	f := func(bytes uint64) bool {
		bytes = (bytes % (1024 * 1024 * 1024)) + 1
		tracker := NewConcurrentBytesTracker(uint64(10)*1024*1024*1024, nil)

		err := tracker.Add(bytes)
		if err != nil {
			return false
		}
		return tracker.Current() == bytes
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_ReleaseRoundTrip(t *testing.T) {
	f := func(bytes uint64) bool {
		bytes = (bytes % (1024 * 1024 * 1024)) + 1
		tracker := NewConcurrentBytesTracker(uint64(10)*1024*1024*1024, nil)

		_ = tracker.Add(bytes)
		tracker.Release(bytes)
		return tracker.Current() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_ThreadSafeCounterUpdates(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numOps := rng.Intn(100) + 1
		bytesPerOp := uint64(rng.Intn(1024*1024)) + 1

		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)

		var wg sync.WaitGroup
		for range numOps {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = tracker.Add(bytesPerOp)
			}()
		}
		wg.Wait()

		if tracker.Current() != uint64(numOps)*bytesPerOp {
			return false
		}

		for range numOps {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tracker.Release(bytesPerOp)
			}()
		}
		wg.Wait()

		return tracker.Current() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_PeakBytesMetricTracksPeak(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numOps := rng.Intn(50) + 1

		reg := prometheus.NewRegistry()
		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, reg).(*concurrentBytesTracker)

		var totalBytes uint64
		var maxSeen uint64

		for range numOps {
			bytes := uint64(rng.Intn(1024*1024)) + 1
			_ = tracker.Add(bytes)
			totalBytes += bytes
			if current := tracker.Current(); current > maxSeen {
				maxSeen = current
			}
		}

		if tracker.peakBytes.Load() < maxSeen {
			return false
		}

		tracker.Release(totalBytes)
		return tracker.Current() == 0 && tracker.peakBytes.Load() >= maxSeen
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_PositiveLimitEnforcement(t *testing.T) {
	f := func(limit uint64, bytesToAdd uint64) bool {
		limit = (limit % (10 * 1024 * 1024 * 1024)) + 1
		bytesToAdd = limit + (bytesToAdd % (1024 * 1024 * 1024)) + 1

		tracker := NewConcurrentBytesTracker(limit, nil)
		err := tracker.Add(bytesToAdd)
		return err != nil
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_BelowLimitAccepts(t *testing.T) {
	f := func(limit uint64, bytesToAdd uint64) bool {
		limit = (limit % (10 * 1024 * 1024 * 1024)) + 1024
		bytesToAdd = bytesToAdd % (limit + 1)

		tracker := NewConcurrentBytesTracker(limit, nil)
		return tracker.Add(bytesToAdd) == nil
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_RejectionDoesNotCountBytes(t *testing.T) {
	f := func(limit uint64, numRejections uint8) bool {
		limit = (limit % (1024 * 1024 * 1024)) + 1024
		numRejections = (numRejections % 10) + 1

		tracker := NewConcurrentBytesTracker(limit, nil)
		if err := tracker.Add(limit); err != nil {
			return false
		}

		for i := uint8(0); i < numRejections; i++ {
			if tracker.Add(1) == nil {
				return false
			}
		}

		return tracker.Current() == limit
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_RejectionReturnsPoolExhausted(t *testing.T) {
	f := func(limit uint64, bytesToAdd uint64) bool {
		limit = (limit % (1024 * 1024 * 1024)) + 1
		bytesToAdd = limit + (bytesToAdd % (1024 * 1024 * 1024)) + 1

		tracker := NewConcurrentBytesTracker(limit, nil)
		return tracker.Add(bytesToAdd) == pool.ErrPoolExhausted
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_RecoveryAfterRelease(t *testing.T) {
	f := func(limit uint64, bytesToAdd uint64) bool {
		limit = (limit % (1024 * 1024 * 1024)) + 1024
		bytesToAdd = (bytesToAdd % limit) + 1

		tracker := NewConcurrentBytesTracker(limit, nil)
		if err := tracker.Add(limit); err != nil {
			return false
		}
		if tracker.Add(1) == nil {
			return false
		}

		tracker.Release(limit)
		return tracker.Add(bytesToAdd) == nil
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_ConcurrentDecrementsCorrectness(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numOps := rng.Intn(100) + 10

		tracker := NewConcurrentBytesTracker(uint64(100)*1024*1024*1024, nil)

		var totalBytes uint64
		bytesPerOp := make([]uint64, numOps)
		for i := range numOps {
			bytes := uint64(rng.Intn(1024*1024)) + 1
			bytesPerOp[i] = bytes
			totalBytes += bytes
			_ = tracker.Add(bytes)
		}

		if tracker.Current() != totalBytes {
			return false
		}

		var wg sync.WaitGroup
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				tracker.Release(bytesPerOp[idx])
			}(i)
		}
		wg.Wait()

		return tracker.Current() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_PanicRecoveryCleanup(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		bytesLimit := uint64(rng.Intn(1024*1024*1024)) + 1024
		bytesToTrack := uint64(rng.Intn(1024*1024)) + 1

		tracker := NewConcurrentBytesTracker(bytesLimit, nil)

		func() {
			defer func() { recover() }()

			if err := tracker.Add(bytesToTrack); err != nil {
				return
			}
			defer tracker.Release(bytesToTrack)

			panic("simulated panic")
		}()

		return tracker.Current() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestProperty_PanicRecoveryWithRequestTracker(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		bytesLimit := uint64(rng.Intn(1024*1024*1024)) + 1024
		numLimiters := rng.Intn(10) + 1

		tracker := NewConcurrentBytesTracker(bytesLimit, nil)
		reqTracker := newRequestBytesTracker(tracker)

		func() {
			defer func() { recover() }()
			defer reqTracker.ReleaseAll()

			for range numLimiters {
				inner := newMockBytesLimiter(bytesLimit)
				limiter := newTrackingBytesLimiter(inner, reqTracker)

				bytes := uint64(rng.Intn(1024*1024)) + 1
				limiter.ReserveWithType(bytes, store.PostingsFetched)
			}

			panic("simulated panic")
		}()

		return tracker.Current() == 0
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestPropertyAddReturnsErrPoolExhaustedIffOverLimit(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		limit := uint64(rng.Intn(10*1024*1024)) + 1

		tracker := NewConcurrentBytesTracker(limit, nil)
		defer tracker.Stop()

		numAdds := rng.Intn(20) + 1
		var cumulativeBytes uint64

		for i := 0; i < numAdds; i++ {
			bytes := uint64(rng.Intn(int(limit))) + 1
			err := tracker.Add(bytes)

			if cumulativeBytes+bytes > limit {
				if err != pool.ErrPoolExhausted {
					return false
				}
			} else {
				if err != nil {
					return false
				}
				cumulativeBytes += bytes
			}
		}

		return tracker.Current() == cumulativeBytes
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}

func TestPropertyAddReturnsNilErrorWhenLimitingDisabled(t *testing.T) {
	f := func(bytes uint64) bool {
		bytes = (bytes % (1024 * 1024 * 1024)) + 1
		tracker := NewConcurrentBytesTracker(0, nil)
		defer tracker.Stop()
		return tracker.Add(bytes) == nil
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 100}))
}
