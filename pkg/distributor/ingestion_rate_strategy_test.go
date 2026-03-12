package distributor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestIngestionRateStrategy(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		limits                        validation.Limits
		ring                          ReadLifecycler
		expectedLimit                 float64
		expectedBurst                 int
		expectedNativeHistogramsLimit float64
		expectedNativeHistogramsBurst int
	}{
		"local rate limiter should just return configured limits": {
			limits: validation.Limits{
				IngestionRateStrategy:             validation.LocalIngestionRateStrategy,
				IngestionRate:                     float64(1000),
				IngestionBurstSize:                10000,
				NativeHistogramIngestionRate:      float64(100),
				NativeHistogramIngestionBurstSize: 100,
			},
			ring:                          nil,
			expectedLimit:                 float64(1000),
			expectedBurst:                 10000,
			expectedNativeHistogramsLimit: float64(100),
			expectedNativeHistogramsBurst: 100,
		},
		"global rate limiter should share the limit across the number of distributors": {
			limits: validation.Limits{
				IngestionRateStrategy:             validation.GlobalIngestionRateStrategy,
				IngestionRate:                     float64(1000),
				IngestionBurstSize:                10000,
				NativeHistogramIngestionRate:      float64(100),
				NativeHistogramIngestionBurstSize: 100,
			},
			ring: func() ReadLifecycler {
				ring := newReadLifecyclerMock()
				ring.On("HealthyInstancesCount").Return(2)
				return ring
			}(),
			expectedLimit:                 float64(500),
			expectedBurst:                 10000,
			expectedNativeHistogramsLimit: float64(50),
			expectedNativeHistogramsBurst: 100,
		},
		"global rate limiter should handle the special case of inf ingestion rate": {
			limits: validation.Limits{
				IngestionRateStrategy:             validation.GlobalIngestionRateStrategy,
				IngestionRate:                     float64(rate.Inf),
				IngestionBurstSize:                0,
				NativeHistogramIngestionRate:      float64(rate.Inf),
				NativeHistogramIngestionBurstSize: 0,
			},
			ring: func() ReadLifecycler {
				ring := newReadLifecyclerMock()
				ring.On("HealthyInstancesCount").Return(2)
				return ring
			}(),
			expectedLimit:                 float64(rate.Inf),
			expectedBurst:                 0,
			expectedNativeHistogramsLimit: float64(rate.Inf),
			expectedNativeHistogramsBurst: 0,
		},
		"infinite rate limiter should return unlimited settings": {
			limits: validation.Limits{
				IngestionRateStrategy:             "infinite",
				IngestionRate:                     float64(1000),
				IngestionBurstSize:                10000,
				NativeHistogramIngestionRate:      float64(100),
				NativeHistogramIngestionBurstSize: 100,
			},
			ring:                          nil,
			expectedLimit:                 float64(rate.Inf),
			expectedBurst:                 0,
			expectedNativeHistogramsLimit: float64(rate.Inf),
			expectedNativeHistogramsBurst: 0,
		},
	}

	for testName, testData := range tests {

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			var strategy limiter.RateLimiterStrategy
			var nativeHistogramsStrategy limiter.RateLimiterStrategy

			// Init limits overrides
			overrides := validation.NewOverrides(testData.limits, nil)

			// Instance the strategy
			switch testData.limits.IngestionRateStrategy {
			case validation.LocalIngestionRateStrategy:
				strategy = newLocalIngestionRateStrategy(overrides)
				nativeHistogramsStrategy = newLocalNativeHistogramIngestionRateStrategy(overrides)
			case validation.GlobalIngestionRateStrategy:
				strategy = newGlobalIngestionRateStrategy(overrides, testData.ring)
				nativeHistogramsStrategy = newGlobalNativeHistogramIngestionRateStrategy(overrides, testData.ring)
			case "infinite":
				strategy = newInfiniteIngestionRateStrategy()
				nativeHistogramsStrategy = newInfiniteIngestionRateStrategy()
			default:
				require.Fail(t, "Unknown strategy")
			}

			assert.Equal(t, strategy.Limit("test"), testData.expectedLimit)
			assert.Equal(t, strategy.Burst("test"), testData.expectedBurst)
			assert.Equal(t, nativeHistogramsStrategy.Limit("test"), testData.expectedNativeHistogramsLimit)
			assert.Equal(t, nativeHistogramsStrategy.Burst("test"), testData.expectedNativeHistogramsBurst)
		})
	}
}

func TestGlobalIngestionRateStrategy_BurstBehavior(t *testing.T) {
	t.Parallel()

	const (
		globalRate      = 4000000.0 // 4M samples/sec global
		burstSize       = 800000    // 800K burst (not divided)
		numDistributors = 10
	)

	// Per-distributor rate should be globalRate / numDistributors = 400K/sec
	expectedPerDistributorRate := globalRate / float64(numDistributors)

	limits := validation.Limits{
		IngestionRateStrategy: validation.GlobalIngestionRateStrategy,
		IngestionRate:         globalRate,
		IngestionBurstSize:    burstSize,
	}
	overrides := validation.NewOverrides(limits, nil)

	ring := newReadLifecyclerMock()
	ring.On("HealthyInstancesCount").Return(numDistributors)

	strategy := newGlobalIngestionRateStrategy(overrides, ring)

	t.Run("rate is divided across distributors", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, expectedPerDistributorRate, strategy.Limit("test"))
	})

	t.Run("burst is not divided across distributors", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, burstSize, strategy.Burst("test"))
	})

	t.Run("token bucket allows burst then throttles sustained over-rate traffic", func(t *testing.T) {
		t.Parallel()
		rl := limiter.NewRateLimiter(strategy, 10*time.Second)
		now := time.Now()

		// The bucket starts full at burstSize (800K tokens).
		// Per-distributor rate refills at 400K tokens/sec.
		// If we consume at 600K/sec (200K over rate), we drain the burst
		// at 200K/sec, so burst lasts 800K / 200K = 4 seconds.

		batchSize := 60000  // 60K samples per batch
		batchesPerSec := 10 // 10 batches/sec = 600K samples/sec

		batchInterval := time.Second / time.Duration(batchesPerSec)
		totalAllowed := 0
		firstRejectedAt := time.Duration(0)

		// Simulate 10 seconds of traffic
		for i := 0; i < batchesPerSec*10; i++ {
			ts := now.Add(batchInterval * time.Duration(i))
			if rl.AllowN(ts, "test", batchSize) {
				totalAllowed++
			} else if firstRejectedAt == 0 {
				firstRejectedAt = batchInterval * time.Duration(i)
			}
		}

		// Should allow some batches initially (burst absorbs the overage)
		assert.Greater(t, totalAllowed, 0, "some batches should be allowed initially")

		// Should eventually reject (burst exhausted)
		assert.Greater(t, firstRejectedAt, time.Duration(0), "should eventually reject batches")

		// First rejection should happen around 4 seconds (800K burst / 200K overage per sec)
		// Allow some tolerance due to discrete batch timing
		assert.Greater(t, firstRejectedAt, 2*time.Second, "burst should sustain overage for more than 2s")
		assert.Less(t, firstRejectedAt, 6*time.Second, "burst should be exhausted before 6s")
	})

	t.Run("sustained traffic at exactly per-distributor rate is not throttled", func(t *testing.T) {
		t.Parallel()
		rl := limiter.NewRateLimiter(strategy, 10*time.Second)
		now := time.Now()

		// Send at exactly the per-distributor rate: 400K/sec in 40K batches, 10/sec
		batchSize := 40000
		batchesPerSec := 10
		batchInterval := time.Second / time.Duration(batchesPerSec)

		// Simulate 30 seconds of traffic at the exact rate
		for i := 0; i < batchesPerSec*30; i++ {
			ts := now.Add(batchInterval * time.Duration(i))
			allowed := rl.AllowN(ts, "test", batchSize)
			assert.True(t, allowed, "batch %d at %v should be allowed at sustained rate", i, batchInterval*time.Duration(i))
		}
	})

	t.Run("single request exceeding burst size is always rejected", func(t *testing.T) {
		t.Parallel()
		rl := limiter.NewRateLimiter(strategy, 10*time.Second)
		now := time.Now()

		// A single request larger than burst size should always be rejected
		assert.False(t, rl.AllowN(now, "test", burstSize+1))
	})
}

type readLifecyclerMock struct {
	mock.Mock
}

func newReadLifecyclerMock() *readLifecyclerMock {
	return &readLifecyclerMock{}
}

func (m *readLifecyclerMock) HealthyInstancesCount() int {
	args := m.Called()
	return args.Int(0)
}
