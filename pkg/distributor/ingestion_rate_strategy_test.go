package distributor

import (
	"testing"

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
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			var strategy limiter.RateLimiterStrategy
			var nativeHistogramsStrategy limiter.RateLimiterStrategy

			// Init limits overrides
			overrides, err := validation.NewOverrides(testData.limits, nil)
			require.NoError(t, err)

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
