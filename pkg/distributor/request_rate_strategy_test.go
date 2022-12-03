package distributor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestRequestRateStrategy(t *testing.T) {
	tests := map[string]struct {
		limits        validation.Limits
		ring          ReadLifecycler
		expectedLimit float64
		expectedBurst int
	}{
		"local rate limiter should just return configured limits": {
			limits: validation.Limits{
				IngestionRateStrategy: validation.LocalIngestionRateStrategy,
				RequestRate:           2,
				RequestBurstSize:      4,
			},
			ring:          nil,
			expectedLimit: 2,
			expectedBurst: 4,
		},
		"global rate limiter should share the limit across the number of distributors": {
			limits: validation.Limits{
				IngestionRateStrategy: validation.GlobalIngestionRateStrategy,
				RequestRate:           2,
				RequestBurstSize:      4,
			},
			ring: func() ReadLifecycler {
				ring := newReadLifecyclerMock()
				ring.On("HealthyInstancesCount").Return(2)
				return ring
			}(),
			expectedLimit: 1,
			expectedBurst: 4,
		},
		"infinite rate limiter should return unlimited settings": {
			limits: validation.Limits{
				IngestionRateStrategy: "infinite",
				RequestRate:           2,
				RequestBurstSize:      4,
			},
			ring:          nil,
			expectedLimit: float64(rate.Inf),
			expectedBurst: 0,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			var strategy limiter.RateLimiterStrategy

			// Init limits overrides
			overrides, err := validation.NewOverrides(testData.limits, nil)
			require.NoError(t, err)

			// Instance the strategy
			switch testData.limits.IngestionRateStrategy {
			case validation.LocalIngestionRateStrategy:
				strategy = newLocalRequestRateStrategy(overrides)
			case validation.GlobalIngestionRateStrategy:
				strategy = newGlobalRequestRateStrategy(overrides, testData.ring)
			case "infinite":
				strategy = newInfiniteRequestRateStrategy()
			default:
				require.Fail(t, "Unknown strategy")
			}

			assert.Equal(t, strategy.Limit("test"), testData.expectedLimit)
			assert.Equal(t, strategy.Burst("test"), testData.expectedBurst)
		})
	}
}
