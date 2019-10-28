package distributor

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/servicediscovery"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestIngestionRateStrategy(t *testing.T) {
	tests := map[string]struct {
		limits        validation.Limits
		registry      servicediscovery.ReadRegistry
		expectedLimit float64
		expectedBurst int
	}{
		"local rate limiter should just return configured limits": {
			limits: validation.Limits{
				IngestionRateStrategy: validation.LocalIngestionRateStrategy,
				IngestionRate:         float64(1000),
				IngestionBurstSize:    10000,
			},
			registry:      nil,
			expectedLimit: float64(1000),
			expectedBurst: 10000,
		},
		"global rate limiter should share the limit across the number of distributors": {
			limits: validation.Limits{
				IngestionRateStrategy: validation.GlobalIngestionRateStrategy,
				IngestionRate:         float64(1000),
				IngestionBurstSize:    10000,
			},
			registry: func() servicediscovery.ReadRegistry {
				registry := servicediscovery.NewReadRegistryMock()
				registry.On("HealthyCount").Return(2)
				return registry
			}(),
			expectedLimit: float64(500),
			expectedBurst: 10000,
		},
		"infinite rate limiter should return unlimited settings": {
			limits: validation.Limits{
				IngestionRateStrategy: "infinite",
				IngestionRate:         float64(1000),
				IngestionBurstSize:    10000,
			},
			registry:      nil,
			expectedLimit: float64(rate.Inf),
			expectedBurst: 0,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			var strategy limiter.RateLimiterStrategy

			// Init limits overrides
			overrides, err := validation.NewOverrides(testData.limits)
			require.NoError(t, err)

			// Instance the strategy
			switch testData.limits.IngestionRateStrategy {
			case validation.LocalIngestionRateStrategy:
				strategy = newLocalIngestionRateStrategy(overrides)
			case validation.GlobalIngestionRateStrategy:
				strategy = newGlobalIngestionRateStrategy(overrides, testData.registry)
			case "infinite":
				strategy = newInfiniteIngestionRateStrategy()
			default:
				require.Fail(t, "Unknown strategy")
			}

			assert.Equal(t, strategy.Limit("test"), testData.expectedLimit)
			assert.Equal(t, strategy.Burst("test"), testData.expectedBurst)
		})
	}
}
