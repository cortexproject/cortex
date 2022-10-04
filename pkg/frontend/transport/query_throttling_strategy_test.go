package transport

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestQueryThrottlingStrategy(t *testing.T) {
	tests := map[string]struct {
		limits        validation.Limits
		expectedLimit float64
		expectedBurst int
	}{
		"local rate limiter should just return configured limits": {
			limits: validation.Limits{
				QueryDataBytesRatePerUser:      float64(1000),
				QueryDataBytesBurstSizePerUser: 10000,
			},
			expectedLimit: float64(1000),
			expectedBurst: 10000,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Init limits overrides
			overrides, err := validation.NewOverrides(testData.limits, nil)
			require.NoError(t, err)

			strategy := newLocalQueryDataBytesRateStrategy(overrides)

			assert.Equal(t, strategy.Limit("test"), testData.expectedLimit)
			assert.Equal(t, strategy.Burst("test"), testData.expectedBurst)
		})
	}
}
