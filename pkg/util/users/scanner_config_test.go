package users

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUsersScannerConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config    UsersScannerConfig
		expectErr error
	}{
		"valid list strategy": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyList,
				MaxStalePeriod: time.Hour,
				CacheTTL:       0,
			},
			expectErr: nil,
		},
		"valid user_index strategy": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyUserIndex,
				MaxStalePeriod: time.Hour,
				CacheTTL:       0,
			},
			expectErr: nil,
		},
		"invalid strategy": {
			config: UsersScannerConfig{
				Strategy:       "invalid",
				MaxStalePeriod: time.Hour,
				CacheTTL:       0,
			},
			expectErr: ErrInvalidUserScannerStrategy,
		},
		"zero max stale period with user_index strategy": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyUserIndex,
				MaxStalePeriod: 0,
				CacheTTL:       0,
			},
			expectErr: ErrInvalidMaxStalePeriod,
		},
		"negative max stale period with user_index strategy": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyUserIndex,
				MaxStalePeriod: -time.Hour,
				CacheTTL:       0,
			},
			expectErr: ErrInvalidMaxStalePeriod,
		},
		"zero max stale period with list strategy": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyList,
				MaxStalePeriod: 0,
				CacheTTL:       0,
			},
			expectErr: nil,
		},
		"negative max stale period with list strategy": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyList,
				MaxStalePeriod: -time.Hour,
				CacheTTL:       0,
			},
			expectErr: nil,
		},
		"negative cache TTL": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyList,
				MaxStalePeriod: time.Hour,
				CacheTTL:       -time.Hour,
			},
			expectErr: ErrInvalidCacheTTL,
		},
		"zero cache TTL": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyList,
				MaxStalePeriod: time.Hour,
				CacheTTL:       0,
			},
			expectErr: nil,
		},
		"positive cache TTL": {
			config: UsersScannerConfig{
				Strategy:       UserScanStrategyList,
				MaxStalePeriod: time.Hour,
				CacheTTL:       time.Hour,
			},
			expectErr: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			err := testData.config.Validate()
			if testData.expectErr != nil {
				assert.ErrorIs(t, err, testData.expectErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
