package bucket

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHedgedRequest_Validate(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		cfg      *HedgedRequestConfig
		expected error
	}{
		"should fail if hedged request quantile is less than 0": {
			cfg: &HedgedRequestConfig{
				Quantile: -0.1,
			},
			expected: errInvalidQuantile,
		},
		"should fail if hedged request quantile is more than 1": {
			cfg: &HedgedRequestConfig{
				Quantile: 1.1,
			},
			expected: errInvalidQuantile,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			err := testData.cfg.Validate()
			require.Equal(t, testData.expected, err)
		})
	}

}
