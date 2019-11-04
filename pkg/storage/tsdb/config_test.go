package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config      Config
		expectedErr error
	}{
		"should pass on S3 backend": {
			config: Config{
				Backend: "s3",
			},
			expectedErr: nil,
		},
		"should pass on GCS backend": {
			config: Config{
				Backend: "gcs",
			},
			expectedErr: nil,
		},
		"should pass on unknown backend": {
			config: Config{
				Backend: "unknown",
			},
			expectedErr: errUnsupportedBackend,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actualErr := testData.config.Validate()
			assert.Equal(t, testData.expectedErr, actualErr)
		})
	}
}
