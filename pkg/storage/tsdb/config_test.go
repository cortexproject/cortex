package tsdb

import (
	"flag"
	"testing"
	"time"

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

func TestConfig_DurationList(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cfg            Config
		expectedRanges []int64
		f              func(*Config)
	}{
		"default to 2h": {
			cfg:            Config{},
			expectedRanges: []int64{7200000},
			f: func(c *Config) {
				c.RegisterFlags(&flag.FlagSet{})
			},
		},
		"parse ranges correctly": {
			cfg: Config{
				BlockRanges: []time.Duration{
					2 * time.Hour,
					10 * time.Hour,
					50 * time.Hour,
				},
			},
			expectedRanges: []int64{7200000, 36000000, 180000000},
			f:              func(*Config) {},
		},
		"handle multiple flag parse": {
			cfg:            Config{},
			expectedRanges: []int64{7200000},
			f: func(c *Config) {
				c.RegisterFlags(&flag.FlagSet{})
				c.RegisterFlags(&flag.FlagSet{})
			},
		},
	}

	for name, data := range tests {
		testdata := data

		t.Run(name, func(t *testing.T) {
			testdata.f(&testdata.cfg)
			assert.Equal(t, testdata.expectedRanges, testdata.cfg.BlockRanges.ToMillisecondRanges())
		})
	}
}
