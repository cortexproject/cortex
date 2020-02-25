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
				Backend:                   "s3",
				HeadCompactionInterval:    1 * time.Minute,
				HeadCompactionConcurrency: 5,
			},
			expectedErr: nil,
		},
		"should pass on GCS backend": {
			config: Config{
				Backend:                   "gcs",
				HeadCompactionInterval:    1 * time.Minute,
				HeadCompactionConcurrency: 5,
			},
			expectedErr: nil,
		},
		"should fail on unknown backend": {
			config: Config{
				Backend: "unknown",
			},
			expectedErr: errUnsupportedBackend,
		},
		"should fail on invalid ship concurrency": {
			config: Config{
				Backend:         "s3",
				ShipInterval:    time.Minute,
				ShipConcurrency: 0,
			},
			expectedErr: errInvalidShipConcurrency,
		},
		"should pass on invalid ship concurrency but shipping is disabled": {
			config: Config{
				Backend:                   "s3",
				ShipInterval:              0,
				ShipConcurrency:           0,
				HeadCompactionInterval:    1 * time.Minute,
				HeadCompactionConcurrency: 5,
			},
			expectedErr: nil,
		},
		"should fail on invalid compaction interval": {
			config: Config{
				Backend:                "s3",
				HeadCompactionInterval: 0 * time.Minute,
			},
			expectedErr: errInvalidCompactionInterval,
		},
		"should fail on too high compaction interval": {
			config: Config{
				Backend:                "s3",
				HeadCompactionInterval: 10 * time.Minute,
			},
			expectedErr: errInvalidCompactionInterval,
		},
		"should fail on invalid compaction concurrency": {
			config: Config{
				Backend:                   "s3",
				HeadCompactionInterval:    time.Minute,
				HeadCompactionConcurrency: 0,
			},
			expectedErr: errInvalidCompactionConcurrency,
		},
		"should pass on on valid compaction config": {
			config: Config{
				Backend:                   "s3",
				HeadCompactionInterval:    time.Minute,
				HeadCompactionConcurrency: 10,
			},
			expectedErr: nil,
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
			assert.Equal(t, testdata.expectedRanges, testdata.cfg.BlockRanges.ToMilliseconds())
		})
	}
}
