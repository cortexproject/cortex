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
		config      BlocksStorageConfig
		expectedErr error
	}{
		"should pass on S3 backend": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    1 * time.Minute,
					HeadCompactionConcurrency: 5,
					StripeSize:                2,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: nil,
		},
		"should pass on GCS backend": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "gcs",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    1 * time.Minute,
					HeadCompactionConcurrency: 5,
					StripeSize:                2,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: nil,
		},
		"should fail on unknown storage backend": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "unknown",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					StripeSize:  2,
					BlockRanges: DurationList{1 * time.Minute},
				},
			},
			expectedErr: errUnsupportedStorageBackend,
		},
		"should fail on invalid ship concurrency": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					ShipInterval:    time.Minute,
					ShipConcurrency: 0,
					StripeSize:      2,
					BlockRanges:     DurationList{1 * time.Minute},
				},
			},
			expectedErr: errInvalidShipConcurrency,
		},
		"should pass on invalid ship concurrency but shipping is disabled": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					ShipInterval:              0,
					ShipConcurrency:           0,
					HeadCompactionInterval:    1 * time.Minute,
					HeadCompactionConcurrency: 5,
					StripeSize:                2,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: nil,
		},
		"should fail on invalid compaction interval": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval: 0 * time.Minute,
					StripeSize:             2,
					BlockRanges:            DurationList{1 * time.Minute},
				},
			},
			expectedErr: errInvalidCompactionInterval,
		},
		"should fail on too high compaction interval": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval: 10 * time.Minute,
					StripeSize:             2,
					BlockRanges:            DurationList{1 * time.Minute},
				},
			},
			expectedErr: errInvalidCompactionInterval,
		},
		"should fail on invalid compaction concurrency": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    time.Minute,
					HeadCompactionConcurrency: 0,
					StripeSize:                2,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: errInvalidCompactionConcurrency,
		},
		"should pass on on valid compaction config": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    time.Minute,
					HeadCompactionConcurrency: 10,
					StripeSize:                2,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: nil,
		},
		"should fail on negative stripe size": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    1 * time.Minute,
					HeadCompactionConcurrency: 5,
					StripeSize:                -2,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: errInvalidStripeSize,
		},
		"should fail on stripe size 0": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    1 * time.Minute,
					HeadCompactionConcurrency: 5,
					StripeSize:                0,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: errInvalidStripeSize,
		},
		"should fail on stripe size 1": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    1 * time.Minute,
					HeadCompactionConcurrency: 5,
					StripeSize:                1,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: errInvalidStripeSize,
		},
		"should pass on stripe size": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    1 * time.Minute,
					HeadCompactionConcurrency: 5,
					StripeSize:                1 << 14,
					BlockRanges:               DurationList{1 * time.Minute},
				},
			},
			expectedErr: nil,
		},
		"should fail on empty block ranges": {
			config: BlocksStorageConfig{
				Bucket: BucketConfig{
					Backend: "s3",
				},
				TSDB: TSDBConfig{
					HeadCompactionInterval:    1 * time.Minute,
					HeadCompactionConcurrency: 5,
					StripeSize:                8,
				},
				BucketStore: BucketStoreConfig{
					IndexCache: IndexCacheConfig{
						Backend: "inmemory",
					},
				},
			},
			expectedErr: errEmptyBlockranges,
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
		cfg            BlocksStorageConfig
		expectedRanges []int64
		f              func(*BlocksStorageConfig)
	}{
		"default to 2h": {
			cfg:            BlocksStorageConfig{},
			expectedRanges: []int64{7200000},
			f: func(c *BlocksStorageConfig) {
				c.RegisterFlags(&flag.FlagSet{})
			},
		},
		"parse ranges correctly": {
			cfg: BlocksStorageConfig{
				TSDB: TSDBConfig{
					BlockRanges: []time.Duration{
						2 * time.Hour,
						10 * time.Hour,
						50 * time.Hour,
					},
				},
			},
			expectedRanges: []int64{7200000, 36000000, 180000000},
			f:              func(*BlocksStorageConfig) {},
		},
		"handle multiple flag parse": {
			cfg:            BlocksStorageConfig{},
			expectedRanges: []int64{7200000},
			f: func(c *BlocksStorageConfig) {
				c.RegisterFlags(&flag.FlagSet{})
				c.RegisterFlags(&flag.FlagSet{})
			},
		},
	}

	for name, data := range tests {
		testdata := data

		t.Run(name, func(t *testing.T) {
			testdata.f(&testdata.cfg)
			assert.Equal(t, testdata.expectedRanges, testdata.cfg.TSDB.BlockRanges.ToMilliseconds())
		})
	}
}
