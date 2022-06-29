package compactor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	cortex_storage_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestConfig_ShouldSupportYamlConfig(t *testing.T) {
	yamlCfg := `
block_ranges: [2h, 48h]
consistency_delay: 1h
block_sync_concurrency: 123
data_dir: /tmp
compaction_interval: 15m
compaction_retries: 123
`

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	assert.NoError(t, yaml.Unmarshal([]byte(yamlCfg), &cfg))
	assert.Equal(t, cortex_tsdb.DurationList{2 * time.Hour, 48 * time.Hour}, cfg.BlockRanges)
	assert.Equal(t, time.Hour, cfg.ConsistencyDelay)
	assert.Equal(t, 123, cfg.BlockSyncConcurrency)
	assert.Equal(t, "/tmp", cfg.DataDir)
	assert.Equal(t, 15*time.Minute, cfg.CompactionInterval)
	assert.Equal(t, 123, cfg.CompactionRetries)
}

func TestConfig_ShouldSupportCliFlags(t *testing.T) {
	fs := flag.NewFlagSet("", flag.PanicOnError)
	cfg := Config{}
	cfg.RegisterFlags(fs)
	require.NoError(t, fs.Parse([]string{
		"-compactor.block-ranges=2h,48h",
		"-compactor.consistency-delay=1h",
		"-compactor.block-sync-concurrency=123",
		"-compactor.data-dir=/tmp",
		"-compactor.compaction-interval=15m",
		"-compactor.compaction-retries=123",
	}))

	assert.Equal(t, cortex_tsdb.DurationList{2 * time.Hour, 48 * time.Hour}, cfg.BlockRanges)
	assert.Equal(t, time.Hour, cfg.ConsistencyDelay)
	assert.Equal(t, 123, cfg.BlockSyncConcurrency)
	assert.Equal(t, "/tmp", cfg.DataDir)
	assert.Equal(t, 15*time.Minute, cfg.CompactionInterval)
	assert.Equal(t, 123, cfg.CompactionRetries)
}

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup      func(cfg *Config)
		initLimits func(*validation.Limits)
		expected   string
	}{
		"should pass with the default config": {
			setup:      func(cfg *Config) {},
			initLimits: func(_ *validation.Limits) {},
			expected:   "",
		},
		"should pass with only 1 block range period": {
			setup: func(cfg *Config) {
				cfg.BlockRanges = cortex_tsdb.DurationList{time.Hour}
			},
			initLimits: func(_ *validation.Limits) {},
			expected:   "",
		},
		"should fail with non divisible block range periods": {
			setup: func(cfg *Config) {
				cfg.BlockRanges = cortex_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour, 30 * time.Hour}
			},

			initLimits: func(_ *validation.Limits) {},
			expected:   errors.Errorf(errInvalidBlockRanges, 30*time.Hour, 24*time.Hour).Error(),
		},
		"should pass with valid shuffle sharding config": {
			setup: func(cfg *Config) {
				cfg.ShardingStrategy = util.ShardingStrategyShuffle
				cfg.ShardingEnabled = true
			},
			initLimits: func(limits *validation.Limits) {
				limits.CompactorTenantShardSize = 1
			},
			expected: "",
		},
		"should fail with bad compactor tenant shard size": {
			setup: func(cfg *Config) {
				cfg.ShardingStrategy = util.ShardingStrategyShuffle
				cfg.ShardingEnabled = true
			},
			initLimits: func(_ *validation.Limits) {},
			expected:   errInvalidTenantShardSize.Error(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := &Config{}
			limits := validation.Limits{}
			flagext.DefaultValues(cfg, &limits)
			testData.setup(cfg)
			testData.initLimits(&limits)

			if actualErr := cfg.Validate(limits); testData.expected != "" {
				assert.EqualError(t, actualErr, testData.expected)
			} else {
				assert.NoError(t, actualErr)
			}
		})
	}
}

func TestCompactor_ShouldDoNothingOnNoUserBlocks(t *testing.T) {
	t.Parallel()

	// No user blocks stored in the bucket.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{}, nil)
	cfg := prepareConfig()
	c, _, _, logs, registry := prepare(t, cfg, bucketClient, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	assert.Equal(t, prom_testutil.ToFloat64(c.compactionRunInterval), cfg.CompactionInterval.Seconds())

	assert.Equal(t, []string{
		`level=info component=cleaner msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner msg="successfully completed blocks cleanup and maintenance"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=info component=compactor msg="discovered users from bucket" users=0`,
	}, strings.Split(strings.TrimSpace(logs.String()), "\n"))

	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
		# TYPE cortex_compactor_runs_started_total counter
		# HELP cortex_compactor_runs_started_total Total number of compaction runs started.
		cortex_compactor_runs_started_total 1

		# TYPE cortex_compactor_runs_completed_total counter
		# HELP cortex_compactor_runs_completed_total Total number of compaction runs successfully completed.
		cortex_compactor_runs_completed_total 1

		# TYPE cortex_compactor_runs_failed_total counter
		# HELP cortex_compactor_runs_failed_total Total number of compaction runs failed.
		cortex_compactor_runs_failed_total 0

		# HELP cortex_compactor_garbage_collected_blocks_total Total number of blocks marked for deletion by compactor.
		# TYPE cortex_compactor_garbage_collected_blocks_total counter
		cortex_compactor_garbage_collected_blocks_total 0

		# HELP cortex_compactor_garbage_collection_duration_seconds Time it took to perform garbage collection iteration.
		# TYPE cortex_compactor_garbage_collection_duration_seconds histogram
		cortex_compactor_garbage_collection_duration_seconds_bucket{le="+Inf"} 0
		cortex_compactor_garbage_collection_duration_seconds_sum 0
		cortex_compactor_garbage_collection_duration_seconds_count 0

		# HELP cortex_compactor_garbage_collection_failures_total Total number of failed garbage collection operations.
		# TYPE cortex_compactor_garbage_collection_failures_total counter
		cortex_compactor_garbage_collection_failures_total 0

		# HELP cortex_compactor_garbage_collection_total Total number of garbage collection operations.
		# TYPE cortex_compactor_garbage_collection_total counter
		cortex_compactor_garbage_collection_total 0

		# HELP cortex_compactor_meta_sync_consistency_delay_seconds Configured consistency delay in seconds.
		# TYPE cortex_compactor_meta_sync_consistency_delay_seconds gauge
		cortex_compactor_meta_sync_consistency_delay_seconds 0

		# HELP cortex_compactor_meta_sync_duration_seconds Duration of the blocks metadata synchronization in seconds.
		# TYPE cortex_compactor_meta_sync_duration_seconds histogram
		cortex_compactor_meta_sync_duration_seconds_bucket{le="+Inf"} 0
		cortex_compactor_meta_sync_duration_seconds_sum 0
		cortex_compactor_meta_sync_duration_seconds_count 0

		# HELP cortex_compactor_meta_sync_failures_total Total blocks metadata synchronization failures.
		# TYPE cortex_compactor_meta_sync_failures_total counter
		cortex_compactor_meta_sync_failures_total 0

		# HELP cortex_compactor_meta_syncs_total Total blocks metadata synchronization attempts.
		# TYPE cortex_compactor_meta_syncs_total counter
		cortex_compactor_meta_syncs_total 0

		# HELP cortex_compactor_group_compaction_runs_completed_total Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.
		# TYPE cortex_compactor_group_compaction_runs_completed_total counter
		cortex_compactor_group_compaction_runs_completed_total 0

		# HELP cortex_compactor_group_compaction_runs_started_total Total number of group compaction attempts.
		# TYPE cortex_compactor_group_compaction_runs_started_total counter
		cortex_compactor_group_compaction_runs_started_total 0

		# HELP cortex_compactor_group_compactions_failures_total Total number of failed group compactions.
		# TYPE cortex_compactor_group_compactions_failures_total counter
		cortex_compactor_group_compactions_failures_total 0

		# HELP cortex_compactor_group_compactions_total Total number of group compaction attempts that resulted in a new block.
		# TYPE cortex_compactor_group_compactions_total counter
		cortex_compactor_group_compactions_total 0

		# HELP cortex_compactor_group_vertical_compactions_total Total number of group compaction attempts that resulted in a new block based on overlapping blocks.
		# TYPE cortex_compactor_group_vertical_compactions_total counter
		cortex_compactor_group_vertical_compactions_total 0

		# TYPE cortex_compactor_block_cleanup_failures_total counter
		# HELP cortex_compactor_block_cleanup_failures_total Total number of blocks failed to be deleted.
		cortex_compactor_block_cleanup_failures_total 0

		# HELP cortex_compactor_blocks_cleaned_total Total number of blocks deleted.
		# TYPE cortex_compactor_blocks_cleaned_total counter
		cortex_compactor_blocks_cleaned_total 0

		# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
		# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
		cortex_compactor_blocks_marked_for_deletion_total{reason="compaction"} 0
		cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total 1

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total 0
	`),
		"cortex_compactor_runs_started_total",
		"cortex_compactor_runs_completed_total",
		"cortex_compactor_runs_failed_total",
		"cortex_compactor_garbage_collected_blocks_total",
		"cortex_compactor_garbage_collection_duration_seconds",
		"cortex_compactor_garbage_collection_failures_total",
		"cortex_compactor_garbage_collection_total",
		"cortex_compactor_meta_sync_consistency_delay_seconds",
		"cortex_compactor_meta_sync_duration_seconds",
		"cortex_compactor_meta_sync_failures_total",
		"cortex_compactor_meta_syncs_total",
		"cortex_compactor_group_compaction_runs_completed_total",
		"cortex_compactor_group_compaction_runs_started_total",
		"cortex_compactor_group_compactions_failures_total",
		"cortex_compactor_group_compactions_total",
		"cortex_compactor_group_vertical_compactions_total",
		"cortex_compactor_block_cleanup_failures_total",
		"cortex_compactor_blocks_cleaned_total",
		"cortex_compactor_blocks_marked_for_deletion_total",
		"cortex_compactor_blocks_marked_for_no_compaction_total",
		"cortex_compactor_block_cleanup_started_total",
		"cortex_compactor_block_cleanup_completed_total",
		"cortex_compactor_block_cleanup_failed_total",
	))
}

func TestCompactor_ShouldRetryCompactionOnFailureWhileDiscoveringUsersFromBucket(t *testing.T) {
	t.Parallel()

	// Fail to iterate over the bucket while discovering users.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", nil, errors.New("failed to iterate the bucket"))

	c, _, _, logs, registry := prepare(t, prepareConfig(), bucketClient, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until all retry attempts have completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsFailed)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Ensure the bucket iteration has been retried the configured number of times.
	bucketClient.AssertNumberOfCalls(t, "Iter", 1+3)

	assert.Equal(t, []string{
		`level=info component=cleaner msg="started blocks cleanup and maintenance"`,
		`level=error component=cleaner msg="failed to run blocks cleanup and maintenance" err="failed to discover users from bucket: failed to iterate the bucket"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=error component=compactor msg="failed to discover users from bucket" err="failed to iterate the bucket"`,
	}, strings.Split(strings.TrimSpace(logs.String()), "\n"))

	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
		# TYPE cortex_compactor_runs_started_total counter
		# HELP cortex_compactor_runs_started_total Total number of compaction runs started.
		cortex_compactor_runs_started_total 1

		# TYPE cortex_compactor_runs_completed_total counter
		# HELP cortex_compactor_runs_completed_total Total number of compaction runs successfully completed.
		cortex_compactor_runs_completed_total 0

		# TYPE cortex_compactor_runs_failed_total counter
		# HELP cortex_compactor_runs_failed_total Total number of compaction runs failed.
		cortex_compactor_runs_failed_total 1

		# HELP cortex_compactor_garbage_collected_blocks_total Total number of blocks marked for deletion by compactor.
		# TYPE cortex_compactor_garbage_collected_blocks_total counter
		cortex_compactor_garbage_collected_blocks_total 0

		# HELP cortex_compactor_garbage_collection_duration_seconds Time it took to perform garbage collection iteration.
		# TYPE cortex_compactor_garbage_collection_duration_seconds histogram
		cortex_compactor_garbage_collection_duration_seconds_bucket{le="+Inf"} 0
		cortex_compactor_garbage_collection_duration_seconds_sum 0
		cortex_compactor_garbage_collection_duration_seconds_count 0

		# HELP cortex_compactor_garbage_collection_failures_total Total number of failed garbage collection operations.
		# TYPE cortex_compactor_garbage_collection_failures_total counter
		cortex_compactor_garbage_collection_failures_total 0

		# HELP cortex_compactor_garbage_collection_total Total number of garbage collection operations.
		# TYPE cortex_compactor_garbage_collection_total counter
		cortex_compactor_garbage_collection_total 0

		# HELP cortex_compactor_meta_sync_consistency_delay_seconds Configured consistency delay in seconds.
		# TYPE cortex_compactor_meta_sync_consistency_delay_seconds gauge
		cortex_compactor_meta_sync_consistency_delay_seconds 0

		# HELP cortex_compactor_meta_sync_duration_seconds Duration of the blocks metadata synchronization in seconds.
		# TYPE cortex_compactor_meta_sync_duration_seconds histogram
		cortex_compactor_meta_sync_duration_seconds_bucket{le="+Inf"} 0
		cortex_compactor_meta_sync_duration_seconds_sum 0
		cortex_compactor_meta_sync_duration_seconds_count 0

		# HELP cortex_compactor_meta_sync_failures_total Total blocks metadata synchronization failures.
		# TYPE cortex_compactor_meta_sync_failures_total counter
		cortex_compactor_meta_sync_failures_total 0

		# HELP cortex_compactor_meta_syncs_total Total blocks metadata synchronization attempts.
		# TYPE cortex_compactor_meta_syncs_total counter
		cortex_compactor_meta_syncs_total 0

		# HELP cortex_compactor_group_compaction_runs_completed_total Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.
		# TYPE cortex_compactor_group_compaction_runs_completed_total counter
		cortex_compactor_group_compaction_runs_completed_total 0

		# HELP cortex_compactor_group_compaction_runs_started_total Total number of group compaction attempts.
		# TYPE cortex_compactor_group_compaction_runs_started_total counter
		cortex_compactor_group_compaction_runs_started_total 0

		# HELP cortex_compactor_group_compactions_failures_total Total number of failed group compactions.
		# TYPE cortex_compactor_group_compactions_failures_total counter
		cortex_compactor_group_compactions_failures_total 0

		# HELP cortex_compactor_group_compactions_total Total number of group compaction attempts that resulted in a new block.
		# TYPE cortex_compactor_group_compactions_total counter
		cortex_compactor_group_compactions_total 0

		# HELP cortex_compactor_group_vertical_compactions_total Total number of group compaction attempts that resulted in a new block based on overlapping blocks.
		# TYPE cortex_compactor_group_vertical_compactions_total counter
		cortex_compactor_group_vertical_compactions_total 0

		# TYPE cortex_compactor_block_cleanup_failures_total counter
		# HELP cortex_compactor_block_cleanup_failures_total Total number of blocks failed to be deleted.
		cortex_compactor_block_cleanup_failures_total 0

		# HELP cortex_compactor_blocks_cleaned_total Total number of blocks deleted.
		# TYPE cortex_compactor_blocks_cleaned_total counter
		cortex_compactor_blocks_cleaned_total 0

		# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
		# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
		cortex_compactor_blocks_marked_for_deletion_total{reason="compaction"} 0
		cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total 0

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total 1
	`),
		"cortex_compactor_runs_started_total",
		"cortex_compactor_runs_completed_total",
		"cortex_compactor_runs_failed_total",
		"cortex_compactor_garbage_collected_blocks_total",
		"cortex_compactor_garbage_collection_duration_seconds",
		"cortex_compactor_garbage_collection_failures_total",
		"cortex_compactor_garbage_collection_total",
		"cortex_compactor_meta_sync_consistency_delay_seconds",
		"cortex_compactor_meta_sync_duration_seconds",
		"cortex_compactor_meta_sync_failures_total",
		"cortex_compactor_meta_syncs_total",
		"cortex_compactor_group_compaction_runs_completed_total",
		"cortex_compactor_group_compaction_runs_started_total",
		"cortex_compactor_group_compactions_failures_total",
		"cortex_compactor_group_compactions_total",
		"cortex_compactor_group_vertical_compactions_total",
		"cortex_compactor_block_cleanup_failures_total",
		"cortex_compactor_blocks_cleaned_total",
		"cortex_compactor_blocks_marked_for_deletion_total",
		"cortex_compactor_blocks_marked_for_no_compaction_total",
		"cortex_compactor_block_cleanup_started_total",
		"cortex_compactor_block_cleanup_completed_total",
		"cortex_compactor_block_cleanup_failed_total",
	))
}

func TestCompactor_ShouldIncrementCompactionErrorIfFailedToCompactASingleTenant(t *testing.T) {
	t.Parallel()

	userID := "test-user"
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{userID}, nil)
	bucketClient.MockIter(userID+"/", []string{userID + "/01DTVP434PA9VFXSW2JKB3392D", userID + "/01FN6CDF3PNEWWRY5MPGJPE3EX"}, nil)
	bucketClient.MockIter(userID+"/markers/", nil, nil)
	bucketClient.MockExists(path.Join(userID, cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet(userID+"/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet(userID+"/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", "", nil)
	bucketClient.MockGet(userID+"/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet(userID+"/bucket-index.json.gz", "", nil)
	bucketClient.MockUpload(userID+"/bucket-index.json.gz", nil)

	c, _, tsdbPlannerMock, _, registry := prepare(t, prepareConfig(), bucketClient, nil)
	tsdbPlannerMock.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, errors.New("Failed to plan"))
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until all retry attempts have completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsFailed)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
		# TYPE cortex_compactor_runs_started_total counter
		# HELP cortex_compactor_runs_started_total Total number of compaction runs started.
		cortex_compactor_runs_started_total 1

		# TYPE cortex_compactor_runs_completed_total counter
		# HELP cortex_compactor_runs_completed_total Total number of compaction runs successfully completed.
		cortex_compactor_runs_completed_total 0

		# TYPE cortex_compactor_runs_failed_total counter
		# HELP cortex_compactor_runs_failed_total Total number of compaction runs failed.
		cortex_compactor_runs_failed_total 1
	`),
		"cortex_compactor_runs_started_total",
		"cortex_compactor_runs_completed_total",
		"cortex_compactor_runs_failed_total",
	))
}

func TestCompactor_ShouldIterateOverUsersAndRunCompaction(t *testing.T) {
	t.Parallel()

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockExists(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01FN6CDF3PNEWWRY5MPGJPE3EX"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ", "user-2/01FN3V83ABR9992RF8WRJZ76ZQ"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json", mockBlockMetaJSON("01FN3V83ABR9992RF8WRJZ76ZQ"), nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-2/bucket-index.json.gz", "", nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-2/bucket-index.json.gz", nil)

	c, _, tsdbPlanner, logs, registry := prepare(t, prepareConfig(), bucketClient, nil)

	// Mock the planner as if there's no compaction to do,
	// in order to simplify tests (all in all, we just want to
	// test our logic and not TSDB compactor which we expect to
	// be already tested).
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Ensure a plan has been executed for the blocks of each user.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 2)

	assert.Len(t, tsdbPlanner.getNoCompactBlocks(), 0)

	assert.ElementsMatch(t, []string{
		`level=info component=cleaner msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-1 msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-1 msg="completed blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-2 msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-2 msg="completed blocks cleanup and maintenance"`,
		`level=info component=cleaner msg="successfully completed blocks cleanup and maintenance"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=info component=compactor msg="discovered users from bucket" users=2`,
		`level=info component=compactor msg="starting compaction of user blocks" user=user-1`,
		`level=info component=compactor org_id=user-1 msg="start sync of metas"`,
		`level=info component=compactor org_id=user-1 msg="start of GC"`,
		`level=info component=compactor org_id=user-1 msg="start of compactions"`,
		`level=info component=compactor org_id=user-1 msg="compaction iterations done"`,
		`level=info component=compactor msg="successfully compacted user blocks" user=user-1`,
		`level=info component=compactor msg="starting compaction of user blocks" user=user-2`,
		`level=info component=compactor org_id=user-2 msg="start sync of metas"`,
		`level=info component=compactor org_id=user-2 msg="start of GC"`,
		`level=info component=compactor org_id=user-2 msg="start of compactions"`,
		`level=info component=compactor org_id=user-2 msg="compaction iterations done"`,
		`level=info component=compactor msg="successfully compacted user blocks" user=user-2`,
	}, removeIgnoredLogs(strings.Split(strings.TrimSpace(logs.String()), "\n")))

	// Instead of testing for shipper metrics, we only check our metrics here.
	// Real shipper metrics are too variable to embed into a test.
	testedMetrics := []string{
		"cortex_compactor_runs_started_total", "cortex_compactor_runs_completed_total", "cortex_compactor_runs_failed_total",
		"cortex_compactor_blocks_cleaned_total", "cortex_compactor_block_cleanup_failures_total", "cortex_compactor_blocks_marked_for_deletion_total",
		"cortex_compactor_block_cleanup_started_total", "cortex_compactor_block_cleanup_completed_total", "cortex_compactor_block_cleanup_failed_total",
		"cortex_compactor_blocks_marked_for_no_compaction_total",
	}
	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
		# TYPE cortex_compactor_runs_started_total counter
		# HELP cortex_compactor_runs_started_total Total number of compaction runs started.
		cortex_compactor_runs_started_total 1

		# TYPE cortex_compactor_runs_completed_total counter
		# HELP cortex_compactor_runs_completed_total Total number of compaction runs successfully completed.
		cortex_compactor_runs_completed_total 1

		# TYPE cortex_compactor_runs_failed_total counter
		# HELP cortex_compactor_runs_failed_total Total number of compaction runs failed.
		cortex_compactor_runs_failed_total 0

		# TYPE cortex_compactor_block_cleanup_failures_total counter
		# HELP cortex_compactor_block_cleanup_failures_total Total number of blocks failed to be deleted.
		cortex_compactor_block_cleanup_failures_total 0

		# HELP cortex_compactor_blocks_cleaned_total Total number of blocks deleted.
		# TYPE cortex_compactor_blocks_cleaned_total counter
		cortex_compactor_blocks_cleaned_total 0

		# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
		# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
		cortex_compactor_blocks_marked_for_deletion_total{reason="compaction"} 0
		cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total 1

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total 0

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0
	`), testedMetrics...))
}

func TestCompactor_ShouldNotCompactBlocksMarkedForDeletion(t *testing.T) {
	t.Parallel()

	cfg := prepareConfig()
	cfg.DeletionDelay = 10 * time.Minute // Delete block after 10 minutes

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ"}, nil)
	bucketClient.MockExists(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), false, nil)

	// Block that has just been marked for deletion. It will not be deleted just yet, and it also will not be compacted.
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", mockDeletionMarkJSON("01DTVP434PA9VFXSW2JKB3392D", time.Now()), nil)
	bucketClient.MockGet("user-1/markers/01DTVP434PA9VFXSW2JKB3392D-deletion-mark.json", mockDeletionMarkJSON("01DTVP434PA9VFXSW2JKB3392D", time.Now()), nil)

	// This block will be deleted by cleaner.
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", mockDeletionMarkJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ", time.Now().Add(-cfg.DeletionDelay)), nil)
	bucketClient.MockGet("user-1/markers/01DTW0ZCPDDNV4BV83Q2SV4QAZ-deletion-mark.json", mockDeletionMarkJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ", time.Now().Add(-cfg.DeletionDelay)), nil)

	bucketClient.MockIter("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ", []string{
		"user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json",
		"user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json",
	}, nil)

	bucketClient.MockIter("user-1/markers/", []string{
		"user-1/markers/01DTVP434PA9VFXSW2JKB3392D-deletion-mark.json",
		"user-1/markers/01DTW0ZCPDDNV4BV83Q2SV4QAZ-deletion-mark.json",
	}, nil)

	bucketClient.MockDelete("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", nil)
	bucketClient.MockDelete("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", nil)
	bucketClient.MockDelete("user-1/markers/01DTW0ZCPDDNV4BV83Q2SV4QAZ-deletion-mark.json", nil)
	bucketClient.MockDelete("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)

	c, _, tsdbPlanner, logs, registry := prepare(t, cfg, bucketClient, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Since both blocks are marked for deletion, none of them are going to be compacted.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 0)

	assert.ElementsMatch(t, []string{
		`level=info component=cleaner msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-1 msg="started blocks cleanup and maintenance"`,
		`level=debug component=cleaner org_id=user-1 msg="deleted file" file=01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json bucket=mock`,
		`level=debug component=cleaner org_id=user-1 msg="deleted file" file=01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json bucket=mock`,
		`level=info component=cleaner org_id=user-1 msg="deleted block marked for deletion" block=01DTW0ZCPDDNV4BV83Q2SV4QAZ`,
		`level=info component=cleaner org_id=user-1 msg="completed blocks cleanup and maintenance"`,
		`level=info component=cleaner msg="successfully completed blocks cleanup and maintenance"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=info component=compactor msg="discovered users from bucket" users=1`,
		`level=info component=compactor msg="starting compaction of user blocks" user=user-1`,
		`level=info component=compactor org_id=user-1 msg="start sync of metas"`,
		`level=info component=compactor org_id=user-1 msg="start of GC"`,
		`level=info component=compactor org_id=user-1 msg="start of compactions"`,
		`level=info component=compactor org_id=user-1 msg="compaction iterations done"`,
		`level=info component=compactor msg="successfully compacted user blocks" user=user-1`,
	}, removeIgnoredLogs(strings.Split(strings.TrimSpace(logs.String()), "\n")))

	// Instead of testing for shipper metrics, we only check our metrics here.
	// Real shipper metrics are too variable to embed into a test.
	testedMetrics := []string{
		"cortex_compactor_runs_started_total", "cortex_compactor_runs_completed_total", "cortex_compactor_runs_failed_total",
		"cortex_compactor_blocks_cleaned_total", "cortex_compactor_block_cleanup_failures_total", "cortex_compactor_blocks_marked_for_deletion_total",
		"cortex_compactor_block_cleanup_started_total", "cortex_compactor_block_cleanup_completed_total", "cortex_compactor_block_cleanup_failed_total",
		"cortex_compactor_blocks_marked_for_no_compaction_total",
	}
	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
		# TYPE cortex_compactor_runs_started_total counter
		# HELP cortex_compactor_runs_started_total Total number of compaction runs started.
		cortex_compactor_runs_started_total 1

		# TYPE cortex_compactor_runs_completed_total counter
		# HELP cortex_compactor_runs_completed_total Total number of compaction runs successfully completed.
		cortex_compactor_runs_completed_total 1

		# TYPE cortex_compactor_runs_failed_total counter
		# HELP cortex_compactor_runs_failed_total Total number of compaction runs failed.
		cortex_compactor_runs_failed_total 0

		# TYPE cortex_compactor_block_cleanup_failures_total counter
		# HELP cortex_compactor_block_cleanup_failures_total Total number of blocks failed to be deleted.
		cortex_compactor_block_cleanup_failures_total 0

		# HELP cortex_compactor_blocks_cleaned_total Total number of blocks deleted.
		# TYPE cortex_compactor_blocks_cleaned_total counter
		cortex_compactor_blocks_cleaned_total 1

		# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
		# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
		cortex_compactor_blocks_marked_for_deletion_total{reason="compaction"} 0
		cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total 1

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total 0

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0
	`), testedMetrics...))
}

func TestCompactor_ShouldNotCompactBlocksMarkedForSkipCompact(t *testing.T) {
	t.Parallel()

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockExists(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01FN6CDF3PNEWWRY5MPGJPE3EX"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ", "user-2/01FN3V83ABR9992RF8WRJZ76ZQ"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", mockNoCompactBlockJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", mockNoCompactBlockJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)

	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json", mockBlockMetaJSON("01FN3V83ABR9992RF8WRJZ76ZQ"), nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/no-compact-mark.json", "", nil)

	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-2/bucket-index.json.gz", "", nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-2/bucket-index.json.gz", nil)

	c, _, tsdbPlanner, _, registry := prepare(t, prepareConfig(), bucketClient, nil)

	tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Planner still called for user with all blocks makred for skip compaction.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 2)

	assert.ElementsMatch(t, []string{"01DTVP434PA9VFXSW2JKB3392D", "01FN6CDF3PNEWWRY5MPGJPE3EX"}, tsdbPlanner.getNoCompactBlocks())

	testedMetrics := []string{"cortex_compactor_blocks_marked_for_no_compaction_total"}

	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0
	`), testedMetrics...))
}

func TestCompactor_ShouldNotCompactBlocksForUsersMarkedForDeletion(t *testing.T) {
	t.Parallel()

	cfg := prepareConfig()
	cfg.DeletionDelay = 10 * time.Minute      // Delete block after 10 minutes
	cfg.TenantCleanupDelay = 10 * time.Minute // To make sure it's not 0.

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D"}, nil)
	bucketClient.MockGet(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), `{"deletion_time": 1}`, nil)
	bucketClient.MockUpload(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), nil)

	bucketClient.MockIter("user-1/01DTVP434PA9VFXSW2JKB3392D", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "user-1/01DTVP434PA9VFXSW2JKB3392D/index"}, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/index", "some index content", nil)
	bucketClient.MockExists("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", false, nil)

	bucketClient.MockDelete("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", nil)
	bucketClient.MockDelete("user-1/01DTVP434PA9VFXSW2JKB3392D/index", nil)
	bucketClient.MockDelete("user-1/bucket-index.json.gz", nil)

	c, _, tsdbPlanner, logs, registry := prepare(t, cfg, bucketClient, nil)

	// Mock the planner as if there's no compaction to do,
	// in order to simplify tests (all in all, we just want to
	// test our logic and not TSDB compactor which we expect to
	// be already tested).
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// No user is compacted, single user we have is marked for deletion.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 0)

	assert.ElementsMatch(t, []string{
		`level=info component=cleaner msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-1 msg="deleting blocks for tenant marked for deletion"`,
		`level=debug component=cleaner org_id=user-1 msg="deleted file" file=01DTVP434PA9VFXSW2JKB3392D/meta.json bucket=mock`,
		`level=debug component=cleaner org_id=user-1 msg="deleted file" file=01DTVP434PA9VFXSW2JKB3392D/index bucket=mock`,
		`level=info component=cleaner org_id=user-1 msg="deleted block" block=01DTVP434PA9VFXSW2JKB3392D`,
		`level=info component=cleaner org_id=user-1 msg="deleted blocks for tenant marked for deletion" deletedBlocks=1`,
		`level=info component=cleaner org_id=user-1 msg="updating finished time in tenant deletion mark"`,
		`level=info component=cleaner msg="successfully completed blocks cleanup and maintenance"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=info component=compactor msg="discovered users from bucket" users=1`,
		`level=debug component=compactor msg="skipping user because it is marked for deletion" user=user-1`,
	}, removeIgnoredLogs(strings.Split(strings.TrimSpace(logs.String()), "\n")))

	// Instead of testing for shipper metrics, we only check our metrics here.
	// Real shipper metrics are too variable to embed into a test.
	testedMetrics := []string{
		"cortex_compactor_runs_started_total", "cortex_compactor_runs_completed_total", "cortex_compactor_runs_failed_total",
		"cortex_compactor_blocks_cleaned_total", "cortex_compactor_block_cleanup_failures_total", "cortex_compactor_blocks_marked_for_deletion_total",
		"cortex_compactor_block_cleanup_started_total", "cortex_compactor_block_cleanup_completed_total", "cortex_compactor_block_cleanup_failed_total",
		"cortex_bucket_blocks_count", "cortex_bucket_blocks_marked_for_deletion_count", "cortex_bucket_index_last_successful_update_timestamp_seconds",
		"cortex_compactor_blocks_marked_for_no_compaction_total",
	}
	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
		# TYPE cortex_compactor_runs_started_total counter
		# HELP cortex_compactor_runs_started_total Total number of compaction runs started.
		cortex_compactor_runs_started_total 1

		# TYPE cortex_compactor_runs_completed_total counter
		# HELP cortex_compactor_runs_completed_total Total number of compaction runs successfully completed.
		cortex_compactor_runs_completed_total 1

		# TYPE cortex_compactor_runs_failed_total counter
		# HELP cortex_compactor_runs_failed_total Total number of compaction runs failed.
		cortex_compactor_runs_failed_total 0

		# TYPE cortex_compactor_block_cleanup_failures_total counter
		# HELP cortex_compactor_block_cleanup_failures_total Total number of blocks failed to be deleted.
		cortex_compactor_block_cleanup_failures_total 0

		# HELP cortex_compactor_blocks_cleaned_total Total number of blocks deleted.
		# TYPE cortex_compactor_blocks_cleaned_total counter
		cortex_compactor_blocks_cleaned_total 1

		# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
		# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
		cortex_compactor_blocks_marked_for_deletion_total{reason="compaction"} 0
		cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total 1

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total 0

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0
	`), testedMetrics...))
}

func TestCompactor_ShouldSkipOutOrOrderBlocks(t *testing.T) {
	bucketClient, tmpDir := cortex_storage_testutil.PrepareFilesystemBucket(t)
	bucketClient = bucketindex.BucketWithGlobalMarkers(bucketClient)

	b1 := createTSDBBlock(t, bucketClient, "user-1", 10, 20, map[string]string{"__name__": "Teste"})
	b2 := createTSDBBlock(t, bucketClient, "user-1", 20, 30, map[string]string{"__name__": "Teste"})

	err := testutil.PutOutOfOrderIndex(path.Join(tmpDir, "user-1", b1.String()), 10, 20)
	require.NoError(t, err)

	cfg := prepareConfig()
	cfg.SkipBlocksWithOutOfOrderChunksEnabled = true
	c, tsdbCompac, tsdbPlanner, _, registry := prepare(t, cfg, bucketClient, nil)

	tsdbCompac.On("Compact", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(b1, nil)

	tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{
		{
			BlockMeta: tsdb.BlockMeta{
				ULID:    b1,
				MinTime: 10,
				MaxTime: 20,
			},
		},
		{
			BlockMeta: tsdb.BlockMeta{
				ULID:    b2,
				MinTime: 20,
				MaxTime: 30,
			},
		},
	}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 5*time.Second, true, func() interface{} {
		if _, err := os.Stat(path.Join(tmpDir, "user-1", b1.String(), "no-compact-mark.json")); err == nil {
			return true
		}
		return false
	})

	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
			# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
			# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
			cortex_compactor_blocks_marked_for_no_compaction_total 1
		`), "cortex_compactor_blocks_marked_for_no_compaction_total"))
}

func TestCompactor_ShouldCompactAllUsersOnShardingEnabledButOnlyOneInstanceRunning(t *testing.T) {
	t.Parallel()

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockExists(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01FN6CDF3PNEWWRY5MPGJPE3EX"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ", "user-2/01FN3V83ABR9992RF8WRJZ76ZQ"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json", mockBlockMetaJSON("01FN3V83ABR9992RF8WRJZ76ZQ"), nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-2/bucket-index.json.gz", "", nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-2/bucket-index.json.gz", nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfig()
	cfg.ShardingEnabled = true
	cfg.ShardingRing.InstanceID = "compactor-1"
	cfg.ShardingRing.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.KVStore.Mock = ringStore

	c, _, tsdbPlanner, logs, _ := prepare(t, cfg, bucketClient, nil)

	// Mock the planner as if there's no compaction to do,
	// in order to simplify tests (all in all, we just want to
	// test our logic and not TSDB compactor which we expect to
	// be already tested).
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 5*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Ensure a plan has been executed for the blocks of each user.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 2)

	assert.ElementsMatch(t, []string{
		`level=info component=compactor msg="waiting until compactor is ACTIVE in the ring"`,
		`level=info component=compactor msg="compactor is ACTIVE in the ring"`,
		`level=info component=cleaner msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-1 msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-1 msg="completed blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-2 msg="started blocks cleanup and maintenance"`,
		`level=info component=cleaner org_id=user-2 msg="completed blocks cleanup and maintenance"`,
		`level=info component=cleaner msg="successfully completed blocks cleanup and maintenance"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=info component=compactor msg="discovered users from bucket" users=2`,
		`level=info component=compactor msg="starting compaction of user blocks" user=user-1`,
		`level=info component=compactor org_id=user-1 msg="start sync of metas"`,
		`level=info component=compactor org_id=user-1 msg="start of GC"`,
		`level=info component=compactor org_id=user-1 msg="start of compactions"`,
		`level=info component=compactor org_id=user-1 msg="compaction iterations done"`,
		`level=info component=compactor msg="successfully compacted user blocks" user=user-1`,
		`level=info component=compactor msg="starting compaction of user blocks" user=user-2`,
		`level=info component=compactor org_id=user-2 msg="start sync of metas"`,
		`level=info component=compactor org_id=user-2 msg="start of GC"`,
		`level=info component=compactor org_id=user-2 msg="start of compactions"`,
		`level=info component=compactor org_id=user-2 msg="compaction iterations done"`,
		`level=info component=compactor msg="successfully compacted user blocks" user=user-2`,
	}, removeIgnoredLogs(strings.Split(strings.TrimSpace(logs.String()), "\n")))
}

func TestCompactor_ShouldCompactOnlyUsersOwnedByTheInstanceOnShardingEnabledAndMultipleInstancesRunning(t *testing.T) {
	t.Parallel()

	numUsers := 100

	// Setup user IDs
	userIDs := make([]string, 0, numUsers)
	for i := 1; i <= numUsers; i++ {
		userIDs = append(userIDs, fmt.Sprintf("user-%d", i))
	}

	// Mock the bucket to contain all users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", userIDs, nil)
	for _, userID := range userIDs {
		bucketClient.MockIter(userID+"/", []string{userID + "/01DTVP434PA9VFXSW2JKB3392D"}, nil)
		bucketClient.MockIter(userID+"/markers/", nil, nil)
		bucketClient.MockExists(path.Join(userID, cortex_tsdb.TenantDeletionMarkPath), false, nil)
		bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
		bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
		bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
		bucketClient.MockGet(userID+"/bucket-index.json.gz", "", nil)
		bucketClient.MockUpload(userID+"/bucket-index.json.gz", nil)
	}

	// Create a shared KV Store
	kvstore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create two compactors
	var compactors []*Compactor
	var logs []*concurrency.SyncBuffer

	for i := 1; i <= 2; i++ {
		cfg := prepareConfig()
		cfg.ShardingEnabled = true
		cfg.ShardingRing.InstanceID = fmt.Sprintf("compactor-%d", i)
		cfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
		cfg.ShardingRing.WaitStabilityMinDuration = 3 * time.Second
		cfg.ShardingRing.WaitStabilityMaxDuration = 10 * time.Second
		cfg.ShardingRing.KVStore.Mock = kvstore

		c, _, tsdbPlanner, l, _ := prepare(t, cfg, bucketClient, nil)
		defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

		compactors = append(compactors, c)
		logs = append(logs, l)

		// Mock the planner as if there's no compaction to do,
		// in order to simplify tests (all in all, we just want to
		// test our logic and not TSDB compactor which we expect to
		// be already tested).
		tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)
	}

	// Start all compactors
	for _, c := range compactors {
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	}

	// Wait until a run has been completed on each compactor
	for _, c := range compactors {
		cortex_testutil.Poll(t, 10*time.Second, 1.0, func() interface{} {
			return prom_testutil.ToFloat64(c.compactionRunsCompleted)
		})
	}

	// Ensure that each user has been compacted by the correct instance
	for _, userID := range userIDs {
		_, l, err := findCompactorByUserID(compactors, logs, userID)
		require.NoError(t, err)
		assert.Contains(t, l.String(), fmt.Sprintf(`level=info component=compactor msg="successfully compacted user blocks" user=%s`, userID))
	}
}

func TestCompactor_ShouldCompactOnlyShardsOwnedByTheInstanceOnShardingEnabledWithShuffleShardingAndMultipleInstancesRunning(t *testing.T) {
	t.Parallel()

	numUsers := 3

	// Setup user IDs
	userIDs := make([]string, 0, numUsers)
	for i := 1; i <= numUsers; i++ {
		userIDs = append(userIDs, fmt.Sprintf("user-%d", i))
	}

	startTime := int64(1574776800000)
	// Define blocks mapping block IDs to start and end times
	blocks := map[string]map[string]int64{
		"01DTVP434PA9VFXSW2JKB3392D": {
			"startTime": startTime,
			"endTime":   startTime + time.Hour.Milliseconds()*2,
		},
		"01DTVP434PA9VFXSW2JKB3392E": {
			"startTime": startTime,
			"endTime":   startTime + time.Hour.Milliseconds()*2,
		},
		"01DTVP434PA9VFXSW2JKB3392F": {
			"startTime": startTime + time.Hour.Milliseconds()*2,
			"endTime":   startTime + time.Hour.Milliseconds()*4,
		},
		"01DTVP434PA9VFXSW2JKB3392G": {
			"startTime": startTime + time.Hour.Milliseconds()*2,
			"endTime":   startTime + time.Hour.Milliseconds()*4,
		},
		// Add another new block as the final block so that the previous groups will be planned for compaction
		"01DTVP434PA9VFXSW2JKB3392H": {
			"startTime": startTime + time.Hour.Milliseconds()*4,
			"endTime":   startTime + time.Hour.Milliseconds()*6,
		},
	}

	// Mock the bucket to contain all users, each one with five blocks, 2 sets of overlapping blocks and 1 separate block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", userIDs, nil)

	// Keys with a value greater than 1 will be groups that should be compacted
	groupHashes := make(map[uint32]int)
	for _, userID := range userIDs {
		blockDirectory := []string{}

		for blockID, blockTimes := range blocks {
			bucketClient.MockGet(userID+"/"+blockID+"/meta.json", mockBlockMetaJSONWithTime(blockID, userID, blockTimes["startTime"], blockTimes["endTime"]), nil)
			bucketClient.MockGet(userID+"/"+blockID+"/deletion-mark.json", "", nil)
			bucketClient.MockGet(userID+"/"+blockID+"/no-compact-mark.json", "", nil)
			blockDirectory = append(blockDirectory, userID+"/"+blockID)

			// Get all of the unique group hashes so that they can be used to ensure all groups were compacted
			groupHash := hashGroup(userID, blockTimes["startTime"], blockTimes["endTime"])
			groupHashes[groupHash]++
		}

		bucketClient.MockIter(userID+"/", blockDirectory, nil)
		bucketClient.MockIter(userID+"/markers/", nil, nil)
		bucketClient.MockExists(path.Join(userID, cortex_tsdb.TenantDeletionMarkPath), false, nil)
		bucketClient.MockGet(userID+"/bucket-index.json.gz", "", nil)
		bucketClient.MockUpload(userID+"/bucket-index.json.gz", nil)
	}

	// Create a shared KV Store
	kvstore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create four compactors
	var compactors []*Compactor
	var logs []*concurrency.SyncBuffer

	for i := 1; i <= 4; i++ {
		cfg := prepareConfig()
		cfg.ShardingEnabled = true
		cfg.ShardingStrategy = util.ShardingStrategyShuffle
		cfg.ShardingRing.InstanceID = fmt.Sprintf("compactor-%d", i)
		cfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
		cfg.ShardingRing.WaitStabilityMinDuration = 3 * time.Second
		cfg.ShardingRing.WaitStabilityMaxDuration = 10 * time.Second
		cfg.ShardingRing.KVStore.Mock = kvstore

		limits := &validation.Limits{}
		flagext.DefaultValues(limits)
		limits.CompactorTenantShardSize = 3

		c, _, tsdbPlanner, l, _ := prepare(t, cfg, bucketClient, limits)
		defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

		compactors = append(compactors, c)
		logs = append(logs, l)

		// Mock the planner as if there's no compaction to do,
		// in order to simplify tests (all in all, we just want to
		// test our logic and not TSDB compactor which we expect to
		// be already tested).
		tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)
	}

	// Start all compactors
	for _, c := range compactors {
		require.NoError(t, c.StartAsync(context.Background()))
	}
	// Wait for all the compactors to get into the Running state without errors.
	// Cannot use StartAndAwaitRunning as this would cause the compactions to start before
	// all the compactors are initialized
	for _, c := range compactors {
		require.NoError(t, c.AwaitRunning(context.Background()))
	}

	// Wait until a run has been completed on each compactor
	for _, c := range compactors {
		cortex_testutil.Poll(t, 60*time.Second, 1.0, func() interface{} {
			return prom_testutil.ToFloat64(c.compactionRunsCompleted)
		})
	}

	// Ensure that each group was only compacted by exactly one compactor
	for groupHash, blockCount := range groupHashes {

		l, found, err := checkLogsForCompaction(compactors, logs, groupHash)
		require.NoError(t, err)

		// If the blockCount < 2 then the group shouldn't have been compacted, therefore not found in the logs
		if blockCount < 2 {
			assert.False(t, found)
		} else {
			assert.Contains(t, l.String(), fmt.Sprintf(`msg="found compactable group for user" group_hash=%d`, groupHash))

		}
	}
}

// checkLogsForCompaction checks the logs to see if a compaction has happened on the groupHash,
// if there has been a compaction it will return the logs of the compactor that handled the group
// and will return true. Otherwise this function will return a nil value for the logs and false
// as the group was not compacted
func checkLogsForCompaction(compactors []*Compactor, logs []*concurrency.SyncBuffer, groupHash uint32) (*concurrency.SyncBuffer, bool, error) {
	var log *concurrency.SyncBuffer

	// Make sure that the group_hash is only owned by a single compactor
	for _, l := range logs {
		owned := strings.Contains(l.String(), fmt.Sprintf(`msg="found compactable group for user" group_hash=%d`, groupHash))

		// Ensure the group is not owned by multiple compactors
		if owned && log != nil {
			return nil, false, fmt.Errorf("group with group_hash=%d owned by multiple compactors", groupHash)
		}
		if owned {
			log = l
		}
	}

	// Return an false if we've not been able to find it
	if log == nil {
		return nil, false, nil
	}

	return log, true, nil
}

func createTSDBBlock(t *testing.T, bkt objstore.Bucket, userID string, minT, maxT int64, externalLabels map[string]string) ulid.ULID {
	// Create a temporary dir for TSDB.
	tempDir := t.TempDir()

	// Create a temporary dir for the snapshot.
	snapshotDir := t.TempDir()

	// Create a new TSDB.
	db, err := tsdb.Open(tempDir, nil, nil, &tsdb.Options{
		MinBlockDuration:  int64(2 * 60 * 60 * 1000), // 2h period
		MaxBlockDuration:  int64(2 * 60 * 60 * 1000), // 2h period
		RetentionDuration: int64(15 * 86400 * 1000),  // 15 days
	}, nil)
	require.NoError(t, err)

	db.DisableCompactions()

	// Append a sample at the beginning and one at the end of the time range.
	for i, ts := range []int64{minT, maxT - 1} {
		lbls := labels.Labels{labels.Label{Name: "series_id", Value: strconv.Itoa(i)}}

		app := db.Appender(context.Background())
		_, err := app.Append(0, lbls, ts, float64(i))
		require.NoError(t, err)

		err = app.Commit()
		require.NoError(t, err)
	}

	require.NoError(t, db.Compact())
	require.NoError(t, db.Snapshot(snapshotDir, true))

	// Look for the created block (we expect one).
	entries, err := ioutil.ReadDir(snapshotDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, entries[0].IsDir())

	blockID, err := ulid.Parse(entries[0].Name())
	require.NoError(t, err)

	// Inject Thanos external labels to the block.
	meta := metadata.Thanos{
		Labels: externalLabels,
		Source: "test",
	}
	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(snapshotDir, blockID.String()), meta, nil)
	require.NoError(t, err)

	// Copy the block files to the bucket.
	srcRoot := filepath.Join(snapshotDir, blockID.String())
	require.NoError(t, filepath.Walk(srcRoot, func(file string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Read the file content in memory.
		content, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}

		// Upload it to the bucket.
		relPath, err := filepath.Rel(srcRoot, file)
		if err != nil {
			return err
		}

		return bkt.Upload(context.Background(), path.Join(userID, blockID.String(), relPath), bytes.NewReader(content))
	}))

	return blockID
}

func createDeletionMark(t *testing.T, bkt objstore.Bucket, userID string, blockID ulid.ULID, deletionTime time.Time) {
	content := mockDeletionMarkJSON(blockID.String(), deletionTime)
	blockPath := path.Join(userID, blockID.String())
	markPath := path.Join(blockPath, metadata.DeletionMarkFilename)

	require.NoError(t, bkt.Upload(context.Background(), markPath, strings.NewReader(content)))
}

func createNoCompactionMark(t *testing.T, bkt objstore.Bucket, userID string, blockID ulid.ULID) {
	content := mockNoCompactBlockJSON(blockID.String())
	blockPath := path.Join(userID, blockID.String())
	markPath := path.Join(blockPath, metadata.NoCompactMarkFilename)

	require.NoError(t, bkt.Upload(context.Background(), markPath, strings.NewReader(content)))
}

func findCompactorByUserID(compactors []*Compactor, logs []*concurrency.SyncBuffer, userID string) (*Compactor, *concurrency.SyncBuffer, error) {
	var compactor *Compactor
	var log *concurrency.SyncBuffer

	for i, c := range compactors {
		owned, err := c.ownUserForCompaction(userID)
		if err != nil {
			return nil, nil, err
		}

		// Ensure the user is not owned by multiple compactors
		if owned && compactor != nil {
			return nil, nil, fmt.Errorf("user %s owned by multiple compactors", userID)
		}
		if owned {
			compactor = c
			log = logs[i]
		}
	}

	// Return an error if we've not been able to find it
	if compactor == nil {
		return nil, nil, fmt.Errorf("user %s not owned by any compactor", userID)
	}

	return compactor, log, nil
}

func removeIgnoredLogs(input []string) []string {
	ignoredLogStringsMap := map[string]struct{}{
		// Since we moved to the component logger from the global logger for the ring these lines are now expected, but are just ring setup information.
		`level=info component=compactor msg="ring doesn't exist in KV store yet"`:                                                                                 {},
		`level=info component=compactor msg="not loading tokens from file, tokens file path is empty"`:                                                            {},
		`level=info component=compactor msg="instance not found in ring, adding with no tokens" ring=compactor`:                                                   {},
		`level=debug component=compactor msg="JoinAfter expired" ring=compactor`:                                                                                  {},
		`level=info component=compactor msg="auto-joining cluster after timeout" ring=compactor`:                                                                  {},
		`level=info component=compactor msg="lifecycler loop() exited gracefully" ring=compactor`:                                                                 {},
		`level=info component=compactor msg="changing instance state from" old_state=ACTIVE new_state=LEAVING ring=compactor`:                                     {},
		`level=error component=compactor msg="failed to set state to LEAVING" ring=compactor err="Changing instance state from LEAVING -> LEAVING is disallowed"`: {},
		`level=error component=compactor msg="failed to set state to LEAVING" ring=compactor err="Changing instance state from JOINING -> LEAVING is disallowed"`: {},
		`level=debug component=compactor msg="unregistering instance from ring" ring=compactor`:                                                                   {},
		`level=info component=compactor msg="instance removed from the KV store" ring=compactor`:                                                                  {},
		`level=info component=compactor msg="observing tokens before going ACTIVE" ring=compactor`:                                                                {},
		`level=info component=compactor msg="lifecycler entering final sleep before shutdown" final_sleep=0s`:                                                     {},
	}

	out := make([]string, 0, len(input))
	durationRe := regexp.MustCompile(`\s?duration=\S+`)

	for i := 0; i < len(input); i++ {
		log := input[i]
		if strings.Contains(log, "block.MetaFetcher") || strings.Contains(log, "block.BaseFetcher") {
			continue
		}

		if _, exists := ignoredLogStringsMap[log]; exists {
			continue
		}

		// Remove any duration from logs.
		log = durationRe.ReplaceAllString(log, "")

		out = append(out, log)
	}

	return out
}

func prepareConfig() Config {
	compactorCfg := Config{}
	flagext.DefaultValues(&compactorCfg)

	compactorCfg.retryMinBackoff = 0
	compactorCfg.retryMaxBackoff = 0

	// The migration is tested in a dedicated test.
	compactorCfg.BlockDeletionMarksMigrationEnabled = false

	// Do not wait for ring stability by default, in order to speed up tests.
	compactorCfg.ShardingRing.WaitStabilityMinDuration = 0
	compactorCfg.ShardingRing.WaitStabilityMaxDuration = 0

	// Set lower timeout for waiting on compactor to become ACTIVE in the ring for unit tests
	compactorCfg.ShardingRing.WaitActiveInstanceTimeout = 5 * time.Second

	return compactorCfg
}

func prepare(t *testing.T, compactorCfg Config, bucketClient objstore.Bucket, limits *validation.Limits) (*Compactor, *tsdbCompactorMock, *tsdbPlannerMock, *concurrency.SyncBuffer, prometheus.Gatherer) {
	storageCfg := cortex_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&storageCfg)

	// Create a temporary directory for compactor data.
	compactorCfg.DataDir = t.TempDir()

	tsdbCompactor := &tsdbCompactorMock{}
	tsdbPlanner := &tsdbPlannerMock{
		noCompactMarkFilters: []*compact.GatherNoCompactionMarkFilter{},
	}
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)
	registry := prometheus.NewRegistry()

	if limits == nil {
		limits = &validation.Limits{}
		flagext.DefaultValues(limits)
	}

	overrides, err := validation.NewOverrides(*limits, nil)
	require.NoError(t, err)

	bucketClientFactory := func(ctx context.Context) (objstore.Bucket, error) {
		return bucketClient, nil
	}

	blocksCompactorFactory := func(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (compact.Compactor, PlannerFactory, error) {
		return tsdbCompactor,
			func(_ log.Logger, _ Config, noCompactMarkFilter *compact.GatherNoCompactionMarkFilter) compact.Planner {
				tsdbPlanner.noCompactMarkFilters = append(tsdbPlanner.noCompactMarkFilters, noCompactMarkFilter)
				return tsdbPlanner
			},
			nil
	}

	var blocksGrouperFactory BlocksGrouperFactory
	if compactorCfg.ShardingStrategy == util.ShardingStrategyShuffle {
		blocksGrouperFactory = ShuffleShardingGrouperFactory
	} else {
		blocksGrouperFactory = DefaultBlocksGrouperFactory
	}

	c, err := newCompactor(compactorCfg, storageCfg, overrides, logger, registry, bucketClientFactory, blocksGrouperFactory, blocksCompactorFactory, overrides)
	require.NoError(t, err)

	return c, tsdbCompactor, tsdbPlanner, logs, registry
}

type tsdbCompactorMock struct {
	mock.Mock
}

func (m *tsdbCompactorMock) Plan(dir string) ([]string, error) {
	args := m.Called(dir)
	return args.Get(0).([]string), args.Error(1)
}

func (m *tsdbCompactorMock) Write(dest string, b tsdb.BlockReader, mint, maxt int64, parent *tsdb.BlockMeta) (ulid.ULID, error) {
	args := m.Called(dest, b, mint, maxt, parent)
	return args.Get(0).(ulid.ULID), args.Error(1)
}

func (m *tsdbCompactorMock) Compact(dest string, dirs []string, open []*tsdb.Block) (ulid.ULID, error) {
	args := m.Called(dest, dirs, open)
	return args.Get(0).(ulid.ULID), args.Error(1)
}

type tsdbPlannerMock struct {
	mock.Mock
	noCompactMarkFilters []*compact.GatherNoCompactionMarkFilter
}

func (m *tsdbPlannerMock) Plan(ctx context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	args := m.Called(ctx, metasByMinTime)
	return args.Get(0).([]*metadata.Meta), args.Error(1)
}

func (m *tsdbPlannerMock) getNoCompactBlocks() []string {

	result := []string{}

	for _, noCompactMarkFilter := range m.noCompactMarkFilters {
		for _, mark := range noCompactMarkFilter.NoCompactMarkedBlocks() {
			result = append(result, mark.ID.String())
		}
	}

	return result
}

func mockBlockMetaJSON(id string) string {
	meta := tsdb.BlockMeta{
		Version: 1,
		ULID:    ulid.MustParse(id),
		MinTime: 1574776800000,
		MaxTime: 1574784000000,
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{ulid.MustParse(id)},
		},
	}

	content, err := json.Marshal(meta)
	if err != nil {
		panic("failed to marshal mocked block meta")
	}

	return string(content)
}

func mockNoCompactBlockJSON(id string) string {
	meta := metadata.NoCompactMark{
		Version:       1,
		ID:            ulid.MustParse(id),
		NoCompactTime: time.Now().Unix(),
		Details:       "yolo",
		Reason:        "testing",
	}

	content, err := json.Marshal(meta)
	if err != nil {
		panic("failed to marshal mocked block no compact mark")
	}

	return string(content)
}

func mockDeletionMarkJSON(id string, deletionTime time.Time) string {
	meta := metadata.DeletionMark{
		Version:      metadata.DeletionMarkVersion1,
		ID:           ulid.MustParse(id),
		DeletionTime: deletionTime.Unix(),
	}

	content, err := json.Marshal(meta)
	if err != nil {
		panic("failed to marshal mocked block deletion mark")
	}

	return string(content)
}

func mockBlockMetaJSONWithTime(id string, orgID string, minTime int64, maxTime int64) string {
	meta := metadata.Meta{
		Thanos: metadata.Thanos{
			Labels: map[string]string{"__org_id__": orgID},
		},
	}

	meta.BlockMeta = tsdb.BlockMeta{
		Version: 1,
		ULID:    ulid.MustParse(id),
		MinTime: minTime,
		MaxTime: maxTime,
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{ulid.MustParse(id)},
		},
	}

	content, err := json.Marshal(meta)
	if err != nil {
		panic("failed to marshal mocked block meta")
	}

	return string(content)
}

func TestCompactor_DeleteLocalSyncFiles(t *testing.T) {
	numUsers := 10

	// Setup user IDs
	userIDs := make([]string, 0, numUsers)
	for i := 1; i <= numUsers; i++ {
		userIDs = append(userIDs, fmt.Sprintf("user-%d", i))
	}

	inmem := objstore.NewInMemBucket()
	for _, userID := range userIDs {
		id, err := ulid.New(ulid.Now(), rand.Reader)
		require.NoError(t, err)
		require.NoError(t, inmem.Upload(context.Background(), userID+"/"+id.String()+"/meta.json", strings.NewReader(mockBlockMetaJSON(id.String()))))
	}

	// Create a shared KV Store
	kvstore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create two compactors
	var compactors []*Compactor

	for i := 1; i <= 2; i++ {
		cfg := prepareConfig()
		cfg.CompactionInterval = 10 * time.Minute // We will only call compaction manually.

		cfg.ShardingEnabled = true
		cfg.ShardingRing.InstanceID = fmt.Sprintf("compactor-%d", i)
		cfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
		cfg.ShardingRing.WaitStabilityMinDuration = 3 * time.Second
		cfg.ShardingRing.WaitStabilityMaxDuration = 10 * time.Second
		cfg.ShardingRing.KVStore.Mock = kvstore

		// Each compactor will get its own temp dir for storing local files.
		c, _, tsdbPlanner, _, _ := prepare(t, cfg, inmem, nil)
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
		})

		compactors = append(compactors, c)

		// Mock the planner as if there's no compaction to do,
		// in order to simplify tests (all in all, we just want to
		// test our logic and not TSDB compactor which we expect to
		// be already tested).
		tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)
	}

	require.Equal(t, 2, len(compactors))
	c1 := compactors[0]
	c2 := compactors[1]

	// Start first compactor
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c1))

	// Wait until a run has been completed on first compactor. This happens as soon as compactor starts.
	cortex_testutil.Poll(t, 10*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c1.compactionRunsCompleted)
	})

	require.NoError(t, os.Mkdir(c1.metaSyncDirForUser("new-user"), 0600))

	// Verify that first compactor has synced all the users, plus there is one extra we have just created.
	require.Equal(t, numUsers+1, len(c1.listTenantsWithMetaSyncDirectories()))

	// Now start second compactor, and wait until it runs compaction.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c2))
	cortex_testutil.Poll(t, 10*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c2.compactionRunsCompleted)
	})

	// Let's check how many users second compactor has.
	c2Users := len(c2.listTenantsWithMetaSyncDirectories())
	require.NotZero(t, c2Users)

	// Force new compaction cycle on first compactor. It will run the cleanup of un-owned users at the end of compaction cycle.
	c1.compactUsers(context.Background())
	c1Users := len(c1.listTenantsWithMetaSyncDirectories())

	// Now compactor 1 should have cleaned old sync files.
	require.NotEqual(t, numUsers, c1Users)
	require.Equal(t, numUsers, c1Users+c2Users)
}

func TestCompactor_ShouldFailCompactionOnTimeout(t *testing.T) {
	t.Parallel()

	// Mock the bucket
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{}, nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfig()
	cfg.ShardingEnabled = true
	cfg.ShardingRing.InstanceID = "compactor-1"
	cfg.ShardingRing.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.KVStore.Mock = ringStore

	// Set ObservePeriod to longer than the timeout period to mock a timeout while waiting on ring to become ACTIVE
	cfg.ShardingRing.ObservePeriod = time.Second * 10

	c, _, _, logs, _ := prepare(t, cfg, bucketClient, nil)

	// Try to start the compactor with a bad consul kv-store. The
	err := services.StartAndAwaitRunning(context.Background(), c)

	// Assert that the compactor timed out
	assert.Equal(t, context.DeadlineExceeded, err)

	assert.ElementsMatch(t, []string{
		`level=info component=compactor msg="waiting until compactor is ACTIVE in the ring"`,
		`level=error component=compactor msg="compactor failed to become ACTIVE in the ring" err="context deadline exceeded"`,
	}, removeIgnoredLogs(strings.Split(strings.TrimSpace(logs.String()), "\n")))
}
