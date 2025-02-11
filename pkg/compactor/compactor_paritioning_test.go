package compactor

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
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

func TestPartitionConfig_ShouldSupportYamlConfig(t *testing.T) {
	yamlCfg := `
block_ranges: [2h, 48h]
consistency_delay: 1h
block_sync_concurrency: 123
data_dir: /tmp
compaction_interval: 15m
compaction_retries: 123
compaction_strategy: partitioning
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
	assert.Equal(t, util.CompactionStrategyPartitioning, cfg.CompactionStrategy)
}

func TestPartitionConfig_ShouldSupportCliFlags(t *testing.T) {
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
		"-compactor.compaction-strategy=partitioning",
	}))

	assert.Equal(t, cortex_tsdb.DurationList{2 * time.Hour, 48 * time.Hour}, cfg.BlockRanges)
	assert.Equal(t, time.Hour, cfg.ConsistencyDelay)
	assert.Equal(t, 123, cfg.BlockSyncConcurrency)
	assert.Equal(t, "/tmp", cfg.DataDir)
	assert.Equal(t, 15*time.Minute, cfg.CompactionInterval)
	assert.Equal(t, 123, cfg.CompactionRetries)
	assert.Equal(t, util.CompactionStrategyPartitioning, cfg.CompactionStrategy)
}

func TestPartitionConfig_Validate(t *testing.T) {
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
		"should fail with duration values of zero": {
			setup: func(cfg *Config) {
				cfg.BlockRanges = cortex_tsdb.DurationList{2 * time.Hour, 0, 24 * time.Hour, 30 * time.Hour}
			},
			initLimits: func(_ *validation.Limits) {},
			expected:   errors.Errorf("compactor block range period cannot be zero").Error(),
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
		"should pass with valid compaction strategy config": {
			setup: func(cfg *Config) {
				cfg.ShardingEnabled = true
				cfg.CompactionStrategy = util.CompactionStrategyPartitioning
			},
			initLimits: func(_ *validation.Limits) {},
			expected:   "",
		},
		"should fail with bad compaction strategy": {
			setup: func(cfg *Config) {
				cfg.CompactionStrategy = "dummy"
			},
			initLimits: func(_ *validation.Limits) {},
			expected:   errInvalidCompactionStrategy.Error(),
		},
		"should fail with partitioning compaction strategy but sharding disabled": {
			setup: func(cfg *Config) {
				cfg.ShardingEnabled = false
				cfg.CompactionStrategy = util.CompactionStrategyPartitioning
			},
			initLimits: func(_ *validation.Limits) {},
			expected:   errInvalidCompactionStrategyPartitioning.Error(),
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

func TestPartitionCompactor_SkipCompactionWhenCmkError(t *testing.T) {
	t.Parallel()
	userID := "user-1"

	ss := bucketindex.Status{Status: bucketindex.CustomerManagedKeyError, Version: bucketindex.SyncStatusFileVersion}
	content, err := json.Marshal(ss)
	require.NoError(t, err)

	// No user blocks stored in the bucket.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{userID}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockIter(userID+"/", []string{userID + "/01DTVP434PA9VFXSW2JKB3392D"}, nil)
	bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockIter(userID+"/markers/", nil, nil)
	bucketClient.MockGet(userID+"/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload(userID+"/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete(userID+"/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockGet(userID+"/bucket-index-sync-status.json", string(content), nil)
	bucketClient.MockGet(userID+"/bucket-index.json.gz", "", nil)
	bucketClient.MockUpload(userID+"/bucket-index-sync-status.json", nil)
	bucketClient.MockUpload(userID+"/bucket-index.json.gz", nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath(userID), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath(userID), false, nil)
	bucketClient.MockIter(userID+"/"+PartitionedGroupDirectory, nil, nil)

	cfg := prepareConfigForPartitioning()
	c, _, _, logs, _ := prepareForPartitioning(t, cfg, bucketClient, nil, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
	assert.Contains(t, strings.Split(strings.TrimSpace(logs.String()), "\n"), `level=info component=compactor msg="skipping compactUser due CustomerManagedKeyError" user=user-1`)
}

func TestPartitionCompactor_ShouldDoNothingOnNoUserBlocks(t *testing.T) {
	t.Parallel()

	// No user blocks stored in the bucket.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	cfg := prepareConfigForPartitioning()
	c, _, _, logs, registry := prepareForPartitioning(t, cfg, bucketClient, nil, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	assert.Equal(t, prom_testutil.ToFloat64(c.CompactionRunInterval), cfg.CompactionInterval.Seconds())

	assert.ElementsMatch(t, []string{
		`level=info component=compactor msg="compactor started"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=info component=compactor msg="discovered users from bucket" users=0`,
	}, removeIgnoredLogs(strings.Split(strings.TrimSpace(logs.String()), "\n")))

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

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total{user_status="active"} 1
		cortex_compactor_block_cleanup_started_total{user_status="deleted"} 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total{user_status="active"} 1
		cortex_compactor_block_cleanup_completed_total{user_status="deleted"} 1
	`),
		"cortex_compactor_runs_started_total",
		"cortex_compactor_runs_completed_total",
		"cortex_compactor_runs_failed_total",
		"cortex_compactor_garbage_collected_blocks_total",
		"cortex_compactor_block_cleanup_failures_total",
		"cortex_compactor_blocks_cleaned_total",
		"cortex_compactor_blocks_marked_for_deletion_total",
		"cortex_compactor_blocks_marked_for_no_compaction_total",
		"cortex_compactor_block_cleanup_started_total",
		"cortex_compactor_block_cleanup_completed_total",
		"cortex_compactor_block_cleanup_failed_total",
	))
}

func TestPartitionCompactor_ShouldRetryCompactionOnFailureWhileDiscoveringUsersFromBucket(t *testing.T) {
	t.Parallel()

	// Fail to iterate over the bucket while discovering users.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("__markers__", nil, errors.New("failed to iterate the bucket"))
	bucketClient.MockIter("", nil, errors.New("failed to iterate the bucket"))

	c, _, _, logs, registry := prepareForPartitioning(t, prepareConfigForPartitioning(), bucketClient, nil, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until all retry attempts have completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsFailed)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Ensure the bucket iteration has been retried the configured number of times.
	bucketClient.AssertNumberOfCalls(t, "Iter", 1+3)

	assert.ElementsMatch(t, []string{
		`level=error component=cleaner msg="failed to scan users on startup" err="failed to discover users from bucket: failed to iterate the bucket"`,
		`level=info component=compactor msg="compactor started"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=error component=compactor msg="failed to discover users from bucket" err="failed to iterate the bucket"`,
	}, removeIgnoredLogs(strings.Split(strings.TrimSpace(logs.String()), "\n")))

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

		# TYPE cortex_compactor_block_cleanup_failures_total counter
		# HELP cortex_compactor_block_cleanup_failures_total Total number of blocks failed to be deleted.
		cortex_compactor_block_cleanup_failures_total 0

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total{user_status="active"} 1
		cortex_compactor_block_cleanup_failed_total{user_status="deleted"} 1

		# HELP cortex_compactor_blocks_cleaned_total Total number of blocks deleted.
		# TYPE cortex_compactor_blocks_cleaned_total counter
		cortex_compactor_blocks_cleaned_total 0

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0
	`),
		"cortex_compactor_runs_started_total",
		"cortex_compactor_runs_completed_total",
		"cortex_compactor_runs_failed_total",
		"cortex_compactor_garbage_collected_blocks_total",
		"cortex_compactor_block_cleanup_failures_total",
		"cortex_compactor_blocks_cleaned_total",
		"cortex_compactor_blocks_marked_for_deletion_total",
		"cortex_compactor_blocks_marked_for_no_compaction_total",
		"cortex_compactor_block_cleanup_started_total",
		"cortex_compactor_block_cleanup_completed_total",
		"cortex_compactor_block_cleanup_failed_total",
	))
}

func TestPartitionCompactor_ShouldIncrementCompactionErrorIfFailedToCompactASingleTenant(t *testing.T) {
	t.Parallel()

	userID := "test-user"
	partitionedGroupID := getPartitionedGroupID(userID)
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{userID}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockIter(userID+"/", []string{userID + "/01DTVP434PA9VFXSW2JKB3392D", userID + "/01FN6CDF3PNEWWRY5MPGJPE3EX", userID + "/01DTVP434PA9VFXSW2JKB3392D/meta.json", userID + "/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json"}, nil)
	bucketClient.MockIter(userID+"/markers/", nil, nil)
	bucketClient.MockGet(userID+"/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload(userID+"/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete(userID+"/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath(userID), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath(userID), false, nil)
	bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload(userID+"/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", nil)
	bucketClient.MockGet(userID+"/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet(userID+"/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet(userID+"/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", "", nil)
	bucketClient.MockGet(userID+"/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet(userID+"/01FN6CDF3PNEWWRY5MPGJPE3EX/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload(userID+"/01FN6CDF3PNEWWRY5MPGJPE3EX/partition-0-visit-mark.json", nil)
	bucketClient.MockGet(userID+"/bucket-index.json.gz", "", nil)
	bucketClient.MockUpload(userID+"/bucket-index.json.gz", nil)
	bucketClient.MockUpload(userID+"/bucket-index-sync-status.json", nil)
	bucketClient.MockGet(userID+"/partitioned-groups/"+partitionedGroupID+".json", "", nil)
	bucketClient.MockUpload(userID+"/partitioned-groups/"+partitionedGroupID+".json", nil)
	bucketClient.MockIter(userID+"/"+PartitionedGroupDirectory, nil, nil)

	c, _, tsdbPlannerMock, _, registry := prepareForPartitioning(t, prepareConfigForPartitioning(), bucketClient, nil, nil)
	tsdbPlannerMock.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, errors.New("Failed to plan"))
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until all retry attempts have completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsFailed)
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

func TestPartitionCompactor_ShouldCompactAndRemoveUserFolder(t *testing.T) {
	partitionedGroupID1 := getPartitionedGroupID("user-1")
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/partition-0-visit-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockGet("user-1/partitioned-groups/"+partitionedGroupID1+".json", "", nil)
	bucketClient.MockUpload("user-1/partitioned-groups/"+partitionedGroupID1+".json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)

	c, _, tsdbPlanner, _, _ := prepareForPartitioning(t, prepareConfigForPartitioning(), bucketClient, nil, nil)

	// Make sure the user folder is created and is being used
	// This will be called during compaction
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		_, err := os.Stat(c.compactDirForUser("user-1"))
		require.NoError(t, err)
	}).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	_, err := os.Stat(c.compactDirForUser("user-1"))
	require.True(t, os.IsNotExist(err))
}

func TestPartitionCompactor_ShouldIterateOverUsersAndRunCompaction(t *testing.T) {
	t.Parallel()

	partitionedGroupID1 := getPartitionedGroupID("user-1")
	partitionedGroupID2 := getPartitionedGroupID("user-2")

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-2"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-2"), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ", "user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", "user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockGet("user-2/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-2/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-2/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/partition-0-visit-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/partition-0-visit-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json", mockBlockMetaJSON("01FN3V83ABR9992RF8WRJZ76ZQ"), nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/partition-0-visit-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-2/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-2/bucket-index-sync-status.json", "", nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-2/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockUpload("user-2/bucket-index-sync-status.json", nil)
	bucketClient.MockGet("user-1/partitioned-groups/"+partitionedGroupID1+".json", "", nil)
	bucketClient.MockUpload("user-1/partitioned-groups/"+partitionedGroupID1+".json", nil)
	bucketClient.MockGet("user-2/partitioned-groups/"+partitionedGroupID2+".json", "", nil)
	bucketClient.MockUpload("user-2/partitioned-groups/"+partitionedGroupID2+".json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)
	bucketClient.MockIter("user-2/"+PartitionedGroupDirectory, nil, nil)

	c, _, tsdbPlanner, logs, registry := prepareForPartitioning(t, prepareConfigForPartitioning(), bucketClient, nil, nil)

	// Mock the planner as if there's no compaction to do,
	// in order to simplify tests (all in all, we just want to
	// test our logic and not TSDB compactor which we expect to
	// be already tested).
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Ensure a plan has been executed for the blocks of each user.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 2)

	assert.Len(t, tsdbPlanner.getNoCompactBlocks(), 0)

	assert.ElementsMatch(t, []string{
		`level=info component=compactor msg="compactor started"`,
		`level=info component=compactor msg="discovering users from bucket"`,
		`level=info component=compactor msg="discovered users from bucket" users=2`,
		`level=info component=compactor msg="starting compaction of user blocks" user=user-2`,
		`level=info component=compactor org_id=user-2 msg="start sync of metas"`,
		`level=info component=compactor org_id=user-2 msg="start of GC"`,
		`level=info component=compactor org_id=user-2 msg="start of compactions"`,
		`level=info component=compactor org_id=user-2 msg="compaction iterations done"`,
		`level=info component=compactor msg="successfully compacted user blocks" user=user-2`,
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
		cortex_compactor_blocks_cleaned_total 0

		# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
		# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
        cortex_compactor_blocks_marked_for_deletion_total{reason="compaction",user="user-1"} 0
        cortex_compactor_blocks_marked_for_deletion_total{reason="compaction",user="user-2"} 0
        cortex_compactor_blocks_marked_for_deletion_total{reason="retention",user="user-1"} 0
        cortex_compactor_blocks_marked_for_deletion_total{reason="retention",user="user-2"} 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total{user_status="active"} 1
		cortex_compactor_block_cleanup_started_total{user_status="deleted"} 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total{user_status="active"} 1
		cortex_compactor_block_cleanup_completed_total{user_status="deleted"} 1

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0
	`), testedMetrics...))
}

func TestPartitionCompactor_ShouldNotCompactBlocksMarkedForDeletion(t *testing.T) {
	t.Parallel()

	cfg := prepareConfigForPartitioning()
	cfg.DeletionDelay = 10 * time.Minute // Delete block after 10 minutes

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ"}, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)

	// Block that has just been marked for deletion. It will not be deleted just yet, and it also will not be compacted.
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", mockDeletionMarkJSON("01DTVP434PA9VFXSW2JKB3392D", time.Now()), nil)
	bucketClient.MockGet("user-1/markers/01DTVP434PA9VFXSW2JKB3392D-deletion-mark.json", mockDeletionMarkJSON("01DTVP434PA9VFXSW2JKB3392D", time.Now()), nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)

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

	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)

	bucketClient.MockDelete("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", nil)
	bucketClient.MockDelete("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", nil)
	bucketClient.MockDelete("user-1/markers/01DTW0ZCPDDNV4BV83Q2SV4QAZ-deletion-mark.json", nil)
	bucketClient.MockDelete("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)

	c, _, tsdbPlanner, logs, registry := prepareForPartitioning(t, cfg, bucketClient, nil, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Since both blocks are marked for deletion, none of them are going to be compacted.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 0)

	assert.ElementsMatch(t, []string{
		`level=info component=compactor msg="compactor started"`,
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
		cortex_compactor_blocks_marked_for_deletion_total{reason="compaction",user="user-1"} 0
        cortex_compactor_blocks_marked_for_deletion_total{reason="retention",user="user-1"} 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total{user_status="active"} 1
		cortex_compactor_block_cleanup_started_total{user_status="deleted"} 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total{user_status="active"} 1
		cortex_compactor_block_cleanup_completed_total{user_status="deleted"} 1

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0
	`), testedMetrics...))
}

func TestPartitionCompactor_ShouldNotCompactBlocksMarkedForSkipCompact(t *testing.T) {
	t.Parallel()

	partitionedGroupID1 := getPartitionedGroupID("user-1")
	partitionedGroupID2 := getPartitionedGroupID("user-2")
	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-2"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-2"), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ", "user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", "user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockGet("user-2/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-2/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-2/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", mockNoCompactBlockJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", mockNoCompactBlockJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/partition-0-visit-mark.json", nil)

	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/partition-0-visit-mark.json", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json", mockBlockMetaJSON("01FN3V83ABR9992RF8WRJZ76ZQ"), nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/partition-0-visit-mark.json", nil)

	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-2/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-2/bucket-index-sync-status.json", "", nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-2/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockUpload("user-2/bucket-index-sync-status.json", nil)
	bucketClient.MockGet("user-1/partitioned-groups/"+partitionedGroupID1+".json", "", nil)
	bucketClient.MockUpload("user-1/partitioned-groups/"+partitionedGroupID1+".json", nil)
	bucketClient.MockGet("user-2/partitioned-groups/"+partitionedGroupID2+".json", "", nil)
	bucketClient.MockUpload("user-2/partitioned-groups/"+partitionedGroupID2+".json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)
	bucketClient.MockIter("user-2/"+PartitionedGroupDirectory, nil, nil)

	c, _, tsdbPlanner, _, registry := prepareForPartitioning(t, prepareConfigForPartitioning(), bucketClient, nil, nil)

	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
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

func TestPartitionCompactor_ShouldNotCompactBlocksForUsersMarkedForDeletion(t *testing.T) {
	t.Parallel()

	cfg := prepareConfigForPartitioning()
	cfg.DeletionDelay = 10 * time.Minute      // Delete block after 10 minutes
	cfg.TenantCleanupDelay = 10 * time.Minute // To make sure it's not 0.

	partitionedGroupID1 := getPartitionedGroupID("user-1")
	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockIter("__markers__", []string{"__markers__/user-1/"}, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D"}, nil)
	bucketClient.MockGet(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), `{"deletion_time": 1}`, nil)
	bucketClient.MockUpload(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), nil)

	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)

	bucketClient.MockIter("user-1/01DTVP434PA9VFXSW2JKB3392D", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "user-1/01DTVP434PA9VFXSW2JKB3392D/index"}, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/index", "some index content", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", nil)
	bucketClient.MockExists("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", false, nil)

	bucketClient.MockDelete("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", nil)
	bucketClient.MockDelete("user-1/01DTVP434PA9VFXSW2JKB3392D/index", nil)
	bucketClient.MockDelete("user-1/bucket-index.json.gz", nil)
	bucketClient.MockDelete("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockGet("user-1/partitioned-groups/"+partitionedGroupID1+".json", "", nil)
	bucketClient.MockUpload("user-1/partitioned-groups/"+partitionedGroupID1+".json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)

	c, _, tsdbPlanner, logs, registry := prepareForPartitioning(t, cfg, bucketClient, nil, nil)

	// Mock the planner as if there's no compaction to do,
	// in order to simplify tests (all in all, we just want to
	// test our logic and not TSDB compactor which we expect to
	// be already tested).
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// No user is compacted, single user we have is marked for deletion.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 0)

	assert.ElementsMatch(t, []string{
		`level=info component=compactor msg="compactor started"`,
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

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total{user_status="active"} 1
		cortex_compactor_block_cleanup_started_total{user_status="deleted"} 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total{user_status="active"} 1
		cortex_compactor_block_cleanup_completed_total{user_status="deleted"} 1

		# HELP cortex_compactor_blocks_marked_for_no_compaction_total Total number of blocks marked for no compact during a compaction run.
		# TYPE cortex_compactor_blocks_marked_for_no_compaction_total counter
		cortex_compactor_blocks_marked_for_no_compaction_total 0
	`), testedMetrics...))
}

func TestPartitionCompactor_ShouldSkipOutOrOrderBlocks(t *testing.T) {
	bucketClient, tmpDir := cortex_storage_testutil.PrepareFilesystemBucket(t)
	bucketClient = bucketindex.BucketWithGlobalMarkers(bucketClient)

	b1 := createTSDBBlock(t, bucketClient, "user-1", 10, 20, map[string]string{"__name__": "Teste"})
	b2 := createTSDBBlock(t, bucketClient, "user-1", 20, 30, map[string]string{"__name__": "Teste"})

	// Read bad index file.
	indexFile, err := os.Open("testdata/out_of_order_chunks/index")
	require.NoError(t, err)
	indexFileStat, err := indexFile.Stat()
	require.NoError(t, err)

	dir := path.Join(tmpDir, "user-1", b1.String())
	outputFile, err := os.OpenFile(path.Join(dir, "index"), os.O_RDWR|os.O_TRUNC, 0755)
	require.NoError(t, err)

	n, err := io.Copy(outputFile, indexFile)
	require.NoError(t, err)
	require.Equal(t, indexFileStat.Size(), n)

	cfg := prepareConfigForPartitioning()
	cfg.SkipBlocksWithOutOfOrderChunksEnabled = true
	c, tsdbCompac, tsdbPlanner, _, registry := prepareForPartitioning(t, cfg, bucketClient, nil, nil)

	tsdbCompac.On("CompactWithBlockPopulator", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(b1, nil)

	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{
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
	cortex_testutil.Poll(t, 20*time.Second, true, func() interface{} {
		if _, err := os.Stat(path.Join(dir, "no-compact-mark.json")); err == nil {
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

func TestPartitionCompactor_ShouldCompactAllUsersOnShardingEnabledButOnlyOneInstanceRunning(t *testing.T) {
	t.Parallel()

	partitionedGroupID1 := getPartitionedGroupID("user-1")
	partitionedGroupID2 := getPartitionedGroupID("user-2")
	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-2"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-2"), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ", "user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", "user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockGet("user-2/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-2/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-2/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/partition-0-visit-mark.json", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/partition-0-visit-mark.json", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/meta.json", mockBlockMetaJSON("01FN3V83ABR9992RF8WRJZ76ZQ"), nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/partition-0-visit-mark.json", "", nil)
	bucketClient.MockUpload("user-2/01FN3V83ABR9992RF8WRJZ76ZQ/partition-0-visit-mark.json", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-2/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-2/bucket-index-sync-status.json", "", nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-2/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockUpload("user-2/bucket-index-sync-status.json", nil)
	bucketClient.MockGet("user-1/partitioned-groups/"+partitionedGroupID1+".json", "", nil)
	bucketClient.MockUpload("user-1/partitioned-groups/"+partitionedGroupID1+".json", nil)
	bucketClient.MockGet("user-2/partitioned-groups/"+partitionedGroupID2+".json", "", nil)
	bucketClient.MockUpload("user-2/partitioned-groups/"+partitionedGroupID2+".json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)
	bucketClient.MockIter("user-2/"+PartitionedGroupDirectory, nil, nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfigForPartitioning()
	cfg.ShardingEnabled = true
	cfg.ShardingRing.InstanceID = "compactor-1"
	cfg.ShardingRing.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.KVStore.Mock = ringStore

	c, _, tsdbPlanner, logs, _ := prepareForPartitioning(t, cfg, bucketClient, nil, nil)

	// Mock the planner as if there's no compaction to do,
	// in order to simplify tests (all in all, we just want to
	// test our logic and not TSDB compactor which we expect to
	// be already tested).
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	// Ensure a plan has been executed for the blocks of each user.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 2)

	assert.ElementsMatch(t, []string{
		`level=info component=compactor msg="auto joined with new tokens" ring=compactor state=ACTIVE`,
		`level=info component=compactor msg="waiting until compactor is ACTIVE in the ring"`,
		`level=info component=compactor msg="compactor is ACTIVE in the ring"`,
		`level=info component=compactor msg="compactor started"`,
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

func TestPartitionCompactor_ShouldCompactOnlyUsersOwnedByTheInstanceOnShardingEnabledAndMultipleInstancesRunning(t *testing.T) {

	numUsers := 100

	// Setup user IDs
	userIDs := make([]string, 0, numUsers)
	for i := 1; i <= numUsers; i++ {
		userIDs = append(userIDs, fmt.Sprintf("user-%d", i))
	}

	// Mock the bucket to contain all users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", userIDs, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	for _, userID := range userIDs {
		partitionedGroupID := getPartitionedGroupID(userID)
		bucketClient.MockIter(userID+"/", []string{userID + "/01DTVP434PA9VFXSW2JKB3392D"}, nil)
		bucketClient.MockIter(userID+"/markers/", nil, nil)
		bucketClient.MockGet(userID+"/markers/cleaner-visit-marker.json", "", nil)
		bucketClient.MockUpload(userID+"/markers/cleaner-visit-marker.json", nil)
		bucketClient.MockDelete(userID+"/markers/cleaner-visit-marker.json", nil)
		bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath(userID), false, nil)
		bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath(userID), false, nil)
		bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
		bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
		bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
		bucketClient.MockGet(userID+"/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", "", nil)
		bucketClient.MockGet(userID+"/bucket-index-sync-status.json", "", nil)
		bucketClient.MockGet(userID+"/bucket-index.json.gz", "", nil)
		bucketClient.MockUpload(userID+"/bucket-index.json.gz", nil)
		bucketClient.MockUpload(userID+"/bucket-index-sync-status.json", nil)
		bucketClient.MockGet(userID+"/partitioned-groups/"+partitionedGroupID+".json", "", nil)
		bucketClient.MockUpload(userID+"/partitioned-groups/"+partitionedGroupID+".json", nil)
		bucketClient.MockIter(userID+"/"+PartitionedGroupDirectory, nil, nil)
	}

	// Create a shared KV Store
	kvstore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create two compactors
	var compactors []*Compactor
	var logs []*concurrency.SyncBuffer

	for i := 1; i <= 2; i++ {
		cfg := prepareConfigForPartitioning()
		cfg.ShardingEnabled = true
		cfg.ShardingRing.InstanceID = fmt.Sprintf("compactor-%d", i)
		cfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
		cfg.ShardingRing.WaitStabilityMinDuration = time.Second
		cfg.ShardingRing.WaitStabilityMaxDuration = 5 * time.Second
		cfg.ShardingRing.KVStore.Mock = kvstore

		c, _, tsdbPlanner, l, _ := prepareForPartitioning(t, cfg, bucketClient, nil, nil)
		defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

		compactors = append(compactors, c)
		logs = append(logs, l)

		// Mock the planner as if there's no compaction to do,
		// in order to simplify tests (all in all, we just want to
		// test our logic and not TSDB compactor which we expect to
		// be already tested).
		tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)
	}

	// Start all compactors
	for _, c := range compactors {
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	}

	// Wait until a run has been completed on each compactor
	for _, c := range compactors {
		cortex_testutil.Poll(t, 60*time.Second, true, func() interface{} {
			return prom_testutil.ToFloat64(c.CompactionRunsCompleted) >= 1
		})
	}

	// Ensure that each user has been compacted by the correct instance
	for _, userID := range userIDs {
		_, l, err := findCompactorByUserID(compactors, logs, userID)
		require.NoError(t, err)
		assert.Contains(t, l.String(), fmt.Sprintf(`level=info component=compactor msg="successfully compacted user blocks" user=%s`, userID))
	}
}

func TestPartitionCompactor_ShouldCompactOnlyShardsOwnedByTheInstanceOnShardingEnabledWithShuffleShardingAndMultipleInstancesRunning(t *testing.T) {
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
	bucketClient.MockIter("__markers__", []string{}, nil)

	// Keys with a value greater than 1 will be groups that should be compacted
	groupHashes := make(map[uint32]int)
	for _, userID := range userIDs {
		blockFiles := []string{}

		for blockID, blockTimes := range blocks {
			groupHash := hashGroup(userID, blockTimes["startTime"], blockTimes["endTime"])
			visitMarker := partitionVisitMarker{
				CompactorID:        "test-compactor",
				VisitTime:          time.Now().Unix(),
				PartitionedGroupID: groupHash,
				PartitionID:        0,
				Status:             Pending,
				Version:            PartitionVisitMarkerVersion1,
			}
			visitMarkerFileContent, _ := json.Marshal(visitMarker)
			bucketClient.MockGet(userID+"/bucket-index-sync-status.json", "", nil)
			bucketClient.MockGet(userID+"/"+blockID+"/meta.json", mockBlockMetaJSONWithTime(blockID, userID, blockTimes["startTime"], blockTimes["endTime"]), nil)
			bucketClient.MockGet(userID+"/"+blockID+"/deletion-mark.json", "", nil)
			bucketClient.MockGet(userID+"/"+blockID+"/no-compact-mark.json", "", nil)
			bucketClient.MockGet(userID+"/"+blockID+"/partition-0-visit-mark.json", "", nil)
			bucketClient.MockGet(userID+"/partitioned-groups/visit-marks/"+fmt.Sprint(groupHash)+"/partition-0-visit-mark.json", string(visitMarkerFileContent), nil)
			bucketClient.MockGetRequireUpload(userID+"/partitioned-groups/visit-marks/"+fmt.Sprint(groupHash)+"/partition-0-visit-mark.json", string(visitMarkerFileContent), nil)
			bucketClient.MockUpload(userID+"/partitioned-groups/visit-marks/"+fmt.Sprint(groupHash)+"/partition-0-visit-mark.json", nil)
			// Iter with recursive so expected to get objects rather than directories.
			blockFiles = append(blockFiles, path.Join(userID, blockID), path.Join(userID, blockID, block.MetaFilename))

			// Get all of the unique group hashes so that they can be used to ensure all groups were compacted
			groupHashes[groupHash]++
			bucketClient.MockGet(userID+"/partitioned-groups/"+fmt.Sprint(groupHash)+".json", "", nil)
			bucketClient.MockUpload(userID+"/partitioned-groups/"+fmt.Sprint(groupHash)+".json", nil)
		}

		bucketClient.MockIter(userID+"/", blockFiles, nil)
		bucketClient.MockIter(userID+"/markers/", nil, nil)
		bucketClient.MockGet(userID+"/markers/cleaner-visit-marker.json", "", nil)
		bucketClient.MockUpload(userID+"/markers/cleaner-visit-marker.json", nil)
		bucketClient.MockDelete(userID+"/markers/cleaner-visit-marker.json", nil)
		bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath(userID), false, nil)
		bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath(userID), false, nil)
		bucketClient.MockGet(userID+"/bucket-index.json.gz", "", nil)
		bucketClient.MockUpload(userID+"/bucket-index.json.gz", nil)
		bucketClient.MockUpload(userID+"/bucket-index-sync-status.json", nil)
		bucketClient.MockIter(userID+"/"+PartitionedGroupDirectory, nil, nil)
	}

	// Create a shared KV Store
	kvstore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create four compactors
	var compactors []*Compactor
	var logs []*concurrency.SyncBuffer

	for i := 1; i <= 4; i++ {
		cfg := prepareConfigForPartitioning()
		cfg.ShardingEnabled = true
		cfg.CompactionInterval = 15 * time.Second
		cfg.ShardingStrategy = util.ShardingStrategyShuffle
		cfg.ShardingRing.InstanceID = fmt.Sprintf("compactor-%d", i)
		cfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
		cfg.ShardingRing.WaitStabilityMinDuration = time.Second
		cfg.ShardingRing.WaitStabilityMaxDuration = 5 * time.Second
		cfg.ShardingRing.KVStore.Mock = kvstore

		limits := &validation.Limits{}
		flagext.DefaultValues(limits)
		limits.CompactorTenantShardSize = 3

		c, _, tsdbPlanner, l, _ := prepareForPartitioning(t, cfg, bucketClient, limits, nil)
		defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

		compactors = append(compactors, c)
		logs = append(logs, l)

		// Mock the planner as if there's no compaction to do,
		// in order to simplify tests (all in all, we just want to
		// test our logic and not TSDB compactor which we expect to
		// be already tested).
		tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)
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
			return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
		})
	}

	// Ensure that each group was only compacted by exactly one compactor
	for groupHash, blockCount := range groupHashes {

		l, found, err := checkLogsForPartitionCompaction(compactors, logs, groupHash)
		require.NoError(t, err)

		// If the blockCount < 2 then the group shouldn't have been compacted, therefore not found in the logs
		if blockCount < 2 {
			assert.False(t, found)
		} else {
			assert.True(t, found)
			assert.Contains(t, l.String(), fmt.Sprintf(`group_hash=%d msg="found compactable group for user"`, groupHash))
		}
	}
}

// checkLogsForPartitionCompaction checks the logs to see if a compaction has happened on the groupHash,
// if there has been a compaction it will return the logs of the compactor that handled the group
// and will return true. Otherwise this function will return a nil value for the logs and false
// as the group was not compacted
func checkLogsForPartitionCompaction(compactors []*Compactor, logs []*concurrency.SyncBuffer, groupHash uint32) (*concurrency.SyncBuffer, bool, error) {
	var log *concurrency.SyncBuffer

	for _, l := range logs {
		owned := strings.Contains(l.String(), fmt.Sprintf(`group_hash=%d msg="found compactable group for user"`, groupHash))
		if owned {
			log = l
		}
	}

	// Return false if we've not been able to find it
	if log == nil {
		return nil, false, nil
	}

	return log, true, nil
}

func prepareConfigForPartitioning() Config {
	compactorCfg := prepareConfig()

	compactorCfg.CompactionStrategy = util.CompactionStrategyPartitioning

	return compactorCfg
}

func prepareForPartitioning(t *testing.T, compactorCfg Config, bucketClient objstore.InstrumentedBucket, limits *validation.Limits, tsdbGrouper *tsdbGrouperMock) (*Compactor, *tsdbCompactorMock, *tsdbPlannerMock, *concurrency.SyncBuffer, prometheus.Gatherer) {
	storageCfg := cortex_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&storageCfg)
	storageCfg.BucketStore.BlockDiscoveryStrategy = string(cortex_tsdb.RecursiveDiscovery)

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

	bucketClientFactory := func(ctx context.Context) (objstore.InstrumentedBucket, error) {
		return bucketClient, nil
	}

	blocksCompactorFactory := func(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (compact.Compactor, PlannerFactory, error) {
		return tsdbCompactor,
			func(ctx context.Context, bkt objstore.InstrumentedBucket, _ log.Logger, _ Config, noCompactMarkFilter *compact.GatherNoCompactionMarkFilter, ringLifecycle *ring.Lifecycler, _ string, _ prometheus.Counter, _ prometheus.Counter, _ *compactorMetrics) compact.Planner {
				tsdbPlanner.noCompactMarkFilters = append(tsdbPlanner.noCompactMarkFilters, noCompactMarkFilter)
				return tsdbPlanner
			},
			nil
	}

	var blocksGrouperFactory BlocksGrouperFactory
	if tsdbGrouper != nil {
		blocksGrouperFactory = func(_ context.Context, _ Config, _ objstore.InstrumentedBucket, _ log.Logger, _ prometheus.Counter, _ prometheus.Counter, _ prometheus.Counter, _ *compact.SyncerMetrics, _ *compactorMetrics, _ *ring.Ring, _ *ring.Lifecycler, _ Limits, _ string, _ *compact.GatherNoCompactionMarkFilter, _ int) compact.Grouper {
			return tsdbGrouper
		}
	} else {
		if compactorCfg.ShardingStrategy == util.ShardingStrategyShuffle {
			blocksGrouperFactory = ShuffleShardingGrouperFactory
		} else {
			blocksGrouperFactory = DefaultBlocksGrouperFactory
		}
	}

	var blockDeletableCheckerFactory BlockDeletableCheckerFactory
	if compactorCfg.ShardingStrategy == util.ShardingStrategyShuffle {
		blockDeletableCheckerFactory = PartitionCompactionBlockDeletableCheckerFactory
	} else {
		blockDeletableCheckerFactory = DefaultBlockDeletableCheckerFactory
	}

	var compactionLifecycleCallbackFactory CompactionLifecycleCallbackFactory
	if compactorCfg.ShardingStrategy == util.ShardingStrategyShuffle {
		compactionLifecycleCallbackFactory = ShardedCompactionLifecycleCallbackFactory
	} else {
		compactionLifecycleCallbackFactory = DefaultCompactionLifecycleCallbackFactory
	}

	c, err := newCompactor(compactorCfg, storageCfg, logger, registry, bucketClientFactory, blocksGrouperFactory, blocksCompactorFactory, blockDeletableCheckerFactory, compactionLifecycleCallbackFactory, overrides, 1)
	require.NoError(t, err)

	return c, tsdbCompactor, tsdbPlanner, logs, registry
}

type tsdbGrouperMock struct {
	mock.Mock
}

func (m *tsdbGrouperMock) Groups(blocks map[ulid.ULID]*metadata.Meta) (res []*compact.Group, err error) {
	args := m.Called(blocks)
	return args.Get(0).([]*compact.Group), args.Error(1)
}

var (
	BlockMinTime = int64(1574776800000)
	BlockMaxTime = int64(1574784000000)
)

func getPartitionedGroupID(userID string) string {
	return fmt.Sprint(hashGroup(userID, BlockMinTime, BlockMaxTime))
}

func mockBlockGroup(userID string, ids []string, bkt *bucket.ClientMock) *compact.Group {
	dummyCounter := prometheus.NewCounter(prometheus.CounterOpts{})
	group, _ := compact.NewGroup(
		log.NewNopLogger(),
		bkt,
		getPartitionedGroupID(userID),
		nil,
		0,
		true,
		true,
		dummyCounter,
		dummyCounter,
		dummyCounter,
		dummyCounter,
		dummyCounter,
		dummyCounter,
		dummyCounter,
		dummyCounter,
		metadata.NoneFunc,
		1,
		1,
	)
	for _, id := range ids {
		meta := mockBlockMeta(id)
		err := group.AppendMeta(&metadata.Meta{
			BlockMeta: meta,
		})
		if err != nil {
			continue
		}
	}
	return group
}

func TestPartitionCompactor_DeleteLocalSyncFiles(t *testing.T) {
	numUsers := 10

	// Setup user IDs
	userIDs := make([]string, 0, numUsers)
	for i := 1; i <= numUsers; i++ {
		userIDs = append(userIDs, fmt.Sprintf("user-%d", i))
	}

	inmem := objstore.WithNoopInstr(objstore.NewInMemBucket())
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
		cfg := prepareConfigForPartitioning()

		cfg.ShardingEnabled = true
		cfg.ShardingRing.InstanceID = fmt.Sprintf("compactor-%d", i)
		cfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
		cfg.ShardingRing.WaitStabilityMinDuration = time.Second
		cfg.ShardingRing.WaitStabilityMaxDuration = 5 * time.Second
		cfg.ShardingRing.KVStore.Mock = kvstore

		// Each compactor will get its own temp dir for storing local files.
		c, _, tsdbPlanner, _, _ := prepareForPartitioning(t, cfg, inmem, nil, nil)
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
		})

		compactors = append(compactors, c)

		// Mock the planner as if there's no compaction to do,
		// in order to simplify tests (all in all, we just want to
		// test our logic and not TSDB compactor which we expect to
		// be already tested).
		tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)
	}

	require.Equal(t, 2, len(compactors))
	c1 := compactors[0]
	c2 := compactors[1]

	// Start first compactor
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c1))

	// Wait until a run has been completed on first compactor. This happens as soon as compactor starts.
	cortex_testutil.Poll(t, 20*time.Second, true, func() interface{} {
		return prom_testutil.ToFloat64(c1.CompactionRunsCompleted) >= 1
	})

	require.NoError(t, os.Mkdir(c1.metaSyncDirForUser("new-user"), 0600))

	// Verify that first compactor has synced all the users, plus there is one extra we have just created.
	require.Equal(t, numUsers+1, len(c1.listTenantsWithMetaSyncDirectories()))

	// Now start second compactor, and wait until it runs compaction.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c2))
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c2.CompactionRunsCompleted)
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

func TestPartitionCompactor_ShouldFailCompactionOnTimeout(t *testing.T) {
	t.Parallel()

	// Mock the bucket
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfigForPartitioning()
	cfg.ShardingEnabled = true
	cfg.ShardingRing.InstanceID = "compactor-1"
	cfg.ShardingRing.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.KVStore.Mock = ringStore

	// Set ObservePeriod to longer than the timeout period to mock a timeout while waiting on ring to become ACTIVE
	cfg.ShardingRing.ObservePeriod = time.Second * 10

	c, _, _, logs, _ := prepareForPartitioning(t, cfg, bucketClient, nil, nil)

	// Try to start the compactor with a bad consul kv-store. The
	err := services.StartAndAwaitRunning(context.Background(), c)

	// Assert that the compactor timed out
	assert.Equal(t, context.DeadlineExceeded, err)

	assert.ElementsMatch(t, []string{
		`level=info component=compactor msg="compactor started"`,
		`level=info component=compactor msg="waiting until compactor is ACTIVE in the ring"`,
		`level=info component=compactor msg="auto joined with new tokens" ring=compactor state=JOINING`,
		`level=error component=compactor msg="compactor failed to become ACTIVE in the ring" err="context deadline exceeded"`,
	}, removeIgnoredLogs(strings.Split(strings.TrimSpace(logs.String()), "\n")))
}

func TestPartitionCompactor_ShouldNotHangIfPlannerReturnNothing(t *testing.T) {
	t.Parallel()

	ss := bucketindex.Status{Status: bucketindex.CustomerManagedKeyError, Version: bucketindex.SyncStatusFileVersion}
	content, err := json.Marshal(ss)
	require.NoError(t, err)

	partitionedGroupID := getPartitionedGroupID("user-1")
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/partition-0-visit-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/partition-0-visit-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", string(content), nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)
	bucketClient.MockGet("user-1/partitioned-groups/visit-marks/"+string(partitionedGroupID)+"/partition-0-visit-mark.json", "", nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfigForPartitioning()
	cfg.ShardingEnabled = true
	cfg.ShardingRing.InstanceID = "compactor-1"
	cfg.ShardingRing.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.KVStore.Mock = ringStore

	tsdbGrouper := tsdbGrouperMock{}
	mockGroups := []*compact.Group{mockBlockGroup("user-1", []string{"01DTVP434PA9VFXSW2JKB3392D", "01DTW0ZCPDDNV4BV83Q2SV4QAZ"}, bucketClient)}
	tsdbGrouper.On("Groups", mock.Anything).Return(mockGroups, nil)

	c, _, tsdbPlanner, _, _ := prepareForPartitioning(t, cfg, bucketClient, nil, &tsdbGrouper)
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
}

func TestPartitionCompactor_ShouldNotFailCompactionIfAccessDeniedErrDuringMetaSync(t *testing.T) {
	t.Parallel()

	ss := bucketindex.Status{Status: bucketindex.Ok, Version: bucketindex.SyncStatusFileVersion}
	content, err := json.Marshal(ss)
	require.NoError(t, err)

	partitionedGroupID := getPartitionedGroupID("user-1")
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ", "user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), bucket.ErrKeyPermissionDenied)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", bucket.ErrKeyPermissionDenied)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", bucket.ErrKeyPermissionDenied)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), bucket.ErrKeyPermissionDenied)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", bucket.ErrKeyPermissionDenied)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", bucket.ErrKeyPermissionDenied)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", string(content), nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)
	bucketClient.MockGet("user-1/partitioned-groups/visit-marks/"+string(partitionedGroupID)+"/partition-0-visit-mark.json", "", nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfigForPartitioning()
	cfg.ShardingEnabled = true
	cfg.ShardingRing.InstanceID = "compactor-1"
	cfg.ShardingRing.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.KVStore.Mock = ringStore

	c, _, tsdbPlanner, _, _ := prepareForPartitioning(t, cfg, bucketClient, nil, nil)
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
}

func TestPartitionCompactor_ShouldNotFailCompactionIfAccessDeniedErrReturnedFromBucket(t *testing.T) {
	t.Parallel()

	ss := bucketindex.Status{Status: bucketindex.Ok, Version: bucketindex.SyncStatusFileVersion}
	content, err := json.Marshal(ss)
	require.NoError(t, err)

	partitionedGroupID := getPartitionedGroupID("user-1")
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ", "user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTW0ZCPDDNV4BV83Q2SV4QAZ/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", string(content), nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockIter("user-1/"+PartitionedGroupDirectory, nil, nil)
	bucketClient.MockGet("user-1/partitioned-groups/visit-marks/"+string(partitionedGroupID)+"/partition-0-visit-mark.json", "", nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfigForPartitioning()
	cfg.ShardingEnabled = true
	cfg.ShardingRing.InstanceID = "compactor-1"
	cfg.ShardingRing.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.KVStore.Mock = ringStore

	c, _, tsdbPlanner, _, _ := prepareForPartitioning(t, cfg, bucketClient, nil, nil)
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{}, bucket.ErrKeyPermissionDenied)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, 20*time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
}
