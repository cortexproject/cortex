package compactor

import (
	"bytes"
	"context"
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

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/objstore"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
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
		setup    func(cfg *Config)
		expected string
	}{
		"should pass with the default config": {
			setup:    func(cfg *Config) {},
			expected: "",
		},
		"should pass with only 1 block range period": {
			setup: func(cfg *Config) {
				cfg.BlockRanges = cortex_tsdb.DurationList{time.Hour}
			},
			expected: "",
		},
		"should fail with non divisible block range periods": {
			setup: func(cfg *Config) {
				cfg.BlockRanges = cortex_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour, 30 * time.Hour}
			},
			expected: errors.Errorf(errInvalidBlockRanges, 30*time.Hour, 24*time.Hour).Error(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := &Config{}
			flagext.DefaultValues(cfg)
			testData.setup(cfg)

			if actualErr := cfg.Validate(); testData.expected != "" {
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

	c, _, _, logs, registry, cleanup := prepare(t, prepareConfig(), bucketClient)
	defer cleanup()
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

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
		cortex_compactor_blocks_marked_for_deletion_total 0

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

	c, _, _, logs, registry, cleanup := prepare(t, prepareConfig(), bucketClient)
	defer cleanup()
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
		cortex_compactor_blocks_marked_for_deletion_total 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

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
		"cortex_compactor_block_cleanup_started_total",
		"cortex_compactor_block_cleanup_completed_total",
		"cortex_compactor_block_cleanup_failed_total",
	))
}

func TestCompactor_ShouldIterateOverUsersAndRunCompaction(t *testing.T) {
	t.Parallel()

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockExists(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ"}, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-2/bucket-index.json.gz", "", nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-2/bucket-index.json.gz", nil)

	c, _, tsdbPlanner, logs, registry, cleanup := prepare(t, prepareConfig(), bucketClient)
	defer cleanup()

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
		cortex_compactor_blocks_marked_for_deletion_total 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total 1

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total 0
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

	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", mockDeletionMarkJSON("01DTVP434PA9VFXSW2JKB3392D", time.Now()), nil)
	bucketClient.MockGet("user-1/markers/01DTVP434PA9VFXSW2JKB3392D-deletion-mark.json", mockDeletionMarkJSON("01DTVP434PA9VFXSW2JKB3392D", time.Now()), nil)

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

	c, _, tsdbPlanner, logs, registry, cleanup := prepare(t, cfg, bucketClient)
	defer cleanup()

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

	// Only one user's block is compacted.
	tsdbPlanner.AssertNumberOfCalls(t, "Plan", 1)

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
		cortex_compactor_blocks_marked_for_deletion_total 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total 1

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total 0
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

	c, _, tsdbPlanner, logs, registry, cleanup := prepare(t, cfg, bucketClient)
	defer cleanup()

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
		cortex_compactor_blocks_marked_for_deletion_total 0

		# TYPE cortex_compactor_block_cleanup_started_total counter
		# HELP cortex_compactor_block_cleanup_started_total Total number of blocks cleanup runs started.
		cortex_compactor_block_cleanup_started_total 1

		# TYPE cortex_compactor_block_cleanup_completed_total counter
		# HELP cortex_compactor_block_cleanup_completed_total Total number of blocks cleanup runs successfully completed.
		cortex_compactor_block_cleanup_completed_total 1

		# TYPE cortex_compactor_block_cleanup_failed_total counter
		# HELP cortex_compactor_block_cleanup_failed_total Total number of blocks cleanup runs failed.
		cortex_compactor_block_cleanup_failed_total 0
	`), testedMetrics...))
}

func TestCompactor_ShouldCompactAllUsersOnShardingEnabledButOnlyOneInstanceRunning(t *testing.T) {
	t.Parallel()

	// Mock the bucket to contain two users, each one with one block.
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockExists(path.Join("user-1", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", cortex_tsdb.TenantDeletionMarkPath), false, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("user-2/markers/", nil, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-2/bucket-index.json.gz", "", nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-2/bucket-index.json.gz", nil)

	cfg := prepareConfig()
	cfg.ShardingEnabled = true
	cfg.ShardingRing.InstanceID = "compactor-1"
	cfg.ShardingRing.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.KVStore.Mock = consul.NewInMemoryClient(ring.GetCodec())

	c, _, tsdbPlanner, logs, _, cleanup := prepare(t, cfg, bucketClient)
	defer cleanup()

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
		bucketClient.MockGet(userID+"/bucket-index.json.gz", "", nil)
		bucketClient.MockUpload(userID+"/bucket-index.json.gz", nil)
	}

	// Create a shared KV Store
	kvstore := consul.NewInMemoryClient(ring.GetCodec())

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

		c, _, tsdbPlanner, l, _, cleanup := prepare(t, cfg, bucketClient)
		defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck
		defer cleanup()

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

func createTSDBBlock(t *testing.T, bkt objstore.Bucket, userID string, minT, maxT int64, externalLabels map[string]string) ulid.ULID {
	// Create a temporary dir for TSDB.
	tempDir, err := ioutil.TempDir(os.TempDir(), "tsdb")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir) //nolint:errcheck

	// Create a temporary dir for the snapshot.
	snapshotDir, err := ioutil.TempDir(os.TempDir(), "snapshot")
	require.NoError(t, err)
	defer os.RemoveAll(snapshotDir) //nolint:errcheck

	// Create a new TSDB.
	db, err := tsdb.Open(tempDir, nil, nil, &tsdb.Options{
		MinBlockDuration:  int64(2 * 60 * 60 * 1000), // 2h period
		MaxBlockDuration:  int64(2 * 60 * 60 * 1000), // 2h period
		RetentionDuration: int64(15 * 86400 * 1000),  // 15 days
	})
	require.NoError(t, err)

	db.DisableCompactions()

	// Append a sample at the beginning and one at the end of the time range.
	for i, ts := range []int64{minT, maxT - 1} {
		lbls := labels.Labels{labels.Label{Name: "series_id", Value: strconv.Itoa(i)}}

		app := db.Appender(context.Background())
		_, err := app.Add(lbls, ts, float64(i))
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

func findCompactorByUserID(compactors []*Compactor, logs []*concurrency.SyncBuffer, userID string) (*Compactor, *concurrency.SyncBuffer, error) {
	var compactor *Compactor
	var log *concurrency.SyncBuffer

	for i, c := range compactors {
		owned, err := c.ownUser(userID)
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
	out := make([]string, 0, len(input))
	durationRe := regexp.MustCompile(`\s?duration=\S+`)

	for i := 0; i < len(input); i++ {
		log := input[i]
		if strings.Contains(log, "block.MetaFetcher") || strings.Contains(log, "block.BaseFetcher") {
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

	return compactorCfg
}

func prepare(t *testing.T, compactorCfg Config, bucketClient objstore.Bucket) (*Compactor, *tsdbCompactorMock, *tsdbPlannerMock, *concurrency.SyncBuffer, prometheus.Gatherer, func()) {
	storageCfg := cortex_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&storageCfg)

	// Create a temporary directory for compactor data.
	dataDir, err := ioutil.TempDir(os.TempDir(), "compactor-test")
	require.NoError(t, err)

	compactorCfg.DataDir = dataDir
	cleanup := func() {
		os.RemoveAll(dataDir)
	}

	tsdbCompactor := &tsdbCompactorMock{}
	tsdbPlanner := &tsdbPlannerMock{}
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)
	registry := prometheus.NewRegistry()

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	bucketClientFactory := func(ctx context.Context) (objstore.Bucket, error) {
		return bucketClient, nil
	}

	blocksCompactorFactory := func(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (compact.Compactor, compact.Planner, error) {
		return tsdbCompactor, tsdbPlanner, nil
	}

	c, err := newCompactor(compactorCfg, storageCfg, overrides, logger, registry, bucketClientFactory, DefaultBlocksGrouperFactory, blocksCompactorFactory)
	require.NoError(t, err)

	return c, tsdbCompactor, tsdbPlanner, logs, registry, cleanup
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
}

func (m *tsdbPlannerMock) Plan(ctx context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	args := m.Called(ctx, metasByMinTime)
	return args.Get(0).([]*metadata.Meta), args.Error(1)
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

func mockDeletionMarkJSON(id string, deletionTime time.Time) string {
	meta := metadata.DeletionMark{
		Version:      metadata.DeletionMarkVersion1,
		ID:           ulid.MustParse(id),
		DeletionTime: deletionTime.Unix(),
	}

	content, err := json.Marshal(meta)
	if err != nil {
		panic("failed to marshal mocked block meta")
	}

	return string(content)
}

func TestAllowedUser(t *testing.T) {
	testCases := map[string]struct {
		enabled, disabled map[string]struct{}
		user              string
		expected          bool
	}{
		"no enabled or disabled": {
			user:     "test",
			expected: true,
		},

		"only enabled, enabled": {
			enabled:  map[string]struct{}{"user": {}},
			user:     "user",
			expected: true,
		},

		"only enabled, disabled": {
			enabled:  map[string]struct{}{"user": {}},
			user:     "not user",
			expected: false,
		},

		"only disabled, disabled": {
			disabled: map[string]struct{}{"user": {}},
			user:     "user",
			expected: false,
		},

		"only disabled, enabled": {
			disabled: map[string]struct{}{"user": {}},
			user:     "not user",
			expected: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, isAllowedUser(tc.enabled, tc.disabled, tc.user))
		})
	}
}
