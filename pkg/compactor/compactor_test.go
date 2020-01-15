package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
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

func TestCompactor_ShouldDoNothingOnNoUserBlocks(t *testing.T) {
	t.Parallel()

	c, bucketClient, _, logs, registry := prepare(t)

	// No user blocks stored in the bucket.
	bucketClient.MockIter("", []string{}, nil)

	// Wait until a run has completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	c.Shutdown()

	assert.Equal(t, []string{
		`level=info msg="discovering users from bucket"`,
		`level=info msg="discovered users from bucket" users=0`,
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
	`)))
}

func TestCompactor_ShouldRetryOnFailureWhileDiscoveringUsersFromBucket(t *testing.T) {
	t.Parallel()

	c, bucketClient, _, logs, registry := prepare(t)

	// Fail to iterate over the bucket while discovering users.
	bucketClient.MockIter("", nil, errors.New("failed to iterate the bucket"))

	// Wait until all retry attempts have completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsFailed)
	})

	c.Shutdown()

	// Ensure the bucket iteration has been retried the configured number of times.
	bucketClient.AssertNumberOfCalls(t, "Iter", 3)

	assert.Equal(t, []string{
		`level=info msg="discovering users from bucket"`,
		`level=error msg="failed to discover users from bucket" err="failed to iterate the bucket"`,
		`level=info msg="discovering users from bucket"`,
		`level=error msg="failed to discover users from bucket" err="failed to iterate the bucket"`,
		`level=info msg="discovering users from bucket"`,
		`level=error msg="failed to discover users from bucket" err="failed to iterate the bucket"`,
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
	`)))
}

func TestCompactor_ShouldIterateOverUsersAndRunCompaction(t *testing.T) {
	t.Parallel()

	c, bucketClient, tsdbCompactor, logs, registry := prepare(t)

	// Mock the bucket to contain two users, each one with one block.
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D"}, nil)
	bucketClient.MockIter("user-2/", []string{"user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ"}, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-2/01DTW0ZCPDDNV4BV83Q2SV4QAZ/meta.json", mockBlockMetaJSON("01DTW0ZCPDDNV4BV83Q2SV4QAZ"), nil)

	// Mock the compactor as if there's no compaction to do,
	// in order to simplify tests (all in all, we just want to
	// test our logic and not TSDB compactor which we expect to
	// be already tested).
	tsdbCompactor.On("Plan", mock.Anything).Return([]string{}, nil)

	// Wait until a run has completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.compactionRunsCompleted)
	})

	c.Shutdown()

	// Ensure a plan has been executed for the blocks of each user.
	tsdbCompactor.AssertNumberOfCalls(t, "Plan", 2)

	assert.Equal(t, []string{
		`level=info msg="discovering users from bucket"`,
		`level=info msg="discovered users from bucket" users=2`,
		`level=info msg="starting compaction of user blocks" user=user-1`,
		`level=info msg="start sync of metas"`,
		`level=debug msg="download meta" block=01DTVP434PA9VFXSW2JKB3392D`,
		`level=info msg="start of GC"`,
		`level=info msg="start of compaction"`,
		`level=info msg="successfully compacted user blocks" user=user-1`,
		`level=info msg="starting compaction of user blocks" user=user-2`,
		`level=info msg="start sync of metas"`,
		`level=debug msg="download meta" block=01DTW0ZCPDDNV4BV83Q2SV4QAZ`,
		`level=info msg="start of GC"`,
		`level=info msg="start of compaction"`,
		`level=info msg="successfully compacted user blocks" user=user-2`,
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
	`)))
}

func prepare(t *testing.T) (*Compactor, *cortex_tsdb.BucketClientMock, *tsdbCompactorMock, *bytes.Buffer, prometheus.Gatherer) {
	compactorCfg := Config{}
	storageCfg := cortex_tsdb.Config{}
	flagext.DefaultValues(&compactorCfg, &storageCfg)
	compactorCfg.retryMinBackoff = 0
	compactorCfg.retryMaxBackoff = 0

	bucketClient := &cortex_tsdb.BucketClientMock{}
	tsdbCompactor := &tsdbCompactorMock{}
	logs := &bytes.Buffer{}
	logger := log.NewLogfmtLogger(logs)
	registry := prometheus.NewRegistry()

	ctx, cancelCtx := context.WithCancel(context.Background())
	c, err := newCompactor(ctx, cancelCtx, compactorCfg, storageCfg, bucketClient, tsdbCompactor, logger, registry)
	require.NoError(t, err)

	return c, bucketClient, tsdbCompactor, logs, registry
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

func mockBlockMetaJSON(id string) string {
	meta := tsdb.BlockMeta{
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
