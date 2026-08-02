package parquetconverter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/parquet"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	cortex_errors "github.com/cortexproject/cortex/pkg/util/errors"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestConverter(t *testing.T) {
	cfg := prepareConfig()
	user := "user"
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	dir := t.TempDir()

	cfg.Ring.InstanceID = "parquet-converter-1"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.KVStore.Mock = ringStore
	bucketClient, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	userBucket := bucket.NewPrefixedBucketClient(bucketClient, user)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.ParquetConverterEnabled = true

	userSpecificSortColumns := []string{"cluster", "namespace"}

	// Create a mock tenant limits implementation
	tenantLimits := &mockTenantLimits{
		limits: map[string]*validation.Limits{
			user: {
				ParquetConverterSortColumns: userSpecificSortColumns,
				ParquetConverterEnabled:     true,
			},
		},
	}

	c, logger, _ := prepare(t, cfg, objstore.WithNoopInstr(bucketClient), limits, tenantLimits)

	ctx := context.Background()

	lbls := labels.FromStrings("__name__", "test")

	blocks := []ulid.ULID{}
	// Create blocks
	for _, duration := range []time.Duration{2 * time.Hour, 24 * time.Hour} {
		rnd := rand.New(rand.NewSource(time.Now().Unix()))
		id, err := e2e.CreateBlock(ctx, rnd, dir, []labels.Labels{lbls}, 2, 0, duration.Milliseconds(), time.Minute.Milliseconds(), 10)
		require.NoError(t, err)
		blocks = append(blocks, id)
	}

	for _, bIds := range blocks {
		blockDir := fmt.Sprintf("%s/%s", dir, bIds.String())
		b, err := tsdb.OpenBlock(nil, blockDir, nil, nil)
		require.NoError(t, err)
		err = block.Upload(ctx, logger, userBucket, b.Dir(), metadata.NoneFunc)
		require.NoError(t, err)
	}

	// Try to start the compactor with a bad consul kv-store. The
	err = services.StartAndAwaitRunning(context.Background(), c)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, c) // nolint:errcheck

	blocksConverted := []ulid.ULID{}

	test.Poll(t, 3*time.Minute, 1, func() any {
		blocksConverted = blocksConverted[:0]
		for _, bIds := range blocks {
			m, err := parquet.ReadConverterMark(ctx, bIds, userBucket, logger)
			require.NoError(t, err)
			if m.Version == parquet.CurrentVersion {
				blocksConverted = append(blocksConverted, bIds)
				// Verify that shards field is populated (should be > 0)
				require.Greater(t, m.Shards, 0, "expected shards to be greater than 0 for block %s", bIds.String())
			}
		}
		return len(blocksConverted)
	})

	// Verify metrics after conversion
	require.Equal(t, float64(len(blocksConverted)), testutil.ToFloat64(c.metrics.convertedBlocks.WithLabelValues(user)))
	require.Greater(t, testutil.ToFloat64(c.metrics.convertBlockDuration.WithLabelValues(user)), 0.0)
	require.Equal(t, 1.0, testutil.ToFloat64(c.metrics.ownedUsers))

	// Verify all files are there
	for _, block := range blocksConverted {
		for _, file := range []string{
			fmt.Sprintf("%s/parquet-converter-mark.json", block.String()),
			fmt.Sprintf("parquet-markers/%s-parquet-converter-mark.json", block.String()),
			fmt.Sprintf("%s/0.chunks.parquet", block.String()),
			fmt.Sprintf("%s/0.labels.parquet", block.String()),
		} {
			ok, err := userBucket.Exists(ctx, file)
			require.NoError(t, err)
			require.True(t, ok)
		}
	}

	syncedTenants := c.listTenantsWithMetaSyncDirectories()
	require.Len(t, syncedTenants, 1)
	require.Contains(t, syncedTenants, user)

	// Mark user as deleted
	require.NoError(t, users.WriteTenantDeletionMark(context.Background(), objstore.WithNoopInstr(bucketClient), user, users.NewTenantDeletionMark(time.Now())))

	// Should clean sync folders
	test.Poll(t, time.Minute, 0, func() any {
		return len(c.listTenantsWithMetaSyncDirectories())
	})

	// Verify metrics after user deletion
	test.Poll(t, time.Minute*10, true, func() any {
		if testutil.ToFloat64(c.metrics.convertedBlocks.WithLabelValues(user)) != 0.0 {
			return false
		}
		if testutil.ToFloat64(c.metrics.convertBlockDuration.WithLabelValues(user)) != 0.0 {
			return false
		}
		if testutil.ToFloat64(c.metrics.convertBlockFailures.WithLabelValues(user)) != 0.0 {
			return false
		}
		if testutil.ToFloat64(c.metrics.ownedUsers) != 0.0 {
			return false
		}
		return true
	})
}

func prepareConfig() Config {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.ConversionInterval = time.Second
	return cfg
}

func TestConverter_SplitsBlockIntoMultipleShards(t *testing.T) {
	cfg := prepareConfig()
	// Configure the converter so that each parquet shard holds at most
	// numRowGroups * maxRowsPerRowGroup = 1 * 2 = 2 series.
	cfg.NumRowGroups = 1
	cfg.MaxRowsPerRowGroup = 2

	user := "user-1"
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	dir := t.TempDir()

	cfg.Ring.InstanceID = "parquet-converter-1"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.KVStore.Mock = ringStore
	bucketClient, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	userBucket := bucket.NewPrefixedBucketClient(bucketClient, user)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.ParquetConverterEnabled = true

	c, logger, _ := prepare(t, cfg, objstore.WithNoopInstr(bucketClient), limits, nil)

	ctx := context.Background()

	// Create 5 unique series so that the block is split into
	// ceil(5 / 2) = 3 parquet shards.
	const numSeries = 5
	const expectedShards = 3
	series := make([]labels.Labels, 0, numSeries)
	for i := range numSeries {
		series = append(series, labels.FromStrings("__name__", "test", "series", fmt.Sprintf("%d", i)))
	}

	// Create and upload a 24h block. It must be larger than the first configured
	// block range (2h) so that the converter does not skip it as a raw TSDB block.
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	blockID, err := e2e.CreateBlock(ctx, rnd, dir, series, 2, 0, 24*time.Hour.Milliseconds(), time.Minute.Milliseconds(), 10)
	require.NoError(t, err)
	blockDir := fmt.Sprintf("%s/%s", dir, blockID.String())
	b, err := tsdb.OpenBlock(nil, blockDir, nil, nil)
	require.NoError(t, err)
	err = block.Upload(ctx, logger, userBucket, b.Dir(), metadata.NoneFunc)
	require.NoError(t, err)

	// Start the converter.
	err = services.StartAndAwaitRunning(context.Background(), c)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, c) // nolint:errcheck

	// Wait until the block is converted and assert it was split into multiple shards.
	test.Poll(t, 3*time.Minute, expectedShards, func() any {
		m, err := parquet.ReadConverterMark(ctx, blockID, userBucket, logger)
		require.NoError(t, err)
		if m.Version != parquet.CurrentVersion {
			return -1
		}
		return m.Shards
	})

	// Verify that one labels/chunks parquet file exists per shard.
	for shard := range expectedShards {
		for _, file := range []string{
			fmt.Sprintf("%s/%d.chunks.parquet", blockID.String(), shard),
			fmt.Sprintf("%s/%d.labels.parquet", blockID.String(), shard),
		} {
			ok, err := userBucket.Exists(ctx, file)
			require.NoError(t, err)
			require.True(t, ok, "expected shard file %s to exist", file)
		}
	}

	// Verify there is no extra shard beyond the expected count.
	ok, err := userBucket.Exists(ctx, fmt.Sprintf("%s/%d.chunks.parquet", blockID.String(), expectedShards))
	require.NoError(t, err)
	require.False(t, ok, "expected no shard file at index %d", expectedShards)
}

func prepare(t *testing.T, cfg Config, bucketClient objstore.InstrumentedBucket, limits *validation.Limits, tenantLimits validation.TenantLimits) (*Converter, log.Logger, prometheus.Gatherer) {
	storageCfg := cortex_tsdb.BlocksStorageConfig{}
	blockRanges := cortex_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	flagext.DefaultValues(&storageCfg)
	storageCfg.BucketStore.BlockDiscoveryStrategy = string(cortex_tsdb.RecursiveDiscovery)
	bucketClient = bucketindex.BucketWithGlobalMarkers(bucketClient)

	// Create a temporary directory for compactor data.
	cfg.DataDir = t.TempDir()

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)
	registry := prometheus.NewRegistry()

	if limits == nil {
		limits = &validation.Limits{}
		flagext.DefaultValues(limits)
	}

	overrides := validation.NewOverrides(*limits, tenantLimits)

	scanner, err := users.NewScanner(users.UsersScannerConfig{
		Strategy: users.UserScanStrategyList,
	}, bucketClient, logger, registry)
	require.NoError(t, err)
	c := newConverter(cfg, bucketClient, storageCfg, blockRanges.ToMilliseconds(), logger, registry, overrides, scanner)
	return c, logger, registry
}

func TestConverter_CleanupMetricsForNotOwnedUser(t *testing.T) {
	// Create a new registry for testing
	reg := prometheus.NewRegistry()

	// Create a new converter with test configuration
	cfg := Config{}
	storageCfg := cortex_tsdb.BlocksStorageConfig{}
	limits := &validation.Overrides{}
	converter := newConverter(cfg, nil, storageCfg, []int64{7200000}, nil, reg, limits, nil)

	// Add some test metrics for a user
	userID := "test-user"
	converter.metrics.convertedBlocks.WithLabelValues(userID).Inc()
	converter.metrics.convertBlockDuration.WithLabelValues(userID).Set(1.0)
	converter.metrics.convertBlockFailures.WithLabelValues(userID).Inc()

	// Verify metrics exist before cleanup
	assert.Equal(t, 1.0, testutil.ToFloat64(converter.metrics.convertedBlocks.WithLabelValues(userID)))
	assert.Equal(t, 1.0, testutil.ToFloat64(converter.metrics.convertBlockDuration.WithLabelValues(userID)))
	assert.Equal(t, 1.0, testutil.ToFloat64(converter.metrics.convertBlockFailures.WithLabelValues(userID)))

	// Set lastOwnedUsers to empty (user was never owned)
	converter.lastOwnedUsers = map[string]struct{}{}
	// Clean up metrics for the user will do nothing as the user was never owned
	converter.cleanupMetricsForNotOwnedUser(userID)
	assert.Equal(t, 1.0, testutil.ToFloat64(converter.metrics.convertedBlocks.WithLabelValues(userID)))
	assert.Equal(t, 1.0, testutil.ToFloat64(converter.metrics.convertBlockDuration.WithLabelValues(userID)))
	assert.Equal(t, 1.0, testutil.ToFloat64(converter.metrics.convertBlockFailures.WithLabelValues(userID)))

	// Mark the user as previously owned
	converter.lastOwnedUsers = map[string]struct{}{
		userID: {},
	}

	// Clean up metrics for the user
	converter.cleanupMetricsForNotOwnedUser(userID)

	// Verify metrics are deleted
	assert.Equal(t, 0.0, testutil.ToFloat64(converter.metrics.convertedBlocks.WithLabelValues(userID)))
	assert.Equal(t, 0.0, testutil.ToFloat64(converter.metrics.convertBlockDuration.WithLabelValues(userID)))
	assert.Equal(t, 0.0, testutil.ToFloat64(converter.metrics.convertBlockFailures.WithLabelValues(userID)))
}

func TestConverter_BlockConversionFailure(t *testing.T) {
	// Create a new registry for testing
	reg := prometheus.NewRegistry()

	// Create a new converter with test configuration
	cfg := Config{
		MaxRowsPerRowGroup:  1e6,
		MetaSyncConcurrency: 1,
		DataDir:             t.TempDir(),
	}
	logger := log.NewNopLogger()
	storageCfg := cortex_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&storageCfg)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	overrides := validation.NewOverrides(*limits, nil)
	limits.ParquetConverterEnabled = true

	// Create a filesystem bucket for initial block upload
	fsBucket, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)

	// Create test labels
	lbls := labels.FromStrings("__name__", "test")

	// Create a real TSDB block
	dir := t.TempDir()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	blockID, err := e2e.CreateBlock(context.Background(), rnd, dir, []labels.Labels{lbls}, 2, 0, 2*time.Hour.Milliseconds(), time.Minute.Milliseconds(), 10)
	require.NoError(t, err)
	bdir := path.Join(dir, blockID.String())

	userID := "test-user"

	// Upload the block to filesystem bucket
	err = block.Upload(context.Background(), logger, bucket.NewPrefixedBucketClient(fsBucket, userID), bdir, metadata.NoneFunc)
	require.NoError(t, err)

	// Create a mock bucket that wraps the filesystem bucket but fails uploads
	mockBucket := &mockBucket{
		Bucket:        fsBucket,
		uploadFailure: fmt.Errorf("mock upload failure"),
	}

	converter := newConverter(cfg, objstore.WithNoopInstr(mockBucket), storageCfg, []int64{3600000, 7200000}, nil, reg, overrides, nil)
	converter.ringLifecycler = &ring.Lifecycler{
		Addr: "1.2.3.4",
	}

	err = converter.convertUser(context.Background(), logger, &RingMock{ReadRing: &ring.Ring{}}, userID)
	require.NoError(t, err)

	// Verify the failure metric was incremented
	assert.Equal(t, 1.0, testutil.ToFloat64(converter.metrics.convertBlockFailures.WithLabelValues(userID)))
}

func TestConverter_ShouldNotFailOnAccessDenyError(t *testing.T) {
	// Create a new registry for testing
	reg := prometheus.NewRegistry()

	// Create a new converter with test configuration
	cfg := Config{
		MaxRowsPerRowGroup:  1e6,
		MetaSyncConcurrency: 1,
		DataDir:             t.TempDir(),
	}
	logger := log.NewNopLogger()
	storageCfg := cortex_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&storageCfg)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	overrides := validation.NewOverrides(*limits, nil)
	limits.ParquetConverterEnabled = true

	// Create a filesystem bucket for initial block upload
	fsBucket, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)

	// Create test labels
	lbls := labels.FromStrings("__name__", "test")

	// Create a real TSDB block
	dir := t.TempDir()
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	blockID, err := e2e.CreateBlock(context.Background(), rnd, dir, []labels.Labels{lbls}, 2, 0, 2*time.Hour.Milliseconds(), time.Minute.Milliseconds(), 10)
	require.NoError(t, err)
	bdir := path.Join(dir, blockID.String())

	userID := "test-user"

	// Upload the block to filesystem bucket
	err = block.Upload(context.Background(), logger, bucket.NewPrefixedBucketClient(fsBucket, userID), bdir, metadata.NoneFunc)
	require.NoError(t, err)

	var mb *mockBucket
	t.Run("get failure", func(t *testing.T) {
		// Create a mock bucket that wraps the filesystem bucket but fails with permission denied error.
		mb = &mockBucket{
			Bucket:     fsBucket,
			getFailure: cortex_errors.WithCause(errors.New("dummy error"), status.Error(codes.PermissionDenied, "dummy")),
		}
	})

	t.Run("upload failure", func(t *testing.T) {
		// Create a mock bucket that wraps the filesystem bucket but fails with permission denied error.
		mb = &mockBucket{
			Bucket:        fsBucket,
			uploadFailure: cortex_errors.WithCause(errors.New("dummy error"), status.Error(codes.PermissionDenied, "dummy")),
		}
	})

	converter := newConverter(cfg, objstore.WithNoopInstr(mb), storageCfg, []int64{3600000, 7200000}, nil, reg, overrides, nil)
	converter.ringLifecycler = &ring.Lifecycler{
		Addr: "1.2.3.4",
	}

	err = converter.convertUser(context.Background(), logger, &RingMock{ReadRing: &ring.Ring{}}, userID)
	require.Error(t, err)

	// Verify the failure metric was not incremented
	assert.Equal(t, 0.0, testutil.ToFloat64(converter.metrics.convertBlockFailures.WithLabelValues(userID)))
}

// mockBucket implements objstore.Bucket for testing
type mockBucket struct {
	objstore.Bucket
	uploadFailure error
	getFailure    error
}

func (m *mockBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if m.uploadFailure != nil {
		return m.uploadFailure
	}
	return m.Bucket.Upload(ctx, name, r, opts...)
}

func (m *mockBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if m.getFailure != nil && strings.Contains(name, "index") {
		return nil, m.getFailure
	}
	return m.Bucket.Get(ctx, name)
}

type RingMock struct {
	ring.ReadRing
}

func (r *RingMock) Get(key uint32, op ring.Operation, bufDescs []ring.InstanceDesc, bufHosts []string, bufZones map[string]int) (ring.ReplicationSet, error) {
	return ring.ReplicationSet{
		Instances: []ring.InstanceDesc{
			{
				Addr: "1.2.3.4",
			},
		},
	}, nil
}

// mockTenantLimits implements the validation.TenantLimits interface for testing
type mockTenantLimits struct {
	limits map[string]*validation.Limits
}

func (m *mockTenantLimits) ByUserID(userID string) *validation.Limits {
	if limits, ok := m.limits[userID]; ok {
		return limits
	}
	return nil
}

func (m *mockTenantLimits) AllByUserID() map[string]*validation.Limits {
	return m.limits
}

func TestConverter_SkipBlocksWithExistingValidMarker(t *testing.T) {
	cfg := prepareConfig()
	user := "user"
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	dir := t.TempDir()

	cfg.Ring.InstanceID = "parquet-converter-1"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.KVStore.Mock = ringStore
	bucketClient, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	userBucket := bucket.NewPrefixedBucketClient(bucketClient, user)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.ParquetConverterEnabled = true
	limits.ParquetConverterMaxBlockLabelNames = 1

	c, logger, _ := prepare(t, cfg, objstore.WithNoopInstr(bucketClient), limits, nil)

	ctx := context.Background()

	lbls := labels.FromStrings("__name__", "test")

	// Create a block
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	blockID, err := e2e.CreateBlock(ctx, rnd, dir, []labels.Labels{lbls}, 2, 0, 2*time.Hour.Milliseconds(), time.Minute.Milliseconds(), 10)
	require.NoError(t, err)

	// Upload the block to the bucket
	blockDir := fmt.Sprintf("%s/%s", dir, blockID.String())
	b, err := tsdb.OpenBlock(nil, blockDir, nil, nil)
	require.NoError(t, err)
	err = block.Upload(ctx, logger, userBucket, b.Dir(), metadata.NoneFunc)
	require.NoError(t, err)

	// Write a converter mark with version 1 to simulate an already converted block
	markerV1 := parquet.ConverterMark{
		Version: parquet.ParquetConverterMarkVersion1,
		Shards:  2, // Simulate a block with 2 shards
	}
	markerBytes, err := json.Marshal(markerV1)
	require.NoError(t, err)
	markerPath := path.Join(blockID.String(), parquet.ConverterMarkerFileName)
	err = userBucket.Upload(ctx, markerPath, bytes.NewReader(markerBytes))
	require.NoError(t, err)

	// Verify the marker exists with version 1 and has shards
	marker, err := parquet.ReadConverterMark(ctx, blockID, userBucket, logger)
	require.NoError(t, err)
	require.Equal(t, parquet.ParquetConverterMarkVersion1, marker.Version)
	require.Equal(t, 2, marker.Shards)

	// Start the converter
	err = services.StartAndAwaitRunning(context.Background(), c)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, c) // nolint:errcheck

	// Wait a bit for the converter to process blocks
	time.Sleep(5 * time.Second)

	// Verify the marker version is still 1 (i.e., the block was not converted again)
	markerAfter, err := parquet.ReadConverterMark(ctx, blockID, userBucket, logger)
	require.NoError(t, err)
	require.Equal(t, parquet.ParquetConverterMarkVersion1, markerAfter.Version, "block with existing marker version 1 should not be converted again")

	// Verify that no conversion happened by checking the convertedBlocks metric
	// It should be 0 since the block was already converted
	assert.Equal(t, 0.0, testutil.ToFloat64(c.metrics.convertedBlocks.WithLabelValues(user)))
}

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		numRowGroups int
		expectedErr  error
	}{
		"negative num row groups is invalid": {
			numRowGroups: -1,
			expectedErr:  errInvalidNumRowGroups,
		},
		"zero num row groups is valid (unlimited, single shard)": {
			numRowGroups: 0,
			expectedErr:  nil,
		},
		"positive num row groups is valid": {
			numRowGroups: 5,
			expectedErr:  nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := prepareConfig()
			cfg.NumRowGroups = tc.numRowGroups
			require.Equal(t, tc.expectedErr, cfg.Validate())
		})
	}
}

func TestNewConverter_NumRowGroupsOption(t *testing.T) {
	tests := map[string]struct {
		numRowGroups          int
		expectNumRowGroupsOpt bool
	}{
		"zero does not pass WithNumRowGroups (library default)": {
			numRowGroups:          0,
			expectNumRowGroupsOpt: false,
		},
		"positive passes WithNumRowGroups": {
			numRowGroups:          3,
			expectNumRowGroupsOpt: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := prepareConfig()
			cfg.NumRowGroups = tc.numRowGroups

			c := newConverter(cfg, nil, cortex_tsdb.BlocksStorageConfig{}, nil, log.NewNopLogger(), prometheus.NewPedanticRegistry(), nil, nil)
			// WithColDuration and WithRowGroupSize are always present; WithNumRowGroups
			// is appended only when NumRowGroups > 0.
			expectedLen := 2
			if tc.expectNumRowGroupsOpt {
				expectedLen = 3
			}
			require.Len(t, c.baseConverterOptions, expectedLen)
		})
	}
}

func TestConvertWithMaxNumColumns(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	db, err := tsdb.Open(dbDir, nil, nil, &tsdb.Options{
		RetentionDuration: int64(24 * time.Hour / time.Millisecond),
		NoLockfile:        true,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Create series with many unique label names to exceed column limit
	app := db.Appender(ctx)
	for i := range 10 {
		lblBuilder := labels.NewBuilder(labels.EmptyLabels())
		lblBuilder.Set(labels.MetricName, fmt.Sprintf("metric_%d", i))
		for j := range 5 {
			lblBuilder.Set(fmt.Sprintf("label_%d_%d", i, j), fmt.Sprintf("value_%d", j))
		}
		_, err := app.Append(0, lblBuilder.Labels(), int64(i)*1000, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	head := db.Head()
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	// With low column limit, should produce multiple shards
	shards, err := convert.ConvertTSDBBlock(
		ctx, bkt, head.MinTime(), head.MaxTime(),
		[]convert.Convertible{head},
		slog.Default(),
		convert.WithMaxNumColumns(20),
	)
	require.NoError(t, err)
	require.Greater(t, shards, 1, "expected multiple shards with low column limit")

	// With high column limit, should produce a single shard
	bkt2, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt2.Close() })

	shards2, err := convert.ConvertTSDBBlock(
		ctx, bkt2, head.MinTime(), head.MaxTime(),
		[]convert.Convertible{head},
		slog.Default(),
		convert.WithMaxNumColumns(10000),
	)
	require.NoError(t, err)
	require.Equal(t, 1, shards2, "expected single shard with high column limit")
}

func TestConverter_WriteNoConvertMarkForBlockWithTooManyLabels(t *testing.T) {
	cfg := prepareConfig()
	user := "user"
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	dir := t.TempDir()

	cfg.Ring.InstanceID = "parquet-converter-1"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.KVStore.Mock = ringStore
	bucketClient, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	userBucket := bucket.NewPrefixedBucketClient(bucketClient, user)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.ParquetConverterEnabled = true
	limits.ParquetConverterMaxBlockLabelNames = 1

	c, logger, _ := prepare(t, cfg, objstore.WithNoopInstr(bucketClient), limits, nil)

	ctx := context.Background()

	lbls := labels.FromStrings("__name__", "test", "job", "foo")

	// Create a block
	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	// 2h blocks are skipped by ShouldConvertBlockToParquet
	blockID, err := e2e.CreateBlock(ctx, rnd, dir, []labels.Labels{lbls}, 2, 0, 4*time.Hour.Milliseconds(), time.Minute.Milliseconds(), 10)
	require.NoError(t, err)

	// Upload the block to the bucket
	blockDir := fmt.Sprintf("%s/%s", dir, blockID.String())
	b, err := tsdb.OpenBlock(nil, blockDir, nil, nil)
	require.NoError(t, err)
	err = block.Upload(ctx, logger, userBucket, b.Dir(), metadata.NoneFunc)
	require.NoError(t, err)

	err = services.StartAndAwaitRunning(context.Background(), c)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, c) // nolint:errcheck

	// Start the converter
	err = c.convertUser(ctx, logger, c.ring, user)
	require.NoError(t, err)

	// Verify the marker was written correctly
	readNoConvertMark, err := parquet.ReadNoConvertMark(ctx, blockID, userBucket, logger)
	require.NoError(t, err)
	require.True(t, parquet.ValidNoConvertMarkVersion(readNoConvertMark.Version))
	require.Equal(t, parquet.NoConvertReasonTooManyLabels, readNoConvertMark.Reason)
	require.Equal(t, 2, readNoConvertMark.LabelNamesCount)
	require.Equal(t, 1, readNoConvertMark.MaxBlockLabelNames)

	// Confirm conversion did not happen
	assert.Equal(t, 0.0, testutil.ToFloat64(c.metrics.convertedBlocks.WithLabelValues(user)))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.skippedBlocks.WithLabelValues(user, parquet.NoConvertReasonTooManyLabels)))
}

func TestConverter_NoConvertMarkHandling(t *testing.T) {
	cfg := prepareConfig()
	user := "user"
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	dir := t.TempDir()

	cfg.Ring.InstanceID = "parquet-converter-1"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.KVStore.Mock = ringStore
	bucketClient, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	userBucket := bucket.NewPrefixedBucketClient(bucketClient, user)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.ParquetConverterEnabled = true
	limits.ParquetConverterMaxBlockLabelNames = 3

	c, logger, _ := prepare(t, cfg, objstore.WithNoopInstr(bucketClient), limits, nil)

	ctx := context.Background()
	lbls := labels.FromStrings("__name__", "test", "job", "foo")
	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	createAndUploadBlock := func(mint, maxt int64) ulid.ULID {
		t.Helper()

		blockID, err := e2e.CreateBlock(ctx, rnd, dir, []labels.Labels{lbls}, 2, mint, maxt, time.Minute.Milliseconds(), 10)
		require.NoError(t, err)

		blockDir := fmt.Sprintf("%s/%s", dir, blockID.String())
		b, err := tsdb.OpenBlock(nil, blockDir, nil, nil)
		require.NoError(t, err)
		err = block.Upload(ctx, logger, userBucket, b.Dir(), metadata.NoneFunc)
		require.NoError(t, err)

		return blockID
	}

	manuallyMarkedBlockID := createAndUploadBlock(0, 4*time.Hour.Milliseconds())
	limitIncreasedBlockID := createAndUploadBlock(4*time.Hour.Milliseconds(), 8*time.Hour.Milliseconds())
	stillTooManyLabelsBlockID := createAndUploadBlock(8*time.Hour.Milliseconds(), 12*time.Hour.Milliseconds())

	writeNoConvertMark := func(blockID ulid.ULID, noConvertMark parquet.NoConvertMark) {
		t.Helper()

		markerBytes, err := json.Marshal(noConvertMark)
		require.NoError(t, err)
		markerPath := path.Join(blockID.String(), parquet.NoConvertMarkerFileName)
		err = userBucket.Upload(ctx, markerPath, bytes.NewReader(markerBytes))
		require.NoError(t, err)
	}

	writeNoConvertMark(manuallyMarkedBlockID, parquet.NoConvertMark{
		Version: parquet.CurrentNoConvertMarkVersion,
		Reason:  "manually uploaded",
	})
	writeNoConvertMark(limitIncreasedBlockID, parquet.NoConvertMark{
		Version:            parquet.CurrentNoConvertMarkVersion,
		Reason:             parquet.NoConvertReasonTooManyLabels,
		LabelNamesCount:    2,
		MaxBlockLabelNames: 1,
	})
	writeNoConvertMark(stillTooManyLabelsBlockID, parquet.NoConvertMark{
		Version:            parquet.CurrentNoConvertMarkVersion,
		Reason:             parquet.NoConvertReasonTooManyLabels,
		LabelNamesCount:    4,
		MaxBlockLabelNames: 1,
	})

	err = services.StartAndAwaitRunning(context.Background(), c)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, c) // nolint:errcheck

	err = c.convertUser(ctx, logger, c.ring, user)
	require.NoError(t, err)

	assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.convertedBlocks.WithLabelValues(user)))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.skippedBlocks.WithLabelValues(user, parquet.NoConvertReasonMarkerExists)))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.skippedBlocks.WithLabelValues(user, parquet.NoConvertReasonTooManyLabels)))

	markerAfter, err := parquet.ReadNoConvertMark(ctx, manuallyMarkedBlockID, userBucket, logger)
	require.NoError(t, err)
	require.True(t, parquet.ValidNoConvertMarkVersion(markerAfter.Version))
	require.Equal(t, "manually uploaded", markerAfter.Reason)

	converterMark, err := parquet.ReadConverterMark(ctx, manuallyMarkedBlockID, userBucket, logger)
	require.NoError(t, err)
	require.False(t, parquet.ValidConverterMarkVersion(converterMark.Version))

	converterMark, err = parquet.ReadConverterMark(ctx, limitIncreasedBlockID, userBucket, logger)
	require.NoError(t, err)
	require.True(t, parquet.ValidConverterMarkVersion(converterMark.Version))

	converterMark, err = parquet.ReadConverterMark(ctx, stillTooManyLabelsBlockID, userBucket, logger)
	require.NoError(t, err)
	require.False(t, parquet.ValidConverterMarkVersion(converterMark.Version))
}

func TestEffectiveMaxBlockLabelNamesLeavesRoomForGeneratedColumns(t *testing.T) {
	mint := int64(0)
	maxt := 2 * parquetConverterDataColumnDuration.Milliseconds()
	expectedReservedColumns := parquetConverterSystemColumnCount + 3

	require.Equal(t, 10, effectiveMaxBlockLabelNames(10, mint, maxt))
	require.Equal(t, parquetgo.MaxColumnIndex-expectedReservedColumns, effectiveMaxBlockLabelNames(parquetgo.MaxColumnIndex, mint, maxt))
	require.Equal(t, 0, effectiveMaxBlockLabelNames(0, mint, maxt))
}
