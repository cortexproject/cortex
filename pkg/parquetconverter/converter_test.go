package parquetconverter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
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
	"github.com/cortexproject/cortex/pkg/storage/tsdb/users"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	cortex_errors "github.com/cortexproject/cortex/pkg/util/errors"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
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

	test.Poll(t, 3*time.Minute, 1, func() interface{} {
		blocksConverted = blocksConverted[:0]
		for _, bIds := range blocks {
			m, err := parquet.ReadConverterMark(ctx, bIds, userBucket, logger)
			require.NoError(t, err)
			if m.Version == parquet.CurrentVersion {
				blocksConverted = append(blocksConverted, bIds)
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
	require.NoError(t, cortex_tsdb.WriteTenantDeletionMark(context.Background(), objstore.WithNoopInstr(bucketClient), user, cortex_tsdb.NewTenantDeletionMark(time.Now())))

	// Should clean sync folders
	test.Poll(t, time.Minute, 0, func() interface{} {
		return len(c.listTenantsWithMetaSyncDirectories())
	})

	// Verify metrics after user deletion
	test.Poll(t, time.Minute*10, true, func() interface{} {
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

	scanner, err := users.NewScanner(cortex_tsdb.UsersScannerConfig{
		Strategy: cortex_tsdb.UserScanStrategyList,
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

func TestConverter_SortColumns(t *testing.T) {
	bucketClient, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.ParquetConverterEnabled = true

	testCases := []struct {
		desc                string
		cfg                 Config
		expectedSortColumns []string
	}{
		{
			desc: "no additional sort columns are added",
			cfg: Config{
				MetaSyncConcurrency: 1,
				DataDir:             t.TempDir(),
			},
			expectedSortColumns: []string{labels.MetricName},
		},
		{
			desc: "additional sort columns are added",
			cfg: Config{
				MetaSyncConcurrency: 1,
				DataDir:             t.TempDir(),
				SortColumns:         []string{"cluster", "namespace"},
			},
			expectedSortColumns: []string{labels.MetricName, "cluster", "namespace"},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			c, _, _ := prepare(t, tC.cfg, objstore.WithNoopInstr(bucketClient), limits, nil)
			assert.Equal(t, tC.expectedSortColumns, c.cfg.SortColumns,
				"Converter should be created with the expected sort columns")
		})
	}
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
