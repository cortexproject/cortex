package bucketindex

import (
	"bytes"
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestLoader_GetIndex_ShouldLazyLoadBucketIndex(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt := prepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", idx))

	// Create the loader.
	loader := NewLoader(prepareLoaderConfig(), bkt, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	// Ensure no index has been loaded yet.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 0
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 0
	`),
		"cortex_bucket_index_loads_total",
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))

	// Request the index multiple times.
	for i := 0; i < 10; i++ {
		actualIdx, err := loader.GetIndex(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, idx, actualIdx)
	}

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 0
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 1
	`),
		"cortex_bucket_index_loads_total",
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_GetIndex_ShouldCacheError(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt := prepareFilesystemBucket(t)

	// Create the loader.
	loader := NewLoader(prepareLoaderConfig(), bkt, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	// Request the index multiple times.
	for i := 0; i < 10; i++ {
		_, err := loader.GetIndex(ctx, "user-1")
		require.Equal(t, ErrIndexNotFound, err)
	}

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 1
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 1
	`),
		"cortex_bucket_index_loads_total",
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldUpdateIndexInBackgroundOnPreviousLoadSuccess(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt := prepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Hour, // Intentionally high to not hit it.
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Update the bucket index.
	idx.Blocks = append(idx.Blocks, &Block{ID: ulid.MustNew(2, nil), MinTime: 20, MaxTime: 30})
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", idx))

	// Wait until the index has been updated in background.
	test.Poll(t, 3*time.Second, 2, func() interface{} {
		actualIdx, err := loader.GetIndex(ctx, "user-1")
		if err != nil {
			return 0
		}
		return len(actualIdx.Blocks)
	})

	actualIdx, err = loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_load_failures_total Total number of bucket index loading failures.
		# TYPE cortex_bucket_index_load_failures_total counter
		cortex_bucket_index_load_failures_total 0
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
	`),
		"cortex_bucket_index_load_failures_total",
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldUpdateIndexInBackgroundOnPreviousLoadFailure(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt := prepareFilesystemBucket(t)

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Hour, // Intentionally high to not hit it.
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	_, err := loader.GetIndex(ctx, "user-1")
	assert.Equal(t, ErrIndexNotFound, err)

	// Upload the bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", idx))

	// Wait until the index has been updated in background.
	test.Poll(t, 3*time.Second, nil, func() interface{} {
		_, err := loader.GetIndex(ctx, "user-1")
		return err
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
	`),
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldNotCacheErrorOnBackgroundUpdates(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt := prepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Write a corrupted index.
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", IndexCompressedFilename), strings.NewReader("invalid!}")))

	// Wait until the first failure has been tracked.
	test.Poll(t, 3*time.Second, true, func() interface{} {
		return testutil.ToFloat64(loader.loadFailures) > 0
	})

	actualIdx, err = loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 1
	`),
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldOffloadIndexIfNotFoundDuringBackgroundUpdates(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt := prepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           time.Hour, // Intentionally high to not hit it.
	}

	loader := NewLoader(cfg, bkt, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Delete the index
	require.NoError(t, DeleteIndex(ctx, bkt, "user-1"))

	// Wait until the index is offloaded.
	test.Poll(t, 3*time.Second, float64(0), func() interface{} {
		return testutil.ToFloat64(loader.loaded)
	})

	_, err = loader.GetIndex(ctx, "user-1")
	require.Equal(t, ErrIndexNotFound, err)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
	`),
		"cortex_bucket_index_loaded",
	))
}

func TestLoader_ShouldOffloadIndexIfIdleTimeoutIsReachedDuringBackgroundUpdates(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	bkt := prepareFilesystemBucket(t)

	// Create a bucket index.
	idx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{
			{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20},
		},
		BlockDeletionMarks: nil,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, WriteIndex(ctx, bkt, "user-1", idx))

	// Create the loader.
	cfg := LoaderConfig{
		CheckInterval:         time.Second,
		UpdateOnStaleInterval: time.Second,
		UpdateOnErrorInterval: time.Second,
		IdleTimeout:           0, // Offload at first check.
	}

	loader := NewLoader(cfg, bkt, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, loader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, loader))
	})

	actualIdx, err := loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Wait until the index is offloaded.
	test.Poll(t, 3*time.Second, float64(0), func() interface{} {
		return testutil.ToFloat64(loader.loaded)
	})

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loaded Number of bucket indexes currently loaded in-memory.
		# TYPE cortex_bucket_index_loaded gauge
		cortex_bucket_index_loaded 0
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 1
	`),
		"cortex_bucket_index_loaded",
		"cortex_bucket_index_loads_total",
	))

	// Load it again.
	actualIdx, err = loader.GetIndex(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, idx, actualIdx)

	// Ensure metrics have been updated accordingly.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_index_loads_total Total number of bucket index loading attempts.
		# TYPE cortex_bucket_index_loads_total counter
		cortex_bucket_index_loads_total 2
	`),
		"cortex_bucket_index_loads_total",
	))
}

func prepareLoaderConfig() LoaderConfig {
	return LoaderConfig{
		CheckInterval:         time.Minute,
		UpdateOnStaleInterval: 15 * time.Minute,
		UpdateOnErrorInterval: time.Minute,
		IdleTimeout:           time.Hour,
	}
}
