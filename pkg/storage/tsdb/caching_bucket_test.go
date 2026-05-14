package tsdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

type countingBucket struct {
	objstore.Bucket
	getCount int64
}

func (b *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.getCount++
	return b.Bucket.Get(ctx, name)
}

func (b *countingBucket) WithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	return b
}

func (b *countingBucket) ReaderWithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b
}

func Test_BucketCacheBackendValidation(t *testing.T) {
	tests := map[string]struct {
		cfg         BucketCacheBackend
		expectedErr error
	}{
		"valid bucket cache type ('')": {
			cfg: BucketCacheBackend{
				Backend: "",
			},
			expectedErr: nil,
		},
		"valid bucket cache type (in-memory)": {
			cfg: BucketCacheBackend{
				Backend: CacheBackendInMemory,
			},
			expectedErr: nil,
		},
		"valid bucket cache type (memcached)": {
			cfg: BucketCacheBackend{
				Backend: CacheBackendMemcached,
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
			},
			expectedErr: nil,
		},
		"valid bucket cache type (redis)": {
			cfg: BucketCacheBackend{
				Backend: CacheBackendRedis,
				Redis: RedisClientConfig{
					Addresses: "localhost:6379",
				},
			},
			expectedErr: nil,
		},
		"invalid bucket cache type": {
			cfg: BucketCacheBackend{
				Backend: "dummy",
			},
			expectedErr: errUnsupportedBucketCacheBackend,
		},
		"valid multi bucket cache type": {
			cfg: BucketCacheBackend{
				Backend: fmt.Sprintf("%s,%s,%s", CacheBackendInMemory, CacheBackendMemcached, CacheBackendRedis),
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
				Redis: RedisClientConfig{
					Addresses: "localhost:6379",
				},
				MultiLevel: MultiLevelBucketCacheConfig{
					MaxAsyncConcurrency: 1,
					MaxAsyncBufferSize:  1,
					MaxBackfillItems:    1,
				},
			},
			expectedErr: nil,
		},
		"duplicate multi bucket cache type": {
			cfg: BucketCacheBackend{
				Backend: fmt.Sprintf("%s,%s", CacheBackendInMemory, CacheBackendInMemory),
				MultiLevel: MultiLevelBucketCacheConfig{
					MaxAsyncConcurrency: 1,
					MaxAsyncBufferSize:  1,
					MaxBackfillItems:    1,
				},
			},
			expectedErr: errDuplicatedBucketCacheBackend,
		},
		"invalid max async concurrency": {
			cfg: BucketCacheBackend{
				Backend: fmt.Sprintf("%s,%s", CacheBackendInMemory, CacheBackendMemcached),
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
				MultiLevel: MultiLevelBucketCacheConfig{
					MaxAsyncConcurrency: 0,
					MaxAsyncBufferSize:  1,
					MaxBackfillItems:    1,
				},
			},
			expectedErr: errInvalidMaxAsyncConcurrency,
		},
		"invalid max async buffer size": {
			cfg: BucketCacheBackend{
				Backend: fmt.Sprintf("%s,%s", CacheBackendInMemory, CacheBackendMemcached),
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
				MultiLevel: MultiLevelBucketCacheConfig{
					MaxAsyncConcurrency: 1,
					MaxAsyncBufferSize:  0,
					MaxBackfillItems:    1,
				},
			},
			expectedErr: errInvalidMaxAsyncBufferSize,
		},
		"invalid max back fill items": {
			cfg: BucketCacheBackend{
				Backend: fmt.Sprintf("%s,%s", CacheBackendInMemory, CacheBackendMemcached),
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
				MultiLevel: MultiLevelBucketCacheConfig{
					MaxAsyncConcurrency: 1,
					MaxAsyncBufferSize:  1,
					MaxBackfillItems:    0,
				},
			},
			expectedErr: errInvalidMaxBackfillItems,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			assert.Equal(t, tc.expectedErr, err)
		})
	}
}

func Test_BucketIndexCache(t *testing.T) {
	const bucketIndexFile = "user1/bucket-index.json.gz"
	const fileContent = "test-content"
	ctx := context.Background()

	tests := map[string]struct {
		ttl          time.Duration
		expectCached bool
	}{
		"TTL > 0 caches bucket-index": {
			ttl:          5 * time.Minute,
			expectCached: true,
		},
		"TTL = 0 does not cache bucket-index": {
			ttl:          0,
			expectCached: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			inmem := objstore.NewInMemBucket()
			require.NoError(t, inmem.Upload(ctx, bucketIndexFile, bytes.NewReader([]byte(fileContent))))

			wrappedBucket := &countingBucket{Bucket: inmem}
			metadataCfg := MetadataCacheConfig{
				BucketCacheBackend: BucketCacheBackend{
					Backend:  CacheBackendInMemory,
					InMemory: InMemoryBucketCacheConfig{MaxSizeBytes: 1024 * 1024},
				},
				BucketIndexContentTTL: tc.ttl,
				BucketIndexMaxSize:    1024 * 1024,
			}

			bkt, err := CreateCachingBucket(ChunksCacheConfig{}, metadataCfg, ParquetLabelsCacheConfig{}, NewMatchers(), wrappedBucket, log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(t, err)

			r, err := bkt.Get(ctx, bucketIndexFile)
			require.NoError(t, err)
			_, _ = io.ReadAll(r)
			_ = r.Close()
			assert.Equal(t, int64(1), wrappedBucket.getCount)

			r, err = bkt.Get(ctx, bucketIndexFile)
			require.NoError(t, err)
			_, _ = io.ReadAll(r)
			_ = r.Close()

			if tc.expectCached {
				assert.Equal(t, int64(1), wrappedBucket.getCount, "second Get should be served by the cache")
			} else {
				assert.Equal(t, int64(2), wrappedBucket.getCount, "second Get should be served by the bucket")
			}
		})
	}
}

func Test_BucketIndexCacheForCompactor(t *testing.T) {
	const bucketIndexFile = "user1/bucket-index.json.gz"
	const fileContent = "test-content"
	ctx := context.Background()

	tests := map[string]struct {
		ttl          time.Duration
		expectCached bool
	}{
		"TTL > 0 caches bucket-index": {
			ttl:          5 * time.Minute,
			expectCached: true,
		},
		"TTL = 0 does not cache bucket-index": {
			ttl:          0,
			expectCached: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			inmem := objstore.NewInMemBucket()
			require.NoError(t, inmem.Upload(ctx, bucketIndexFile, bytes.NewReader([]byte(fileContent))))

			wrappedBucket := &countingBucket{Bucket: inmem}
			metadataCfg := MetadataCacheConfig{
				BucketCacheBackend: BucketCacheBackend{
					Backend:  CacheBackendInMemory,
					InMemory: InMemoryBucketCacheConfig{MaxSizeBytes: 1024 * 1024},
				},
				BucketIndexContentTTL: tc.ttl,
				BucketIndexMaxSize:    1024 * 1024,
			}

			bkt, err := CreateCachingBucketForCompactor(metadataCfg, false, wrappedBucket, log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(t, err)

			r, err := bkt.Get(ctx, bucketIndexFile)
			require.NoError(t, err)
			_, _ = io.ReadAll(r)
			_ = r.Close()
			assert.Equal(t, int64(1), wrappedBucket.getCount)

			r, err = bkt.Get(ctx, bucketIndexFile)
			require.NoError(t, err)
			_, _ = io.ReadAll(r)
			_ = r.Close()

			if tc.expectCached {
				assert.Equal(t, int64(1), wrappedBucket.getCount, "second Get should be served by the cache")
			} else {
				assert.Equal(t, int64(2), wrappedBucket.getCount, "second Get should be served by the bucket")
			}
		})
	}
}

func TestIsTenantDir(t *testing.T) {
	assert.False(t, isTenantBlocksDir(""))
	assert.True(t, isTenantBlocksDir("test"))
	assert.True(t, isTenantBlocksDir("test/"))
	assert.False(t, isTenantBlocksDir("test/block"))
	assert.False(t, isTenantBlocksDir("test/block/chunks"))
}

func TestIsBucketIndexFile(t *testing.T) {
	assert.False(t, isBucketIndexFiles(""))
	assert.False(t, isBucketIndexFiles("test"))
	assert.False(t, isBucketIndexFiles("test/block"))
	assert.False(t, isBucketIndexFiles("test/block/chunks"))
	assert.True(t, isBucketIndexFiles("test/bucket-index.json.gz"))
	assert.True(t, isBucketIndexFiles("test/bucket-index-sync-status.json"))
}

func TestIsBlockIndexFile(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	assert.False(t, isBlockIndexFile(""))
	assert.False(t, isBlockIndexFile("/index"))
	assert.False(t, isBlockIndexFile("test/index"))
	assert.False(t, isBlockIndexFile("/test/index"))
	assert.True(t, isBlockIndexFile(fmt.Sprintf("%s/index", blockID.String())))
	assert.True(t, isBlockIndexFile(fmt.Sprintf("/%s/index", blockID.String())))
}
