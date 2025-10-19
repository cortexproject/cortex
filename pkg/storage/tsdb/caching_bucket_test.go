package tsdb

import (
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

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
