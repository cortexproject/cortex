package tsdb

import (
	"fmt"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func Test_ChunkCacheBackendValidation(t *testing.T) {
	tests := map[string]struct {
		cfg         ChunkCacheBackend
		expectedErr error
	}{
		"valid chunk cache type ('')": {
			cfg: ChunkCacheBackend{
				Backend: "",
			},
			expectedErr: nil,
		},
		"valid chunk cache type (in-memory)": {
			cfg: ChunkCacheBackend{
				Backend: CacheBackendInMemory,
			},
			expectedErr: nil,
		},
		"valid chunk cache type (memcached)": {
			cfg: ChunkCacheBackend{
				Backend: CacheBackendMemcached,
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
			},
			expectedErr: nil,
		},
		"valid chunk cache type (redis)": {
			cfg: ChunkCacheBackend{
				Backend: CacheBackendRedis,
				Redis: RedisClientConfig{
					Addresses: "localhost:6379",
				},
			},
			expectedErr: nil,
		},
		"invalid chunk cache type": {
			cfg: ChunkCacheBackend{
				Backend: "dummy",
			},
			expectedErr: errUnsupportedChunkCacheBackend,
		},
		"valid multi chunk cache type": {
			cfg: ChunkCacheBackend{
				Backend: fmt.Sprintf("%s,%s,%s", CacheBackendInMemory, CacheBackendMemcached, CacheBackendRedis),
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
				Redis: RedisClientConfig{
					Addresses: "localhost:6379",
				},
				MultiLevel: MultiLevelChunkCacheConfig{
					MaxAsyncConcurrency: 1,
					MaxAsyncBufferSize:  1,
					MaxBackfillItems:    1,
				},
			},
			expectedErr: nil,
		},
		"duplicate multi chunk cache type": {
			cfg: ChunkCacheBackend{
				Backend: fmt.Sprintf("%s,%s", CacheBackendInMemory, CacheBackendInMemory),
				MultiLevel: MultiLevelChunkCacheConfig{
					MaxAsyncConcurrency: 1,
					MaxAsyncBufferSize:  1,
					MaxBackfillItems:    1,
				},
			},
			expectedErr: errDuplicatedChunkCacheBackend,
		},
		"invalid max async concurrency": {
			cfg: ChunkCacheBackend{
				Backend: fmt.Sprintf("%s,%s", CacheBackendInMemory, CacheBackendMemcached),
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
				MultiLevel: MultiLevelChunkCacheConfig{
					MaxAsyncConcurrency: 0,
					MaxAsyncBufferSize:  1,
					MaxBackfillItems:    1,
				},
			},
			expectedErr: errInvalidMaxAsyncConcurrency,
		},
		"invalid max async buffer size": {
			cfg: ChunkCacheBackend{
				Backend: fmt.Sprintf("%s,%s", CacheBackendInMemory, CacheBackendMemcached),
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
				MultiLevel: MultiLevelChunkCacheConfig{
					MaxAsyncConcurrency: 1,
					MaxAsyncBufferSize:  0,
					MaxBackfillItems:    1,
				},
			},
			expectedErr: errInvalidMaxAsyncBufferSize,
		},
		"invalid max back fill items": {
			cfg: ChunkCacheBackend{
				Backend: fmt.Sprintf("%s,%s", CacheBackendInMemory, CacheBackendMemcached),
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
				MultiLevel: MultiLevelChunkCacheConfig{
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
