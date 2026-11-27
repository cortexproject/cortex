package tsdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestIndexCacheConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg      IndexCacheConfig
		expected error
	}{
		"default config should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				return cfg
			}(),
		},
		"unsupported backend should fail": {
			cfg: IndexCacheConfig{
				Backend: "xxx",
			},
			expected: errUnsupportedIndexCacheBackend,
		},
		"no memcached addresses should fail": {
			cfg: IndexCacheConfig{
				Backend: "memcached",
			},
			expected: errNoCacheAddresses,
		},
		"one memcached address should pass": {
			cfg: IndexCacheConfig{
				Backend: "memcached",
				Memcached: MemcachedIndexCacheConfig{
					ClientConfig: MemcachedClientConfig{
						Addresses: "dns+localhost:11211",
					},
					TTL: time.Hour,
				},
			},
		},
		"invalid enabled items memcached": {
			cfg: IndexCacheConfig{
				Backend: "memcached",
				Memcached: MemcachedIndexCacheConfig{
					ClientConfig: MemcachedClientConfig{
						Addresses: "dns+localhost:11211",
					},
					EnabledItems: []string{"foo", "bar"},
					TTL:          time.Hour,
				},
			},
			expected: fmt.Errorf("unsupported item type foo"),
		},
		"invalid enabled items inmemory": {
			cfg: IndexCacheConfig{
				Backend: "inmemory",
				InMemory: InMemoryIndexCacheConfig{
					EnabledItems: []string{"foo", "bar"},
				},
			},
			expected: fmt.Errorf("unsupported item type foo"),
		},
		"invalid enabled items redis": {
			cfg: IndexCacheConfig{
				Backend: "redis",
				Redis: RedisIndexCacheConfig{
					ClientConfig: RedisClientConfig{
						Addresses: "test",
					},
					EnabledItems: []string{"foo", "bar"},
					TTL:          time.Hour,
				},
			},
			expected: fmt.Errorf("unsupported item type foo"),
		},
		"zero memcached ttl should pass": {
			cfg: IndexCacheConfig{
				Backend: "memcached",
				Memcached: MemcachedIndexCacheConfig{
					ClientConfig: MemcachedClientConfig{
						Addresses: "dns+localhost:11211",
					},
				},
			},
		},
		"negative memcached ttl should fail": {
			cfg: IndexCacheConfig{
				Backend: "memcached",
				Memcached: MemcachedIndexCacheConfig{
					ClientConfig: MemcachedClientConfig{
						Addresses: "dns+localhost:11211",
					},
					TTL: -time.Hour,
				},
			},
			expected: errInvalidIndexCacheTTL,
		},
		"one redis address with ttl should pass": {
			cfg: IndexCacheConfig{
				Backend: "redis",
				Redis: RedisIndexCacheConfig{
					ClientConfig: RedisClientConfig{
						Addresses: "test",
					},
					TTL: 7 * 24 * time.Hour,
				},
			},
		},
		"zero redis ttl should pass": {
			cfg: IndexCacheConfig{
				Backend: "redis",
				Redis: RedisIndexCacheConfig{
					ClientConfig: RedisClientConfig{
						Addresses: "test",
					},
				},
			},
		},
		"negative redis ttl should fail": {
			cfg: IndexCacheConfig{
				Backend: "redis",
				Redis: RedisIndexCacheConfig{
					ClientConfig: RedisClientConfig{
						Addresses: "test",
					},
					TTL: -time.Second,
				},
			},
			expected: errInvalidIndexCacheTTL,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}

func TestRemoteIndexCacheTTL(t *testing.T) {
	assert.Equal(t, defaultTTL, remoteIndexCacheTTL(0))
	assert.Equal(t, time.Hour, remoteIndexCacheTTL(time.Hour))
	assert.Equal(t, 30*24*time.Hour, remoteIndexCacheTTL(30*24*time.Hour))
}
