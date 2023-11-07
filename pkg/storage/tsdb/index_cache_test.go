package tsdb

import (
	"fmt"
	"testing"

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
			expected: errNoIndexCacheAddresses,
		},
		"one memcached address should pass": {
			cfg: IndexCacheConfig{
				Backend: "memcached",
				Memcached: MemcachedIndexCacheConfig{
					ClientConfig: MemcachedClientConfig{
						Addresses: "dns+localhost:11211",
					},
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
				},
			},
			expected: fmt.Errorf("unsupported item type foo"),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}
