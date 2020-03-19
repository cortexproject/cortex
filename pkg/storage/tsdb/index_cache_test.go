package tsdb

import (
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
					Addresses: "dns+localhost:11211",
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}

func TestMemcachedIndexCacheConfig_GetAddresses(t *testing.T) {
	tests := map[string]struct {
		cfg      MemcachedIndexCacheConfig
		expected []string
	}{
		"no addresses": {
			cfg: MemcachedIndexCacheConfig{
				Addresses: "",
			},
			expected: []string{},
		},
		"one address": {
			cfg: MemcachedIndexCacheConfig{
				Addresses: "dns+localhost:11211",
			},
			expected: []string{"dns+localhost:11211"},
		},
		"two addresses": {
			cfg: MemcachedIndexCacheConfig{
				Addresses: "dns+memcached-1:11211,dns+memcached-2:11211",
			},
			expected: []string{"dns+memcached-1:11211", "dns+memcached-2:11211"},
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.GetAddresses())
		})
	}
}
