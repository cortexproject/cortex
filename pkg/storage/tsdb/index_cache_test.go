package tsdb

import (
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
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
				Memcached: MemcachedClientConfig{
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
