package tsdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestFifoCacheDisabled(t *testing.T) {
	cfg := PostingsCacheConfig{}
	cfg.Enabled = false
	m := NewPostingCacheMetrics(prometheus.DefaultRegisterer)
	timeNow := time.Now
	cache := newFifoCache[int](cfg, "test", m, timeNow)
	old, loaded := cache.getOrStore("key1", 1)
	require.False(t, loaded)
	require.Equal(t, 1, old)
	require.False(t, cache.contains("key1"))
}

func TestFifoCacheExpire(t *testing.T) {
	tc := map[string]struct {
		cfg                PostingsCacheConfig
		expectedFinalItems int
		ttlExpire          bool
	}{
		"MaxItems": {
			expectedFinalItems: 3,
			cfg: PostingsCacheConfig{
				MaxItems: 3,
				Enabled:  true,
				Ttl:      time.Hour,
				MaxBytes: 10 << 20,
			},
		},
		"MaxBytes": {
			expectedFinalItems: 10,
			cfg: PostingsCacheConfig{
				MaxItems: 10 << 20,
				Enabled:  true,
				Ttl:      time.Hour,
				MaxBytes: 80, // 10*8,
			},
		},
		"TTL": {
			expectedFinalItems: 10,
			ttlExpire:          true,
			cfg: PostingsCacheConfig{
				MaxItems: 10 << 20,
				Enabled:  true,
				Ttl:      time.Hour,
				MaxBytes: 80, // 10*8,
			},
		},
	}

	for name, c := range tc {
		t.Run(name, func(t *testing.T) {
			m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
			timeNow := time.Now
			cache := newFifoCache[int](c.cfg, "test", m, timeNow)

			numberOfKeys := 100

			for i := 0; i < numberOfKeys; i++ {
				key := fmt.Sprintf("key%d", i)
				old, loaded := cache.getOrStore(key, 1)
				require.False(t, loaded)
				cache.created(key, time.Now(), 8)
				require.Equal(t, 1, old)
				require.True(t, cache.contains(key))
				old, loaded = cache.getOrStore(key, 1)
				require.True(t, loaded)
				require.Equal(t, 1, old)
			}

			totalCacheSize := 0

			for i := 0; i < numberOfKeys; i++ {
				key := fmt.Sprintf("key%d", i)
				if cache.contains(key) {
					totalCacheSize++
				}
			}

			require.Equal(t, c.expectedFinalItems, totalCacheSize)

			if c.ttlExpire {
				for i := 0; i < numberOfKeys; i++ {
					key := fmt.Sprintf("key%d", i)
					cache.timeNow = func() time.Time {
						return timeNow().Add(2 * c.cfg.Ttl)
					}
					_, loaded := cache.getOrStore(key, 1)
					require.False(t, loaded)
				}
			}
		})
	}
}
