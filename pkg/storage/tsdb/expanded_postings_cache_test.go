package tsdb

import (
	"context"
	"fmt"
	"strings"
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
	old, loaded := cache.getPromiseForKey("key1", func() (int, int64, error) {
		return 1, 0, nil
	})
	require.False(t, loaded)
	v, err := old.result(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, v)
	require.False(t, cache.contains("key1"))
}

func TestFifoCacheExpire(t *testing.T) {

	keySize := 20
	numberOfKeys := 100

	tc := map[string]struct {
		cfg                PostingsCacheConfig
		expectedFinalItems int
		ttlExpire          bool
	}{
		"MaxBytes": {
			expectedFinalItems: 10,
			cfg: PostingsCacheConfig{
				Enabled:  true,
				Ttl:      time.Hour,
				MaxBytes: int64(10 * (8 + keySize)),
			},
		},
		"TTL": {
			expectedFinalItems: numberOfKeys,
			ttlExpire:          true,
			cfg: PostingsCacheConfig{
				Enabled:  true,
				Ttl:      time.Hour,
				MaxBytes: 10 << 20,
			},
		},
	}

	for name, c := range tc {
		t.Run(name, func(t *testing.T) {
			m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
			timeNow := time.Now
			cache := newFifoCache[int](c.cfg, "test", m, timeNow)

			for i := 0; i < numberOfKeys; i++ {
				key := RepeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
				p, loaded := cache.getPromiseForKey(key, func() (int, int64, error) {
					return 1, 8, nil
				})
				require.False(t, loaded)
				v, err := p.result(context.Background())
				require.NoError(t, err)
				require.Equal(t, 1, v)
				require.True(t, cache.contains(key))
				p, loaded = cache.getPromiseForKey(key, func() (int, int64, error) {
					return 1, 0, nil
				})
				require.True(t, loaded)
				v, err = p.result(context.Background())
				require.NoError(t, err)
				require.Equal(t, 1, v)
			}

			totalCacheSize := 0

			for i := 0; i < numberOfKeys; i++ {
				key := RepeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
				if cache.contains(key) {
					totalCacheSize++
				}
			}

			require.Equal(t, c.expectedFinalItems, totalCacheSize)

			if c.ttlExpire {
				cache.timeNow = func() time.Time {
					return timeNow().Add(2 * c.cfg.Ttl)
				}

				for i := 0; i < numberOfKeys; i++ {
					key := RepeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
					originalSize := cache.cachedBytes
					p, loaded := cache.getPromiseForKey(key, func() (int, int64, error) {
						return 2, 18, nil
					})
					require.False(t, loaded)
					v, err := p.result(context.Background())
					require.NoError(t, err)
					// New value
					require.Equal(t, 2, v)
					// Total Size Updated
					require.Equal(t, originalSize+10, cache.cachedBytes)
				}
			}
		})
	}
}

func RepeatStringIfNeeded(seed string, length int) string {
	if len(seed) > length {
		return seed
	}

	return strings.Repeat(seed, 1+length/len(seed))[:max(length, len(seed))]
}
