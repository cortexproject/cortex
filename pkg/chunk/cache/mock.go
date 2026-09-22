package cache

import (
	"context"
	"sync"
	"time"
)

// MockCache is a simple in-memory cache for testing that also captures the last TTL used.
type MockCache struct {
	sync.Mutex
	cache   map[string][]byte
	lastTTL time.Duration
	// DefaultTTL, when set, is applied when a Store call passes a TTL of 0, mirroring
	// the global default validity of real cache backends (Memcached/Redis/FIFO).
	DefaultTTL time.Duration
}

// Store records the resolved TTL. Mirroring real cache backends (Memcached/Redis/FIFO),
// a TTL of 0 falls back to the configured default validity.
func (m *MockCache) Store(_ context.Context, keys []string, bufs [][]byte, ttl time.Duration) {
	m.Lock()
	defer m.Unlock()
	if ttl == 0 {
		ttl = m.DefaultTTL
	}
	m.lastTTL = ttl
	for i := range keys {
		m.cache[keys[i]] = bufs[i]
	}
}

func (m *MockCache) Fetch(ctx context.Context, keys []string, ttl time.Duration) (found []string, bufs [][]byte, missing []string) {
	m.Lock()
	defer m.Unlock()
	for _, key := range keys {
		buf, ok := m.cache[key]
		if ok {
			found = append(found, key)
			bufs = append(bufs, buf)
		} else {
			missing = append(missing, key)
		}
	}
	return
}

func (m *MockCache) Stop() {
}

// GetLastTTL returns the TTL from the last Store call (useful for testing TTL behavior).
func (m *MockCache) GetLastTTL() time.Duration {
	m.Lock()
	defer m.Unlock()
	return m.lastTTL
}

// NewMockCache makes a new MockCache.
func NewMockCache() Cache {
	return &MockCache{
		cache: map[string][]byte{},
	}
}

// NewNoopCache returns a no-op cache.
func NewNoopCache() Cache {
	return NewTiered(nil)
}
