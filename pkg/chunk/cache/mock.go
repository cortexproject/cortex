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
}

func (m *MockCache) Store(_ context.Context, keys []string, bufs [][]byte, ttl time.Duration) {
	m.Lock()
	defer m.Unlock()
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
