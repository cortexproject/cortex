package cache

import (
	"context"
	"sync"
)

type mockCache struct {
	sync.Mutex
	cache map[string][]byte
}

func (m *mockCache) Store(_ context.Context, data map[string][]byte) {
	m.Lock()
	defer m.Unlock()
	for k, v := range data {
		m.cache[k] = v
	}
}

func (m *mockCache) Fetch(ctx context.Context, keys []string) (found map[string][]byte, missing []string) {
	m.Lock()
	defer m.Unlock()
	for _, key := range keys {
		buf, ok := m.cache[key]
		if ok {
			found[key] = buf
		} else {
			missing = append(missing, key)
		}
	}
	return
}

func (m *mockCache) Stop() {
}

// NewMockCache makes a new MockCache.
func NewMockCache() Cache {
	return &mockCache{
		cache: map[string][]byte{},
	}
}

// NewNoopCache returns a no-op cache.
func NewNoopCache() Cache {
	return NewTiered(nil)
}
