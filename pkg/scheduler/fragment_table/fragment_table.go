package fragment_table

import (
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/distributed_execution"
)

type fragmentEntry struct {
	addr      string
	createdAt time.Time
}

// FragmentTable maintains a mapping between query fragments and their assigned querier addresses.
// Entries automatically expire after a configured duration to prevent stale mappings.
type FragmentTable struct {
	mappings   map[distributed_execution.FragmentKey]*fragmentEntry
	mu         sync.RWMutex
	expiration time.Duration
}

// NewFragmentTable creates a new FragmentTable with the specified expiration duration.
// It starts a background goroutine that periodically removes expired entries.
// The cleanup interval is set to half of the expiration duration.
func NewFragmentTable(expiration time.Duration) *FragmentTable {
	ft := &FragmentTable{
		mappings:   make(map[distributed_execution.FragmentKey]*fragmentEntry),
		expiration: expiration,
	}
	go ft.periodicCleanup()
	return ft
}

// AddAddressByID associates a querier address with a specific fragment of a query.
// The association will automatically expire after the configured duration.
func (f *FragmentTable) AddAddressByID(queryID uint64, fragmentID uint64, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
	f.mappings[key] = &fragmentEntry{
		addr:      addr,
		createdAt: time.Now(),
	}
}

// GetAddrByID retrieves the querier address associated with a specific fragment.
func (f *FragmentTable) GetAddrByID(queryID uint64, fragmentID uint64) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
	if entry, ok := f.mappings[key]; ok {
		return entry.addr, true
	}
	return "", false
}

func (f *FragmentTable) cleanupExpired() {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	keysToDelete := make([]distributed_execution.FragmentKey, 0)
	for key, entry := range f.mappings {
		if now.Sub(entry.createdAt) > f.expiration {
			keysToDelete = append(keysToDelete, key)
		}
	}
	for _, key := range keysToDelete {
		delete(f.mappings, key)
	}
}

func (f *FragmentTable) periodicCleanup() {
	ticker := time.NewTicker(f.expiration / 2)
	defer ticker.Stop()
	for range ticker.C {
		f.cleanupExpired()
	}
}
