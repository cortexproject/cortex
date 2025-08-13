package fragment_table

import (
	"sync"

	"github.com/cortexproject/cortex/pkg/distributed_execution"
)

type FragmentTable struct {
	mappings map[distributed_execution.FragmentKey]string
	mu       sync.RWMutex
}

func NewFragmentTable() *FragmentTable {
	return &FragmentTable{
		mappings: make(map[distributed_execution.FragmentKey]string),
	}
}

func (f *FragmentTable) AddMapping(queryID uint64, fragmentID uint64, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
	f.mappings[key] = addr
}

func (f *FragmentTable) GetAllChildAddresses(queryID uint64, fragmentIDs []uint64) ([]string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	addresses := make([]string, 0, len(fragmentIDs))

	for _, fragmentID := range fragmentIDs {
		key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
		if addr, ok := f.mappings[key]; ok {
			addresses = append(addresses, addr)
		} else {
			return nil, false
		}
	}
	return addresses, true
}

func (f *FragmentTable) GetChildAddr(queryID uint64, fragmentID uint64) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
	if addr, ok := f.mappings[key]; ok {
		return addr, true
	}
	return "", false
}

func (f *FragmentTable) ClearMappings(queryID uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	keysToDelete := make([]distributed_execution.FragmentKey, 0)
	for key := range f.mappings {
		if key.GetQueryID() == queryID {
			keysToDelete = append(keysToDelete, key)
		}
	}

	for _, key := range keysToDelete {
		delete(f.mappings, key)
	}
}
