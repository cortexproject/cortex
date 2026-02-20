package distributed_execution

import (
	"sync"
	"time"
)

const (
	DefaultTTL = 1 * time.Minute
)

// QueryTracker manages the lifecycle and state of query fragments during distributed execution.
// It provides thread-safe access to fragment results and their status.
type QueryTracker struct {
	sync.RWMutex
	cache map[FragmentKey]FragmentResult
}

// FragmentStatus represents the current state of a query fragment.
type FragmentStatus string

const (
	StatusWriting FragmentStatus = "writing"
	StatusDone    FragmentStatus = "done"
	StatusError   FragmentStatus = "error"
)

// FragmentResult holds the result data and metadata for a query fragment.
type FragmentResult struct {
	Data       interface{}
	Status     FragmentStatus
	Expiration time.Time
}

// NewQueryTracker creates a new QueryTracker instance with an initialized cache.
func NewQueryTracker() *QueryTracker {
	return &QueryTracker{
		cache: make(map[FragmentKey]FragmentResult),
	}
}

func (qt *QueryTracker) Size() int {
	return len(qt.cache)
}

// InitWriting initializes a new fragment entry with writing status.
func (qt *QueryTracker) InitWriting(key FragmentKey) {
	qt.Lock()
	defer qt.Unlock()
	qt.cache[key] = FragmentResult{
		Status:     StatusWriting,
		Expiration: time.Now().Add(DefaultTTL),
	}
}

// SetComplete marks a fragment as complete with its result data.
func (qt *QueryTracker) SetComplete(key FragmentKey, data interface{}) {
	qt.Lock()
	defer qt.Unlock()
	qt.cache[key] = FragmentResult{
		Data:       data,
		Status:     StatusDone,
		Expiration: time.Now().Add(DefaultTTL),
	}
}

// SetError marks a fragment as failed.
func (qt *QueryTracker) SetError(key FragmentKey) {
	qt.Lock()
	defer qt.Unlock()
	qt.cache[key] = FragmentResult{
		Status:     StatusError,
		Expiration: time.Now().Add(DefaultTTL),
	}
}

// IsReady checks if a fragment has completed processing successfully.
func (qt *QueryTracker) IsReady(key FragmentKey) bool {
	qt.RLock()
	defer qt.RUnlock()
	if result, ok := qt.cache[key]; ok {
		return result.Status == StatusDone
	}
	return false
}

// Get retrieves the fragment result and existence status for a given key.
func (qt *QueryTracker) Get(key FragmentKey) (FragmentResult, bool) {
	qt.RLock()
	defer qt.RUnlock()
	result, ok := qt.cache[key]
	return result, ok
}

// GetFragmentStatus returns the current status of a fragment.
func (qt *QueryTracker) GetFragmentStatus(key FragmentKey) FragmentStatus {
	qt.RLock()
	defer qt.RUnlock()
	result, ok := qt.cache[key]
	if !ok {
		return FragmentStatus("")
	}
	return result.Status
}

// CleanExpired removes all expired fragment entries from the cache.
func (qt *QueryTracker) CleanExpired() {
	qt.Lock()
	defer qt.Unlock()
	now := time.Now()
	for key, result := range qt.cache {
		if now.After(result.Expiration) {
			delete(qt.cache, key)
		}
	}
}

// ClearQuery removes all fragments associated with the specified query ID.
func (qt *QueryTracker) ClearQuery(queryID uint64) {
	qt.Lock()
	defer qt.Unlock()
	for key := range qt.cache {
		if key.queryID == queryID {
			delete(qt.cache, key)
		}
	}
}
