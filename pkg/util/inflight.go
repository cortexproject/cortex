package util

import "sync"

type InflightRequestTracker struct {
	mu  sync.RWMutex
	cnt int
}

func NewInflightRequestTracker() *InflightRequestTracker {
	return &InflightRequestTracker{}
}

func (t *InflightRequestTracker) Inc() {
	t.mu.Lock()
	t.cnt++
	t.mu.Unlock()
}

func (t *InflightRequestTracker) Dec() {
	t.mu.Lock()
	t.cnt--
	t.mu.Unlock()
}

func (t *InflightRequestTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cnt
}
