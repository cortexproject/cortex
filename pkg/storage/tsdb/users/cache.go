package users

import (
	"context"
	"sync"
	"time"
)

// cachedScanner is a scanner that caches the result of the underlying scanner.
type cachedScanner struct {
	scanner Scanner

	mtx           sync.RWMutex
	lastUpdatedAt time.Time
	ttl           time.Duration

	active, deleting, deleted []string
}

func (s *cachedScanner) ScanUsers(ctx context.Context) ([]string, []string, []string, error) {
	s.mtx.RLock()
	// Check if we have a valid cached result
	if !s.lastUpdatedAt.Before(time.Now().Add(-s.ttl)) {
		active := s.active
		deleting := s.deleting
		deleted := s.deleted
		s.mtx.RUnlock()
		return active, deleting, deleted, nil
	}
	s.mtx.RUnlock()

	// TODO: move to promise based.
	active, deleting, deleted, err := s.scanner.ScanUsers(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	s.mtx.Lock()
	s.active = active
	s.deleting = deleting
	s.deleted = deleted
	s.lastUpdatedAt = time.Now()
	s.mtx.Unlock()

	return active, deleting, deleted, nil
}
