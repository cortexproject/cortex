package users

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

// cachedScanner is a scanner that caches the result of the underlying scanner.
type cachedScanner struct {
	scanner Scanner

	mtx           sync.RWMutex
	lastUpdatedAt time.Time
	ttl           time.Duration

	active, deleting, deleted []string

	requests prometheus.Counter
	hits     prometheus.Counter
}

func newCachedScanner(scanner Scanner, cfg tsdb.UsersScannerConfig, reg prometheus.Registerer) *cachedScanner {
	return &cachedScanner{
		scanner: scanner,
		ttl:     cfg.CacheTTL,
		requests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_cached_users_scanner_requests_total",
			Help: "Total number of scans made to the cache scanner",
		}),
		hits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_cached_users_scanner_hits_total",
			Help: "Total number of hits of scanner cache",
		}),
	}
}

func (s *cachedScanner) ScanUsers(ctx context.Context) ([]string, []string, []string, error) {
	s.requests.Inc()
	s.mtx.Lock()
	defer s.mtx.Unlock()
	// Check if we have a valid cached result
	if !s.lastUpdatedAt.Before(time.Now().Add(-s.ttl)) {
		active := s.active
		deleting := s.deleting
		deleted := s.deleted
		s.hits.Inc()
		return active, deleting, deleted, nil
	}

	// TODO: move to promise based.
	active, deleting, deleted, err := s.scanner.ScanUsers(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	s.active = active
	s.deleting = deleting
	s.deleted = deleted
	s.lastUpdatedAt = time.Now()

	return active, deleting, deleted, nil
}
