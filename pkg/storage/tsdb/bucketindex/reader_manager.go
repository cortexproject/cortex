package bucketindex

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/objstore"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// TODO track metrics: refresh duration, get success/failures
// TODO comment
type ReaderManager struct {
	services.Service

	bkt            objstore.Bucket
	logger         log.Logger
	updateInterval time.Duration
	idleTimeout    time.Duration

	indexesMx sync.RWMutex
	indexes   map[string]*cachedIndex
}

func NewReaderManager(bucketClient objstore.Bucket, logger log.Logger, updateInterval, idleTimeout time.Duration) *ReaderManager {
	m := &ReaderManager{
		bkt:            bucketClient,
		logger:         logger,
		updateInterval: updateInterval,
		idleTimeout:    idleTimeout,
		indexes:        map[string]*cachedIndex{},
	}

	// Apply a jitter to the sync frequency in order to increase the probability
	// of hitting the shared cache (if any).
	checkInterval := util.DurationWithJitter(time.Minute, 0.2)
	m.Service = services.NewTimerService(checkInterval, nil, m.checkCachedIndexes, nil)

	return m
}

// GetIndex returns the bucket index for the given user. It returns the in-memory cached
// index if available, or load it from the bucket otherwise.
func (m *ReaderManager) GetIndex(ctx context.Context, userID string) (*Index, error) {
	m.indexesMx.RLock()
	entry := m.indexes[userID]
	m.indexesMx.RUnlock()

	// We don't check if the index is stale because it's the responsability
	// of the background job.
	if entry != nil {
		entry.requestedAt.Store(time.Now().Unix())
		return entry.index, entry.err
	}

	startTime := time.Now()
	idx, err := ReadIndex(ctx, m.bkt, userID, m.logger)
	if err != nil {
		// Cache the error, to avoid hammering the object store in case of persistent issues
		// (eg. corrupted bucket index or not existing).
		m.cacheIndex(userID, nil, err)

		level.Error(m.logger).Log("msg", "unable to load bucket index", "user", userID, "err", err)
		return nil, err
	}

	// Cache the index.
	m.cacheIndex(userID, idx, nil)

	level.Info(m.logger).Log("msg", "loaded bucket index", "user", userID, "duration", time.Since(startTime))
	return idx, nil
}

func (m *ReaderManager) cacheIndex(userID string, idx *Index, err error) {
	now := time.Now()

	m.indexesMx.Lock()
	defer m.indexesMx.Unlock()

	// Not an issue if, due to concurrency, another index was already cached
	// and we overwrite it: last will win.
	m.indexes[userID] = &cachedIndex{
		index:       idx,
		err:         err,
		updatedAt:   atomic.NewInt64(now.Unix()),
		requestedAt: atomic.NewInt64(now.Unix()),
	}
}

// TODO doc
func (m *ReaderManager) checkCachedIndexes(ctx context.Context) error {
	var (
		toUpdate []string
		toDelete []string
	)

	// Build a list of users for which we should update or delete the index.
	m.indexesMx.RLock()
	for userID, entry := range m.indexes {
		if time.Since(entry.getRequestedAt()) > m.idleTimeout {
			toDelete = append(toDelete, userID)
		} else if entry.err != nil || time.Since(entry.getUpdatedAt()) > m.updateInterval {
			// TODO test: an index should always be updated if was in the error state
			// TODO in case of error should be updated way more frequently
			toUpdate = append(toUpdate, userID)
		}
	}
	m.indexesMx.RUnlock()

	// Delete unused indexes, if confirmed they're still unused.
	m.indexesMx.Lock()
	for _, userID := range toDelete {
		if idx := m.indexes[userID]; time.Since(idx.getRequestedAt()) > m.idleTimeout {
			delete(m.indexes, userID)
			level.Info(m.logger).Log("msg", "unloaded idle bucket index", "user", userID)
		}
	}
	m.indexesMx.Unlock()

	// Update used indexes.
	for _, userID := range toUpdate {
		// TODO let configure this timeout
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		idx, err := ReadIndex(readCtx, m.bkt, userID, m.logger)
		if err != nil {
			// TODO test: ensure the error is not cached
			level.Error(m.logger).Log("msg", "unable to update bucket index", "user", userID, "err", err)
			continue
		}

		// Cache it.
		m.indexesMx.Lock()
		m.indexes[userID].index = idx
		m.indexes[userID].err = nil
		m.indexes[userID].updatedAt.Store(time.Now().Unix())
		m.indexesMx.Unlock()
	}

	// Never return error, otherwise the service terminates.
	return nil
}

type cachedIndex struct {
	// We cache either the index or the error occurred while fetching it. They're
	// mutually exclusive.
	index *Index
	err   error

	// Unix timestamp (seconds) of when the index has been updated from the storage the last time.
	updatedAt *atomic.Int64

	// Unix timestamp (seconds) of when the index has been requested the last time.
	requestedAt *atomic.Int64
}

func (i *cachedIndex) getUpdatedAt() time.Time {
	return time.Unix(i.updatedAt.Load(), 0)
}

func (i *cachedIndex) getRequestedAt() time.Time {
	return time.Unix(i.requestedAt.Load(), 0)
}
