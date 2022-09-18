package purger

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// TombstonesSet holds all the pending delete requests for a user
type TombstonesSet interface {
	// GetDeletedIntervals returns non-overlapping, sorted  deleted intervals.
	GetDeletedIntervals(lbls labels.Labels, from, to model.Time) []model.Interval

	// Len returns number of tombstones that are there
	Len() int

	// HasTombstonesForInterval tells whether there are any tombstones which overlapping given interval
	HasTombstonesForInterval(from, to model.Time) bool
}

type noopTombstonesSet struct {
}

// TombstonesLoader loads delete requests and gen numbers from store and keeps checking for updates.
// It keeps checking for changes in gen numbers, which also means changes in delete requests and reloads specific users delete requests.
type TombstonesLoader interface {
	// GetPendingTombstones returns all pending tombstones
	GetPendingTombstones(userID string) (TombstonesSet, error)

	// GetPendingTombstonesForInterval returns all pending tombstones between two times
	GetPendingTombstonesForInterval(userID string, from, to model.Time) (TombstonesSet, error)

	// GetStoreCacheGenNumber returns store cache gen number for a user
	GetStoreCacheGenNumber(tenantIDs []string) string

	// GetResultsCacheGenNumber returns results cache gen number for a user
	GetResultsCacheGenNumber(tenantIDs []string) string
}

type noopTombstonesLoader struct {
	ts noopTombstonesSet
}

// NewNoopTombstonesLoader creates a TombstonesLoader that does nothing
func NewNoopTombstonesLoader() TombstonesLoader {
	return &noopTombstonesLoader{}
}

func (tl *noopTombstonesLoader) GetPendingTombstones(userID string) (TombstonesSet, error) {
	return &tl.ts, nil
}

func (tl *noopTombstonesLoader) GetPendingTombstonesForInterval(userID string, from, to model.Time) (TombstonesSet, error) {
	return &tl.ts, nil
}

func (tl *noopTombstonesLoader) GetStoreCacheGenNumber(tenantIDs []string) string {
	return ""
}

func (tl *noopTombstonesLoader) GetResultsCacheGenNumber(tenantIDs []string) string {
	return ""
}

func (ts noopTombstonesSet) GetDeletedIntervals(lbls labels.Labels, from, to model.Time) []model.Interval {
	return nil
}

func (ts noopTombstonesSet) Len() int {
	return 0
}

func (ts noopTombstonesSet) HasTombstonesForInterval(from, to model.Time) bool {
	return false
}
