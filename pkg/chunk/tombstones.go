package chunk

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/cortexproject/cortex/pkg/configs/legacy_promql"
	"github.com/cortexproject/cortex/pkg/util"
)

const tombstonesReloadDuration = 15 * time.Minute

// TombstonesSet holds all the pending delete requests for a user
type TombstonesSet struct {
	tombstones                               []DeleteRequest
	oldestTombstoneStart, newestTombstoneEnd model.Time // Used as optimization to find whether we want to iterate over tombstones or not
}

// tombstonesLoader loads delete requests and gen numbers from store and keeps checking for updates.
// It keeps checking for changes in gen numbers, which also means changes in delete requests and reloads specific users delete requests.
type tombstonesLoader struct {
	tombstones    map[string]*TombstonesSet
	tombstonesMtx sync.RWMutex

	cacheGenNumbers    map[string]*CacheGenNumbers
	cacheGenNumbersMtx sync.RWMutex
	deleteStore        DeleteStore
	quit               chan struct{}
}

// NewTombstonesLoader creates a tombstonesLoader
func NewTombstonesLoader(deleteStore DeleteStore) TombstonesLoader {
	tl := tombstonesLoader{
		tombstones:      map[string]*TombstonesSet{},
		deleteStore:     deleteStore,
		cacheGenNumbers: map[string]*CacheGenNumbers{},
	}
	go tl.loop()

	return &tl
}

// Stop stops tombstonesLoader
func (tl *tombstonesLoader) Stop() {
	close(tl.quit)
}

func (tl *tombstonesLoader) loop() {
	tombstonesReloadTimer := time.NewTicker(tombstonesReloadDuration)
	for {
		select {
		case <-tombstonesReloadTimer.C:
			err := tl.reloadTombstones()
			if err != nil {
				level.Error(util.Logger).Log("msg", "error reloading tombstones", "err", err)
			}
		case <-tl.quit:
			return
		}
	}
}

func (tl *tombstonesLoader) reloadTombstones() error {
	updatedGenNumbers := make(map[string]*CacheGenNumbers)
	tl.cacheGenNumbersMtx.RLock()

	// check for updates in loaded gen numbers
	for userID, oldGenNumbers := range tl.cacheGenNumbers {
		newGenNumbers, err := tl.deleteStore.GetCacheGenerationNumbers(context.Background(), userID)
		if err != nil {
			return err
		}

		if *oldGenNumbers != *newGenNumbers {
			updatedGenNumbers[userID] = newGenNumbers
		}
	}

	tl.cacheGenNumbersMtx.RUnlock()

	// for all the updated gen numbers, reload delete requests
	for userID, genNumbers := range updatedGenNumbers {
		err := tl.loadPendingTombstones(userID)
		if err != nil {
			return err
		}

		tl.cacheGenNumbersMtx.Lock()
		tl.cacheGenNumbers[userID] = genNumbers
		tl.cacheGenNumbersMtx.Unlock()
	}

	return nil
}

// GetPendingTombstones returns all pending tombstones
func (tl *tombstonesLoader) GetPendingTombstones(userID string) (TombstonesAnalyzer, error) {
	tl.tombstonesMtx.RLock()

	tombstoneSet, isOK := tl.tombstones[userID]
	if isOK {
		tl.tombstonesMtx.RUnlock()
		return tombstoneSet, nil
	}

	tl.tombstonesMtx.RUnlock()
	err := tl.loadPendingTombstones(userID)
	if err != nil {
		return nil, err
	}

	tl.tombstonesMtx.RLock()
	defer tl.tombstonesMtx.RUnlock()

	return tl.tombstones[userID], nil
}

func (tl *tombstonesLoader) loadPendingTombstones(userID string) error {
	pendingDeleteRequests, err := tl.deleteStore.GetPendingDeleteRequestsForUser(context.Background(), userID)
	if err != nil {
		return err
	}

	tombstoneSet := TombstonesSet{tombstones: pendingDeleteRequests, oldestTombstoneStart: model.Now()}
	for i := range tombstoneSet.tombstones {
		tombstoneSet.tombstones[i].Matchers = make([][]*labels.Matcher, len(tombstoneSet.tombstones[i].Selectors))

		for j, selector := range tombstoneSet.tombstones[i].Selectors {
			tombstoneSet.tombstones[i].Matchers[j], err = promql.ParseMetricSelector(selector)

			if err != nil {
				return err
			}
		}

		if tombstoneSet.tombstones[i].StartTime < tombstoneSet.oldestTombstoneStart {
			tombstoneSet.oldestTombstoneStart = tombstoneSet.tombstones[i].StartTime
		}

		if tombstoneSet.tombstones[i].EndTime > tombstoneSet.newestTombstoneEnd {
			tombstoneSet.newestTombstoneEnd = tombstoneSet.tombstones[i].EndTime
		}
	}

	tl.tombstonesMtx.Lock()
	defer tl.tombstonesMtx.Unlock()
	tl.tombstones[userID] = &tombstoneSet

	return nil
}

// GetDeletedIntervals returns non-overlapping, sorted  deleted intervals.
func (ts TombstonesSet) GetDeletedIntervals(labels labels.Labels, from, to model.Time) []model.Interval {
	if len(ts.tombstones) == 0 || to < ts.oldestTombstoneStart || from > ts.newestTombstoneEnd {
		return nil
	}

	deletedTimeRanges := []model.Interval{}

	for i := range ts.tombstones {
		overlaps, overlappingIntervalStart, overlappingIntervalEnd := getOverlappingInterval(from, to, ts.tombstones[i].StartTime, ts.tombstones[i].EndTime)

		if !overlaps {
			continue
		}

		matches := false
		for _, matchers := range ts.tombstones[i].Matchers {
			if util.CompareMatchersWithLabels(matchers, labels) {
				matches = true
				break
			}
		}

		if !matches {
			continue
		}

		deletedTimeRange := model.Interval{Start: overlappingIntervalStart, End: overlappingIntervalEnd}
		if overlappingIntervalStart == from && overlappingIntervalEnd == to {
			// whole interval deleted
			return []model.Interval{deletedTimeRange}
		}

		deletedTimeRanges = append(deletedTimeRanges, deletedTimeRange)
	}

	if len(deletedTimeRanges) == 0 {
		return nil
	}

	return mergeTimeRanges(deletedTimeRanges)
}

// Len returns number of tombstones that are there
func (ts TombstonesSet) Len() int {
	return len(ts.tombstones)
}

// HasTombstonesForInterval tells whether there are any tombstones which overlapping given interval
func (ts TombstonesSet) HasTombstonesForInterval(from, to model.Time) bool {
	if to < ts.oldestTombstoneStart || from > ts.newestTombstoneEnd {
		return false
	}

	return true
}

// GetStoreCacheGenNumber returns store cache gen number for a user
func (tl *tombstonesLoader) GetStoreCacheGenNumber(userID string) (string, error) {
	cacheGenNumbers, err := tl.getCacheGenNumbers(userID)
	if err != nil {
		return "", err
	}

	return cacheGenNumbers.store, nil
}

// GetResultsCacheGenNumber returns results cache gen number for a user
func (tl *tombstonesLoader) GetResultsCacheGenNumber(userID string) (string, error) {
	cacheGenNumbers, err := tl.getCacheGenNumbers(userID)
	if err != nil {
		return "", err
	}

	return cacheGenNumbers.results, nil
}

func (tl *tombstonesLoader) getCacheGenNumbers(userID string) (*CacheGenNumbers, error) {
	tl.cacheGenNumbersMtx.RLock()
	if genNumbers, isOK := tl.cacheGenNumbers[userID]; isOK {
		tl.cacheGenNumbersMtx.RUnlock()
		return genNumbers, nil
	}

	tl.cacheGenNumbersMtx.RUnlock()
	genNumbers, err := tl.deleteStore.GetCacheGenerationNumbers(context.Background(), userID)
	if err != nil {
		return nil, err
	}

	tl.cacheGenNumbersMtx.Lock()
	defer tl.cacheGenNumbersMtx.Unlock()

	tl.cacheGenNumbers[userID] = genNumbers
	return genNumbers, nil
}

// sorts and merges overlapping intervals
func mergeTimeRanges(timeRanges []model.Interval) []model.Interval {
	if len(timeRanges) <= 1 {
		return timeRanges
	}

	mergedTimeRanges := make([]model.Interval, 0, len(timeRanges))
	sort.Slice(timeRanges, func(i, j int) bool {
		return timeRanges[i].Start < timeRanges[j].Start
	})

	ongoingTrFrom, ongoingTrTo := timeRanges[0].Start, timeRanges[0].End
	for i := 1; i < len(timeRanges); {
		if timeRanges[i].Start > ongoingTrTo {
			mergedTimeRanges = append(mergedTimeRanges, model.Interval{Start: ongoingTrFrom, End: ongoingTrTo})
			ongoingTrFrom = timeRanges[i].Start
			ongoingTrTo = timeRanges[i].End
			continue
		}

		if timeRanges[i].End > ongoingTrTo {
			ongoingTrTo = timeRanges[i].End
		}
		i++
	}

	return mergedTimeRanges
}

func getOverlappingInterval(from, to, fromInterval, toInterval model.Time) (bool, model.Time, model.Time) {
	if fromInterval > from {
		from = fromInterval
	}

	if toInterval < to {
		to = toInterval
	}
	return from < to, from, to
}
