package discardedseries

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	vendMetricsInterval = 30 * time.Second
)

type SeriesCounter struct {
	*sync.RWMutex
	seriesCountMap map[uint64]struct{}
}

type UserCounter struct {
	*sync.RWMutex
	userSeriesMap map[string]*SeriesCounter
}

type DiscardedSeriesTracker struct {
	*sync.RWMutex
	labelUserMap         map[string]*UserCounter
	discardedSeriesGauge *prometheus.GaugeVec
}

func NewDiscardedSeriesTracker(discardedSeriesGauge *prometheus.GaugeVec) *DiscardedSeriesTracker {
	discardedSeriesTracker := &DiscardedSeriesTracker{
		RWMutex:              &sync.RWMutex{},
		labelUserMap:         make(map[string]*UserCounter),
		discardedSeriesGauge: discardedSeriesGauge,
	}
	return discardedSeriesTracker
}

func (t *DiscardedSeriesTracker) Track(reason string, user string, series uint64) {
	t.RLock()
	userCounter, ok := t.labelUserMap[reason]
	t.RUnlock()
	if !ok {
		t.Lock()
		userCounter, ok = t.labelUserMap[reason]
		if !ok {
			userCounter = &UserCounter{
				RWMutex:       &sync.RWMutex{},
				userSeriesMap: make(map[string]*SeriesCounter),
			}
			t.labelUserMap[reason] = userCounter
		}
		t.Unlock()
	}

	userCounter.RLock()
	seriesCounter, ok := userCounter.userSeriesMap[user]
	userCounter.RUnlock()
	if !ok {
		userCounter.Lock()
		seriesCounter, ok = userCounter.userSeriesMap[user]
		if !ok {
			seriesCounter = &SeriesCounter{
				RWMutex:        &sync.RWMutex{},
				seriesCountMap: make(map[uint64]struct{}),
			}
			userCounter.userSeriesMap[user] = seriesCounter
		}
		userCounter.Unlock()
	}

	seriesCounter.RLock()
	_, ok = seriesCounter.seriesCountMap[series]
	seriesCounter.RUnlock()
	if !ok {
		seriesCounter.Lock()
		seriesCounter.seriesCountMap[series] = struct{}{}
		seriesCounter.Unlock()
	}
}

func (t *DiscardedSeriesTracker) UpdateMetrics() {
	usersToDelete := make([]string, 0)
	t.RLock()
	for label, userCounter := range t.labelUserMap {
		userCounter.RLock()
		for user, seriesCounter := range userCounter.userSeriesMap {
			seriesCounter.Lock()
			seriesCount := len(seriesCounter.seriesCountMap)
			t.discardedSeriesGauge.WithLabelValues(label, user).Set(float64(seriesCount))
			clear(seriesCounter.seriesCountMap)
			if seriesCount == 0 {
				usersToDelete = append(usersToDelete, user)
			}
			seriesCounter.Unlock()
		}
		userCounter.RUnlock()
		userCounter.Lock()
		for _, user := range usersToDelete {
			if len(userCounter.userSeriesMap[user].seriesCountMap) == 0 {
				delete(userCounter.userSeriesMap, user)
			}
		}
		userCounter.Unlock()
	}
	t.RUnlock()
}

func (t *DiscardedSeriesTracker) StartDiscardedSeriesGoroutine() {
	go func() {
		ticker := time.NewTicker(vendMetricsInterval)
		for range ticker.C {
			t.UpdateMetrics()
		}
	}()
}

// only used in testing
func (t *DiscardedSeriesTracker) getSeriesCount(label string, user string) int {
	if userCounter, ok := t.labelUserMap[label]; ok {
		if seriesCounter, ok := userCounter.userSeriesMap[user]; ok {
			return len(seriesCounter.seriesCountMap)
		}
	}
	return -1
}
