package discardedseries

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	vendMetricsInterval = 30 * time.Second
)

type seriesCounterStruct struct {
	*sync.RWMutex
	seriesCountMap map[uint64]struct{}
	labelsetId     string
}

type userCounterStruct struct {
	*sync.RWMutex
	userSeriesMap map[string]*seriesCounterStruct
}

type DiscardedSeriesTracker struct {
	*sync.RWMutex
	reasonUserMap        map[string]*userCounterStruct
	discardedSeriesGauge *prometheus.GaugeVec
}

func NewDiscardedSeriesTracker(discardedSeriesGauge *prometheus.GaugeVec) *DiscardedSeriesTracker {
	tracker := &DiscardedSeriesTracker{
		RWMutex:              &sync.RWMutex{},
		reasonUserMap:        make(map[string]*userCounterStruct),
		discardedSeriesGauge: discardedSeriesGauge,
	}
	return tracker
}

func (t *DiscardedSeriesTracker) Track(reason string, user string, series uint64) {
	t.RLock()
	userCounter, ok := t.reasonUserMap[reason]
	t.RUnlock()
	if !ok {
		t.Lock()
		userCounter, ok = t.reasonUserMap[reason]
		if !ok {
			userCounter = &userCounterStruct{
				RWMutex:       &sync.RWMutex{},
				userSeriesMap: make(map[string]*seriesCounterStruct),
			}
			t.reasonUserMap[reason] = userCounter
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
			seriesCounter = &seriesCounterStruct{
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
		_, ok = seriesCounter.seriesCountMap[series]
		if !ok {
			seriesCounter.seriesCountMap[series] = struct{}{}
		}
		seriesCounter.Unlock()
	}
}

func (t *DiscardedSeriesTracker) UpdateMetrics() {
	t.RLock()
	for reason, userCounter := range t.reasonUserMap {
		usersToDelete := make([]string, 0)
		userCounter.RLock()
		for user, seriesCounter := range userCounter.userSeriesMap {
			seriesCounter.Lock()
			count := len(seriesCounter.seriesCountMap)
			t.discardedSeriesGauge.WithLabelValues(reason, user).Set(float64(count))
			clear(seriesCounter.seriesCountMap)
			if count == 0 {
				usersToDelete = append(usersToDelete, user)
			}
			seriesCounter.Unlock()
		}
		userCounter.RUnlock()
		if len(usersToDelete) > 0 {
			userCounter.Lock()
			for _, user := range usersToDelete {
				if _, ok := userCounter.userSeriesMap[user]; ok {
					t.discardedSeriesGauge.DeleteLabelValues(reason, user)
					delete(userCounter.userSeriesMap, user)
				}
			}
			userCounter.Unlock()
		}
	}
	t.RUnlock()
}

func (t *DiscardedSeriesTracker) StartVendDiscardedSeriesMetricGoroutine() {
	go func() {
		ticker := time.NewTicker(vendMetricsInterval)
		for range ticker.C {
			t.UpdateMetrics()
		}
	}()
}

// only used in testing
func (t *DiscardedSeriesTracker) getSeriesCount(reason string, user string) int {
	if userCounter, ok := t.reasonUserMap[reason]; ok {
		if seriesCounter, ok := userCounter.userSeriesMap[user]; ok {
			return len(seriesCounter.seriesCountMap)
		}
	}
	return 0
}
