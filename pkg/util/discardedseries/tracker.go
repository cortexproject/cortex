package discardedseries

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	vendMetricsInterval = 30 * time.Second
)

type labelCounterStruct struct {
	*sync.RWMutex
	*labels.Labels
	inCurrentCycle bool
}

type seriesCounterStruct struct {
	*sync.RWMutex
	seriesCountMap map[uint64]*labelCounterStruct
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

func (t *DiscardedSeriesTracker) Track(reason string, user string, labels *labels.Labels) {
	series := labels.Hash()
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
				seriesCountMap: make(map[uint64]*labelCounterStruct),
			}
			userCounter.userSeriesMap[user] = seriesCounter
		}
		userCounter.Unlock()
	}

	seriesCounter.RLock()
	labelCounter, ok := seriesCounter.seriesCountMap[series]
	seriesCounter.RUnlock()
	if !ok {
		seriesCounter.Lock()
		labelCounter, ok = seriesCounter.seriesCountMap[series]
		if !ok {
			labelCounter = &labelCounterStruct{
				Labels:         labels,
				RWMutex:        &sync.RWMutex{},
				inCurrentCycle: true,
			}
			seriesCounter.seriesCountMap[series] = labelCounter
		}
		seriesCounter.Unlock()
	}

	labelCounter.Lock()
	labelCounter.inCurrentCycle = true
	labelCounter.Unlock()
}

func (t *DiscardedSeriesTracker) UpdateMetrics() {
	usersToDelete := make([]string, 0)
	t.RLock()
	for reason, userCounter := range t.reasonUserMap {
		userCounter.RLock()
		for user, seriesCounter := range userCounter.userSeriesMap {
			seriesCounter.Lock()
			for hash, labelCounter := range seriesCounter.seriesCountMap {
				labelCounter.Lock()
				if labelCounter.inCurrentCycle {
					t.discardedSeriesGauge.WithLabelValues(reason, user, labelCounter.String()).Set(1.0)
					labelCounter.inCurrentCycle = false
				} else {
					t.discardedSeriesGauge.DeleteLabelValues(reason, user, labelCounter.String())
					delete(seriesCounter.seriesCountMap, hash)
				}
				labelCounter.Unlock()
			}
			if len(seriesCounter.seriesCountMap) == 0 {
				usersToDelete = append(usersToDelete, user)
			}
			seriesCounter.Unlock()
		}
		userCounter.RUnlock()
		userCounter.Lock()
		for _, user := range usersToDelete {
			_, ok := userCounter.userSeriesMap[user]
			if ok && userCounter.userSeriesMap[user].seriesCountMap != nil {
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
func (t *DiscardedSeriesTracker) getSeriesCount(reason string, user string) int {
	count := -1
	if userCounter, ok := t.reasonUserMap[reason]; ok {
		if seriesCounter, ok := userCounter.userSeriesMap[user]; ok {
			count = 0
			for _, label := range seriesCounter.seriesCountMap {
				if label.inCurrentCycle {
					count++
				}
			}
		}
	}
	return count
}
