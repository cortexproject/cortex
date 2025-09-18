package discardedseries

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	vendMetricsInterval    = 30 * time.Second
	perLabelsetSeriesLimit = "per_labelset_series_limit"
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

type labelsetCounterStruct struct {
	*sync.RWMutex
	labelsetSeriesMap map[uint64]*seriesCounterStruct
}

type DiscardedSeriesTracker struct {
	*sync.RWMutex
	reasonUserMap        map[string]*userCounterStruct
	discardedSeriesGauge *prometheus.GaugeVec
}

type DiscardedSeriesPerLabelsetTracker struct {
	*sync.RWMutex
	userLabelsetMap                 map[string]*labelsetCounterStruct
	discardedSeriesPerLabelsetGauge *prometheus.GaugeVec
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
	usersToDelete := make([]string, 0)
	t.RLock()
	for reason, userCounter := range t.reasonUserMap {
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

func NewDiscardedSeriesPerLabelsetTracker(discardedSeriesPerLabelsetGauge *prometheus.GaugeVec) *DiscardedSeriesPerLabelsetTracker {
	tracker := &DiscardedSeriesPerLabelsetTracker{
		RWMutex:                         &sync.RWMutex{},
		userLabelsetMap:                 make(map[string]*labelsetCounterStruct),
		discardedSeriesPerLabelsetGauge: discardedSeriesPerLabelsetGauge,
	}
	return tracker
}

func (t *DiscardedSeriesPerLabelsetTracker) Track(user string, series uint64, matchedLabelsetHash uint64, matchedLabelsetId string) {
	t.RLock()
	labelsetCounter, ok := t.userLabelsetMap[user]
	t.RUnlock()
	if !ok {
		t.Lock()
		labelsetCounter, ok = t.userLabelsetMap[user]
		if !ok {
			labelsetCounter = &labelsetCounterStruct{
				RWMutex:           &sync.RWMutex{},
				labelsetSeriesMap: make(map[uint64]*seriesCounterStruct),
			}
			t.userLabelsetMap[user] = labelsetCounter
		}
		t.Unlock()
	}

	labelsetCounter.RLock()
	seriesCounter, ok := labelsetCounter.labelsetSeriesMap[matchedLabelsetHash]
	labelsetCounter.RUnlock()
	if !ok {
		labelsetCounter.Lock()
		seriesCounter, ok = labelsetCounter.labelsetSeriesMap[matchedLabelsetHash]
		if !ok {
			seriesCounter = &seriesCounterStruct{
				RWMutex:        &sync.RWMutex{},
				seriesCountMap: make(map[uint64]struct{}),
				labelsetId:     matchedLabelsetId,
			}
			labelsetCounter.labelsetSeriesMap[matchedLabelsetHash] = seriesCounter
		}
		labelsetCounter.Unlock()
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

func (t *DiscardedSeriesPerLabelsetTracker) UpdateMetrics() {
	usersToDelete := make([]string, 0)
	labelsetsToDelete := make([]uint64, 0)
	t.RLock()
	for user, labelsetCounter := range t.userLabelsetMap {
		labelsetCounter.RLock()
		if len(labelsetCounter.labelsetSeriesMap) == 0 {
			usersToDelete = append(usersToDelete, user)
		}
		for labelsetHash, seriesCounter := range labelsetCounter.labelsetSeriesMap {
			seriesCounter.Lock()
			count := len(seriesCounter.seriesCountMap)
			t.discardedSeriesPerLabelsetGauge.WithLabelValues(perLabelsetSeriesLimit, user, seriesCounter.labelsetId).Set(float64(count))
			clear(seriesCounter.seriesCountMap)
			if count == 0 {
				labelsetsToDelete = append(labelsetsToDelete, labelsetHash)
			}
			seriesCounter.Unlock()
		}
		labelsetCounter.RUnlock()
		if len(labelsetsToDelete) > 0 {
			labelsetCounter.Lock()
			for _, labelsetHash := range labelsetsToDelete {
				if _, ok := labelsetCounter.labelsetSeriesMap[labelsetHash]; ok {
					labelsetId := labelsetCounter.labelsetSeriesMap[labelsetHash].labelsetId
					t.discardedSeriesPerLabelsetGauge.DeleteLabelValues(perLabelsetSeriesLimit, user, labelsetId)
					delete(labelsetCounter.labelsetSeriesMap, labelsetHash)
				}
			}
			labelsetCounter.Unlock()
		}
	}
	t.RUnlock()
	if len(usersToDelete) > 0 {
		t.Lock()
		for _, user := range usersToDelete {
			delete(t.userLabelsetMap, user)
		}
		t.Unlock()
	}
}

func (t *DiscardedSeriesPerLabelsetTracker) StartVendDiscardedSeriesMetricGoroutine() {
	go func() {
		ticker := time.NewTicker(vendMetricsInterval)
		for range ticker.C {
			t.UpdateMetrics()
		}
	}()
}

func (t *DiscardedSeriesTracker) getSeriesCount(reason string, user string) int {
	if userCounter, ok := t.reasonUserMap[reason]; ok {
		if seriesCounter, ok := userCounter.userSeriesMap[user]; ok {
			return len(seriesCounter.seriesCountMap)
		}
	}
	return 0
}
