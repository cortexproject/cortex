package discardedseries

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TODO: if we change per labelset series limit from one reasoning to many, we can remove the hardcoded reasoning and add an extra reasoning map
const (
	perLabelsetSeriesLimit = "per_labelset_series_limit"
)

type labelsetCounterStruct struct {
	*sync.RWMutex
	labelsetSeriesMap map[uint64]*seriesCounterStruct
}

type DiscardedSeriesPerLabelsetTracker struct {
	*sync.RWMutex
	userLabelsetMap                 map[string]*labelsetCounterStruct
	discardedSeriesPerLabelsetGauge *prometheus.GaugeVec
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
	t.RLock()
	for user, labelsetCounter := range t.userLabelsetMap {
		labelsetsToDelete := make([]uint64, 0)
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

// only used in testing
func (t *DiscardedSeriesPerLabelsetTracker) getSeriesCount(user string, labelsetLimitHash uint64) int {
	if labelsetCounter, ok := t.userLabelsetMap[user]; ok {
		if seriesCounter, ok := labelsetCounter.labelsetSeriesMap[labelsetLimitHash]; ok {
			return len(seriesCounter.seriesCountMap)
		}
	}
	return 0
}
