package discardedseries

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var trackedLabels = []string{
	"sample_out_of_bounds",
	"sample_out_of_order",
	"sample_too_old",
	"new_value_for_timestamp",
	"per_user_series_limit",
	"per_labelset_series_limit",
	"per_metric_series_limit",
}
var vendMetricsInterval = 30 * time.Second

type SeriesCounter struct {
	*sync.RWMutex
	seriesCountMap map[string]uint64
}

type UserCounter struct {
	*sync.RWMutex
	userSeriesMap map[string]*SeriesCounter
}

type DiscardedSeriesTracker struct {
	labelUserMap         map[string]*UserCounter
	discardedSeriesGauge *prometheus.GaugeVec
}

func NewDiscardedSeriesTracker(discardedSeriesGauge *prometheus.GaugeVec) *DiscardedSeriesTracker {
	labelUserMap := make(map[string]*UserCounter)
	for _, label := range trackedLabels {
		labelUserMap[label] = &UserCounter{
			RWMutex:       &sync.RWMutex{},
			userSeriesMap: make(map[string]*SeriesCounter),
		}
	}
	return &DiscardedSeriesTracker{labelUserMap: labelUserMap, discardedSeriesGauge: discardedSeriesGauge}
}

func (t *DiscardedSeriesTracker) Track(label string, user string, series string) {
	if userCounter, ok := t.labelUserMap[label]; ok {
		locked := false
		if _, ok := userCounter.userSeriesMap[user]; !ok {
			userCounter.Lock()
			locked = true
		}

		// Rechecking after locking to avoid race conditions
		if seriesCounter, ok := userCounter.userSeriesMap[user]; ok {
			seriesCounter.Lock()
			seriesCounter.seriesCountMap[series]++
			seriesCounter.Unlock()
		} else {
			userCounter.userSeriesMap[user] = &SeriesCounter{
				RWMutex:        &sync.RWMutex{},
				seriesCountMap: make(map[string]uint64),
			}
			userCounter.userSeriesMap[user].seriesCountMap[series] = 1
		}

		if locked {
			userCounter.Unlock()
		}
	}
}

func (t *DiscardedSeriesTracker) UpdateMetrics() {
	for label, userCounter := range t.labelUserMap {
		userCounter.Lock()
		for user, seriesCounter := range userCounter.userSeriesMap {
			seriesCounter.Lock()
			for seriesLabel, count := range seriesCounter.seriesCountMap {
				t.discardedSeriesGauge.WithLabelValues(label, user, seriesLabel).Set(float64(count))
				if count == 0 {
					delete(seriesCounter.seriesCountMap, seriesLabel)
				} else {
					seriesCounter.seriesCountMap[seriesLabel] = 0
				}
			}
			if len(seriesCounter.seriesCountMap) == 0 {
				delete(userCounter.userSeriesMap, user)
			}
			seriesCounter.Unlock()
		}
		if len(userCounter.userSeriesMap) == 0 {
			delete(t.labelUserMap, label)
		}
		userCounter.Unlock()
	}
}

func (t *DiscardedSeriesTracker) StartDiscardedSeriesGoroutine() {
	go func() {
		for {
			time.Sleep(vendMetricsInterval)
			t.UpdateMetrics()
		}
	}()
}

// only used in testing
func (t *DiscardedSeriesTracker) getSeriesCount(label string, user string, series string) int {
	if userCounter, ok := t.labelUserMap[label]; ok {
		if seriesCounter, ok := userCounter.userSeriesMap[user]; ok {
			if count, ok := seriesCounter.seriesCountMap[series]; ok {
				return int(count)
			}
		}
	}
	return -1
}
