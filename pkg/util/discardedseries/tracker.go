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
	seriesCountMap map[uint64]struct{}
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

func (t *DiscardedSeriesTracker) Track(label string, user string, series uint64) {
	if userCounter, ok := t.labelUserMap[label]; ok {
		seriesCounter, ok := userCounter.userSeriesMap[user]
		if !ok {
			seriesCounter = &SeriesCounter{
				RWMutex:        &sync.RWMutex{},
				seriesCountMap: make(map[uint64]struct{}),
			}
			userCounter.Lock()
			userCounter.userSeriesMap[user] = seriesCounter
			userCounter.Unlock()
		}

		if _, ok := seriesCounter.seriesCountMap[series]; !ok {
			seriesCounter.Lock()
			seriesCounter.seriesCountMap[series] = struct{}{}
			seriesCounter.Unlock()
		}
	}
}

func (t *DiscardedSeriesTracker) UpdateMetrics() {
	for label, userCounter := range t.labelUserMap {
		userCounter.Lock()
		for user, seriesCounter := range userCounter.userSeriesMap {
			seriesCounter.Lock()
			seriesCount := len(seriesCounter.seriesCountMap)
			t.discardedSeriesGauge.WithLabelValues(label, user).Set(float64(seriesCount))
			clear(seriesCounter.seriesCountMap)
			if seriesCount == 0 {
				delete(userCounter.userSeriesMap, user)
			}
			seriesCounter.Unlock()
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
func (t *DiscardedSeriesTracker) getSeriesCount(label string, user string) int {
	if userCounter, ok := t.labelUserMap[label]; ok {
		if seriesCounter, ok := userCounter.userSeriesMap[user]; ok {
			return len(seriesCounter.seriesCountMap)
		}
	}
	return -1
}
