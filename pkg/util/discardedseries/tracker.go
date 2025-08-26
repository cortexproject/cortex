package discardedseries

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	resetInterval = 30 * time.Second
)

var trackedLabels = []string{"sample_out_of_bounds", "sample_out_of_order", "sample_too_old", "new_value_for_timestamp", "per_user_series_limit", "per_labelset_series_limit", "per_metric_series_limit"}

type SeriesCounter struct {
	*sync.RWMutex
	seriesMap map[string]uint64
}

type WorkspaceCounter struct {
	*sync.RWMutex
	workspaceMap map[string]*SeriesCounter
}

type DiscardedSeriesTracker struct {
	labelMap             map[string]*WorkspaceCounter
	discardedSeriesGauge *prometheus.GaugeVec
}

func NewDiscardedSeriesTracker(discardedSeriesGauge *prometheus.GaugeVec) *DiscardedSeriesTracker {
	labelMap := make(map[string]*WorkspaceCounter)
	for _, label := range trackedLabels {
		labelMap[label] = &WorkspaceCounter{
			RWMutex:      &sync.RWMutex{},
			workspaceMap: make(map[string]*SeriesCounter),
		}
	}
	return &DiscardedSeriesTracker{labelMap: labelMap, discardedSeriesGauge: discardedSeriesGauge}
}

func (t *DiscardedSeriesTracker) Track(label string, workspace string, seriesLabel string) {
	if workspaceCounter, ok := t.labelMap[label]; ok {
		locked := false
		if _, ok := workspaceCounter.workspaceMap[workspace]; !ok {
			workspaceCounter.Lock()
			locked = true
		}

		// Rechecking after locking to avoid race conditions
		if seriesCounter, ok := workspaceCounter.workspaceMap[workspace]; ok {
			seriesCounter.Lock()
			seriesCounter.seriesMap[seriesLabel]++
			seriesCounter.Unlock()
		} else {
			workspaceCounter.workspaceMap[workspace] = &SeriesCounter{
				RWMutex:   &sync.RWMutex{},
				seriesMap: make(map[string]uint64),
			}
			workspaceCounter.workspaceMap[workspace].seriesMap[seriesLabel] = 1
		}

		if locked {
			workspaceCounter.Unlock()
		}
	}
}

func (t *DiscardedSeriesTracker) UpdateMetrics() {
	for label, workspaceCounter := range t.labelMap {
		workspaceCounter.Lock()
		for workspace, seriesCounter := range workspaceCounter.workspaceMap {
			seriesCounter.Lock()
			for seriesLabel, count := range seriesCounter.seriesMap {
				t.discardedSeriesGauge.WithLabelValues(label, workspace, seriesLabel).Set(float64(count))
				if count == 0 {
					delete(seriesCounter.seriesMap, seriesLabel)
				} else {
					seriesCounter.seriesMap[seriesLabel] = 0
				}
			}
			if len(seriesCounter.seriesMap) == 0 {
				delete(workspaceCounter.workspaceMap, workspace)
			}
			seriesCounter.Unlock()
		}
		if len(workspaceCounter.workspaceMap) == 0 {
			delete(t.labelMap, label)
		}
		workspaceCounter.Unlock()
	}
}

func (t *DiscardedSeriesTracker) StartDiscardedSeriesGoroutine() {
	go func() {
		for {
			time.Sleep(resetInterval)
			t.UpdateMetrics()
		}
	}()
}
