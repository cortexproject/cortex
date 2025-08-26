package discardedseries

import (
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	trackedLabels = []string{"sample_out_of_bounds", "sample_out_of_order", "sample_too_old", "new_value_for_timestamp", "per_user_series_limit", "per_labelset_series_limit", "per_metric_series_limit"}
	resetInterval = 30 * time.Second
)

type seriesCounter struct {
	*sync.RWMutex
	seriesMap map[string]uint64
}

type workspaceCounter struct {
	*sync.RWMutex
	workspaceMap map[string]*seriesCounter
}

type DiscardedSeriesTracker struct {
	labelMap map[string]*workspaceCounter
}

func NewDiscardedSeriesTracker() *DiscardedSeriesTracker {
	labelMap := make(map[string]*workspaceCounter)
	for _, label := range trackedLabels {
		labelMap[label] = &workspaceCounter{
			RWMutex:      &sync.RWMutex{},
			workspaceMap: make(map[string]*seriesCounter),
		}
	}
	return &DiscardedSeriesTracker{labelMap: labelMap}
}

func (t *DiscardedSeriesTracker) Track(label string, workspace string, seriesLabel string) {
	if workspaceCounter, ok := t.labelMap[label]; ok {
		locked := false
		if seriesCounter, ok := workspaceCounter.workspaceMap[workspace]; !ok {
			workspaceCounter.Lock()
			locked = true
		}

		// Rechecking after locking to avoid race conditions
		if seriesCounter, ok := workspaceCounter.workspaceMap[workspace]; ok {
			seriesCounter.Lock()
			seriesCounter.seriesMap[seriesLabel]++
			seriesCounter.Unlock()
		} else {
			workspaceCounter.workspaceMap[workspace] = &seriesCounter{
				RWMutex:  &sync.RWMutex{},
				seriesMap: make(map[string]int),
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
