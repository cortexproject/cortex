package distributor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/cortex/pkg/util"
)

const (
	numMetricShards = 128
)

type labelSetTracker struct {
	receivedSamplesPerLabelSet *prometheus.CounterVec

	shards []*labelSetCounterShard
}

func newLabelSetTracker(receivedSamplesPerLabelSet *prometheus.CounterVec) *labelSetTracker {
	shards := make([]*labelSetCounterShard, 0, numMetricShards)
	for i := 0; i < numMetricShards; i++ {
		shards = append(shards, &labelSetCounterShard{
			RWMutex:       &sync.RWMutex{},
			userLabelSets: map[string]map[uint64]labels.Labels{},
		})
	}
	return &labelSetTracker{shards: shards, receivedSamplesPerLabelSet: receivedSamplesPerLabelSet}
}

type labelSetCounterShard struct {
	*sync.RWMutex
	userLabelSets map[string]map[uint64]labels.Labels
}

type samplesLabelSetEntry struct {
	floatSamples     int64
	histogramSamples int64
	labels           labels.Labels
}

func (m *labelSetTracker) increaseSamplesLabelSet(userId string, hash uint64, labelSet labels.Labels, floatSamples, histogramSamples int64) {
	s := m.shards[util.HashFP(model.Fingerprint(hash))%numMetricShards]
	s.Lock()
	if userEntry, ok := s.userLabelSets[userId]; ok {
		if _, ok2 := userEntry[hash]; !ok2 {
			userEntry[hash] = labelSet
		}
	} else {
		s.userLabelSets[userId] = map[uint64]labels.Labels{hash: labelSet}
	}
	// Unlock before we update metrics.
	s.Unlock()

	labelSetStr := labelSet.String()
	if floatSamples > 0 {
		m.receivedSamplesPerLabelSet.WithLabelValues(userId, sampleMetricTypeFloat, labelSetStr).Add(float64(floatSamples))
	}
	if histogramSamples > 0 {
		m.receivedSamplesPerLabelSet.WithLabelValues(userId, sampleMetricTypeHistogram, labelSetStr).Add(float64(histogramSamples))
	}
}

// Clean up dangling user and label set from the tracker as well as metrics.
func (m *labelSetTracker) updateMetrics(userSet map[string]map[uint64]struct{}) {
	for i := 0; i < numMetricShards; i++ {
		shard := m.shards[i]
		shard.Lock()

		for user, userEntry := range shard.userLabelSets {
			limits, ok := userSet[user]
			if !ok {
				// If user is removed, we will delete user metrics in cleanupInactiveUser loop
				// so skip deleting metrics here.
				delete(shard.userLabelSets, user)
				continue
			}
			for h, lbls := range userEntry {
				// This limit no longer exists.
				if _, ok := limits[h]; !ok {
					delete(userEntry, h)
					labelSetStr := lbls.String()
					m.receivedSamplesPerLabelSet.DeleteLabelValues(user, sampleMetricTypeFloat, labelSetStr)
					m.receivedSamplesPerLabelSet.DeleteLabelValues(user, sampleMetricTypeHistogram, labelSetStr)
					continue
				}
			}
		}

		shard.Unlock()
	}
}
