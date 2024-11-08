package distributor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/cortex/pkg/util"
)

const (
	numMetricCounterShards = 128
)

type labelSetCounter struct {
	shards []*labelSetCounterShard
}

func newLabelSetCounter() *labelSetCounter {
	shards := make([]*labelSetCounterShard, 0, numMetricCounterShards)
	for i := 0; i < numMetricCounterShards; i++ {
		shards = append(shards, &labelSetCounterShard{
			RWMutex:       &sync.RWMutex{},
			valuesCounter: map[string]map[uint64]*samplesLabelSetEntry{},
		})
	}
	return &labelSetCounter{shards: shards}
}

type labelSetCounterShard struct {
	*sync.RWMutex
	valuesCounter map[string]map[uint64]*samplesLabelSetEntry
}

type samplesLabelSetEntry struct {
	floatSamples     int64
	histogramSamples int64
	labels           labels.Labels
}

func (s *samplesLabelSetEntry) reset() {
	s.floatSamples = 0
	s.histogramSamples = 0
}

func (m *labelSetCounter) increaseSamplesLabelSet(userId string, hash uint64, labelSet labels.Labels, floatSamples, histogramSamples int64) {
	s := m.shards[util.HashFP(model.Fingerprint(hash))%numMetricCounterShards]
	s.Lock()
	defer s.Unlock()
	if userEntry, ok := s.valuesCounter[userId]; ok {
		if e, ok2 := userEntry[hash]; ok2 {
			e.floatSamples += floatSamples
			e.histogramSamples += histogramSamples
		} else {
			userEntry[hash] = &samplesLabelSetEntry{
				floatSamples:     floatSamples,
				histogramSamples: histogramSamples,
				labels:           labelSet,
			}
		}
	} else {
		s.valuesCounter[userId] = map[uint64]*samplesLabelSetEntry{
			hash: {
				floatSamples:     floatSamples,
				histogramSamples: histogramSamples,
				labels:           labelSet,
			},
		}
	}
}

func (m *labelSetCounter) updateMetrics(userSet map[string]map[uint64]struct{}, receivedSamplesPerLabelSet *prometheus.CounterVec) {
	for i := 0; i < numMetricCounterShards; i++ {
		shard := m.shards[i]
		shard.Lock()

		for user, userEntry := range shard.valuesCounter {
			limits, ok := userSet[user]
			if !ok {
				// If user is removed, we will delete user metrics in cleanupInactiveUser loop
				// so skip deleting metrics here.
				delete(shard.valuesCounter, user)
				continue
			}
			for h, entry := range userEntry {
				labelSetStr := entry.labels.String()
				// This limit no longer exists.
				if _, ok := limits[h]; !ok {
					delete(userEntry, h)
					receivedSamplesPerLabelSet.DeleteLabelValues(user, sampleMetricTypeFloat, labelSetStr)
					receivedSamplesPerLabelSet.DeleteLabelValues(user, sampleMetricTypeHistogram, labelSetStr)
					continue
				}
				receivedSamplesPerLabelSet.WithLabelValues(user, sampleMetricTypeFloat, labelSetStr).Add(float64(entry.floatSamples))
				receivedSamplesPerLabelSet.WithLabelValues(user, sampleMetricTypeHistogram, labelSetStr).Add(float64(entry.histogramSamples))
				// Reset entry counter to 0. Delete it only if it is removed from the limit.
				entry.reset()
			}
		}

		shard.Unlock()
	}
}
