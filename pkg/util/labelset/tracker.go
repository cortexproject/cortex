package labelset

import (
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/cortex/pkg/util"
)

const (
	numMetricShards = 128
)

type LabelSetTracker struct {
	shards []*labelSetCounterShard
}

// NewLabelSetTracker initializes a LabelSetTracker to keep track of active labelset limits.
func NewLabelSetTracker() *LabelSetTracker {
	shards := make([]*labelSetCounterShard, 0, numMetricShards)
	for i := 0; i < numMetricShards; i++ {
		shards = append(shards, &labelSetCounterShard{
			RWMutex:       &sync.RWMutex{},
			userLabelSets: map[string]map[uint64]labels.Labels{},
		})
	}
	return &LabelSetTracker{shards: shards}
}

type labelSetCounterShard struct {
	*sync.RWMutex
	userLabelSets map[string]map[uint64]labels.Labels
}

// Track accepts userID, label set and hash of the label set limit.
func (m *LabelSetTracker) Track(userId string, hash uint64, labelSet labels.Labels) {
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
}

// UpdateMetrics cleans up dangling user and label set from the tracker as well as metrics.
// It takes a function for user to customize the metrics cleanup logic when either a user or
// a specific label set is removed. If a user is removed then removeUser is set to true.
func (m *LabelSetTracker) UpdateMetrics(userSet map[string]map[uint64]struct{}, deleteMetricFunc func(user, labelSetStr string, removeUser bool)) {
	for i := 0; i < numMetricShards; i++ {
		shard := m.shards[i]
		shard.Lock()

		for user, userEntry := range shard.userLabelSets {
			limits, ok := userSet[user]
			// Remove user if it doesn't exist or has no limits anymore.
			if !ok || len(limits) == 0 {
				deleteMetricFunc(user, "", true)
				delete(shard.userLabelSets, user)
				continue
			}
			for h, lbls := range userEntry {
				// This limit no longer exists.
				if _, ok := limits[h]; !ok {
					delete(userEntry, h)
					labelSetStr := lbls.String()
					deleteMetricFunc(user, labelSetStr, false)
					continue
				}
			}
		}

		shard.Unlock()
	}
}

// labelSetExists is used for testing only to check the existence of a label set.
func (m *LabelSetTracker) labelSetExists(userId string, hash uint64, labelSet labels.Labels) bool {
	s := m.shards[util.HashFP(model.Fingerprint(hash))%numMetricShards]
	s.RLock()
	defer s.RUnlock()
	userEntry, ok := s.userLabelSets[userId]
	if !ok {
		return false
	}
	set, ok := userEntry[hash]
	if !ok {
		return false
	}
	return labels.Compare(set, labelSet) == 0
}

// userExists is used for testing only to check the existence of a user.
func (m *LabelSetTracker) userExists(userId string) bool {
	for i := 0; i < numMetricShards; i++ {
		shard := m.shards[i]
		shard.RLock()
		defer shard.RUnlock()
		_, ok := shard.userLabelSets[userId]
		if ok {
			return true
		}
	}
	return false
}
