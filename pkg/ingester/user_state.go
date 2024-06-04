package ingester

import (
	"context"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/segmentio/fasthash/fnv1a"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// DiscardedSamples metric labels
const (
	perUserSeriesLimit     = "per_user_series_limit"
	perMetricSeriesLimit   = "per_metric_series_limit"
	perLabelsetSeriesLimit = "per_labelset_series_limit"
)

const numMetricCounterShards = 128

type metricCounterShard struct {
	mtx sync.Mutex
	m   map[string]int
}

type metricCounter struct {
	limiter *Limiter
	shards  []metricCounterShard

	ignoredMetrics map[string]struct{}
}

func newMetricCounter(limiter *Limiter, ignoredMetricsForSeriesCount map[string]struct{}) *metricCounter {
	shards := make([]metricCounterShard, 0, numMetricCounterShards)
	for i := 0; i < numMetricCounterShards; i++ {
		shards = append(shards, metricCounterShard{
			m: map[string]int{},
		})
	}
	return &metricCounter{
		limiter: limiter,
		shards:  shards,

		ignoredMetrics: ignoredMetricsForSeriesCount,
	}
}

func (m *metricCounter) decreaseSeriesForMetric(metricName string) {
	shard := m.getShard(metricName)
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	shard.m[metricName]--
	if shard.m[metricName] == 0 {
		delete(shard.m, metricName)
	}
}

func (m *metricCounter) getShard(metricName string) *metricCounterShard {
	shard := &m.shards[util.HashFP(model.Fingerprint(fnv1a.HashString64(metricName)))%numMetricCounterShards]
	return shard
}

func (m *metricCounter) canAddSeriesFor(userID, metric string) error {
	if _, ok := m.ignoredMetrics[metric]; ok {
		return nil
	}

	shard := m.getShard(metric)
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	return m.limiter.AssertMaxSeriesPerMetric(userID, shard.m[metric])
}

func (m *metricCounter) increaseSeriesForMetric(metric string) {
	shard := m.getShard(metric)
	shard.mtx.Lock()
	shard.m[metric]++
	shard.mtx.Unlock()
}

type labelSetCounterEntry struct {
	count  int
	labels labels.Labels
}

type labelSetCounterShard struct {
	*sync.RWMutex
	valuesCounter map[uint64]*labelSetCounterEntry
}

type labelSetCounter struct {
	limiter *Limiter
	shards  []*labelSetCounterShard
}

func newLabelSetCounter(limiter *Limiter) *labelSetCounter {
	shards := make([]*labelSetCounterShard, 0, numMetricCounterShards)
	for i := 0; i < numMetricCounterShards; i++ {
		shards = append(shards, &labelSetCounterShard{
			RWMutex:       &sync.RWMutex{},
			valuesCounter: map[uint64]*labelSetCounterEntry{},
		})
	}
	return &labelSetCounter{
		shards:  shards,
		limiter: limiter,
	}
}

func (m *labelSetCounter) canAddSeriesForLabelSet(ctx context.Context, u *userTSDB, metric labels.Labels) error {
	return m.limiter.AssertMaxSeriesPerLabelSet(u.userID, metric, func(set validation.LimitsPerLabelSet) (int, error) {
		s := m.shards[util.HashFP(model.Fingerprint(set.Hash))%numMetricCounterShards]
		s.RLock()
		if r, ok := s.valuesCounter[set.Hash]; ok {
			s.RUnlock()
			return r.count, nil
		}
		s.RUnlock()

		// We still dont keep track of this label value so we need to backfill
		return m.backFillLimit(ctx, u, set, s)
	})
}

func (m *labelSetCounter) backFillLimit(ctx context.Context, u *userTSDB, limit validation.LimitsPerLabelSet, s *labelSetCounterShard) (int, error) {
	ir, err := u.db.Head().Index()
	if err != nil {
		return 0, err
	}

	defer ir.Close()

	s.Lock()
	defer s.Unlock()
	if r, ok := s.valuesCounter[limit.Hash]; !ok {
		postings := make([]index.Postings, 0, len(limit.LabelSet))
		for _, lbl := range limit.LabelSet {
			p, err := ir.Postings(ctx, lbl.Name, lbl.Value)
			if err != nil {
				return 0, err
			}
			postings = append(postings, p)
		}

		p := index.Intersect(postings...)

		totalCount := 0
		for p.Next() {
			totalCount++
		}

		if p.Err() != nil {
			return 0, p.Err()
		}

		s.valuesCounter[limit.Hash] = &labelSetCounterEntry{
			count:  totalCount,
			labels: limit.LabelSet,
		}
		return totalCount, nil
	} else {
		return r.count, nil
	}
}

func (m *labelSetCounter) increaseSeriesLabelSet(u *userTSDB, metric labels.Labels) {
	limits := m.limiter.limitsPerLabelSets(u.userID, metric)
	for _, l := range limits {
		s := m.shards[util.HashFP(model.Fingerprint(l.Hash))%numMetricCounterShards]
		s.Lock()
		if e, ok := s.valuesCounter[l.Hash]; ok {
			e.count++
		} else {
			s.valuesCounter[l.Hash] = &labelSetCounterEntry{
				count:  1,
				labels: l.LabelSet,
			}
		}
		s.Unlock()
	}
}

func (m *labelSetCounter) decreaseSeriesLabelSet(u *userTSDB, metric labels.Labels) {
	limits := m.limiter.limitsPerLabelSets(u.userID, metric)
	for _, l := range limits {
		s := m.shards[util.HashFP(model.Fingerprint(l.Hash))%numMetricCounterShards]
		s.Lock()
		if e, ok := s.valuesCounter[l.Hash]; ok {
			e.count--
		}
		s.Unlock()
	}
}

func (m *labelSetCounter) UpdateMetric(ctx context.Context, u *userTSDB, metrics *ingesterMetrics) error {
	currentLbsLimitHash := map[uint64]validation.LimitsPerLabelSet{}
	for _, l := range m.limiter.limits.LimitsPerLabelSet(u.userID) {
		currentLbsLimitHash[l.Hash] = l
	}

	for i := 0; i < numMetricCounterShards; i++ {
		s := m.shards[i]
		s.RLock()
		for h, entry := range s.valuesCounter {
			lbls := entry.labels.String()
			// This limit no longer exists
			if _, ok := currentLbsLimitHash[h]; !ok {
				metrics.usagePerLabelSet.DeleteLabelValues(u.userID, "max_series", lbls)
				metrics.limitsPerLabelSet.DeleteLabelValues(u.userID, "max_series", lbls)
				continue
			}
			metrics.usagePerLabelSet.WithLabelValues(u.userID, "max_series", lbls).Set(float64(entry.count))
			metrics.limitsPerLabelSet.WithLabelValues(u.userID, "max_series", lbls).Set(float64(currentLbsLimitHash[h].Limits.MaxSeries))
			delete(currentLbsLimitHash, h)
		}
		s.RUnlock()
	}

	// Backfill all limits that are not being tracked yet
	for _, l := range currentLbsLimitHash {
		s := m.shards[util.HashFP(model.Fingerprint(l.Hash))%numMetricCounterShards]
		count, err := m.backFillLimit(ctx, u, l, s)
		if err != nil {
			return err
		}
		lbls := l.LabelSet.String()
		metrics.usagePerLabelSet.WithLabelValues(u.userID, "max_series", lbls).Set(float64(count))
		metrics.limitsPerLabelSet.WithLabelValues(u.userID, "max_series", lbls).Set(float64(l.Limits.MaxSeries))
	}

	return nil
}
