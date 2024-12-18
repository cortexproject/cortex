package ingester

import (
	"context"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
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
	return m.limiter.AssertMaxSeriesPerLabelSet(u.userID, metric, func(allLimits []validation.LimitsPerLabelSet, limit validation.LimitsPerLabelSet) (int, error) {
		s := m.shards[util.HashFP(model.Fingerprint(limit.Hash))%numMetricCounterShards]
		s.RLock()
		if r, ok := s.valuesCounter[limit.Hash]; ok {
			defer s.RUnlock()
			return r.count, nil
		}
		s.RUnlock()

		// We still dont keep track of this label value so we need to backfill
		return m.backFillLimit(ctx, u, false, allLimits, limit, s)
	})
}

func (m *labelSetCounter) backFillLimit(ctx context.Context, u *userTSDB, forceBackfill bool, allLimits []validation.LimitsPerLabelSet, limit validation.LimitsPerLabelSet, s *labelSetCounterShard) (int, error) {
	s.Lock()
	// If not force backfill, use existing counter value.
	if !forceBackfill {
		if r, ok := s.valuesCounter[limit.Hash]; ok {
			s.Unlock()
			return r.count, nil
		}
	}

	defer s.Unlock()
	ir, err := u.db.Head().Index()
	if err != nil {
		return 0, err
	}

	defer ir.Close()

	numSeries := u.db.Head().NumSeries()
	totalCount, err := getCardinalityForLimitsPerLabelSet(ctx, numSeries, ir, allLimits, limit)
	if err != nil {
		return 0, err
	}

	s.valuesCounter[limit.Hash] = &labelSetCounterEntry{
		count:  totalCount,
		labels: limit.LabelSet,
	}
	return totalCount, nil
}

func getCardinalityForLimitsPerLabelSet(ctx context.Context, numSeries uint64, ir tsdb.IndexReader, allLimits []validation.LimitsPerLabelSet, limit validation.LimitsPerLabelSet) (int, error) {
	// Easy path with explicit labels.
	if limit.LabelSet.Len() > 0 {
		p, err := getPostingForLabels(ctx, ir, limit.LabelSet)
		if err != nil {
			return 0, err
		}
		return getPostingCardinality(p)
	}

	// Default partition needs to get cardinality of all series that doesn't belong to any existing partitions.
	postings := make([]index.Postings, 0, len(allLimits)-1)
	for _, l := range allLimits {
		if l.Hash == limit.Hash {
			continue
		}
		p, err := getPostingForLabels(ctx, ir, l.LabelSet)
		if err != nil {
			return 0, err
		}
		postings = append(postings, p)
	}
	mergedCardinality, err := getPostingCardinality(index.Merge(ctx, postings...))
	if err != nil {
		return 0, err
	}

	return int(numSeries) - mergedCardinality, nil
}

func getPostingForLabels(ctx context.Context, ir tsdb.IndexReader, lbls labels.Labels) (index.Postings, error) {
	postings := make([]index.Postings, 0, len(lbls))
	for _, lbl := range lbls {
		p, err := ir.Postings(ctx, lbl.Name, lbl.Value)
		if err != nil {
			return nil, err
		}
		postings = append(postings, p)
	}

	return index.Intersect(postings...), nil
}

func getPostingCardinality(p index.Postings) (int, error) {
	totalCount := 0
	for p.Next() {
		totalCount++
	}

	if p.Err() != nil {
		return 0, p.Err()
	}
	return totalCount, nil
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
	limits := m.limiter.limits.LimitsPerLabelSet(u.userID)
	for _, l := range limits {
		currentLbsLimitHash[l.Hash] = l
	}

	nonDefaultPartitionChanged := false
	for i := 0; i < numMetricCounterShards; i++ {
		s := m.shards[i]
		s.RLock()
		for h, entry := range s.valuesCounter {
			lbls := entry.labels.String()
			// This limit no longer exists
			if _, ok := currentLbsLimitHash[h]; !ok {
				metrics.usagePerLabelSet.DeleteLabelValues(u.userID, "max_series", lbls)
				metrics.limitsPerLabelSet.DeleteLabelValues(u.userID, "max_series", lbls)
				if entry.labels.Len() > 0 {
					nonDefaultPartitionChanged = true
				}
				continue
			}
			// Delay exposing default partition metrics from current label limits as if
			// another label set is added or removed then we need to backfill default partition again.
			if entry.labels.Len() > 0 {
				metrics.usagePerLabelSet.WithLabelValues(u.userID, "max_series", lbls).Set(float64(entry.count))
				metrics.limitsPerLabelSet.WithLabelValues(u.userID, "max_series", lbls).Set(float64(currentLbsLimitHash[h].Limits.MaxSeries))
				delete(currentLbsLimitHash, h)
			}
		}
		s.RUnlock()
	}

	// Check if we need to backfill default partition. We don't need to backfill when any condition meet:
	// 1. Default partition doesn't exist.
	// 2. No new partition added and no old partition removed.
	if !nonDefaultPartitionChanged {
		for _, l := range currentLbsLimitHash {
			if l.LabelSet.Len() > 0 {
				nonDefaultPartitionChanged = true
				break
			}
		}
	}

	// Backfill all limits that are not being tracked yet
	for _, l := range currentLbsLimitHash {
		s := m.shards[util.HashFP(model.Fingerprint(l.Hash))%numMetricCounterShards]
		force := false
		// Force backfill to make sure we update the counter for the default partition
		// when other limits got added or removed. If no partition is changed then we
		// can use the value in the counter.
		if l.LabelSet.Len() == 0 && nonDefaultPartitionChanged {
			force = true
		}
		count, err := m.backFillLimit(ctx, u, force, limits, l, s)
		if err != nil {
			return err
		}
		lbls := l.LabelSet.String()
		metrics.usagePerLabelSet.WithLabelValues(u.userID, "max_series", lbls).Set(float64(count))
		metrics.limitsPerLabelSet.WithLabelValues(u.userID, "max_series", lbls).Set(float64(l.Limits.MaxSeries))
	}

	return nil
}
