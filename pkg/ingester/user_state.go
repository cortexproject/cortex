package ingester

import (
	"sync"

	"github.com/prometheus/common/model"
	"github.com/segmentio/fasthash/fnv1a"

	"github.com/cortexproject/cortex/pkg/util"
)

// DiscardedSamples metric labels
const (
	perUserSeriesLimit   = "per_user_series_limit"
	perMetricSeriesLimit = "per_metric_series_limit"
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
