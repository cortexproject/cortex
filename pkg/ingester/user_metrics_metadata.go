package ingester

import (
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// userMetricsMetadata allows metric metadata of a tenant to be held by the ingester.
// Metadata is kept as a set as it can come from multiple targets that Prometheus scrapes
// with the same metric name.
type userMetricsMetadata struct {
	limiter         *Limiter
	metrics         *ingesterMetrics
	validateMetrics *validation.ValidateMetrics
	userID          string

	mtx              sync.RWMutex
	metricToMetadata map[string]metricMetadataSet
}

func newMetadataMap(l *Limiter, m *ingesterMetrics, v *validation.ValidateMetrics, userID string) *userMetricsMetadata {
	return &userMetricsMetadata{
		metricToMetadata: map[string]metricMetadataSet{},
		limiter:          l,
		metrics:          m,
		validateMetrics:  v,
		userID:           userID,
	}
}

func (mm *userMetricsMetadata) add(metric string, metadata *cortexpb.MetricMetadata) error {
	mm.mtx.Lock()
	defer mm.mtx.Unlock()

	// As we get the set, we also validate two things:
	// 1. The user is allowed to create new metrics to add metadata to.
	// 2. If the metadata set is already present, it hasn't reached the limit of metadata we can append.
	set, ok := mm.metricToMetadata[metric]
	if !ok {
		// Verify that the user can create more metric metadata given we don't have a set for that metric name.
		if err := mm.limiter.AssertMaxMetricsWithMetadataPerUser(mm.userID, len(mm.metricToMetadata)); err != nil {
			mm.validateMetrics.DiscardedMetadata.WithLabelValues(mm.userID, perUserMetadataLimit).Inc()
			return makeLimitError(perUserMetadataLimit, mm.limiter.FormatError(mm.userID, err, labels.FromStrings(labels.MetricName, metric)))
		}
		set = metricMetadataSet{}
		mm.metricToMetadata[metric] = set
	}

	if err := mm.limiter.AssertMaxMetadataPerMetric(mm.userID, len(set)); err != nil {
		mm.validateMetrics.DiscardedMetadata.WithLabelValues(mm.userID, perMetricMetadataLimit).Inc()
		return makeMetricLimitError(perMetricMetadataLimit, labels.FromStrings(labels.MetricName, metric), mm.limiter.FormatError(mm.userID, err, labels.FromStrings(labels.MetricName, metric)))
	}

	// if we have seen this metadata before, it is a no-op and we don't need to change our metrics.
	_, ok = set[*metadata]
	if !ok {
		mm.metrics.memMetadata.Inc()
		mm.metrics.memMetadataCreatedTotal.WithLabelValues(mm.userID).Inc()
	}

	mm.metricToMetadata[metric][*metadata] = time.Now()
	return nil
}

// If deadline is zero, all metadata is purged.
func (mm *userMetricsMetadata) purge(deadline time.Time) {
	mm.mtx.Lock()
	defer mm.mtx.Unlock()
	var deleted int
	for m, s := range mm.metricToMetadata {
		deleted += s.purge(deadline)

		if len(s) <= 0 {
			delete(mm.metricToMetadata, m)
		}
	}

	mm.metrics.memMetadata.Sub(float64(deleted))
	mm.metrics.memMetadataRemovedTotal.WithLabelValues(mm.userID).Add(float64(deleted))
}

func (mm *userMetricsMetadata) toClientMetadata(req *client.MetricsMetadataRequest) []*cortexpb.MetricMetadata {
	mm.mtx.RLock()
	defer mm.mtx.RUnlock()
	r := make([]*cortexpb.MetricMetadata, 0, len(mm.metricToMetadata))
	if req.Limit == 0 {
		return r
	}

	if req.Metric != "" {
		metadataSet, ok := mm.metricToMetadata[req.Metric]
		if !ok {
			return r
		}

		metadataSet.add(req.LimitPerMetric, &r)
		return r
	}

	var metrics int64
	for _, set := range mm.metricToMetadata {
		if req.Limit > 0 && metrics >= req.Limit {
			break
		}
		set.add(req.LimitPerMetric, &r)
		metrics++
	}
	return r
}

func (mns metricMetadataSet) add(limitPerMetric int64, r *[]*cortexpb.MetricMetadata) {
	var metrics int64
	for m := range mns {
		if limitPerMetric > 0 && metrics >= limitPerMetric {
			return
		}
		*r = append(*r, &m)
		metrics++
	}
}

type metricMetadataSet map[cortexpb.MetricMetadata]time.Time

// If deadline is zero time, all metrics are purged.
func (mms metricMetadataSet) purge(deadline time.Time) int {
	var deleted int
	for metadata, t := range mms {
		if deadline.IsZero() || deadline.After(t) {
			delete(mms, metadata)
			deleted++
		}
	}

	return deleted
}
