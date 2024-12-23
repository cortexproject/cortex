package ingester

import (
	"fmt"
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	errMaxSeriesPerMetricLimitExceeded   = errors.New("per-metric series limit exceeded")
	errMaxMetadataPerMetricLimitExceeded = errors.New("per-metric metadata limit exceeded")
	errMaxSeriesPerUserLimitExceeded     = errors.New("per-user series limit exceeded")
	errMaxMetadataPerUserLimitExceeded   = errors.New("per-user metric metadata limit exceeded")
)

type errMaxSeriesPerLabelSetLimitExceeded struct {
	error
	id          string
	localLimit  int
	globalLimit int
}

// RingCount is the interface exposed by a ring implementation which allows
// to count members
type RingCount interface {
	HealthyInstancesCount() int
	ZonesCount() int
}

// Limiter implements primitives to get the maximum number of series
// an ingester can handle for a specific tenant
type Limiter struct {
	limits                 *validation.Overrides
	ring                   RingCount
	replicationFactor      int
	shuffleShardingEnabled bool
	shardByAllLabels       bool
	zoneAwarenessEnabled   bool
	AdminLimitMessage      string
}

// NewLimiter makes a new in-memory series limiter
func NewLimiter(
	limits *validation.Overrides,
	ring RingCount,
	shardingStrategy string,
	shardByAllLabels bool,
	replicationFactor int,
	zoneAwarenessEnabled bool,
	AdminLimitMessage string,
) *Limiter {
	return &Limiter{
		limits:                 limits,
		ring:                   ring,
		replicationFactor:      replicationFactor,
		shuffleShardingEnabled: shardingStrategy == util.ShardingStrategyShuffle,
		shardByAllLabels:       shardByAllLabels,
		zoneAwarenessEnabled:   zoneAwarenessEnabled,
		AdminLimitMessage:      AdminLimitMessage,
	}
}

// AssertMaxSeriesPerMetric limit has not been reached compared to the current
// number of series in input and returns an error if so.
func (l *Limiter) AssertMaxSeriesPerMetric(userID string, series int) error {
	if actualLimit := l.maxSeriesPerMetric(userID); series < actualLimit {
		return nil
	}

	return errMaxSeriesPerMetricLimitExceeded
}

// AssertMaxMetadataPerMetric limit has not been reached compared to the current
// number of metadata per metric in input and returns an error if so.
func (l *Limiter) AssertMaxMetadataPerMetric(userID string, metadata int) error {
	if actualLimit := l.maxMetadataPerMetric(userID); metadata < actualLimit {
		return nil
	}

	return errMaxMetadataPerMetricLimitExceeded
}

// AssertMaxSeriesPerUser limit has not been reached compared to the current
// number of series in input and returns an error if so.
func (l *Limiter) AssertMaxSeriesPerUser(userID string, series int) error {
	if actualLimit := l.maxSeriesPerUser(userID); series < actualLimit {
		return nil
	}

	return errMaxSeriesPerUserLimitExceeded
}

// AssertMaxMetricsWithMetadataPerUser limit has not been reached compared to the current
// number of metrics with metadata in input and returns an error if so.
func (l *Limiter) AssertMaxMetricsWithMetadataPerUser(userID string, metrics int) error {
	if actualLimit := l.maxMetadataPerUser(userID); metrics < actualLimit {
		return nil
	}

	return errMaxMetadataPerUserLimitExceeded
}

// AssertMaxSeriesPerLabelSet limit has not been reached compared to the current
// number of metrics with metadata in input and returns an error if so.
func (l *Limiter) AssertMaxSeriesPerLabelSet(userID string, metric labels.Labels, f func(allLimits []validation.LimitsPerLabelSet, limit validation.LimitsPerLabelSet) (int, error)) error {
	limits := l.limits.LimitsPerLabelSet(userID)
	matchedLimits := validation.LimitsPerLabelSetsForSeries(limits, metric)
	for _, limit := range matchedLimits {
		maxSeriesFunc := func(string) int {
			return limit.Limits.MaxSeries
		}
		local := l.maxByLocalAndGlobal(userID, maxSeriesFunc, maxSeriesFunc)
		if u, err := f(limits, limit); err != nil {
			return err
		} else if u >= local {
			return errMaxSeriesPerLabelSetLimitExceeded{
				id:          limit.Id,
				localLimit:  local,
				globalLimit: limit.Limits.MaxSeries,
			}
		}
	}
	return nil
}

// FormatError returns the input error enriched with the actual limits for the given user.
// It acts as pass-through if the input error is unknown.
func (l *Limiter) FormatError(userID string, err error, lbls labels.Labels) error {
	switch {
	case errors.Is(err, errMaxSeriesPerUserLimitExceeded):
		return l.formatMaxSeriesPerUserError(userID)
	case errors.Is(err, errMaxSeriesPerMetricLimitExceeded):
		return l.formatMaxSeriesPerMetricError(userID, lbls.Get(labels.MetricName))
	case errors.Is(err, errMaxMetadataPerUserLimitExceeded):
		return l.formatMaxMetadataPerUserError(userID)
	case errors.Is(err, errMaxMetadataPerMetricLimitExceeded):
		return l.formatMaxMetadataPerMetricError(userID, lbls.Get(labels.MetricName))
	case errors.As(err, &errMaxSeriesPerLabelSetLimitExceeded{}):
		e := errMaxSeriesPerLabelSetLimitExceeded{}
		errors.As(err, &e)
		return l.formatMaxSeriesPerLabelSetError(e)
	default:
		return err
	}
}

func (l *Limiter) formatMaxSeriesPerUserError(userID string) error {
	actualLimit := l.maxSeriesPerUser(userID)
	localLimit := l.limits.MaxLocalSeriesPerUser(userID)
	globalLimit := l.limits.MaxGlobalSeriesPerUser(userID)

	return fmt.Errorf("per-user series limit of %d exceeded, %s (local limit: %d global limit: %d actual local limit: %d)",
		minNonZero(localLimit, globalLimit), l.AdminLimitMessage, localLimit, globalLimit, actualLimit)
}

func (l *Limiter) formatMaxSeriesPerMetricError(userID string, metric string) error {
	actualLimit := l.maxSeriesPerMetric(userID)
	localLimit := l.limits.MaxLocalSeriesPerMetric(userID)
	globalLimit := l.limits.MaxGlobalSeriesPerMetric(userID)

	return fmt.Errorf("per-metric series limit of %d exceeded for metric %s, %s (local limit: %d global limit: %d actual local limit: %d)",
		minNonZero(localLimit, globalLimit), metric, l.AdminLimitMessage, localLimit, globalLimit, actualLimit)
}

func (l *Limiter) formatMaxMetadataPerUserError(userID string) error {
	actualLimit := l.maxMetadataPerUser(userID)
	localLimit := l.limits.MaxLocalMetricsWithMetadataPerUser(userID)
	globalLimit := l.limits.MaxGlobalMetricsWithMetadataPerUser(userID)

	return fmt.Errorf("per-user metric metadata limit of %d exceeded, %s (local limit: %d global limit: %d actual local limit: %d)",
		minNonZero(localLimit, globalLimit), l.AdminLimitMessage, localLimit, globalLimit, actualLimit)
}

func (l *Limiter) formatMaxMetadataPerMetricError(userID string, metric string) error {
	actualLimit := l.maxMetadataPerMetric(userID)
	localLimit := l.limits.MaxLocalMetadataPerMetric(userID)
	globalLimit := l.limits.MaxGlobalMetadataPerMetric(userID)

	return fmt.Errorf("per-metric metadata limit of %d exceeded for metric %s, %s (local limit: %d global limit: %d actual local limit: %d)",
		minNonZero(localLimit, globalLimit), metric, l.AdminLimitMessage, localLimit, globalLimit, actualLimit)
}

func (l *Limiter) formatMaxSeriesPerLabelSetError(err errMaxSeriesPerLabelSetLimitExceeded) error {
	return fmt.Errorf("per-labelset series limit of %d exceeded (labelSet: %s, local limit: %d global limit: %d actual)",
		minNonZero(err.globalLimit, err.localLimit), err.id, err.localLimit, err.globalLimit)
}

func (l *Limiter) limitsPerLabelSets(userID string, metric labels.Labels) []validation.LimitsPerLabelSet {
	m := l.limits.LimitsPerLabelSet(userID)
	return validation.LimitsPerLabelSetsForSeries(m, metric)
}

func (l *Limiter) maxSeriesPerMetric(userID string) int {
	localLimit := l.limits.MaxLocalSeriesPerMetric(userID)
	globalLimit := l.limits.MaxGlobalSeriesPerMetric(userID)

	if globalLimit > 0 {
		if l.shardByAllLabels {
			// We can assume that series are evenly distributed across ingesters
			// so we do convert the global limit into a local limit
			localLimit = minNonZero(localLimit, l.convertGlobalToLocalLimit(userID, globalLimit))
		} else {
			// Given a metric is always pushed to the same set of ingesters (based on
			// the replication factor), we can configure the per-ingester local limit
			// equal to the global limit.
			localLimit = minNonZero(localLimit, globalLimit)
		}
	}

	// If both the local and global limits are disabled, we just
	// use the largest int value
	if localLimit == 0 {
		localLimit = math.MaxInt32
	}

	return localLimit
}

func (l *Limiter) maxMetadataPerMetric(userID string) int {
	localLimit := l.limits.MaxLocalMetadataPerMetric(userID)
	globalLimit := l.limits.MaxGlobalMetadataPerMetric(userID)

	if globalLimit > 0 {
		if l.shardByAllLabels {
			localLimit = minNonZero(localLimit, l.convertGlobalToLocalLimit(userID, globalLimit))
		} else {
			localLimit = minNonZero(localLimit, globalLimit)
		}
	}

	if localLimit == 0 {
		localLimit = math.MaxInt32
	}

	return localLimit
}

func (l *Limiter) maxSeriesPerUser(userID string) int {
	return l.maxByLocalAndGlobal(
		userID,
		l.limits.MaxLocalSeriesPerUser,
		l.limits.MaxGlobalSeriesPerUser,
	)
}

func (l *Limiter) maxMetadataPerUser(userID string) int {
	return l.maxByLocalAndGlobal(
		userID,
		l.limits.MaxLocalMetricsWithMetadataPerUser,
		l.limits.MaxGlobalMetricsWithMetadataPerUser,
	)
}

func (l *Limiter) maxByLocalAndGlobal(userID string, localLimitFn, globalLimitFn func(string) int) int {
	localLimit := localLimitFn(userID)

	// The global limit is supported only when shard-by-all-labels is enabled,
	// otherwise we wouldn't get an even split of series/metadata across ingesters and
	// can't take a "local decision" without any centralized coordination.
	if l.shardByAllLabels {
		// We can assume that series/metadata are evenly distributed across ingesters
		// so we do convert the global limit into a local limit
		globalLimit := globalLimitFn(userID)
		localLimit = minNonZero(localLimit, l.convertGlobalToLocalLimit(userID, globalLimit))
	}

	// If both the local and global limits are disabled, we just
	// use the largest int value
	if localLimit == 0 {
		localLimit = math.MaxInt32
	}

	return localLimit
}

func (l *Limiter) convertGlobalToLocalLimit(userID string, globalLimit int) int {
	if globalLimit == 0 {
		return 0
	}

	// Given we don't need a super accurate count (ie. when the ingesters
	// topology changes) and we prefer to always be in favor of the tenant,
	// we can use a per-ingester limit equal to:
	// (global limit / number of ingesters) * replication factor
	numIngesters := l.ring.HealthyInstancesCount()

	// May happen because the number of ingesters is asynchronously updated.
	// If happens, we just temporarily ignore the global limit.
	if numIngesters == 0 {
		return 0
	}

	// If the number of available ingesters is greater than the tenant's shard
	// size, then we should honor the shard size because series/metadata won't
	// be written to more ingesters than it.
	if shardSize := l.getShardSize(userID); shardSize > 0 {
		// We use Min() to protect from the case the expected shard size is > available ingesters.
		numIngesters = min(numIngesters, util.ShuffleShardExpectedInstances(shardSize, l.getNumZones()))
	}

	return int((float64(globalLimit) / float64(numIngesters)) * float64(l.replicationFactor))
}

func (l *Limiter) getShardSize(userID string) int {
	if !l.shuffleShardingEnabled {
		return 0
	}

	return l.limits.IngestionTenantShardSize(userID)
}

func (l *Limiter) getNumZones() int {
	if l.zoneAwarenessEnabled {
		return max(l.ring.ZonesCount(), 1)
	}
	return 1
}

func minNonZero(first, second int) int {
	if first == 0 || (second != 0 && first > second) {
		return second
	}

	return first
}
