package ring

import (
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/extract"
)

// TokenForLabels returns the ring token (hash key) for a set of series labels.
// This determines which ingester in the ring is responsible for this series.
// Used by both the distributor (to route) and the ingester (to check ownership).
func TokenForLabels(userID string, labels []cortexpb.LabelAdapter, shouldShardByAllLabels bool) (uint32, error) {
	if shouldShardByAllLabels {
		return ShardByAllLabels(userID, labels), nil
	}

	unsafeMetricName, err := extract.UnsafeMetricNameFromLabelAdapters(labels)
	if err != nil {
		return 0, err
	}
	return ShardByMetricName(userID, unsafeMetricName), nil
}

// TokenForMetadata returns the ring token for metadata routing.
func TokenForMetadata(userID string, metricName string, shouldShardByAllLabels bool) uint32 {
	if shouldShardByAllLabels {
		return ShardByMetricName(userID, metricName)
	}
	return shardByUser(userID)
}

// ShardByAllLabels generates a token from userID + all label name/value pairs.
// This function generates different values for different order of same labels.
func ShardByAllLabels(userID string, labels []cortexpb.LabelAdapter) uint32 {
	h := shardByUser(userID)
	for _, label := range labels {
		if len(label.Value) > 0 {
			h = util.HashAdd32(h, label.Name)
			h = util.HashAdd32(h, label.Value)
		}
	}
	return h
}

// ShardByMetricName returns the token for the given metric.
func ShardByMetricName(userID string, metricName string) uint32 {
	h := shardByUser(userID)
	h = util.HashAdd32(h, metricName)
	return h
}

func shardByUser(userID string) uint32 {
	h := util.HashNew32()
	h = util.HashAdd32(h, userID)
	return h
}
