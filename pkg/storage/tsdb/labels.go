package tsdb

import (
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/prometheus/tsdb/labels"
)

const (
	// TenantIDExternalLabel is the external label set when shipping blocks to the storage
	TenantIDExternalLabel = "__org_id__"
)

// FromLabelAdaptersToLabels converts []LabelAdapter to TSDB labels.Labels.
func FromLabelAdaptersToLabels(input []client.LabelAdapter) labels.Labels {
	result := make(labels.Labels, len(input))

	for i, l := range input {
		result[i] = labels.Label{
			Name:  l.Name,
			Value: l.Value,
		}
	}

	return result
}
