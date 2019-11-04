package tsdb

import (
	"unsafe"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/prometheus/tsdb/labels"
)

const (
	// TenantIDExternalLabel is the external label set when shipping blocks to the storage
	TenantIDExternalLabel = "__org_id__"
)

// FromLabelAdaptersToLabels casts []LabelAdapter to TSDB labels.Labels.
// It uses unsafe, but as LabelAdapter == labels.Label this should be safe.
func FromLabelAdaptersToLabels(input []client.LabelAdapter) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&input))
}
