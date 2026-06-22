//go:build !slicelabels

package tsdb

import "github.com/prometheus/prometheus/model/labels"

// seriesLabelsForLazyMatch returns the series labels built into the provided
// ScratchBuilder for the stringlabels (default) and dedupelabels builds.
func seriesLabelsForLazyMatch(builder *labels.ScratchBuilder, _ *labels.Labels) labels.Labels {
	return builder.Labels()
}
