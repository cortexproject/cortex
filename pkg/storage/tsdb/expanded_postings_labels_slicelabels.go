//go:build slicelabels

package tsdb

import "github.com/prometheus/prometheus/model/labels"

// seriesLabelsForLazyMatch writes the series labels into the reused Labels via
// ScratchBuilder.Overwrite, reusing its backing slice across iterations.
func seriesLabelsForLazyMatch(builder *labels.ScratchBuilder, reused *labels.Labels) labels.Labels {
	builder.Overwrite(reused)
	return *reused
}
