package cortexotlpconverter

import (
	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// TimeSeries returns a slice of the cortexpb.PreallocTimeseries that were converted from OTel format.
func (c *CortexConverter) TimeSeries() []cortexpb.PreallocTimeseries {
	conflicts := 0
	for _, ts := range c.conflicts {
		conflicts += len(ts)
	}

	allTS := make([]cortexpb.PreallocTimeseries, 0, len(c.unique)+conflicts)
	for _, ts := range c.unique {
		allTS = append(allTS, cortexpb.PreallocTimeseries{TimeSeries: ts})
	}
	for _, cTS := range c.conflicts {
		for _, ts := range cTS {
			allTS = append(allTS, cortexpb.PreallocTimeseries{TimeSeries: ts})
		}
	}

	return allTS
}

// Metadata returns a slice of the cortexpb.MetricMetadata that were converted from OTel format.
func (c *CortexConverter) Metadata() []*cortexpb.MetricMetadata {
	return c.metadata
}
