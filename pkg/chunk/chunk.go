package chunk

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	prom_chunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
)

// Chunk contains encoded timeseries data
type Chunk struct {
	// These fields will be in all chunks, including old ones.
	From    model.Time    `json:"from"`
	Through model.Time    `json:"through"`
	Metric  labels.Labels `json:"metric"`

	// We never use Delta encoding (the zero value), so if this entry is
	// missing, we default to DoubleDelta.
	Encoding prom_chunk.Encoding `json:"encoding"`
	Data     prom_chunk.Chunk    `json:"-"`
}

// NewChunk creates a new chunk
func NewChunk(metric labels.Labels, c prom_chunk.Chunk, from, through model.Time) Chunk {
	return Chunk{
		From:     from,
		Through:  through,
		Metric:   metric,
		Encoding: c.Encoding(),
		Data:     c,
	}
}

// Samples returns all SamplePairs for the chunk.
func (c *Chunk) Samples(from, through model.Time) ([]model.SamplePair, error) {
	it := c.Data.NewIterator(nil)
	interval := metric.Interval{OldestInclusive: from, NewestInclusive: through}
	return prom_chunk.RangeValues(it, interval)
}
