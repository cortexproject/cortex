package chunk

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Iterator enables efficient access to the content of a chunk. It is
// generally not safe to use an Iterator concurrently with or after chunk
// mutation.
type Iterator interface {
	// Scans the next value in the chunk. Directly after the iterator has
	// been created, the next value is the first value in the
	// chunk. Otherwise, it is the value following the last value scanned or
	// found (by one of the Find... methods). Returns chunkenc.ValNoe if either
	// the end of the chunk is reached or an error has occurred.
	Scan() chunkenc.ValueType
	// Finds the oldest value at or after the provided time and returns the value type.
	// Returns chunkenc.ValNone if either the chunk contains no value at or after
	// the provided time, or an error has occurred.
	FindAtOrAfter(model.Time) chunkenc.ValueType
	// Returns a batch of the provisded size; NB not idempotent!  Should only be called
	// once per Scan.
	Batch(size int, valType chunkenc.ValueType) Batch
	// Returns the last error encountered. In general, an error signals data
	// corruption in the chunk and requires quarantining.
	Err() error
}

// BatchSize is samples per batch; this was choose by benchmarking all sizes from
// 1 to 128.
const BatchSize = 12

// Batch is a sorted set of (timestamp, value) pairs. They are intended to be small,
// and passed by value. Value can vary depending on the chunk value type.
type Batch struct {
	Timestamps      [BatchSize]int64
	Values          [BatchSize]float64
	Histograms      [BatchSize]*histogram.Histogram
	FloatHistograms [BatchSize]*histogram.FloatHistogram
	Index           int
	Length          int
	ValType         chunkenc.ValueType
}
