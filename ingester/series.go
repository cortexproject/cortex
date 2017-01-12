package ingester

import (
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
)

var discardedSamples = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cortex_ingester_out_of_order_samples_total",
		Help: "The total number of samples that were discarded because their timestamps were at or before the last received sample for a series.",
	},
	[]string{discardReasonLabel},
)

func init() {
	prometheus.MustRegister(discardedSamples)
}

type memorySeries struct {
	metric model.Metric

	// Sorted by start time, overlapping chunk ranges are forbidden.
	chunkDescs []*desc

	// Whether the current head chunk has already been finished.  If true,
	// the current head chunk must not be modified anymore.
	headChunkClosed bool

	// The timestamp & value of the last sample in this series. Needed to
	// ensure timestamp monotonicity during ingestion.
	lastSampleValueSet bool
	lastTime           model.Time
	lastSampleValue    model.SampleValue
}

// newMemorySeries returns a pointer to a newly allocated memorySeries for the
// given metric.
func newMemorySeries(m model.Metric) *memorySeries {
	return &memorySeries{
		metric:   m,
		lastTime: model.Earliest,
	}
}

// add adds a sample pair to the series. It returns the number of newly
// completed chunks (which are now eligible for persistence).
//
// The caller must have locked the fingerprint of the series.
func (s *memorySeries) add(v model.SamplePair) error {
	// Don't report "no-op appends", i.e. where timestamp and sample
	// value are the same as for the last append, as they are a
	// common occurrence when using client-side timestamps
	// (e.g. Pushgateway or federation).
	if s.lastSampleValueSet &&
		v.Timestamp == s.lastTime &&
		v.Value.Equal(s.lastSampleValue) {
		return nil
	}
	if v.Timestamp == s.lastTime {
		discardedSamples.WithLabelValues(duplicateSample).Inc()
		return ErrDuplicateSampleForTimestamp // Caused by the caller.
	}
	if v.Timestamp < s.lastTime {
		discardedSamples.WithLabelValues(outOfOrderTimestamp).Inc()
		return ErrOutOfOrderSample // Caused by the caller.
	}

	if len(s.chunkDescs) == 0 || s.headChunkClosed {
		newHead := newDesc(chunk.New(), v.Timestamp)
		s.chunkDescs = append(s.chunkDescs, newHead)
		s.headChunkClosed = false
	}

	chunks, err := s.head().add(v)
	if err != nil {
		return err
	}
	s.head().C = chunks[0]

	switch len(chunks) {
	case 1: // noop, sample added to the chunk
	case 2:
		// new overflow chunk - in practice there is only ever 1 overflow chunk,
		// thats how the chunk code is constructed.
		s.chunkDescs = append(s.chunkDescs, newDesc(chunks[1], v.Timestamp))
	default:
		panic(fmt.Sprintf("Unexpected: got %d chunks from chunk.Add", len(chunks)))
	}

	return nil
}

func (s *memorySeries) closeHead() {
	s.headChunkClosed = true
}

// firstTime returns the earliest known time for the series. The caller must have
// locked the fingerprint of the memorySeries. This method will panic if this
// series has no chunk descriptors.
func (s *memorySeries) firstTime() model.Time {
	return s.chunkDescs[0].FirstTime
}

// head returns a pointer to the head chunk descriptor. The caller must have
// locked the fingerprint of the memorySeries. This method will panic if this
// series has no chunk descriptors.
func (s *memorySeries) head() *desc {
	return s.chunkDescs[len(s.chunkDescs)-1]
}

func (s *memorySeries) samplesForRange(from, through model.Time) ([]model.SamplePair, error) {
	// Find first chunk with start time after "from".
	fromIdx := sort.Search(len(s.chunkDescs), func(i int) bool {
		return s.chunkDescs[i].FirstTime.After(from)
	})
	// Find first chunk with start time after "through".
	throughIdx := sort.Search(len(s.chunkDescs), func(i int) bool {
		return s.chunkDescs[i].FirstTime.After(through)
	})
	if fromIdx == len(s.chunkDescs) {
		// Even the last chunk starts before "from". Find out if the
		// series ends before "from" and we don't need to do anything.
		lt := s.chunkDescs[len(s.chunkDescs)-1].LastTime
		if lt.Before(from) {
			return nil, nil
		}
	}
	if fromIdx > 0 {
		fromIdx--
	}
	if throughIdx == len(s.chunkDescs) {
		throughIdx--
	}
	var values []model.SamplePair
	in := metric.Interval{
		OldestInclusive: from,
		NewestInclusive: through,
	}
	for idx := fromIdx; idx <= throughIdx; idx++ {
		cd := s.chunkDescs[idx]
		chValues, err := chunk.RangeValues(cd.C.NewIterator(), in)
		if err != nil {
			return nil, err
		}
		values = append(values, chValues...)
	}
	return values, nil
}

type desc struct {
	C         chunk.Chunk // nil if chunk is evicted.
	FirstTime model.Time  // Populated at creation. Immutable.
	LastTime  model.Time  // Populated at creation & on append.
}

func newDesc(c chunk.Chunk, firstTime model.Time) *desc {
	return &desc{
		C:         c,
		FirstTime: firstTime,
		LastTime:  firstTime,
	}
}

// Add adds a sample pair to the underlying chunk. For safe concurrent access,
// The chunk must be pinned, and the caller must have locked the fingerprint of
// the series.
func (d *desc) add(s model.SamplePair) ([]chunk.Chunk, error) {
	cs, err := d.C.Add(s)
	if err != nil {
		return nil, err
	}

	if len(cs) == 1 {
		d.LastTime = s.Timestamp // sample was added to this chunk
	}

	return cs, nil
}
