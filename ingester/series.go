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
		newHead := newDesc(chunk.New(), v.Timestamp, v.Timestamp)
		s.chunkDescs = append(s.chunkDescs, newHead)
		s.headChunkClosed = false
	}

	chunks, err := s.head().add(v)
	if err != nil {
		return err
	}

	// If we get a single chunk result, then just replace the head chunk with it
	// (no need to update first/last time).  Otherwise, we'll need to update first
	// and last time.
	if len(chunks) == 1 {
		s.head().C = chunks[0]
	} else {
		s.chunkDescs = s.chunkDescs[:len(s.chunkDescs)-1]
		for _, c := range chunks {
			lastTime, err := c.NewIterator().LastTimestamp()
			if err != nil {
				return err
			}
			s.chunkDescs = append(s.chunkDescs, newDesc(c, c.FirstTime(), lastTime))
		}
	}

	s.lastTime = v.Timestamp
	s.lastSampleValue = v.Value
	s.lastSampleValueSet = true

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

func (s *memorySeries) setChunks(descs []*desc) error {
	if len(s.chunkDescs) != 0 {
		return fmt.Errorf("series already has chunks")
	}

	s.chunkDescs = descs
	s.lastTime = descs[len(descs)-1].LastTime
	return nil
}

type desc struct {
	C         chunk.Chunk // nil if chunk is evicted.
	FirstTime model.Time  // Populated at creation. Immutable.
	LastTime  model.Time  // Populated at creation & on append.
}

func newDesc(c chunk.Chunk, firstTime model.Time, lastTime model.Time) *desc {
	return &desc{
		C:         c,
		FirstTime: firstTime,
		LastTime:  lastTime,
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
