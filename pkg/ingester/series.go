package ingester

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"
)

var (
	createdChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_chunks_created_total",
		Help: "The total number of chunks the ingester has created.",
	})
)

func init() {
	prometheus.MustRegister(createdChunks)
}

type sortedLabelPairs labelPairs

func (m sortedLabelPairs) Len() int           { return len(m) }
func (m sortedLabelPairs) Less(i, j int) bool { return bytes.Compare(m[i].Name, m[j].Name) < 0 }
func (m sortedLabelPairs) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

func (m labelPairs) sort() sortedLabelPairs {
	c := make(sortedLabelPairs, len(m))
	copy(c, m)
	sort.Sort(c)
	return c
}

func (a sortedLabelPairs) valueForName(name []byte) []byte {
	pos := sort.Search(len(a), func(i int) bool { return bytes.Compare(a[i].Name, name) >= 0 })
	if pos == len(a) || !bytes.Equal(a[pos].Name, name) {
		return nil
	}
	return a[pos].Value
}

// Check if a and b contain the same name/value pairs
func (a sortedLabelPairs) equal(b labelPairs) bool {
	if len(a) != len(b) {
		return false
	}
	for _, pair := range b {
		found := a.valueForName(pair.Name)
		if found == nil || !bytes.Equal(found, pair.Value) {
			return false
		}
	}
	return true
}

type memorySeries struct {
	metric sortedLabelPairs

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

type memorySeriesError struct {
	message   string
	errorType string
}

func (error *memorySeriesError) Error() string {
	return error.message
}

// newMemorySeries returns a pointer to a newly allocated memorySeries for the
// given metric.
func newMemorySeries(m labelPairs) *memorySeries {
	return &memorySeries{
		metric:   m.sort(),
		lastTime: model.Earliest,
	}
}

// helper to extract the not-necessarily-sorted type used elsewhere, without casting everywhere.
func (s *memorySeries) labels() labelPairs {
	return labelPairs(s.metric)
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
		return &memorySeriesError{
			message:   fmt.Sprintf("sample with repeated timestamp but different value for series %v; last value: %v, incoming value: %v", s.metric, s.lastSampleValue, v.Value),
			errorType: "new-value-for-timestamp",
		}
	}
	if v.Timestamp < s.lastTime {
		return &memorySeriesError{
			message:   fmt.Sprintf("sample timestamp out of order for series %v; last timestamp: %v, incoming timestamp: %v", s.metric, s.lastTime, v.Timestamp),
			errorType: "sample-out-of-order",
		}
	}

	if len(s.chunkDescs) == 0 || s.headChunkClosed {
		newHead := newDesc(chunk.New(), v.Timestamp, v.Timestamp)
		s.chunkDescs = append(s.chunkDescs, newHead)
		s.headChunkClosed = false
		createdChunks.Inc()
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
			createdChunks.Inc()
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
	if len(descs) > 0 {
		s.lastTime = descs[len(descs)-1].LastTime
	}
	return nil
}

type desc struct {
	C          chunk.Chunk // nil if chunk is evicted.
	FirstTime  model.Time  // Timestamp of first sample. Populated at creation. Immutable.
	LastTime   model.Time  // Timestamp of last sample. Populated at creation & on append.
	LastUpdate model.Time  // This server's local time on last change
}

func newDesc(c chunk.Chunk, firstTime model.Time, lastTime model.Time) *desc {
	return &desc{
		C:          c,
		FirstTime:  firstTime,
		LastTime:   lastTime,
		LastUpdate: model.Now(),
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
		d.LastUpdate = model.Now()
	}

	return cs, nil
}
