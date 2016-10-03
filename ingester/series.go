// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingester

import (
	"sync"
	"time"

	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/storage/local/chunk"
)

// fingerprintSeriesPair pairs a fingerprint with a memorySeries pointer.
type fingerprintSeriesPair struct {
	fp     model.Fingerprint
	series *memorySeries
}

// seriesMap maps fingerprints to memory series. All its methods are
// goroutine-safe. A SeriesMap is effectively is a goroutine-safe version of
// map[model.Fingerprint]*memorySeries.
type seriesMap struct {
	mtx sync.RWMutex
	m   map[model.Fingerprint]*memorySeries
}

// newSeriesMap returns a newly allocated empty seriesMap. To create a seriesMap
// based on a prefilled map, use an explicit initializer.
func newSeriesMap() *seriesMap {
	return &seriesMap{m: make(map[model.Fingerprint]*memorySeries)}
}

// length returns the number of mappings in the seriesMap.
func (sm *seriesMap) length() int {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	return len(sm.m)
}

// get returns a memorySeries for a fingerprint. Return values have the same
// semantics as the native Go map.
func (sm *seriesMap) get(fp model.Fingerprint) (s *memorySeries, ok bool) {
	sm.mtx.RLock()
	s, ok = sm.m[fp]
	// Note that the RUnlock is not done via defer for performance reasons.
	// TODO(beorn7): Once https://github.com/golang/go/issues/14939 is
	// fixed, revert to the usual defer idiom.
	sm.mtx.RUnlock()
	return
}

// put adds a mapping to the seriesMap. It panics if s == nil.
func (sm *seriesMap) put(fp model.Fingerprint, s *memorySeries) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	if s == nil {
		panic("tried to add nil pointer to seriesMap")
	}
	sm.m[fp] = s
}

// del removes a mapping from the series Map.
func (sm *seriesMap) del(fp model.Fingerprint) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	delete(sm.m, fp)
}

// iter returns a channel that produces all mappings in the seriesMap. The
// channel will be closed once all fingerprints have been received. Not
// consuming all fingerprints from the channel will leak a goroutine. The
// semantics of concurrent modification of seriesMap is the similar as the one
// for iterating over a map with a 'range' clause. However, if the next element
// in iteration order is removed after the current element has been received
// from the channel, it will still be produced by the channel.
func (sm *seriesMap) iter() <-chan fingerprintSeriesPair {
	ch := make(chan fingerprintSeriesPair)
	go func() {
		sm.mtx.RLock()
		for fp, s := range sm.m {
			sm.mtx.RUnlock()
			ch <- fingerprintSeriesPair{fp, s}
			sm.mtx.RLock()
		}
		sm.mtx.RUnlock()
		close(ch)
	}()
	return ch
}

type memorySeries struct {
	metric model.Metric
	// Sorted by start time, overlapping chunk ranges are forbidden.
	chunkDescs []*chunk.Desc
	// The index (within chunkDescs above) of the first chunk.Desc that
	// points to a non-persisted chunk. If all chunks are persisted, then
	// persistWatermark == len(chunkDescs).
	persistWatermark int
	// The modification time of the series file. The zero value of time.Time
	// is used to mark an unknown modification time.
	modTime time.Time
	// The chunkDescs in memory might not have all the chunkDescs for the
	// chunks that are persisted to disk. The missing chunkDescs are all
	// contiguous and at the tail end. chunkDescsOffset is the index of the
	// chunk on disk that corresponds to the first chunk.Desc in memory. If
	// it is 0, the chunkDescs are all loaded. A value of -1 denotes a
	// special case: There are chunks on disk, but the offset to the
	// chunkDescs in memory is unknown. Also, in this special case, there is
	// no overlap between chunks on disk and chunks in memory (implying that
	// upon first persisting of a chunk in memory, the offset has to be
	// set).
	chunkDescsOffset int
	// The savedFirstTime field is used as a fallback when the
	// chunkDescsOffset is not 0. It can be used to save the FirstTime of the
	// first chunk before its chunk desc is evicted. In doubt, this field is
	// just set to the oldest possible timestamp.
	savedFirstTime model.Time
	// The timestamp of the last sample in this series. Needed for fast
	// access for federation and to ensure timestamp monotonicity during
	// ingestion.
	lastTime model.Time
	// The last ingested sample value. Needed for fast access for
	// federation.
	lastSampleValue model.SampleValue
	// Whether lastSampleValue has been set already.
	lastSampleValueSet bool
	// Whether the current head chunk has already been finished.  If true,
	// the current head chunk must not be modified anymore.
	headChunkClosed bool
	// Whether the current head chunk is used by an iterator. In that case,
	// a non-closed head chunk has to be cloned before more samples are
	// appended.
	headChunkUsedByIterator bool
	// Whether the series is inconsistent with the last checkpoint in a way
	// that would require a disk seek during crash recovery.
	dirty bool
}

// newMemorySeries returns a pointer to a newly allocated memorySeries for the
// given metric. chunkDescs and modTime in the new series are set according to
// the provided parameters. chunkDescs can be nil or empty if this is a
// genuinely new time series (i.e. not one that is being unarchived). In that
// case, headChunkClosed is set to false, and firstTime and lastTime are both
// set to model.Earliest. The zero value for modTime can be used if the
// modification time of the series file is unknown (e.g. if this is a genuinely
// new series).
func newMemorySeries(m model.Metric, chunkDescs []*chunk.Desc, modTime time.Time) (*memorySeries, error) {
	var err error
	firstTime := model.Earliest
	lastTime := model.Earliest
	if len(chunkDescs) > 0 {
		firstTime = chunkDescs[0].FirstTime()
		if lastTime, err = chunkDescs[len(chunkDescs)-1].LastTime(); err != nil {
			return nil, err
		}
	}
	return &memorySeries{
		metric:           m,
		chunkDescs:       chunkDescs,
		headChunkClosed:  len(chunkDescs) > 0,
		savedFirstTime:   firstTime,
		lastTime:         lastTime,
		persistWatermark: len(chunkDescs),
		modTime:          modTime,
	}, nil
}

// add adds a sample pair to the series. It returns the number of newly
// completed chunks (which are now eligible for persistence).
//
// The caller must have locked the fingerprint of the series.
func (s *memorySeries) add(v model.SamplePair) (int, error) {
	if len(s.chunkDescs) == 0 || s.headChunkClosed {
		newHead := chunk.NewDesc(chunk.New(), v.Timestamp)
		s.chunkDescs = append(s.chunkDescs, newHead)
		s.headChunkClosed = false
	} else if s.headChunkUsedByIterator && s.head().RefCount() > 1 {
		// We only need to clone the head chunk if the current head
		// chunk was used in an iterator at all and if the refCount is
		// still greater than the 1 we always have because the head
		// chunk is not yet persisted. The latter is just an
		// approximation. We will still clone unnecessarily if an older
		// iterator using a previous version of the head chunk is still
		// around and keep the head chunk pinned. We needed to track
		// pins by version of the head chunk, which is probably not
		// worth the effort.
		chunk.Ops.WithLabelValues(chunk.Clone).Inc()
		// No locking needed here because a non-persisted head chunk can
		// not get evicted concurrently.
		s.head().C = s.head().C.Clone()
		s.headChunkUsedByIterator = false
	}

	chunks, err := s.head().Add(v)
	if err != nil {
		return 0, err
	}
	s.head().C = chunks[0]

	for _, c := range chunks[1:] {
		s.chunkDescs = append(s.chunkDescs, chunk.NewDesc(c, c.FirstTime()))
	}

	// Populate lastTime of now-closed chunks.
	for _, cd := range s.chunkDescs[len(s.chunkDescs)-len(chunks) : len(s.chunkDescs)-1] {
		cd.MaybePopulateLastTime()
	}

	s.lastTime = v.Timestamp
	s.lastSampleValue = v.Value
	s.lastSampleValueSet = true
	return len(chunks) - 1, nil
}

// head returns a pointer to the head chunk descriptor. The caller must have
// locked the fingerprint of the memorySeries. This method will panic if this
// series has no chunk descriptors.
func (s *memorySeries) head() *chunk.Desc {
	return s.chunkDescs[len(s.chunkDescs)-1]
}

// firstTime returns the timestamp of the first sample in the series.
//
// The caller must have locked the fingerprint of the memorySeries.
func (s *memorySeries) firstTime() model.Time {
	if s.chunkDescsOffset == 0 && len(s.chunkDescs) > 0 {
		return s.chunkDescs[0].FirstTime()
	}
	return s.savedFirstTime
}
