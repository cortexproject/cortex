// This file was taken from Prometheus (https://github.com/prometheus/prometheus).
// The original license header is included below:
//
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

package encoding

import (
	"errors"
	"io"
	"sort"

	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
)

// ChunkLen is the length of a chunk in bytes.
const ChunkLen = 1024

var (
	errChunkBoundsExceeded = errors.New("attempted access outside of chunk boundaries")
)

// Chunk is the interface for all chunks. Chunks are generally not
// goroutine-safe.
type Chunk interface {
	// Add adds a SamplePair to the chunks, performs any necessary
	// re-encoding, and creates any necessary overflow chunk.
	// The returned Chunk is the overflow chunk if it was created.
	// The returned Chunk is nil if the sample got appended to the same chunk.
	Add(sample model.SamplePair) (Chunk, error)
	// NewIterator returns an iterator for the chunks.
	// The iterator passed as argument is for re-use. Depending on implementation,
	// the iterator can be re-used or a new iterator can be allocated.
	NewIterator(Iterator) Iterator
	Marshal(io.Writer) error
	UnmarshalFromBuf([]byte) error
	Encoding() Encoding
	Utilization() float64

	// Slice returns a smaller chunk the includes all samples between start and end
	// (inclusive).  Its may over estimate. On some encodings it is a noop.
	Slice(start, end model.Time) Chunk

	// Len returns the number of samples in the chunk.  Implementations may be
	// expensive.
	Len() int

	// Size returns the approximate length of the chunk in bytes.
	Size() int
}

// Iterator enables efficient access to the content of a chunk. It is
// generally not safe to use an Iterator concurrently with or after chunk
// mutation.
type Iterator interface {
	// Scans the next value in the chunk. Directly after the iterator has
	// been created, the next value is the first value in the
	// chunk. Otherwise, it is the value following the last value scanned or
	// found (by one of the Find... methods). Returns false if either the
	// end of the chunk is reached or an error has occurred.
	Scan() bool
	// Finds the oldest value at or after the provided time. Returns false
	// if either the chunk contains no value at or after the provided time,
	// or an error has occurred.
	FindAtOrAfter(model.Time) bool
	// Returns the last value scanned (by the scan method) or found (by one
	// of the find... methods). It returns model.ZeroSamplePair before any of
	// those methods were called.
	Value() model.SamplePair
	// Returns a batch of the provisded size; NB not idempotent!  Should only be called
	// once per Scan.
	Batch(size int) Batch
	// Returns the last error encountered. In general, an error signals data
	// corruption in the chunk and requires quarantining.
	Err() error
}

// BatchSize is samples per batch; this was choose by benchmarking all sizes from
// 1 to 128.
const BatchSize = 12

// Batch is a sorted set of (timestamp, value) pairs.  They are intended to be
// small, and passed by value.
type Batch struct {
	Timestamps [BatchSize]int64
	Values     [BatchSize]float64
	Index      int
	Length     int
}

func (b *Batch) dropValuesForInterval(interval model.Interval) {
	if b.Length == 0 || model.Time(b.Timestamps[0]) > interval.End || model.Time(b.Timestamps[b.Length-1]) < interval.Start {
		return
	}

	removedIdxStart := 0
	for i := range b.Timestamps {
		if inbound(model.Time(b.Timestamps[i]), interval) {
			removedIdxStart = i
			break
		}
	}

	removedIdxEnd := b.Length
	for i := b.Length - 1; i >= 0; i-- {
		if inbound(model.Time(b.Timestamps[i]), interval) {
			removedIdxEnd = i
			break
		}
	}

	if removedIdxEnd == b.Length-1 {
		// We are removing values from tail so just reduce the length and return since
		// Timestamps and Values are of fixed size anyways
		b.Length = removedIdxStart
		return
	}

	copy(b.Timestamps[removedIdxStart:removedIdxEnd+1], b.Timestamps[removedIdxEnd+1:])
	copy(b.Values[removedIdxStart:removedIdxEnd+1], b.Values[removedIdxEnd+1:])

	b.Length = b.Length - (removedIdxEnd - removedIdxStart + 1)
}

// RangeValues is a utility function that retrieves all values within the given
// range from an Iterator.
func RangeValues(it Iterator, in metric.Interval) ([]model.SamplePair, error) {
	result := []model.SamplePair{}
	if !it.FindAtOrAfter(in.OldestInclusive) {
		return result, it.Err()
	}
	for !it.Value().Timestamp.After(in.NewestInclusive) {
		result = append(result, it.Value())
		if !it.Scan() {
			break
		}
	}
	return result, it.Err()
}

// addToOverflowChunk is a utility function that creates a new chunk as overflow
// chunk, adds the provided sample to it, and returns a chunk slice containing
// the provided old chunk followed by the new overflow chunk.
func addToOverflowChunk(s model.SamplePair) (Chunk, error) {
	overflowChunk := New()
	_, err := overflowChunk.Add(s)
	if err != nil {
		return nil, err
	}
	return overflowChunk, nil
}

// transcodeAndAdd is a utility function that transcodes the dst chunk into the
// provided src chunk (plus the necessary overflow chunks) and then adds the
// provided sample. It returns the new chunks (transcoded plus overflow) with
// the new sample at the end.
func transcodeAndAdd(dst Chunk, src Chunk, s model.SamplePair) ([]Chunk, error) {
	Ops.WithLabelValues(Transcode).Inc()

	var (
		head     = dst
		newChunk Chunk
		body     = []Chunk{head}
		err      error
	)

	it := src.NewIterator(nil)
	for it.Scan() {
		if newChunk, err = head.Add(it.Value()); err != nil {
			return nil, err
		}
		if newChunk != nil {
			body = append(body, newChunk)
			head = newChunk
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	if newChunk, err = head.Add(s); err != nil {
		return nil, err
	}
	if newChunk != nil {
		body = append(body, newChunk)
	}
	return body, nil
}

// indexAccessor allows accesses to samples by index.
type indexAccessor interface {
	timestampAtIndex(int) model.Time
	sampleValueAtIndex(int) model.SampleValue
	err() error
}

// indexAccessingChunkIterator is a chunk iterator for chunks for which an
// indexAccessor implementation exists.
type indexAccessingChunkIterator struct {
	len       int
	pos       int
	lastValue model.SamplePair
	acc       indexAccessor
}

func newIndexAccessingChunkIterator(len int, acc indexAccessor) *indexAccessingChunkIterator {
	return &indexAccessingChunkIterator{
		len:       len,
		pos:       -1,
		lastValue: model.ZeroSamplePair,
		acc:       acc,
	}
}

// scan implements Iterator.
func (it *indexAccessingChunkIterator) Scan() bool {
	it.pos++
	if it.pos >= it.len {
		return false
	}
	it.lastValue = model.SamplePair{
		Timestamp: it.acc.timestampAtIndex(it.pos),
		Value:     it.acc.sampleValueAtIndex(it.pos),
	}
	return it.acc.err() == nil
}

// findAtOrAfter implements Iterator.
func (it *indexAccessingChunkIterator) FindAtOrAfter(t model.Time) bool {
	i := sort.Search(it.len, func(i int) bool {
		return !it.acc.timestampAtIndex(i).Before(t)
	})
	if i == it.len || it.acc.err() != nil {
		return false
	}
	it.pos = i
	it.lastValue = model.SamplePair{
		Timestamp: it.acc.timestampAtIndex(i),
		Value:     it.acc.sampleValueAtIndex(i),
	}
	return true
}

// value implements Iterator.
func (it *indexAccessingChunkIterator) Value() model.SamplePair {
	return it.lastValue
}

func (it *indexAccessingChunkIterator) Batch(size int) Batch {
	var batch Batch
	j := 0
	for j < size && it.pos < it.len {
		batch.Timestamps[j] = int64(it.acc.timestampAtIndex(it.pos))
		batch.Values[j] = float64(it.acc.sampleValueAtIndex(it.pos))
		it.pos++
		j++
	}
	// Interface contract is that you call Scan before calling Batch; therefore
	// without this decrement, you'd end up skipping samples.
	it.pos--
	batch.Index = 0
	batch.Length = j
	return batch
}

// err implements Iterator.
func (it *indexAccessingChunkIterator) Err() error {
	return it.acc.err()
}

// deletedChunkIterator helps with iterating over a partially deleted chunk.
// It keeps index of next deleted interval that we would encounter as we keep moving forward with Scan() and Batch() calls
// Usages should make sure deleted intervals that are passed in are sorted
type deletedChunkIterator struct {
	itr              Iterator
	deletedIntervals []model.Interval

	// we always move forward while iterating over data from chunks, so using nextDeletedIntervalsIndex as optimization
	// we quickly validate whether timestamp of fetched value has reached next deleted time interval
	nextDeletedIntervalsIndex int
}

// NewDeletedChunkIterator creates an iterator for a chunk which takes care of deleted data
func NewDeletedChunkIterator(itr Iterator, deletedIntervals []model.Interval) Iterator {
	return &deletedChunkIterator{
		itr:                       itr,
		deletedIntervals:          deletedIntervals,
		nextDeletedIntervalsIndex: 0,
	}
}

// Scan checks if we ran into next deleted interval, if yes then it jumps ahead and fetches data right after deleted interval that we ran into.
func (dci *deletedChunkIterator) Scan() bool {
	if dci.nextDeletedIntervalsIndex == -1 {
		return dci.itr.Scan()
	}

	hasNext := dci.itr.Scan()
	if !hasNext {
		return false
	}

	if inbound(dci.itr.Value().Timestamp, dci.deletedIntervals[dci.nextDeletedIntervalsIndex]) {
		// we have hit a deleted interval, lets move past it
		hasNext := dci.itr.FindAtOrAfter(dci.deletedIntervals[dci.nextDeletedIntervalsIndex].End + 1)
		if !hasNext {
			return false
		}

		dci.incrementDeletedIntervalsIndex()
		return hasNext
	}

	return hasNext
}

// FindAtOrAfter makes sure we reset the nextDeletedIntervalsIndex to hold the index of immediate next deleted interval with
// reference to timestamp we are jumping to.
func (dci *deletedChunkIterator) FindAtOrAfter(t model.Time) bool {
	for i := range dci.deletedIntervals {
		if dci.deletedIntervals[i].Start > t {
			// we have moved past deleted intervals which can include t
			dci.nextDeletedIntervalsIndex = i
			break
		}

		if inbound(t, dci.deletedIntervals[i]) {
			// one of the deleted intervals includes t, lets set t to timestamp greater than end of deleted interval
			t = dci.deletedIntervals[i].End + 1
			dci.nextDeletedIntervalsIndex = i
			dci.incrementDeletedIntervalsIndex()
			break
		}
	}

	return dci.itr.FindAtOrAfter(t)
}

func (dci *deletedChunkIterator) Value() model.SamplePair {
	return dci.itr.Value()
}

// Batch removes values which are overlapping with all the following deleted intervals and sets
// nextDeletedIntervalsIndex to next non-overlapping deleted interval.
func (dci *deletedChunkIterator) Batch(size int) Batch {
	batch := dci.itr.Batch(size)
	if batch.Length == 0 {
		return batch
	}

	// checking whether batch overlaps next deleted interval. If yes, removing values which are overlapping
	for dci.nextDeletedIntervalsIndex != -1 && model.Time(batch.Timestamps[batch.Length-1]) >= dci.deletedIntervals[dci.nextDeletedIntervalsIndex].Start {
		batch.dropValuesForInterval(dci.deletedIntervals[dci.nextDeletedIntervalsIndex])

		if model.Time(batch.Timestamps[batch.Index]) >= dci.deletedIntervals[dci.nextDeletedIntervalsIndex].End {
			// batch moved past deleted intervals at index, lets move to next index
			dci.incrementDeletedIntervalsIndex()
		} else {
			break
		}
	}

	return batch
}

func (dci *deletedChunkIterator) Err() error {
	return dci.itr.Err()
}

func (dci *deletedChunkIterator) incrementDeletedIntervalsIndex() {
	dci.nextDeletedIntervalsIndex++

	if dci.nextDeletedIntervalsIndex >= len(dci.deletedIntervals) {
		dci.nextDeletedIntervalsIndex = -1
	}
}

func inbound(t model.Time, interval model.Interval) bool {
	return interval.Start <= t && t <= interval.End
}
