// Copyright The Prometheus Authors
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

package search

import (
	"context"
	"io"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
)

var _ prom_storage.ChunkSeries = &ConcreteChunksSeries{}

type ConcreteChunksSeries struct {
	lbls labels.Labels
	chks []chunks.Meta
}

func (c ConcreteChunksSeries) Labels() labels.Labels {
	return c.lbls
}

func (c ConcreteChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return prom_storage.NewListChunkSeriesIterator(c.chks...)
}

type IteratorChunksSeries struct {
	lbls labels.Labels
	chks chunks.Iterator
}

func (i *IteratorChunksSeries) Labels() labels.Labels {
	return i.lbls
}

func (i *IteratorChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return i.chks
}

// ChunkCount returns the number of chunks in the series, consuming the inner iterator.
// The current implementation is an expensive operation which reads the chunks from storage.
// It is implemented only to satisfy the Mimir Prometheus fork's extended ChunkSeries interface.
// It may be optimized in the future with extended metadata and indexes in the parquet chunks file.
func (i *IteratorChunksSeries) ChunkCount() (int, error) {
	if i.chks == nil {
		return 0, nil
	}
	count := 0
	for i.chks.Next() {
		count++
	}
	if err := i.chks.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

type ChunkSeriesSetCloser interface {
	prom_storage.ChunkSeriesSet

	// Close releases any memory buffers held by the ChunkSeriesSet or the
	// underlying ChunkSeries. It is not safe to use the ChunkSeriesSet
	// or any of its ChunkSeries after calling Close.
	Close() error
}

type NoChunksConcreteLabelsSeriesSet struct {
	seriesSet        []*ConcreteChunksSeries
	currentSeriesIdx int
}

func NewNoChunksConcreteLabelsSeriesSet(sLbls []labels.Labels) *NoChunksConcreteLabelsSeriesSet {
	seriesSet := make([]*ConcreteChunksSeries, len(sLbls))
	for i, lbls := range sLbls {
		seriesSet[i] = &ConcreteChunksSeries{lbls: lbls}
	}
	return &NoChunksConcreteLabelsSeriesSet{
		seriesSet:        seriesSet,
		currentSeriesIdx: -1,
	}
}

func (s *NoChunksConcreteLabelsSeriesSet) At() prom_storage.ChunkSeries {
	return s.seriesSet[s.currentSeriesIdx]
}

func (s *NoChunksConcreteLabelsSeriesSet) Next() bool {
	if s.currentSeriesIdx+1 == len(s.seriesSet) {
		return false
	}
	s.currentSeriesIdx++
	return true
}

func (s *NoChunksConcreteLabelsSeriesSet) Err() error {
	return nil
}

func (s *NoChunksConcreteLabelsSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *NoChunksConcreteLabelsSeriesSet) Close() error {
	return nil
}

// FilterEmptyChunkSeriesSet is a ChunkSeriesSet that lazily filters out series with no chunks.
// It takes a set of materialized labels and a lazy iterator of chunks.Iterators;
// the labels and iterators are iterated in tandem to yield series with chunks.
// The materialized series callback is applied to each series when the iterator advances during Next().
type FilterEmptyChunkSeriesSet struct {
	ctx     context.Context
	lblsSet []labels.Labels
	chnkSet ChunksIteratorIterator

	currentSeries              *IteratorChunksSeries
	materializedSeriesCallback MaterializedSeriesFunc
	err                        error
}

func NewFilterEmptyChunkSeriesSet(
	ctx context.Context,
	lblsSet []labels.Labels,
	chnkSet ChunksIteratorIterator,
	materializeSeriesCallback MaterializedSeriesFunc,
) *FilterEmptyChunkSeriesSet {
	return &FilterEmptyChunkSeriesSet{
		ctx:                        ctx,
		lblsSet:                    lblsSet,
		chnkSet:                    chnkSet,
		materializedSeriesCallback: materializeSeriesCallback,
	}
}

func (s *FilterEmptyChunkSeriesSet) At() prom_storage.ChunkSeries {
	return s.currentSeries
}

func (s *FilterEmptyChunkSeriesSet) Next() bool {
	for s.chnkSet.Next() {
		if len(s.lblsSet) == 0 {
			s.err = errors.New("less labels than chunks, this should not happen")
			return false
		}
		lbls := s.lblsSet[0]
		s.lblsSet = s.lblsSet[1:]
		iter := s.chnkSet.At()
		if iter.Next() {
			// The series has chunks, keep it
			meta := iter.At()
			s.currentSeries = &IteratorChunksSeries{
				lbls: lbls,
				chks: &PeekedChunksIterator{
					inner:       iter,
					peekedValue: &meta,
				},
			}
			s.err = s.materializedSeriesCallback(s.ctx, s.currentSeries)
			return s.err == nil
		}

		if iter.Err() != nil {
			s.err = iter.Err()
			return false
		}
		// This series has no chunks, skip it and continue to the next
	}
	if s.chnkSet.Err() != nil {
		s.err = s.chnkSet.Err()
	}
	if len(s.lblsSet) > 0 {
		s.err = errors.New("more labels than chunks, this should not happen")
	}
	return false
}

func (s *FilterEmptyChunkSeriesSet) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.chnkSet.Err()
}

func (s *FilterEmptyChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *FilterEmptyChunkSeriesSet) Close() error {
	return s.chnkSet.Close()
}

// PeekedChunksIterator is used to yield the first chunk of chunks.Iterator
// which has already had its Next() method called to check if it has any chunks.
// The already-consumed chunks.Meta is stored in peekedValue and returned on the first call to At().
// Subsequent calls to Next then continue with the inner chunks.Iterator.
type PeekedChunksIterator struct {
	inner       chunks.Iterator
	peekedValue *chunks.Meta
	nextCalled  bool
}

func (i *PeekedChunksIterator) Next() bool {
	if !i.nextCalled {
		i.nextCalled = true
		return true
	}
	if i.peekedValue != nil {
		// This is the second call to Next, discard the peeked value
		i.peekedValue = nil
	}
	return i.inner.Next()
}

func (i *PeekedChunksIterator) At() chunks.Meta {
	if i.peekedValue != nil {
		return *i.peekedValue
	}
	return i.inner.At()
}

func (i *PeekedChunksIterator) Err() error {
	return i.inner.Err()
}

type ChunksIteratorIterator interface {
	Next() bool
	At() chunks.Iterator
	Err() error
	Close() error
}

// MultiColumnChunksDecodingIterator yields a prometheus chunks.Iterator from multiple parquet Columns.
// The column iterators are called in order for each column and zipped together,
// yielding a single iterator that in turn can yield all chunks for the same row.
//
// The "column iterators" are pagesRowValueIterator which are expected to be:
// 1. in order of the columns in the parquet file
// 2. initialized with the same set of pages and row ranges
// An error is returned if the iterators have different lengths.
type MultiColumnChunksDecodingIterator struct {
	mint int64
	maxt int64

	columnValueIterators []*ColumnValueIterator
	d                    *schema.PrometheusParquetChunksDecoder

	// current is a chunk-decoding iterator for the materialized parquet Values
	// combined by calling and zipping all column iterators in order
	current *ValueDecodingChunkIterator
	err     error
}

func (i *MultiColumnChunksDecodingIterator) At() chunks.Iterator {
	return i.current
}

func (i *MultiColumnChunksDecodingIterator) Next() bool {
	if i.err != nil || len(i.columnValueIterators) == 0 {
		return false
	}

	multiColumnValues := make([]parquet.Value, 0, len(i.columnValueIterators))
	for _, columnValueIter := range i.columnValueIterators {
		if !columnValueIter.Next() {
			i.err = columnValueIter.Err()
			if i.err != nil {
				return false
			}
			continue
		}
		at := columnValueIter.At()
		multiColumnValues = append(multiColumnValues, at)
	}
	if len(multiColumnValues) == 0 {
		return false
	}

	i.current = &ValueDecodingChunkIterator{
		mint:   i.mint,
		maxt:   i.maxt,
		values: multiColumnValues,
		d:      i.d,
	}
	return true
}

func (i *MultiColumnChunksDecodingIterator) Err() error {
	return i.err
}

func (i *MultiColumnChunksDecodingIterator) Close() error {
	err := &multierror.Error{}
	for _, iter := range i.columnValueIterators {
		err = multierror.Append(err, iter.Close())
	}
	return err.ErrorOrNil()
}

// ValueDecodingChunkIterator decodes and yields chunks from a parquet Values slice.
type ValueDecodingChunkIterator struct {
	mint   int64
	maxt   int64
	values []parquet.Value
	d      *schema.PrometheusParquetChunksDecoder

	decoded []chunks.Meta
	current chunks.Meta
	err     error
}

func (i *ValueDecodingChunkIterator) At() chunks.Meta {
	return i.current
}

func (i *ValueDecodingChunkIterator) Next() bool {
	if i.err != nil {
		return false
	}
	if len(i.values) == 0 && len(i.decoded) == 0 {
		return false
	}
	if len(i.decoded) > 0 {
		i.current = i.decoded[0]
		i.decoded = i.decoded[1:]
		return true
	}
	value := i.values[0]
	i.values = i.values[1:]

	i.decoded, i.err = i.d.Decode(value.ByteArray(), i.mint, i.maxt)
	return i.Next()
}

func (i *ValueDecodingChunkIterator) Err() error {
	return i.err
}

func (c ConcreteChunksSeries) ChunkCount() (int, error) {
	return len(c.chks), nil
}

type ColumnValueIterator struct {
	currentIteratorIndex int
	rowRangesIterators   []*RowRangesValueIterator

	current parquet.Value
	err     error
}

func (i *ColumnValueIterator) At() parquet.Value {
	return i.current
}

func (i *ColumnValueIterator) Next() bool {
	if i.err != nil {
		return false
	}

	found := false
	for !found {
		if i.currentIteratorIndex >= len(i.rowRangesIterators) {
			return false
		}

		currentIterator := i.rowRangesIterators[i.currentIteratorIndex]
		hasNext := currentIterator.Next()

		if !hasNext {
			if err := currentIterator.Err(); err != nil {
				i.err = err
				_ = currentIterator.Close()
				return false
			}
			// Iterator exhausted without error; close and move on to the next one
			_ = currentIterator.Close()
			i.currentIteratorIndex++
			continue
		}
		i.current = currentIterator.At()
		found = true
	}
	return found
}

func (i *ColumnValueIterator) Err() error {
	return i.err
}

func (i *ColumnValueIterator) Close() error {
	err := &multierror.Error{}
	for _, iter := range i.rowRangesIterators {
		err = multierror.Append(err, iter.Close())
	}
	return err.ErrorOrNil()
}

// RowRangesValueIterator yields individual parquet Values from specified row ranges in its FilePages
type RowRangesValueIterator struct {
	pgs          parquet.Pages
	pageIterator *PageValueIterator

	remainingRr []RowRange
	currentRr   RowRange
	next        int64
	remaining   int64
	currentRow  int64

	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func newRowRangesValueIterator(
	ctx context.Context,
	file storage.ParquetFileView,
	cc parquet.ColumnChunk,
	pageRange PageToReadWithRow,
	dictOff uint64,
	dictSz uint64,
) (*RowRangesValueIterator, error) {
	minOffset := uint64(pageRange.Offset())
	maxOffset := uint64(pageRange.Offset() + pageRange.CompressedSize())

	// if dictOff == 0, it means that the collum is not dictionary encoded
	if dictOff > 0 && int(minOffset-(dictOff+dictSz)) < file.PagePartitioningMaxGapSize() {
		minOffset = dictOff
	}

	pgs, err := file.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		if pgs != nil {
			_ = pgs.Close()
		}
		return nil, errors.Wrap(err, "failed to get pages")
	}

	err = pgs.SeekToRow(pageRange.rows[0].From)
	if err != nil {
		_ = pgs.Close()
		return nil, errors.Wrap(err, "failed to seek to row")
	}

	remainingRr := pageRange.rows

	currentRr := remainingRr[0]
	next := currentRr.From
	remaining := currentRr.Count
	currentRow := currentRr.From

	remainingRr = remainingRr[1:]
	return &RowRangesValueIterator{
		pgs:          pgs,
		pageIterator: new(PageValueIterator),

		remainingRr: remainingRr,
		currentRr:   currentRr,
		next:        next,
		remaining:   remaining,
		currentRow:  currentRow,
	}, nil
}

func (i *RowRangesValueIterator) At() parquet.Value {
	return i.buffer[i.currentBufferIndex]
}

func (i *RowRangesValueIterator) Next() bool {
	if i.err != nil {
		return false
	}

	if len(i.buffer) > 0 && i.currentBufferIndex < len(i.buffer)-1 {
		// Still have buffered values from previous page reads to yield
		i.currentBufferIndex++
		return true
	}

	if len(i.remainingRr) == 0 && i.remaining == 0 {
		// Done; all rows of all pages have been read
		// and all buffered values from the row ranges have been yielded
		return false
	}

	// Read pages until we find values for the next row range.
	found := false
	for !found {
		// Prepare inner iterator
		page, err := i.pgs.ReadPage()
		if err != nil {
			i.err = errors.Wrap(err, "failed to read page")
			return false
		}
		i.pageIterator.Reset(page)
		// Reset page values buffer
		i.currentBufferIndex = 0
		i.buffer = i.buffer[:0]

		for i.pageIterator.Next() {
			if i.currentRow == i.next {
				found = true
				i.buffer = append(i.buffer, i.pageIterator.At())

				i.remaining--
				if i.remaining > 0 {
					i.next = i.next + 1
				} else if len(i.remainingRr) > 0 {
					i.currentRr = i.remainingRr[0]
					i.next = i.currentRr.From
					i.remaining = i.currentRr.Count
					i.remainingRr = i.remainingRr[1:]
				}
			}
			if i.pageIterator.CanSkip() && i.remaining > 0 {
				i.currentRow += i.pageIterator.Skip(i.next - i.currentRow - 1)
			}
			i.currentRow++
		}
		parquet.Release(page)
		if i.pageIterator.Err() != nil {
			i.err = errors.Wrap(i.pageIterator.Err(), "failed to read page values")
			return false
		}
	}
	return found
}

func (i *RowRangesValueIterator) Err() error {
	return i.err
}

func (i *RowRangesValueIterator) Close() error {
	return i.pgs.Close()
}

// PageValueIterator yields individual parquet Values from its Page.
type PageValueIterator struct {
	p parquet.Page

	cachedSymbols map[int32]parquet.Value
	st            SymbolTable

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (i *PageValueIterator) At() parquet.Value {
	if i.vr == nil {
		dicIndex := i.st.GetIndex(i.current)
		// Cache a clone of the current symbol table entry.
		// This allows us to release the original page while avoiding unnecessary future clones.
		if _, ok := i.cachedSymbols[dicIndex]; !ok {
			i.cachedSymbols[dicIndex] = i.st.Get(i.current).Clone()
		}
		return i.cachedSymbols[dicIndex]
	}
	return i.buffer[i.currentBufferIndex].Clone()
}

func (i *PageValueIterator) Next() bool {
	if i.err != nil {
		return false
	}

	i.current++
	if i.current >= int(i.p.NumRows()) {
		return false
	}

	i.currentBufferIndex++

	if i.currentBufferIndex == len(i.buffer) {
		n, err := i.vr.ReadValues(i.buffer[:cap(i.buffer)])
		if err != nil && err != io.EOF {
			i.err = err
		}
		i.buffer = i.buffer[:n]
		i.currentBufferIndex = 0
	}

	return true
}

func (i *PageValueIterator) CanSkip() bool {
	return i.vr == nil
}

func (i *PageValueIterator) Skip(n int64) int64 {
	r := min(n, i.p.NumRows()-int64(i.current)-1)
	i.current += int(r)
	return r
}

func (i *PageValueIterator) Reset(p parquet.Page) {
	i.p = p
	i.vr = nil
	if p.Dictionary() != nil {
		i.st.Reset(p)
		i.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	} else {
		i.vr = p.Values()
		if i.buffer != nil {
			i.buffer = i.buffer[:0]
		} else {
			i.buffer = make([]parquet.Value, 0, 128)
		}
		i.currentBufferIndex = -1
	}
	i.current = -1
}

func (i *PageValueIterator) Err() error {
	return i.err
}
