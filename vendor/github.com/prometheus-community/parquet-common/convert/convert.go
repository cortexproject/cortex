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

package convert

import (
	"context"
	"fmt"
	"io"
	"math"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/thanos-io/objstore"
)

var DefaultConvertOpts = convertOpts{
	name:              "block",
	rowGroupSize:      1e6,
	colDuration:       time.Hour * 8,
	numRowGroups:      math.MaxInt32,
	sortedLabels:      []string{labels.MetricName},
	bloomfilterLabels: []string{labels.MetricName},
	pageBufferSize:    parquet.DefaultPageBufferSize,
	writeBufferSize:   parquet.DefaultWriteBufferSize,
	columnPageBuffers: parquet.DefaultWriterConfig().ColumnPageBuffers,
	concurrency:       runtime.GOMAXPROCS(0),
}

type Convertible interface {
	Index() (tsdb.IndexReader, error)
	Chunks() (tsdb.ChunkReader, error)
	Tombstones() (tombstones.Reader, error)
	Meta() tsdb.BlockMeta
}

type convertOpts struct {
	numRowGroups      int
	rowGroupSize      int
	colDuration       time.Duration
	name              string
	sortedLabels      []string
	bloomfilterLabels []string
	pageBufferSize    int
	writeBufferSize   int
	columnPageBuffers parquet.BufferPool
	concurrency       int
}

func (cfg convertOpts) buildBloomfilterColumns() []parquet.BloomFilterColumn {
	cols := make([]parquet.BloomFilterColumn, 0, len(cfg.bloomfilterLabels))
	for _, label := range cfg.bloomfilterLabels {
		cols = append(cols, parquet.SplitBlockFilter(10, schema.LabelToColumn(label)))
	}

	return cols
}

func (cfg convertOpts) buildSortingColumns() []parquet.SortingColumn {
	cols := make([]parquet.SortingColumn, 0, len(cfg.sortedLabels))

	for _, label := range cfg.sortedLabels {
		cols = append(cols, parquet.Ascending(schema.LabelToColumn(label)))
	}

	return cols
}

type ConvertOption func(*convertOpts)

func WithSortBy(labels ...string) ConvertOption {
	return func(opts *convertOpts) {
		opts.sortedLabels = labels
	}
}

func WithColDuration(d time.Duration) ConvertOption {
	return func(opts *convertOpts) {
		opts.colDuration = d
	}
}

func WithWriteBufferSize(s int) ConvertOption {
	return func(opts *convertOpts) {
		opts.writeBufferSize = s
	}
}

func WithPageBufferSize(s int) ConvertOption {
	return func(opts *convertOpts) {
		opts.pageBufferSize = s
	}
}

func WithName(name string) ConvertOption {
	return func(opts *convertOpts) {
		opts.name = name
	}
}

func WithConcurrency(concurrency int) ConvertOption {
	return func(opts *convertOpts) {
		opts.concurrency = concurrency
	}
}

func WithColumnPageBuffers(buffers parquet.BufferPool) ConvertOption {
	return func(opts *convertOpts) {
		opts.columnPageBuffers = buffers
	}
}

func ConvertTSDBBlock(
	ctx context.Context,
	bkt objstore.Bucket,
	mint, maxt int64,
	blks []Convertible,
	opts ...ConvertOption,
) (int, error) {
	cfg := DefaultConvertOpts

	for _, opt := range opts {
		opt(&cfg)
	}

	rr, err := NewTsdbRowReader(ctx, mint, maxt, cfg.colDuration.Milliseconds(), blks, cfg)
	if err != nil {
		return 0, err
	}

	defer func() { _ = rr.Close() }()
	w := NewShardedWrite(rr, rr.Schema(), bkt, &cfg)
	return w.currentShard, errors.Wrap(w.Write(ctx), "error writing block")
}

var _ parquet.RowReader = &TsdbRowReader{}

type TsdbRowReader struct {
	ctx context.Context

	closers []io.Closer

	seriesSet storage.ChunkSeriesSet

	rowBuilder *parquet.RowBuilder
	tsdbSchema *schema.TSDBSchema

	encoder     *schema.PrometheusParquetChunksEncoder
	totalRead   int64
	concurrency int
}

func NewTsdbRowReader(ctx context.Context, mint, maxt, colDuration int64, blks []Convertible, ops convertOpts) (*TsdbRowReader, error) {
	var (
		seriesSets = make([]storage.ChunkSeriesSet, 0, len(blks))
		closers    = make([]io.Closer, 0, len(blks))
	)

	b := schema.NewBuilder(mint, maxt, colDuration)

	compareFunc := func(a, b labels.Labels) int {
		for _, lb := range ops.sortedLabels {
			if c := strings.Compare(a.Get(lb), b.Get(lb)); c != 0 {
				return c
			}
		}

		return labels.Compare(a, b)
	}

	for _, blk := range blks {
		indexr, err := blk.Index()
		if err != nil {
			return nil, fmt.Errorf("unable to get index reader from block: %s", err)
		}
		closers = append(closers, indexr)

		chunkr, err := blk.Chunks()
		if err != nil {
			return nil, fmt.Errorf("unable to get chunk reader from block: %s", err)
		}
		closers = append(closers, chunkr)

		tombsr, err := blk.Tombstones()
		if err != nil {
			return nil, fmt.Errorf("unable to get tombstone reader from block: %s", err)
		}
		closers = append(closers, tombsr)

		lblns, err := indexr.LabelNames(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get label names from block: %s", err)
		}

		postings := sortedPostings(ctx, indexr, compareFunc, ops.sortedLabels...)
		seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
		seriesSets = append(seriesSets, seriesSet)

		b.AddLabelNameColumn(lblns...)
	}

	cseriesSet := NewMergeChunkSeriesSet(seriesSets, compareFunc, storage.NewConcatenatingChunkSeriesMerger())

	s, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("unable to build index reader from block: %s", err)
	}

	return &TsdbRowReader{
		ctx:         ctx,
		seriesSet:   cseriesSet,
		closers:     closers,
		tsdbSchema:  s,
		concurrency: ops.concurrency,

		rowBuilder: parquet.NewRowBuilder(s.Schema),
		encoder:    schema.NewPrometheusParquetChunksEncoder(s),
	}, nil
}

func (rr *TsdbRowReader) Close() error {
	err := &multierror.Error{}
	for i := range rr.closers {
		err = multierror.Append(err, rr.closers[i].Close())
	}
	return err.ErrorOrNil()
}

func (rr *TsdbRowReader) Schema() *schema.TSDBSchema {
	return rr.tsdbSchema
}

func sortedPostings(ctx context.Context, indexr tsdb.IndexReader, compare func(a, b labels.Labels) int, sortedLabels ...string) index.Postings {
	p := tsdb.AllSortedPostings(ctx, indexr)

	if len(sortedLabels) == 0 {
		return p
	}

	type s struct {
		ref    storage.SeriesRef
		labels labels.Labels
	}
	series := make([]s, 0, 128)

	lb := labels.NewScratchBuilder(10)
	for p.Next() {
		lb.Reset()
		err := indexr.Series(p.At(), &lb, nil)
		if err != nil {
			return index.ErrPostings(fmt.Errorf("expand series: %w", err))
		}

		series = append(series, s{labels: lb.Labels(), ref: p.At()})
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(fmt.Errorf("expand postings: %w", err))
	}

	slices.SortFunc(series, func(a, b s) int { return compare(a.labels, b.labels) })

	// Convert back to list.
	ep := make([]storage.SeriesRef, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	return index.NewListPostings(ep)
}

func (rr *TsdbRowReader) ReadRows(buf []parquet.Row) (int, error) {
	type chunkSeriesPromise struct {
		s              storage.ChunkSeries
		chunkBytesChan chan [][]byte
		err            error
	}

	c := make(chan chunkSeriesPromise, rr.concurrency)

	go func() {
		i := 0
		defer close(c)
		for i < len(buf) && rr.seriesSet.Next() {
			s := rr.seriesSet.At()
			it := s.Iterator(nil)

			promise := chunkSeriesPromise{
				s:              s,
				chunkBytesChan: make(chan [][]byte, 1),
			}

			select {
			case c <- promise:
			case <-rr.ctx.Done():
				return
			}
			go func() {
				chkBytes, err := rr.encoder.Encode(it)
				promise.err = err
				promise.chunkBytesChan <- chkBytes
			}()
			i++
		}
	}()

	i, j := 0, 0
	lblsIdxs := []int{}
	colIndex, ok := rr.tsdbSchema.Schema.Lookup(schema.ColIndexes)
	if !ok {
		return 0, fmt.Errorf("unable to find indexes")
	}

	for promise := range c {
		j++
		if promise.err != nil {
			return i, promise.err
		}

		rr.rowBuilder.Reset()
		lblsIdxs = lblsIdxs[:0]

		promise.s.Labels().Range(func(l labels.Label) {
			colName := schema.LabelToColumn(l.Name)
			lc, _ := rr.tsdbSchema.Schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
			lblsIdxs = append(lblsIdxs, lc.ColumnIndex)
		})

		rr.rowBuilder.Add(colIndex.ColumnIndex, parquet.ValueOf(schema.EncodeIntSlice(lblsIdxs)))

		chkBytes := <-promise.chunkBytesChan
		// skip series that have no chunks in the requested time
		if allChunksEmpty(chkBytes) {
			continue
		}

		for idx, chk := range chkBytes {
			if len(chk) == 0 {
				continue
			}
			rr.rowBuilder.Add(rr.tsdbSchema.DataColsIndexes[idx], parquet.ValueOf(chk))
		}
		buf[i] = rr.rowBuilder.AppendRow(buf[i][:0])
		i++
	}
	rr.totalRead += int64(i)

	if rr.ctx.Err() != nil {
		return i, rr.ctx.Err()
	}

	if j < len(buf) {
		return i, io.EOF
	}

	return i, rr.seriesSet.Err()
}

func allChunksEmpty(chkBytes [][]byte) bool {
	for _, chk := range chkBytes {
		if len(chk) != 0 {
			return false
		}
	}
	return true
}
