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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/thanos-io/objstore"

	"github.com/prometheus-community/parquet-common/schema"
)

var DefaultConvertOpts = convertOpts{
	name:               "block",
	rowGroupSize:       1e6,
	colDuration:        time.Hour * 8,
	numRowGroups:       math.MaxInt32,
	sortedLabels:       []string{labels.MetricName},
	bloomfilterLabels:  []string{labels.MetricName},
	pageBufferSize:     parquet.DefaultPageBufferSize,
	writeBufferSize:    parquet.DefaultWriteBufferSize,
	columnPageBuffers:  parquet.DefaultWriterConfig().ColumnPageBuffers,
	concurrency:        runtime.GOMAXPROCS(0),
	maxSamplesPerChunk: tsdb.DefaultSamplesPerChunk,
}

type Convertible interface {
	Index() (tsdb.IndexReader, error)
	Chunks() (tsdb.ChunkReader, error)
	Tombstones() (tombstones.Reader, error)
	Meta() tsdb.BlockMeta
}

type convertOpts struct {
	numRowGroups          int
	rowGroupSize          int
	colDuration           time.Duration
	name                  string
	sortedLabels          []string
	bloomfilterLabels     []string
	pageBufferSize        int
	writeBufferSize       int
	columnPageBuffers     parquet.BufferPool
	concurrency           int
	maxSamplesPerChunk    int
	labelsCompressionOpts []schema.CompressionOpts
	chunksCompressionOpts []schema.CompressionOpts
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

// WithSortBy configures the labels used for sorting time series data in the output Parquet files.
// The specified labels determine the sort order of rows within row groups, which can significantly
// improve query performance for filters on these labels. By default, data is sorted by __name__.
//
// Parameters:
//   - labels: Label names to sort by, in order of precedence
//
// Example:
//
//	WithSortBy("__name__", "job", "instance")
func WithSortBy(labels ...string) ConvertOption {
	return func(opts *convertOpts) {
		opts.sortedLabels = labels
	}
}

// WithColDuration sets the time duration for each column in the Parquet schema.
// This determines how time series data is partitioned across columns, affecting
// both storage efficiency and query performance. Shorter durations create more
// columns but allow for more precise time-based filtering.
//
// Parameters:
//   - d: Duration for each time column (default: 8 hours)
//
// Example:
//
//	WithColDuration(4 * time.Hour)  // 4-hour columns
func WithColDuration(d time.Duration) ConvertOption {
	return func(opts *convertOpts) {
		opts.colDuration = d
	}
}

// WithWriteBufferSize configures the buffer size used for writing Parquet data.
// Larger buffers can improve write performance by reducing I/O operations,
// but consume more memory during the conversion process.
//
// Parameters:
//   - s: Buffer size in bytes (default: parquet.DefaultWriteBufferSize)
//
// Example:
//
//	WithWriteBufferSize(64 * 1024)  // 64KB buffer
func WithWriteBufferSize(s int) ConvertOption {
	return func(opts *convertOpts) {
		opts.writeBufferSize = s
	}
}

// WithPageBufferSize sets the buffer size for Parquet page operations.
// This affects how data is buffered when reading and writing individual pages
// within the Parquet file format. Larger page buffers can improve performance
// for large datasets but increase memory usage.
//
// Parameters:
//   - s: Page buffer size in bytes (default: parquet.DefaultPageBufferSize)
//
// Example:
//
//	WithPageBufferSize(128 * 1024)  // 128KB page buffer
func WithPageBufferSize(s int) ConvertOption {
	return func(opts *convertOpts) {
		opts.pageBufferSize = s
	}
}

// WithName sets the base name used for generated Parquet files.
// This name is used as a prefix for the output files in the object store bucket.
//
// Parameters:
//   - name: Base name for output files (default: "block")
//
// Example:
//
//	WithName("prometheus-data")  // Files will be named prometheus-data-*
func WithName(name string) ConvertOption {
	return func(opts *convertOpts) {
		opts.name = name
	}
}

// WithNumRowGroups limits the maximum number of row groups to create during conversion.
// Row groups are the primary unit of parallelization in Parquet files. More row groups
// allow for better parallelization but may increase metadata overhead.
//
// Parameters:
//   - n: Maximum number of row groups (default: math.MaxInt32, effectively unlimited)
//
// Example:
//
//	WithNumRowGroups(100)  // Limit to 100 row groups
func WithNumRowGroups(n int) ConvertOption {
	return func(opts *convertOpts) {
		opts.numRowGroups = n
	}
}

// WithRowGroupSize sets the target number of rows per row group in the output Parquet files.
// Larger row groups improve compression and reduce metadata overhead, but require more
// memory during processing and may reduce parallelization opportunities.
//
// Parameters:
//   - size: Target number of rows per row group (default: 1,000,000)
//
// Example:
//
//	WithRowGroupSize(500000)  // 500K rows per row group
func WithRowGroupSize(size int) ConvertOption {
	return func(opts *convertOpts) {
		opts.rowGroupSize = size
	}
}

// WithConcurrency sets the number of concurrent goroutines used during conversion.
// Higher concurrency can improve performance on multi-core systems but increases
// memory usage. The optimal value depends on available CPU cores and memory.
//
// Parameters:
//   - concurrency: Number of concurrent workers (default: runtime.GOMAXPROCS(0))
//
// Example:
//
//	WithConcurrency(8)  // Use 8 concurrent workers
func WithConcurrency(concurrency int) ConvertOption {
	return func(opts *convertOpts) {
		opts.concurrency = concurrency
	}
}

// WithMaxSamplesPerChunk sets the maximum number of samples to include in each chunk
// during the encoding process. This affects how time series data is chunked and can
// impact both compression efficiency and query performance.
//
// Parameters:
//   - samplesPerChunk: Maximum samples per chunk (default: tsdb.DefaultSamplesPerChunk)
//
// Example:
//
//	WithMaxSamplesPerChunk(240)  // Limit chunks to 240 samples each
func WithMaxSamplesPerChunk(samplesPerChunk int) ConvertOption {
	return func(opts *convertOpts) {
		opts.maxSamplesPerChunk = samplesPerChunk
	}
}

func WithColumnPageBuffers(buffers parquet.BufferPool) ConvertOption {
	return func(opts *convertOpts) {
		opts.columnPageBuffers = buffers
	}
}

// WithCompression adds compression options to the conversion process.
// These options will be applied to both labels and chunks projections.
// For separate configuration, use WithLabelsCompression and WithChunksCompression.
func WithCompression(compressionOpts ...schema.CompressionOpts) ConvertOption {
	return func(opts *convertOpts) {
		opts.labelsCompressionOpts = compressionOpts
		opts.chunksCompressionOpts = compressionOpts
	}
}

// WithLabelsCompression configures compression options specifically for the labels projection.
// This allows fine-grained control over how label data is compressed in the output Parquet files,
// which can be optimized differently from chunk data due to different access patterns and data characteristics.
//
// Parameters:
//   - compressionOpts: optional compression options to apply to the projection schema
//
// Example:
//
//	WithLabelsCompression(schema.WithSnappyCompression())
func WithLabelsCompression(compressionOpts ...schema.CompressionOpts) ConvertOption {
	return func(opts *convertOpts) {
		opts.labelsCompressionOpts = compressionOpts
	}
}

// WithChunksCompression configures compression options specifically for the chunks projection.
// This allows optimization of compression settings for time series chunk data, which typically
// has different compression characteristics compared to label metadata.
//
// Parameters:
//   - compressionOpts: optional compression options to apply to the projection schema
//
// Example:
//
//	WithChunksCompression(schema.WithGzipCompression())
func WithChunksCompression(compressionOpts ...schema.CompressionOpts) ConvertOption {
	return func(opts *convertOpts) {
		opts.chunksCompressionOpts = compressionOpts
	}
}

// ConvertTSDBBlock converts one or more TSDB blocks to Parquet format and writes them to an object store bucket.
// It processes time series data within the specified time range and outputs sharded Parquet files.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - bkt: Object store bucket where the converted Parquet files will be written
//   - mint: Minimum timestamp (inclusive) for the time range to convert
//   - maxt: Maximum timestamp (exclusive) for the time range to convert
//   - blks: Slice of Convertible blocks (typically TSDB blocks) to be converted
//   - opts: Optional configuration options to customize the conversion process
//
// Returns:
//   - int: The current shard number after conversion
//   - error: Any error that occurred during the conversion process
//
// The function creates a row reader from the TSDB blocks, generates both labels and chunks
// projections with optional compression, and writes the data to the bucket using a sharded
// writer approach for better performance and parallelization.
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

	labelsProjection, err := rr.Schema().LabelsProjection(cfg.labelsCompressionOpts...)
	if err != nil {
		return 0, errors.Wrap(err, "error getting labels projection from tsdb schema")
	}
	chunksProjection, err := rr.Schema().ChunksProjection(cfg.chunksCompressionOpts...)
	if err != nil {
		return 0, errors.Wrap(err, "error getting chunks projection from tsdb schema")
	}
	outSchemaProjections := []*schema.TSDBProjection{
		labelsProjection, chunksProjection,
	}

	pipeReaderWriter := NewPipeReaderBucketWriter(bkt)
	w := NewShardedWrite(rr, rr.Schema(), outSchemaProjections, pipeReaderWriter, &cfg)
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
			return nil, fmt.Errorf("unable to get index reader from block: %w", err)
		}
		closers = append(closers, indexr)

		chunkr, err := blk.Chunks()
		if err != nil {
			return nil, fmt.Errorf("unable to get chunk reader from block: %w", err)
		}
		closers = append(closers, chunkr)

		tombsr, err := blk.Tombstones()
		if err != nil {
			return nil, fmt.Errorf("unable to get tombstone reader from block: %w", err)
		}
		closers = append(closers, tombsr)

		lblns, err := indexr.LabelNames(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get label names from block: %w", err)
		}

		postings := sortedPostings(ctx, indexr, ops.sortedLabels...)
		seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
		seriesSets = append(seriesSets, seriesSet)

		b.AddLabelNameColumn(lblns...)
	}

	cseriesSet := NewMergeChunkSeriesSet(seriesSets, compareFunc, storage.NewConcatenatingChunkSeriesMerger())

	s, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("unable to build index reader from block: %w", err)
	}

	return &TsdbRowReader{
		ctx:         ctx,
		seriesSet:   cseriesSet,
		closers:     closers,
		tsdbSchema:  s,
		concurrency: ops.concurrency,

		rowBuilder: parquet.NewRowBuilder(s.Schema),
		encoder:    schema.NewPrometheusParquetChunksEncoder(s, ops.maxSamplesPerChunk),
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

func sortedPostings(ctx context.Context, indexr tsdb.IndexReader, sortedLabels ...string) index.Postings {
	p := tsdb.AllSortedPostings(ctx, indexr)

	if len(sortedLabels) == 0 {
		return p
	}

	type s struct {
		ref    storage.SeriesRef
		idx    int
		labels labels.Labels
	}
	series := make([]s, 0, 128)

	scratchBuilder := labels.NewScratchBuilder(10)
	lb := labels.NewBuilder(labels.EmptyLabels())
	i := 0
	for p.Next() {
		scratchBuilder.Reset()
		err := indexr.Series(p.At(), &scratchBuilder, nil)
		if err != nil {
			return index.ErrPostings(fmt.Errorf("expand series: %w", err))
		}
		lb.Reset(scratchBuilder.Labels())

		series = append(series, s{labels: lb.Keep(sortedLabels...).Labels(), ref: p.At(), idx: i})
		i++
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(fmt.Errorf("expand postings: %w", err))
	}

	slices.SortFunc(series, func(a, b s) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.labels.Get(lb), b.labels.Get(lb)); c != 0 {
				return c
			}
		}
		if a.idx < b.idx {
			return -1
		} else if a.idx > b.idx {
			return 1
		}
		return 0
	})

	// Convert back to list.
	ep := make([]storage.SeriesRef, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	return index.NewListPostings(ep)
}

func (rr *TsdbRowReader) ReadRows(buf []parquet.Row) (int, error) {
	type chkBytesOrError struct {
		chkBytes [][]byte
		err      error
	}
	type chunkSeriesPromise struct {
		s storage.ChunkSeries
		c chan chkBytesOrError
	}

	c := make(chan chunkSeriesPromise, rr.concurrency)

	go func() {
		i := 0
		defer close(c)
		for i < len(buf) && rr.seriesSet.Next() {
			s := rr.seriesSet.At()
			it := s.Iterator(nil)

			promise := chunkSeriesPromise{
				s: s,
				c: make(chan chkBytesOrError, 1),
			}

			select {
			case c <- promise:
			case <-rr.ctx.Done():
				return
			}
			go func() {
				chkBytes, err := rr.encoder.Encode(it)
				promise.c <- chkBytesOrError{chkBytes: chkBytes, err: err}
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

		chkBytesOrErr := <-promise.c
		if err := chkBytesOrErr.err; err != nil {
			return 0, fmt.Errorf("unable encode chunks: %w", err)
		}
		chkBytes := chkBytesOrErr.chkBytes

		rr.rowBuilder.Reset()
		lblsIdxs = lblsIdxs[:0]

		promise.s.Labels().Range(func(l labels.Label) {
			colName := schema.LabelToColumn(l.Name)
			lc, _ := rr.tsdbSchema.Schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
			lblsIdxs = append(lblsIdxs, lc.ColumnIndex)
		})

		rr.rowBuilder.Add(colIndex.ColumnIndex, parquet.ValueOf(schema.EncodeIntSlice(lblsIdxs)))

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
