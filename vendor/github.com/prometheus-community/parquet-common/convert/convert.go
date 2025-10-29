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
	"log/slog"
	"math"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

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
	readConcurrency:    runtime.GOMAXPROCS(0),
	writeConcurrency:   1,
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
	readConcurrency       int
	writeConcurrency      int
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

// WithBloomFilterLabels configures which labels should have bloom filters created during conversion.
// Bloom filters enable fast filtering during queries by allowing quick elimination of row groups
// that definitely don't contain a specific label value. This significantly improves query performance
// for high-cardinality labels. By default, bloom filters are created for __name__.
//
// Parameters:
//   - labels: Label names to create bloom filters for
//
// Example:
//
//	WithBloomFilterLabels("__name__", "job", "instance")
func WithBloomFilterLabels(labels ...string) ConvertOption {
	return func(opts *convertOpts) {
		opts.bloomfilterLabels = labels
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

// WithReadConcurrency sets the number of concurrent goroutines used to read TSDB series during conversion.
// Higher concurrency can improve performance on multi-core systems but increases
// memory usage. The optimal value depends on available CPU cores and memory.
//
// Parameters:
//   - concurrency: Number of concurrent workers (default: runtime.GOMAXPROCS(0))
//
// Example:
//
//	WithReadConcurrency(8)  // Use 8 concurrent workers
func WithReadConcurrency(concurrency int) ConvertOption {
	return func(opts *convertOpts) {
		opts.readConcurrency = concurrency
	}
}

// WithWriteConcurrency sets the number of concurrent goroutines used to write Parquet shards during conversion.
// Higher concurrency can improve conversion time on multi-core systems but increases
// CPU and memory usage. The optimal value depends on available CPU cores and memory.
//
// Parameters:
//   - concurrency: Number of concurrent workers (default: runtime.GOMAXPROCS(0))
//
// Example:
//
//	WithWriteConcurrency(8)  // Use 8 concurrent workers
func WithWriteConcurrency(concurrency int) ConvertOption {
	return func(opts *convertOpts) {
		opts.writeConcurrency = concurrency
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
//   - int: The number of shards written for a successful conversion
//   - error: Any error that occurred during the conversion process
//
// The function creates a row reader for each TSDB block and identifies the unique input series.
// Input series are divided into shards based on the configured max row groups and row group size.
// The labels file schema is independently generated from the series present in each shard,
// in order to avoid writing (and later reading) footer and index data for blank columns.
// Shards will be written in parallel if configured by the ConvertOptions.
func ConvertTSDBBlock(
	ctx context.Context,
	bkt objstore.Bucket,
	mint, maxt int64,
	blocks []Convertible,
	logger *slog.Logger,
	opts ...ConvertOption,
) (int, error) {
	cfg := DefaultConvertOpts
	for _, opt := range opts {
		opt(&cfg)
	}

	logger.Info("sharding input series")
	shardedRowReaders, err := shardedTSDBRowReaders(ctx, mint, maxt, cfg.colDuration.Milliseconds(), blocks, cfg)
	if err != nil {
		return 0, errors.Wrap(err, "failed to create sharded TSDB row readers")
	}
	defer func() {
		for _, rr := range shardedRowReaders {
			_ = rr.Close()
		}
	}()

	logger.Info("starting parallel block conversion", "shards", len(shardedRowReaders), "write_concurrency", cfg.writeConcurrency)
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(cfg.writeConcurrency)
	for shard, rr := range shardedRowReaders {
		errGroup.Go(func() error {
			labelsProjection, err := rr.Schema().LabelsProjection(cfg.labelsCompressionOpts...)
			if err != nil {
				return errors.Wrap(err, "error getting labels projection from tsdb schema")
			}
			chunksProjection, err := rr.Schema().ChunksProjection(cfg.chunksCompressionOpts...)
			if err != nil {
				return errors.Wrap(err, "error getting chunks projection from tsdb schema")
			}
			outSchemaProjections := []*schema.TSDBProjection{
				labelsProjection, chunksProjection,
			}

			w := &PreShardedWriter{
				shard:                shard,
				rr:                   rr,
				schema:               rr.Schema(),
				outSchemaProjections: outSchemaProjections,
				pipeReaderWriter:     NewPipeReaderBucketWriter(bkt),
				opts:                 &cfg,
				logger:               logger,
			}
			err = w.Write(ctx)
			if err != nil {
				return errors.Wrap(err, "error writing shard for block")
			}
			return nil
		})
	}

	err = errGroup.Wait()
	if err != nil {
		return 0, errors.Wrap(err, "failed to convert shards in parallel")
	}
	return len(shardedRowReaders), nil
}

type blockIndexReader struct {
	blockID  ulid.ULID
	idx      int // index of the block in the input slice
	reader   tsdb.IndexReader
	postings index.Postings
}

type blockSeries struct {
	blockIdx  int // index of the block in the input slice
	seriesIdx int // index of the series in the block postings
	ref       storage.SeriesRef
	labels    labels.Labels
}

func shardedTSDBRowReaders(
	ctx context.Context,
	mint, maxt, colDuration int64,
	blocks []Convertible,
	opts convertOpts,
) ([]*TSDBRowReader, error) {
	// Blocks can have multiple entries with the same of ULID in the case of head blocks;
	// track all blocks by their index in the input slice rather than assuming unique ULIDs.
	indexReaders := make([]blockIndexReader, len(blocks))
	// Simpler to track and close these readers separate from those used by shard conversion reader/writers.
	defer func() {
		for _, indexReader := range indexReaders {
			_ = indexReader.reader.Close()
		}
	}()
	for i, blk := range blocks {
		indexReader, err := blk.Index()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get index reader from block")
		}
		indexReaders[i] = blockIndexReader{
			blockID:  blk.Meta().ULID,
			idx:      i,
			reader:   indexReader,
			postings: tsdb.AllSortedPostings(ctx, indexReader),
		}
	}

	uniqueSeriesCount, shardedSeries, err := shardSeries(indexReaders, mint, maxt, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to determine unique series count")
	}
	if uniqueSeriesCount == 0 {
		return nil, errors.Wrap(err, "no series found in the specified time range")
	}

	shardTSDBRowReaders := make([]*TSDBRowReader, len(shardedSeries))

	// We close everything if any errors or panic occur
	allClosers := make([]io.Closer, 0, len(blocks)*3)
	ok := false
	defer func() {
		if !ok {
			for _, closer := range allClosers {
				_ = closer.Close()
			}
		}
	}()

	// For each shard, create a TSDBRowReader with:
	//	* a MergeChunkSeriesSet of all blocks' series sets for the shard
	//	* a schema built from only the label names present in the shard
	for shardIdx, shardSeries := range shardedSeries {
		// An index, chunk, and tombstone reader per block each must be closed after usage
		// in order for the prometheus block reader to not hang indefinitely when closed.
		closers := make([]io.Closer, 0, len(shardSeries)*3)
		seriesSets := make([]storage.ChunkSeriesSet, 0, len(blocks))
		schemaBuilder := schema.NewBuilder(mint, maxt, colDuration)

		// For each block with series in the shard,
		// init readers and postings list required to create a tsdb.blockChunkSeriesSet;
		// series sets from all blocks for the shard will be merged by mergeChunkSeriesSet.
		for _, blockSeries := range shardSeries {
			blk := blocks[blockSeries[0].blockIdx]
			// Init all readers for block & add to closers

			// Init separate index readers from above indexReaders to simplify closing logic
			indexr, err := blk.Index()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get index reader from block")
			}
			closers = append(closers, indexr)
			allClosers = append(allClosers, indexr)

			chunkr, err := blk.Chunks()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get chunk reader from block")
			}
			closers = append(closers, chunkr)
			allClosers = append(allClosers, chunkr)

			tombsr, err := blk.Tombstones()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get tombstone reader from block")
			}
			closers = append(closers, tombsr)
			allClosers = append(allClosers, tombsr)

			// Flatten series refs and add all label columns to schema for the shard
			refs := make([]storage.SeriesRef, 0, len(blockSeries))
			for _, series := range blockSeries {
				refs = append(refs, series.ref)
				series.labels.Range(func(l labels.Label) {
					schemaBuilder.AddLabelNameColumn(l.Name)
				})
			}
			postings := index.NewListPostings(refs)
			seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
			seriesSets = append(seriesSets, seriesSet)
		}

		mergeSeriesSet := NewMergeChunkSeriesSet(
			seriesSets, compareBySortedLabelsFunc(opts.sortedLabels), storage.NewConcatenatingChunkSeriesMerger(),
		)

		tsdbSchema, err := schemaBuilder.Build()
		if err != nil {
			return nil, fmt.Errorf("unable to build schema reader from block: %w", err)
		}
		shardTSDBRowReaders[shardIdx] = newTSDBRowReader(
			ctx, closers, mergeSeriesSet, tsdbSchema, opts,
		)
	}
	ok = true
	return shardTSDBRowReaders, nil
}

func shardSeries(
	blockIndexReaders []blockIndexReader,
	mint, maxt int64,
	opts convertOpts,
) (int, []map[int][]blockSeries, error) {
	chks := make([]chunks.Meta, 0, 128)
	allSeries := make([]blockSeries, 0, 128*len(blockIndexReaders))
	// Collect all series from all blocks with chunks in the time range
	for _, blockIndexReader := range blockIndexReaders {
		i := 0
		scratchBuilder := labels.NewScratchBuilder(10)

		for blockIndexReader.postings.Next() {
			scratchBuilder.Reset()
			chks = chks[:0]

			if err := blockIndexReader.reader.Series(blockIndexReader.postings.At(), &scratchBuilder, &chks); err != nil {
				return 0, nil, errors.Wrap(err, "unable to expand series")
			}

			hasChunks := slices.ContainsFunc(chks, func(chk chunks.Meta) bool {
				return mint <= chk.MaxTime && chk.MinTime <= maxt
			})
			if !hasChunks {
				continue
			}

			scratchBuilderLabels := scratchBuilder.Labels()
			allSeries = append(allSeries, blockSeries{
				blockIdx:  blockIndexReader.idx,
				seriesIdx: i,
				ref:       blockIndexReader.postings.At(),
				labels:    scratchBuilderLabels,
			})
		}
	}

	if len(allSeries) == 0 {
		return 0, nil, nil
	}

	slices.SortFunc(allSeries, compareBlockSeriesBySortedLabelsFunc(opts.sortedLabels))

	// Count how many unique series will exist after merging across blocks.
	uniqueSeriesCount := 1
	for i := 1; i < len(allSeries); i++ {
		if labels.Compare(allSeries[i].labels, allSeries[i-1].labels) != 0 {
			uniqueSeriesCount++
		}
	}

	// Divide rows evenly across shards to avoid one small shard at the end;
	// Use (a + b - 1) / b equivalence to math.Ceil(a / b)
	// so integer division does not cut off the remainder series and to avoid floating point issues.
	totalShards := (uniqueSeriesCount + (opts.numRowGroups * opts.rowGroupSize) - 1) / (opts.numRowGroups * opts.rowGroupSize)
	rowsPerShard := (uniqueSeriesCount + totalShards - 1) / totalShards

	// For each shard index i, shardSeries[i] is a map of blockIdx -> []series.
	shardSeries := make([]map[int][]blockSeries, totalShards)
	for i := range shardSeries {
		shardSeries[i] = make(map[int][]blockSeries)
	}

	shardIdx, allSeriesIdx := 0, 0
	for shardIdx < totalShards {
		seriesToShard := allSeries[allSeriesIdx:]

		i, uniqueCount := 0, 0
		matchLabels := labels.Labels{}
		for i < len(seriesToShard) {
			current := seriesToShard[i]
			if labels.Compare(current.labels, matchLabels) != 0 {
				// New unique series

				if uniqueCount >= rowsPerShard {
					// Stop before adding current series if it would exceed the unique series count for the shard.
					// Do not increment, we will start the next shard with this series.
					break
				}

				// Unique series limit is not hit yet for the shard; add the series.
				shardSeries[shardIdx][current.blockIdx] = append(shardSeries[shardIdx][current.blockIdx], current)
				// Increment unique count, update labels to compare against, and move on to next series
				uniqueCount++
				matchLabels = current.labels
				i++
			} else {
				// Same labelset as previous series, add it to the shard but do not increment unique count
				shardSeries[shardIdx][current.blockIdx] = append(shardSeries[shardIdx][current.blockIdx], current)
				// Move on to next series
				i++
			}
			allSeriesIdx++
		}
		shardIdx++
	}

	return uniqueSeriesCount, shardSeries, nil
}

func compareBlockSeriesBySortedLabelsFunc(sortedLabels []string) func(a, b blockSeries) int {
	return func(a, b blockSeries) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.labels.Get(lb), b.labels.Get(lb)); c != 0 {
				return c
			}
		}

		return labels.Compare(a.labels, b.labels)
	}
}

func compareBySortedLabelsFunc(sortedLabels []string) func(a, b labels.Labels) int {
	return func(a, b labels.Labels) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.Get(lb), b.Get(lb)); c != 0 {
				return c
			}
		}

		return labels.Compare(a, b)
	}
}

func allChunksEmpty(chkBytes [][]byte) bool {
	for _, chk := range chkBytes {
		if len(chk) != 0 {
			return false
		}
	}
	return true
}
