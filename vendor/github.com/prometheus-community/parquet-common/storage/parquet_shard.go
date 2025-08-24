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

package storage

import (
	"context"
	"os"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
)

type ParquetFileConfigView interface {
	SkipMagicBytes() bool
	SkipPageIndex() bool
	SkipBloomFilters() bool
	OptimisticRead() bool
	ReadBufferSize() int
	ReadMode() parquet.ReadMode

	// Extended options beyond parquet.FileConfig

	PagePartitioningMaxGapSize() int
}

var DefaultFileOptions = ExtendedFileConfig{
	FileConfig: &parquet.FileConfig{
		SkipPageIndex:    parquet.DefaultSkipPageIndex,
		ReadMode:         parquet.DefaultReadMode,
		SkipMagicBytes:   true,
		SkipBloomFilters: true,
		ReadBufferSize:   4096,
		OptimisticRead:   true,
	},
	pagePartitioningMaxGapSize: 10 * 1024,
}

type ExtendedFileConfig struct {
	*parquet.FileConfig
	pagePartitioningMaxGapSize int
}

func (c ExtendedFileConfig) SkipMagicBytes() bool {
	return c.FileConfig.SkipMagicBytes
}

func (c ExtendedFileConfig) SkipPageIndex() bool {
	return c.FileConfig.SkipPageIndex
}

func (c ExtendedFileConfig) SkipBloomFilters() bool {
	return c.FileConfig.SkipBloomFilters
}

func (c ExtendedFileConfig) OptimisticRead() bool {
	return c.FileConfig.OptimisticRead
}

func (c ExtendedFileConfig) ReadBufferSize() int {
	return c.FileConfig.ReadBufferSize
}

func (c ExtendedFileConfig) ReadMode() parquet.ReadMode {
	return c.FileConfig.ReadMode
}

func (c ExtendedFileConfig) PagePartitioningMaxGapSize() int {
	return c.pagePartitioningMaxGapSize
}

type ParquetFileView interface {
	parquet.FileView
	GetPages(ctx context.Context, cc parquet.ColumnChunk, minOffset, maxOffset int64) (parquet.Pages, error)
	DictionaryPageBounds(rgIdx, colIdx int) (uint64, uint64)

	ReadAtWithContextCloser

	ParquetFileConfigView
}

type ParquetFile struct {
	*parquet.File
	ReadAtWithContextCloser
	ParquetFileConfigView
}

type FileOption func(*ExtendedFileConfig)

func WithFileOptions(options ...parquet.FileOption) FileOption {
	config := parquet.DefaultFileConfig()
	config.Apply(options...)
	return func(opts *ExtendedFileConfig) {
		opts.FileConfig = config
	}
}

// WithPageMaxGapSize set the max gap size between pages that should be downloaded together in a single read call
func WithPageMaxGapSize(pagePartitioningMaxGapSize int) FileOption {
	return func(opts *ExtendedFileConfig) {
		opts.pagePartitioningMaxGapSize = pagePartitioningMaxGapSize
	}
}

func (f *ParquetFile) GetPages(ctx context.Context, cc parquet.ColumnChunk, minOffset, maxOffset int64) (parquet.Pages, error) {
	colChunk := cc.(*parquet.FileColumnChunk)
	reader := f.WithContext(ctx)

	if f.OptimisticRead() {
		reader = NewOptimisticReaderAt(reader, minOffset, maxOffset)
	}

	pages := colChunk.PagesFrom(reader)
	return pages, nil
}

func (f *ParquetFile) DictionaryPageBounds(rgIdx, colIdx int) (uint64, uint64) {
	colMeta := f.Metadata().RowGroups[rgIdx].Columns[colIdx].MetaData

	return uint64(colMeta.DictionaryPageOffset), uint64(colMeta.DataPageOffset - colMeta.DictionaryPageOffset)
}

func Open(ctx context.Context, r ReadAtWithContextCloser, size int64, opts ...FileOption) (*ParquetFile, error) {
	cfg := DefaultFileOptions

	for _, opt := range opts {
		opt(&cfg)
	}

	file, err := parquet.OpenFile(r.WithContext(ctx), size, cfg.FileConfig)
	if err != nil {
		return nil, err
	}

	return &ParquetFile{
		File:                    file,
		ReadAtWithContextCloser: r,
		ParquetFileConfigView:   cfg,
	}, nil
}

func OpenFromBucket(ctx context.Context, bkt objstore.BucketReader, name string, opts ...FileOption) (*ParquetFile, error) {
	attr, err := bkt.Attributes(ctx, name)
	if err != nil {
		return nil, err
	}

	r := NewBucketReadAt(name, bkt)
	return Open(ctx, r, attr.Size, opts...)
}

func OpenFromFile(ctx context.Context, path string, opts ...FileOption) (*ParquetFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r := NewFileReadAt(f)
	pf, err := Open(ctx, r, stat.Size(), opts...)
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	// At this point, the file's lifecycle is managed by the ParquetFile
	return pf, nil
}

type ParquetShard interface {
	LabelsFile() ParquetFileView
	ChunksFile() ParquetFileView
	TSDBSchema() (*schema.TSDBSchema, error)
}

type ParquetOpener interface {
	Open(ctx context.Context, path string, opts ...FileOption) (*ParquetFile, error)
}

type ParquetBucketOpener struct {
	bkt objstore.BucketReader
}

func NewParquetBucketOpener(bkt objstore.BucketReader) *ParquetBucketOpener {
	return &ParquetBucketOpener{
		bkt: bkt,
	}
}

func (o *ParquetBucketOpener) Open(ctx context.Context, name string, opts ...FileOption) (*ParquetFile, error) {
	return OpenFromBucket(ctx, o.bkt, name, opts...)
}

type ParquetLocalFileOpener struct{}

func NewParquetLocalFileOpener() *ParquetLocalFileOpener {
	return &ParquetLocalFileOpener{}
}

func (o *ParquetLocalFileOpener) Open(ctx context.Context, name string, opts ...FileOption) (*ParquetFile, error) {
	return OpenFromFile(ctx, name, opts...)
}

type ParquetShardOpener struct {
	labelsFile, chunksFile *ParquetFile
	schema                 *schema.TSDBSchema
	o                      sync.Once
}

func NewParquetShardOpener(
	ctx context.Context,
	name string,
	labelsFileOpener ParquetOpener,
	chunksFileOpener ParquetOpener,
	shard int,
	opts ...FileOption,
) (*ParquetShardOpener, error) {
	cfg := DefaultFileOptions

	for _, opt := range opts {
		opt(&cfg)
	}

	labelsFileName := schema.LabelsPfileNameForShard(name, shard)
	chunksFileName := schema.ChunksPfileNameForShard(name, shard)

	errGroup := errgroup.Group{}

	var labelsFile, chunksFile *ParquetFile

	errGroup.Go(func() (err error) {
		labelsFile, err = labelsFileOpener.Open(ctx, labelsFileName, opts...)
		return err
	})

	errGroup.Go(func() (err error) {
		chunksFile, err = chunksFileOpener.Open(ctx, chunksFileName, opts...)
		return err
	})

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return &ParquetShardOpener{
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}, nil
}

func (s *ParquetShardOpener) LabelsFile() ParquetFileView {
	return s.labelsFile
}

func (s *ParquetShardOpener) ChunksFile() ParquetFileView {
	return s.chunksFile
}

func (s *ParquetShardOpener) TSDBSchema() (*schema.TSDBSchema, error) {
	var err error
	s.o.Do(func() {
		s.schema, err = schema.FromLabelsFile(s.labelsFile.File)
	})
	return s.schema, err
}

func (s *ParquetShardOpener) Close() error {
	err := &multierror.Error{}
	err = multierror.Append(err, s.labelsFile.Close())
	err = multierror.Append(err, s.chunksFile.Close())
	return err.ErrorOrNil()
}
