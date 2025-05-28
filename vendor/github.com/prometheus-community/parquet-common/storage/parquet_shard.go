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
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
)

var DefaultShardOptions = shardOptions{
	optimisticReader: true,
}

type shardOptions struct {
	fileOptions      []parquet.FileOption
	optimisticReader bool
}

type ParquetFile struct {
	*parquet.File
	ReadAtWithContext
	BloomFiltersLoaded bool

	optimisticReader bool
}

type ShardOption func(*shardOptions)

func WithFileOptions(fileOptions ...parquet.FileOption) ShardOption {
	return func(opts *shardOptions) {
		opts.fileOptions = append(opts.fileOptions, fileOptions...)
	}
}

func WithOptimisticReader(optimisticReader bool) ShardOption {
	return func(opts *shardOptions) {
		opts.optimisticReader = optimisticReader
	}
}

func (f *ParquetFile) GetPages(ctx context.Context, cc parquet.ColumnChunk, pagesToRead ...int) (*parquet.FilePages, error) {
	colChunk := cc.(*parquet.FileColumnChunk)
	reader := f.WithContext(ctx)

	if len(pagesToRead) > 0 && f.optimisticReader {
		offset, err := cc.OffsetIndex()
		if err != nil {
			return nil, err
		}
		minOffset := offset.Offset(pagesToRead[0])
		maxOffset := offset.Offset(pagesToRead[len(pagesToRead)-1]) + offset.CompressedPageSize(pagesToRead[len(pagesToRead)-1])
		reader = newOptimisticReaderAt(reader, minOffset, maxOffset)
	}

	pages := colChunk.PagesFrom(reader)
	return pages, nil
}

func OpenFile(r ReadAtWithContext, size int64, opts ...ShardOption) (*ParquetFile, error) {
	cfg := DefaultShardOptions

	for _, opt := range opts {
		opt(&cfg)
	}

	c, err := parquet.NewFileConfig(cfg.fileOptions...)
	if err != nil {
		return nil, err
	}

	file, err := parquet.OpenFile(r, size, cfg.fileOptions...)
	if err != nil {
		return nil, err
	}

	return &ParquetFile{
		File:               file,
		ReadAtWithContext:  r,
		BloomFiltersLoaded: !c.SkipBloomFilters,
		optimisticReader:   cfg.optimisticReader,
	}, nil
}

type ParquetShard struct {
	labelsFile, chunksFile *ParquetFile
	schema                 *schema.TSDBSchema
	o                      sync.Once
}

// OpenParquetShard opens the sharded parquet block,
// using the options param.
func OpenParquetShard(ctx context.Context, bkt objstore.Bucket, name string, shard int, opts ...ShardOption) (*ParquetShard, error) {
	labelsFileName := schema.LabelsPfileNameForShard(name, shard)
	chunksFileName := schema.ChunksPfileNameForShard(name, shard)

	errGroup := errgroup.Group{}

	var labelsFile, chunksFile *ParquetFile

	errGroup.Go(func() error {
		labelsAttr, err := bkt.Attributes(ctx, labelsFileName)
		if err != nil {
			return err
		}
		labelsFile, err = OpenFile(NewBucketReadAt(ctx, labelsFileName, bkt), labelsAttr.Size, opts...)
		return err
	})

	errGroup.Go(func() error {
		chunksFileAttr, err := bkt.Attributes(ctx, chunksFileName)
		if err != nil {
			return err
		}
		chunksFile, err = OpenFile(NewBucketReadAt(ctx, chunksFileName, bkt), chunksFileAttr.Size, opts...)
		return err
	})

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return &ParquetShard{
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}, nil
}

func (b *ParquetShard) LabelsFile() *ParquetFile {
	return b.labelsFile
}

func (b *ParquetShard) ChunksFile() *ParquetFile {
	return b.chunksFile
}

func (b *ParquetShard) TSDBSchema() (*schema.TSDBSchema, error) {
	var err error
	b.o.Do(func() {
		b.schema, err = schema.FromLabelsFile(b.labelsFile.File)
	})
	return b.schema, err
}
