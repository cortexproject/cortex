// Copyright 2021 The Prometheus Authors
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

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

type ShardedWriter struct {
	name string

	rowGroupSize int
	numRowGroups int

	currentShard int

	rr  parquet.RowReader
	s   *schema.TSDBSchema
	bkt objstore.Bucket

	ops *convertOpts
}

func NewShardedWrite(rr parquet.RowReader, s *schema.TSDBSchema, bkt objstore.Bucket, ops *convertOpts) *ShardedWriter {
	return &ShardedWriter{
		name:         ops.name,
		rowGroupSize: ops.rowGroupSize,
		numRowGroups: ops.numRowGroups,
		currentShard: 0,
		rr:           rr,
		s:            s,
		bkt:          bkt,
		ops:          ops,
	}
}

func (c *ShardedWriter) Write(ctx context.Context) error {
	if err := c.convertShards(ctx); err != nil {
		return fmt.Errorf("unable to convert shards: %w", err)
	}
	return nil
}

func (c *ShardedWriter) convertShards(ctx context.Context) error {
	for {
		if ok, err := c.convertShard(ctx); err != nil {
			return fmt.Errorf("unable to convert shard: %s", err)
		} else if !ok {
			break
		}
	}
	return nil
}

func (c *ShardedWriter) convertShard(ctx context.Context) (bool, error) {
	rowsToWrite := c.numRowGroups * c.rowGroupSize

	n, err := c.writeFile(ctx, c.s, rowsToWrite)
	if err != nil {
		return false, err
	}

	c.currentShard++

	if n < int64(rowsToWrite) {
		return false, nil
	}

	return true, nil
}

func (c *ShardedWriter) writeFile(ctx context.Context, schema *schema.TSDBSchema, rowsToWrite int) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fileOpts := []parquet.WriterOption{
		parquet.SortingWriterConfig(
			parquet.SortingColumns(c.ops.buildSortingColumns()...),
		),
		parquet.MaxRowsPerRowGroup(int64(c.rowGroupSize)),
		parquet.BloomFilters(c.ops.buildBloomfilterColumns()...),
		parquet.PageBufferSize(c.ops.pageBufferSize),
		parquet.WriteBufferSize(c.ops.writeBufferSize),
	}

	for k, v := range schema.Metadata {
		fileOpts = append(fileOpts, parquet.KeyValueMetadata(k, v))
	}

	writer, err := newSplitFileWriter(ctx, c.bkt, schema.Schema, c.transformations(),
		fileOpts...,
	)
	if err != nil {
		return 0, fmt.Errorf("unable to create row writer: %s", err)
	}

	n, err := parquet.CopyRows(writer, newLimitReader(c.rr, rowsToWrite))
	if err != nil {
		return 0, fmt.Errorf("unable to copy rows: %s", err)
	}

	err = writer.Close()
	if err != nil {
		return 0, fmt.Errorf("unable to close writer: %s", err)
	}

	return n, nil
}

func (c *ShardedWriter) transformations() map[string]*parquet.Schema {
	return map[string]*parquet.Schema{
		schema.LabelsPfileNameForShard(c.name, c.currentShard): schema.WithCompression(c.s.LabelsProjection()),
		schema.ChunksPfileNameForShard(c.name, c.currentShard): schema.WithCompression(c.s.ChunksProjection()),
	}
}

var _ parquet.RowWriter = &splitPipeFileWriter{}

type fileWriter struct {
	pw   *parquet.GenericWriter[any]
	conv parquet.Conversion
	w    io.WriteCloser
	r    io.ReadCloser
}

type splitPipeFileWriter struct {
	fileWriters map[string]*fileWriter
	errGroup    *errgroup.Group
}

func newSplitFileWriter(ctx context.Context, bkt objstore.Bucket, inSchema *parquet.Schema,
	files map[string]*parquet.Schema, options ...parquet.WriterOption,
) (*splitPipeFileWriter, error) {
	fileWriters := make(map[string]*fileWriter)
	errGroup, ctx := errgroup.WithContext(ctx)
	for file, outSchema := range files {
		conv, err := parquet.Convert(outSchema, inSchema)
		if err != nil {
			return nil, fmt.Errorf("unable to convert schemas")
		}

		r, w := io.Pipe()
		fileWriters[file] = &fileWriter{
			pw:   parquet.NewGenericWriter[any](w, append(options, outSchema)...),
			w:    w,
			r:    r,
			conv: conv,
		}
		errGroup.Go(func() error {
			defer func() { _ = r.Close() }()
			return bkt.Upload(ctx, file, r)
		})
	}
	return &splitPipeFileWriter{
		fileWriters: fileWriters,
		errGroup:    errGroup,
	}, nil
}

func (s *splitPipeFileWriter) WriteRows(rows []parquet.Row) (int, error) {
	for _, writer := range s.fileWriters {
		convertedRows := util.CloneRows(rows)
		_, err := writer.conv.Convert(convertedRows)
		if err != nil {
			return 0, fmt.Errorf("unable to convert rows: %d", err)
		}
		n, err := writer.pw.WriteRows(convertedRows)
		if err != nil {
			return 0, fmt.Errorf("unable to write rows: %d", err)
		}
		if n != len(rows) {
			return 0, fmt.Errorf("unable to write rows: %d != %d", n, len(rows))
		}
	}

	return len(rows), nil
}

func (s *splitPipeFileWriter) Close() error {
	var err error
	for _, fw := range s.fileWriters {
		if errClose := fw.pw.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
		if errClose := fw.w.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}

	if errClose := s.errGroup.Wait(); errClose != nil {
		err = multierror.Append(err, errClose)
	}
	return err
}

type limitReader struct {
	parquet.RowReader
	remaining int
}

func newLimitReader(r parquet.RowReader, limit int) parquet.RowReader {
	return &limitReader{RowReader: r, remaining: limit}
}

func (lr *limitReader) ReadRows(buf []parquet.Row) (int, error) {
	if len(buf) > lr.remaining {
		buf = buf[:lr.remaining]
	}
	n, err := lr.RowReader.ReadRows(buf)
	if err != nil {
		return n, err
	}
	lr.remaining -= n

	if lr.remaining <= 0 {
		return n, io.EOF
	}
	return n, nil
}
