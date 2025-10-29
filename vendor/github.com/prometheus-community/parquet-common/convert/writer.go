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
	"os"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/util/zeropool"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
)

type PreShardedWriter struct {
	shard int

	rr                   parquet.RowReader
	schema               *schema.TSDBSchema
	outSchemaProjections []*schema.TSDBProjection
	pipeReaderWriter     PipeReaderWriter

	opts *convertOpts

	logger *slog.Logger
}

func (c *PreShardedWriter) Write(ctx context.Context) error {
	c.logger.Info("starting conversion for shard", "shard", c.shard)
	if err := c.convertShard(ctx); err != nil {
		return errors.Wrap(err, "failed to convert shard")
	}
	c.logger.Info("finished conversion for shard", "shard", c.shard)
	return nil
}

func (c *PreShardedWriter) convertShard(ctx context.Context) error {
	outSchemas := outSchemasForShard(c.opts.name, c.shard, c.outSchemaProjections)
	_, err := writeFile(ctx, c.schema, outSchemas, c.rr, c.pipeReaderWriter, c.opts)
	return err
}

func writeFile(
	ctx context.Context,
	inSchema *schema.TSDBSchema,
	outSchemas map[string]*schema.TSDBProjection,
	rr parquet.RowReader,
	pipeReaderWriter PipeReaderWriter,
	opts *convertOpts,
) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fileOpts := []parquet.WriterOption{
		parquet.SortingWriterConfig(
			parquet.SortingColumns(opts.buildSortingColumns()...),
		),
		parquet.MaxRowsPerRowGroup(int64(opts.rowGroupSize)),
		parquet.BloomFilters(opts.buildBloomfilterColumns()...),
		parquet.PageBufferSize(opts.pageBufferSize),
		parquet.WriteBufferSize(opts.writeBufferSize),
		parquet.ColumnPageBuffers(opts.columnPageBuffers),
	}

	for k, v := range inSchema.Metadata {
		fileOpts = append(fileOpts, parquet.KeyValueMetadata(k, v))
	}

	writer, err := newSplitFileWriter(
		ctx, inSchema.Schema, outSchemas, pipeReaderWriter, fileOpts...,
	)
	if err != nil {
		return 0, fmt.Errorf("unable to create row writer: %w", err)
	}

	n, err := parquet.CopyRows(writer, newBufferedReader(ctx, newLimitReader(rr, opts.numRowGroups*opts.rowGroupSize)))
	if err != nil {
		return 0, fmt.Errorf("unable to copy rows: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return 0, fmt.Errorf("unable to close writer: %w", err)
	}

	return n, nil
}

func outSchemasForShard(name string, shard int, outSchemaProjections []*schema.TSDBProjection) map[string]*schema.TSDBProjection {
	outSchemas := make(map[string]*schema.TSDBProjection, len(outSchemaProjections))

	for _, projection := range outSchemaProjections {
		outSchemas[projection.FilenameFunc(name, shard)] = projection
	}
	return outSchemas
}

// PipeReaderWriter is used to write serialized data from an io.Reader to the final output destination.
type PipeReaderWriter interface {
	// Write writes data to the output path and returns any error encountered during the write.
	Write(ctx context.Context, r io.Reader, outPath string) error
}

type PipeReaderBucketWriter struct {
	bkt objstore.Bucket
}

func NewPipeReaderBucketWriter(bkt objstore.Bucket) *PipeReaderBucketWriter {
	return &PipeReaderBucketWriter{
		bkt: bkt,
	}
}

func (w *PipeReaderBucketWriter) Write(ctx context.Context, r io.Reader, outPath string) error {
	return w.bkt.Upload(ctx, outPath, r)
}

type PipeReaderFileWriter struct {
	outDir string
}

func NewPipeReaderFileWriter(outDir string) *PipeReaderFileWriter {
	return &PipeReaderFileWriter{
		outDir: outDir,
	}
}

func (w *PipeReaderFileWriter) Write(_ context.Context, r io.Reader, outPath string) error {
	// outPath may include parent path segments in addition to the filename;
	// join with w.outDir to get the full path for creating any necessary parent directories.
	outPath = filepath.Join(w.outDir, outPath)
	outPathDir := filepath.Dir(outPath)
	err := os.MkdirAll(outPathDir, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "error creating directory for writing")
	}

	fileWriterCloser, err := os.Create(outPath)
	if err != nil {
		return errors.Wrap(err, "error opening file for writing")
	}
	defer func() { _ = fileWriterCloser.Close() }()

	_, err = io.Copy(fileWriterCloser, r)
	if err != nil {
		return errors.Wrap(err, "error copying from reader to file writer")
	}
	return nil
}

var _ parquet.RowWriter = &splitPipeFileWriter{}

type fileWriter struct {
	pw   *parquet.GenericWriter[any]
	conv parquet.Conversion
	w    io.WriteCloser
	r    io.ReadCloser
}

// splitPipeFileWriter creates a paired io.Reader and io.Writer from an io.Pipe for each output file.
// The writer receives the serialized data from parquet.GenericWriter and forwards through the pipe
// to the reader, which can be read from to write the data to any destination.
type splitPipeFileWriter struct {
	fileWriters map[string]*fileWriter
	errGroup    *errgroup.Group
}

func newSplitFileWriter(
	ctx context.Context,
	inSchema *parquet.Schema,
	outSchemas map[string]*schema.TSDBProjection,
	pipeReaderWriter PipeReaderWriter,
	options ...parquet.WriterOption,
) (*splitPipeFileWriter, error) {
	fileWriters := make(map[string]*fileWriter)
	errGroup, ctx := errgroup.WithContext(ctx)
	for file, projection := range outSchemas {
		conv, err := parquet.Convert(projection.Schema, inSchema)
		if err != nil {
			return nil, fmt.Errorf("unable to convert schemas")
		}

		r, w := io.Pipe()
		opts := append(options, append(projection.ExtraOptions, projection.Schema)...)
		fileWriters[file] = &fileWriter{
			pw:   parquet.NewGenericWriter[any](w, opts...),
			w:    w,
			r:    r,
			conv: conv,
		}
		errGroup.Go(func() error {
			defer func() { _ = r.Close() }()
			return pipeReaderWriter.Write(ctx, r, file)
		})
	}
	return &splitPipeFileWriter{
		fileWriters: fileWriters,
		errGroup:    errGroup,
	}, nil
}

func (s *splitPipeFileWriter) WriteRows(rows []parquet.Row) (int, error) {
	errGroup := &errgroup.Group{}
	for _, writer := range s.fileWriters {
		errGroup.Go(func() error {
			convertedRows := util.CloneRows(rows)
			_, err := writer.conv.Convert(convertedRows)
			if err != nil {
				return fmt.Errorf("unable to convert rows: %w", err)
			}
			n, err := writer.pw.WriteRows(convertedRows)
			if err != nil {
				return fmt.Errorf("unable to write rows: %w", err)
			}
			if n != len(rows) {
				return fmt.Errorf("unable to write rows: %d != %d", n, len(rows))
			}
			return nil
		})
	}

	return len(rows), errGroup.Wait()
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

var _ parquet.RowReader = &bufferedReader{}

type bufferedReader struct {
	rr parquet.RowReader

	ctx     context.Context
	c       chan []parquet.Row
	errCh   chan error
	rowPool zeropool.Pool[[]parquet.Row]

	current      []parquet.Row
	currentIndex int
}

func newBufferedReader(ctx context.Context, rr parquet.RowReader) *bufferedReader {
	br := &bufferedReader{
		rr:    rr,
		ctx:   ctx,
		c:     make(chan []parquet.Row, 128),
		errCh: make(chan error, 1),
		rowPool: zeropool.New[[]parquet.Row](func() []parquet.Row {
			return make([]parquet.Row, 128)
		}),
	}

	go br.readRows()

	return br
}

func (b *bufferedReader) ReadRows(rows []parquet.Row) (int, error) {
	if b.current == nil {
		select {
		case next, ok := <-b.c:
			if !ok {
				return 0, io.EOF
			}
			b.current = next
			b.currentIndex = 0
		case err := <-b.errCh:
			return 0, err
		}
	}

	current := b.current[b.currentIndex:]
	numRows := min(len(current), len(rows))

	for i := 0; i < numRows; i++ {
		// deep copy slice contents to avoid data race;
		// current may return to the pool while rows is still being read by the caller
		rows[i] = current[i].Clone()
	}

	b.currentIndex += numRows
	if b.currentIndex >= len(b.current) {
		// already read all rows in current; return it to the pool
		b.rowPool.Put(b.current[0:cap(b.current)])
		b.current = nil
	}
	return numRows, nil
}

func (b *bufferedReader) Close() {
	close(b.c)
	close(b.errCh)
}

func (b *bufferedReader) readRows() {
	for {
		select {
		case <-b.ctx.Done():
			b.errCh <- b.ctx.Err()
			return
		default:
			rows := b.rowPool.Get()
			n, err := b.rr.ReadRows(rows)
			if n > 0 {
				b.c <- rows[:n]
			}
			if err != nil {
				if err == io.EOF {
					close(b.c)
					return
				}
				b.errCh <- err
				return
			}
		}
	}
}
