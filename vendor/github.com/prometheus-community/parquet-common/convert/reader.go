package convert

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/prometheus-community/parquet-common/schema"
)

var _ parquet.RowReader = &TSDBRowReader{}

type TSDBRowReader struct {
	ctx context.Context

	closers []io.Closer

	seriesSet storage.ChunkSeriesSet

	rowBuilder *parquet.RowBuilder
	tsdbSchema *schema.TSDBSchema

	encoder     *schema.PrometheusParquetChunksEncoder
	totalRead   int64
	concurrency int
}

func newTSDBRowReader(
	ctx context.Context,
	closers []io.Closer,
	seriesSet storage.ChunkSeriesSet,
	tsdbSchema *schema.TSDBSchema,
	opts convertOpts,
) *TSDBRowReader {
	return &TSDBRowReader{
		ctx:         ctx,
		seriesSet:   seriesSet,
		closers:     closers,
		tsdbSchema:  tsdbSchema,
		concurrency: opts.readConcurrency,

		rowBuilder: parquet.NewRowBuilder(tsdbSchema.Schema),
		encoder:    schema.NewPrometheusParquetChunksEncoder(tsdbSchema, opts.maxSamplesPerChunk),
	}
}

func (rr *TSDBRowReader) Close() error {
	err := &multierror.Error{}
	for i := range rr.closers {
		err = multierror.Append(err, rr.closers[i].Close())
	}
	return err.ErrorOrNil()
}

func (rr *TSDBRowReader) Schema() *schema.TSDBSchema {
	return rr.tsdbSchema
}

func (rr *TSDBRowReader) ReadRows(buf []parquet.Row) (int, error) {
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
	colIndex, ok := rr.tsdbSchema.Schema.Lookup(schema.ColIndexesColumn)
	if !ok {
		return 0, fmt.Errorf("unable to find indexes")
	}
	seriesHashIndex, ok := rr.tsdbSchema.Schema.Lookup(schema.SeriesHashColumn)
	if !ok {
		return 0, fmt.Errorf("unable to find series hash column")
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

		seriesLabels := promise.s.Labels()
		seriesLabels.Range(func(l labels.Label) {
			colName := schema.LabelToColumn(l.Name)
			lc, _ := rr.tsdbSchema.Schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
			lblsIdxs = append(lblsIdxs, lc.ColumnIndex)
		})

		rr.rowBuilder.Add(colIndex.ColumnIndex, parquet.ValueOf(schema.EncodeIntSlice(lblsIdxs)))

		// Compute and store the series hash as a byte slice in big-endian format
		seriesHashValue := labels.StableHash(seriesLabels)
		seriesHashBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seriesHashBytes, seriesHashValue)
		rr.rowBuilder.Add(seriesHashIndex.ColumnIndex, parquet.ValueOf(seriesHashBytes))

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
