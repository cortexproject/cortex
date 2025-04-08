package storage

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	ChunkColumnLength  = 8 * time.Hour
	ChunkColumnsPerDay = 3
)

var (
	errRowOutOrder = errors.New("row out of order")
)

type ParquetWriter struct {
	w  *parquet.GenericWriter[any]
	rb *parquet.RowBuilder

	sortedColumn string
	// keep the last value for each sorted column to make sure they are inserted in order
	sortedColumnLastValue string
	columnIndexMap        map[string]int
}

func NewParquetWriter(w io.Writer, rowsPerRowGroup int64, indexSizeLimit, numDataCols int, columns []string, sortedColumn string) *ParquetWriter {
	schemaGroup := parquet.Group{}
	skipPageBounds := []string{ColHash, ColIndexes}
	// No dictionary here as it this has no repetition, and we would need to download the whole dic
	// when materializing this column to get the final result
	schemaGroup[ColHash] = parquet.Compressed(parquet.Int(64), &parquet.Zstd)
	schemaGroup[ColIndexes] = parquet.Compressed(parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaByteArray), &parquet.Zstd)
	for i := 0; i < numDataCols; i++ {
		c := fmt.Sprintf("%v_%v", ColData, i)
		skipPageBounds = append(skipPageBounds, c)
		schemaGroup[c] = parquet.Compressed(parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaByteArray), &parquet.Zstd)
	}
	skipPageBounds = append(skipPageBounds)

	for _, n := range columns {
		if _, ok := schemaGroup[n]; !ok {
			schemaGroup[n] = parquet.Compressed(parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary)), &parquet.Zstd)
		}
	}

	schema := parquet.NewSchema("", schemaGroup)

	columnIndexMap := make(map[string]int)

	for i, c := range schema.Columns() {
		columnIndexMap[c[0]] = i
	}

	return &ParquetWriter{
		columnIndexMap: columnIndexMap,
		sortedColumn:   sortedColumn,
		w: parquet.NewGenericWriter[any](
			w,
			schema,
			parquet.MaxRowsPerRowGroup(rowsPerRowGroup),
			parquet.ColumnIndexSizeLimit(indexSizeLimit),
			parquet.SkipPageBounds(skipPageBounds...),
			parquet.KeyValueMetadata(SortedColMetadataKey, sortedColumn),
			parquet.KeyValueMetadata(NumberOfDataColumnsMetadataKey, strconv.Itoa(numDataCols)),
			parquet.KeyValueMetadata(IndexSizeLimitMetadataKey, strconv.Itoa(indexSizeLimit)),
		),
		rb: parquet.NewRowBuilder(schema),
	}
}

func (p *ParquetWriter) WriteRows(series []ParquetRow) error {
	rows := make([]parquet.Row, 0, len(series))
	colIdxs := make([]uint64, 0, 1024)

	for _, s := range series {
		p.rb.Reset()
		colIdxs = colIdxs[:0]

		p.rb.Add(p.columnIndexMap[ColHash], parquet.ValueOf(s.Hash))
		for k, v := range s.Columns {

			if p.sortedColumn == k {
				if v < p.sortedColumnLastValue {
					return errRowOutOrder
				}
				p.sortedColumnLastValue = v
			}

			cIdx, ok := p.columnIndexMap[k]
			if !ok {
				return fmt.Errorf("column not found: %s", k)
			}
			p.rb.Add(cIdx, parquet.ValueOf(v))
			colIdxs = append(colIdxs, uint64(p.columnIndexMap[k]))
		}

		slices.Sort(colIdxs)

		p.rb.Add(p.columnIndexMap[ColIndexes], parquet.ValueOf(EncodeUintSlice(colIdxs)))
		for i, data := range s.Data {
			c := fmt.Sprintf("%v_%v", ColData, i)
			p.rb.Add(p.columnIndexMap[c], parquet.ValueOf(data))
		}
		rows = append(rows, p.rb.Row())
	}

	_, err := p.w.WriteRows(rows)

	return err
}

func (p *ParquetWriter) Close() error {
	return p.w.Close()
}
