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
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

var tracer = otel.Tracer("parquet-common")

type Materializer struct {
	b           storage.ParquetShard
	s           *schema.TSDBSchema
	d           *schema.PrometheusParquetChunksDecoder
	partitioner util.Partitioner

	colIdx      int
	concurrency int

	dataColToIndex []int

	rowCountQuota   *Quota
	chunkBytesQuota *Quota
	dataBytesQuota  *Quota

	materializedSeriesCallback       MaterializedSeriesFunc
	materializedLabelsFilterCallback MaterializedLabelsFilterCallback
}

// MaterializedSeriesFunc is a callback function that can be used to add limiter or statistic logics for
// materialized series.
type MaterializedSeriesFunc func(ctx context.Context, series prom_storage.ChunkSeries) error

// NoopMaterializedSeriesFunc is a noop callback function that does nothing.
func NoopMaterializedSeriesFunc(_ context.Context, _ prom_storage.ChunkSeries) error {
	return nil
}

// MaterializedLabelsFilterCallback returns a filter and a boolean indicating if the filter is enabled or not.
// The filter is used to filter series based on their labels.
// The boolean if set to false then it means that the filter is a noop and we can take shortcut to include all series.
// Otherwise, the filter is used to filter series based on their labels.
type MaterializedLabelsFilterCallback func(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool)

// MaterializedLabelsFilter is a filter that can be used to filter series based on their labels.
type MaterializedLabelsFilter interface {
	// Filter returns true if the labels should be included in the result.
	Filter(ls labels.Labels) bool
	// Close is used to close the filter and do some cleanup.
	Close()
}

// NoopMaterializedLabelsFilterCallback is a noop MaterializedLabelsFilterCallback function that filters nothing.
func NoopMaterializedLabelsFilterCallback(ctx context.Context, hints *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) {
	return nil, false
}

func NewMaterializer(s *schema.TSDBSchema,
	d *schema.PrometheusParquetChunksDecoder,
	block storage.ParquetShard,
	concurrency int,
	rowCountQuota *Quota,
	chunkBytesQuota *Quota,
	dataBytesQuota *Quota,
	materializeSeriesCallback MaterializedSeriesFunc,
	materializeLabelsFilterCallback MaterializedLabelsFilterCallback,
) (*Materializer, error) {
	colIdx, ok := block.LabelsFile().Schema().Lookup(schema.ColIndexesColumn)
	if !ok {
		return nil, errors.New(fmt.Sprintf("schema index %s not found", schema.ColIndexesColumn))
	}

	dataColToIndex := make([]int, len(block.ChunksFile().Schema().Columns()))
	for i := 0; i < len(s.DataColsIndexes); i++ {
		c, ok := block.ChunksFile().Schema().Lookup(schema.DataColumn(i))
		if !ok {
			return nil, errors.New(fmt.Sprintf("schema column %s not found", schema.DataColumn(i)))
		}

		dataColToIndex[i] = c.ColumnIndex
	}

	return &Materializer{
		s:                                s,
		d:                                d,
		b:                                block,
		colIdx:                           colIdx.ColumnIndex,
		concurrency:                      concurrency,
		partitioner:                      util.NewGapBasedPartitioner(block.ChunksFile().PagePartitioningMaxGapSize()),
		dataColToIndex:                   dataColToIndex,
		rowCountQuota:                    rowCountQuota,
		chunkBytesQuota:                  chunkBytesQuota,
		dataBytesQuota:                   dataBytesQuota,
		materializedSeriesCallback:       materializeSeriesCallback,
		materializedLabelsFilterCallback: materializeLabelsFilterCallback,
	}, nil
}

// Materialize creates an iterator to reconstruct the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
func (m *Materializer) Materialize(ctx context.Context, hints *prom_storage.SelectHints, rgi int, mint, maxt int64, skipChunks bool, rr []RowRange) (ChunkSeriesSetCloser, error) {
	var err error
	ctx, span := tracer.Start(ctx, "Materializer.Materialize")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	span.SetAttributes(
		attribute.Int("row_group_index", rgi),
		attribute.Int64("mint", mint),
		attribute.Int64("maxt", maxt),
		attribute.Bool("skip_chunks", skipChunks),
		attribute.Int("row_ranges_count", len(rr)),
	)

	if err := m.checkRowCountQuota(rr); err != nil {
		span.SetAttributes(attribute.String("quota_failure", "row_count"))
		return nil, err
	}
	sLbls, err := m.MaterializeAllLabels(ctx, rgi, rr)
	if err != nil {
		return nil, errors.Wrapf(err, "error materializing labels")
	}

	seriesSetLabels, rr := m.FilterSeriesLabels(ctx, hints, sLbls, rr)

	if skipChunks {
		return NewNoChunksConcreteLabelsSeriesSet(seriesSetLabels), nil
	}

	chksIter, err := m.MaterializeChunks(ctx, rgi, mint, maxt, rr)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to create chunks iterator")
	}

	seriesSetIter := NewFilterEmptyChunkSeriesSet(ctx, seriesSetLabels, chksIter, m.materializedSeriesCallback)

	span.SetAttributes(attribute.Int("materialized_series_count", len(seriesSetLabels)))
	return seriesSetIter, nil
}

func (m *Materializer) FilterSeriesLabels(ctx context.Context, hints *prom_storage.SelectHints, sLbls [][]labels.Label, rr []RowRange) ([]labels.Labels, []RowRange) {
	seriesLabels := make([]labels.Labels, 0, len(sLbls))
	labelsFilter, ok := m.materializedLabelsFilterCallback(ctx, hints)
	if !ok {
		for _, s := range sLbls {
			seriesLabels = append(seriesLabels, labels.New(s...))
		}
		return seriesLabels, rr
	}

	defer labelsFilter.Close()

	filteredRR := make([]RowRange, 0, len(rr))
	var currentRange RowRange
	inRange := false
	seriesIdx := 0

	for _, rowRange := range rr {
		for i := int64(0); i < rowRange.Count; i++ {
			actualRowID := rowRange.From + i
			lbls := labels.New(sLbls[seriesIdx]...)

			if labelsFilter.Filter(lbls) {
				seriesLabels = append(seriesLabels, lbls)

				// Handle row range collection
				if !inRange {
					// Start new range
					currentRange = RowRange{
						From:  actualRowID,
						Count: 1,
					}
					inRange = true
				} else if actualRowID == currentRange.From+currentRange.Count {
					// Extend current range
					currentRange.Count++
				} else {
					// Save current range and start new range (non-contiguous)
					filteredRR = append(filteredRR, currentRange)
					currentRange = RowRange{
						From:  actualRowID,
						Count: 1,
					}
				}
			} else {
				// Save current range and reset when we hit a non-matching series
				if inRange {
					filteredRR = append(filteredRR, currentRange)
					inRange = false
				}
			}
			seriesIdx++
		}
	}

	// Save the final range if we have one
	if inRange {
		filteredRR = append(filteredRR, currentRange)
	}

	return seriesLabels, filteredRR
}

// MaterializeAllLabelNames extracts and returns all label names from the schema
// of the labels file. It iterates through all columns in the schema and extracts
// valid label names, filtering out any columns that are not label columns.
//
// This method is useful for discovering all possible label names that exist in
// the parquet file without needing to read any actual data rows.
//
// Returns a slice of all label names found in the schema.
func (m *Materializer) MaterializeAllLabelNames() []string {
	r := make([]string, 0, len(m.b.LabelsFile().Schema().Columns()))
	for _, c := range m.b.LabelsFile().Schema().Columns() {
		lbl, ok := schema.ExtractLabelFromColumn(c[0])
		if !ok {
			continue
		}

		r = append(r, lbl)
	}
	return r
}

// MaterializeLabelNames extracts and returns all unique label names from the specified row ranges
// within a given row group. It reads the column indexes from the labels file and decodes them
// to determine which label columns are present in the data.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - rgi: Row group index to read from
//   - rr: Row ranges to process within the row group
//
// Returns a slice of label names found in the specified ranges, or an error if materialization fails.
func (m *Materializer) MaterializeLabelNames(ctx context.Context, rgi int, rr []RowRange) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cc := labelsRg.ColumnChunks()[m.colIdx]
	colsIdxs, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, cc, false)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	seen := make(map[string]struct{})
	colsMap := make(map[string]struct{}, 10)
	for _, colsIdx := range colsIdxs {
		key := util.YoloString(colsIdx.ByteArray())
		if _, ok := seen[key]; !ok {
			idxs, err := schema.DecodeUintSlice(colsIdx.ByteArray())
			if err != nil {
				return nil, errors.Wrap(err, "failed to decode column index")
			}
			for _, idx := range idxs {
				if _, ok := colsMap[m.b.LabelsFile().Schema().Columns()[idx][0]]; !ok {
					colsMap[m.b.LabelsFile().Schema().Columns()[idx][0]] = struct{}{}
				}
			}
		}
	}
	lbls := make([]string, 0, len(colsMap))
	for col := range colsMap {
		l, ok := schema.ExtractLabelFromColumn(col)
		if !ok {
			return nil, errors.New(fmt.Sprintf("error extracting label name from col %v", col))
		}
		lbls = append(lbls, l)
	}
	return lbls, nil
}

// MaterializeLabelValues extracts and returns all unique values for a specific label name
// from the specified row ranges within a given row group. It reads the label column data
// and deduplicates the values to provide a unique set of label values.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - name: The label name to extract values for
//   - rgi: Row group index to read from
//   - rr: Row ranges to process within the row group
//
// Returns a slice of unique label values for the specified label name, or an error if
// materialization fails. If the label name doesn't exist in the schema, returns an empty slice.
func (m *Materializer) MaterializeLabelValues(ctx context.Context, name string, rgi int, rr []RowRange) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cIdx, ok := m.b.LabelsFile().Schema().Lookup(schema.LabelToColumn(name))
	if !ok {
		return []string{}, nil
	}
	cc := labelsRg.ColumnChunks()[cIdx.ColumnIndex]
	values, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, cc, false)
	if err != nil {
		return nil, errors.Wrap(err, "materializer failed to materialize columns")
	}

	r := make([]string, 0, len(values))
	vMap := make(map[string]struct{}, 10)
	for _, v := range values {
		strValue := util.YoloString(v.ByteArray())
		if _, ok := vMap[strValue]; !ok {
			r = append(r, strValue)
			vMap[strValue] = struct{}{}
		}
	}
	return r, nil
}

// MaterializeAllLabelValues extracts all unique values for a specific label name
// from the entire row group using the dictionary page optimization. This method
// is more efficient than MaterializeLabelValues when you need all values for a
// label across the entire row group, as it reads directly from the dictionary
// page instead of scanning individual data pages.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - name: The label name to extract all values for
//   - rgi: Row group index to read from
//
// Returns a slice of all unique label values for the specified label name from
// the dictionary, or an error if materialization fails. If the label name doesn't
// exist in the schema, returns an empty slice.
func (m *Materializer) MaterializeAllLabelValues(ctx context.Context, name string, rgi int) ([]string, error) {
	labelsRg := m.b.LabelsFile().RowGroups()[rgi]
	cIdx, ok := m.b.LabelsFile().Schema().Lookup(schema.LabelToColumn(name))
	if !ok {
		return []string{}, nil
	}
	cc := labelsRg.ColumnChunks()[cIdx.ColumnIndex]
	pages, err := m.b.LabelsFile().GetPages(ctx, cc, 0, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pages")
	}
	p, err := pages.ReadPage()
	if err != nil {
		return []string{}, errors.Wrap(err, "failed to read page")
	}
	defer parquet.Release(p)

	r := make([]string, 0, p.Dictionary().Len())
	for i := 0; i < p.Dictionary().Len(); i++ {
		r = append(r, p.Dictionary().Index(int32(i)).String())
	}
	return r, nil
}

func (m *Materializer) MaterializeAllLabels(ctx context.Context, rgi int, rr []RowRange) ([][]labels.Label, error) {
	ctx, span := tracer.Start(ctx, "Materializer.MaterializeAllLabels")
	var err error
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	totalRowsRequested := totalRows(rr)
	span.SetAttributes(
		attribute.Int("row_group_index", rgi),
		attribute.Int("row_ranges_count", len(rr)),
		attribute.Int64("total_rows_requested", totalRowsRequested),
	)

	// Get column indexes for all rows in the specified ranges
	columnIndexes, err := m.getColumnIndexes(ctx, rgi, rr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column indexes")
	}

	// Build mapping of which columns are needed for which row ranges
	columnToRowRanges, rowRangeToStartIndex, err := m.buildColumnMappings(rr, columnIndexes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build collum mapping")
	}

	// Materialize label values for each column concurrently
	results := make([][]labels.Label, len(columnIndexes))
	mtx := sync.Mutex{}
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(m.concurrency)
	labelsRowGroup := m.b.LabelsFile().RowGroups()[rgi]

	span.SetAttributes(attribute.Int("goroutine_pool_limit", m.concurrency))

	for columnIndex, rowRanges := range columnToRowRanges {
		errGroup.Go(func() error {
			ctx, span := tracer.Start(ctx, "Materializer.materializeAllLabels.column")
			var err error
			defer func() {
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
				span.End()
			}()

			// Extract label name from column schema
			columnChunk := labelsRowGroup.ColumnChunks()[columnIndex]
			labelName, ok := schema.ExtractLabelFromColumn(m.b.LabelsFile().Schema().Columns()[columnIndex][0])
			if !ok {
				return fmt.Errorf("column %d not found in schema", columnIndex)
			}

			span.SetAttributes(
				attribute.Int("column_index", columnIndex),
				attribute.String("label_name", labelName),
				attribute.Int("row_ranges_count", len(rowRanges)),
			)

			// Materialize the actual label values for this column
			labelValues, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rowRanges, columnChunk, false)
			if err != nil {
				return errors.Wrap(err, "failed to materialize label values")
			}

			span.SetAttributes(attribute.Int("label_values_count", len(labelValues)))

			// Assign label values to the appropriate result positions
			mtx.Lock()
			defer mtx.Unlock()

			valueIndex := 0
			for _, rowRange := range rowRanges {
				startIndex := rowRangeToStartIndex[rowRange]

				for rowInRange := 0; rowInRange < int(rowRange.Count); rowInRange++ {
					if !labelValues[valueIndex].IsNull() {
						results[startIndex+rowInRange] = append(results[startIndex+rowInRange], labels.Label{
							Name:  labelName,
							Value: util.YoloString(labelValues[valueIndex].ByteArray()),
						})
					}
					valueIndex++
				}
			}
			return nil
		})
	}
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	span.SetAttributes(
		attribute.Int("materialized_labels_count", len(results)),
		attribute.Int("columns_materialized", len(columnToRowRanges)),
	)
	return results, nil
}

// getColumnIndexes retrieves the column index data for all rows in the specified ranges
func (m *Materializer) getColumnIndexes(ctx context.Context, rgi int, rr []RowRange) ([]parquet.Value, error) {
	labelsRowGroup := m.b.LabelsFile().RowGroups()[rgi]
	columnChunk := labelsRowGroup.ColumnChunks()[m.colIdx]

	columnIndexes, err := m.materializeColumnSlice(ctx, m.b.LabelsFile(), rgi, rr, columnChunk, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to materialize column indexes")
	}

	return columnIndexes, nil
}

// buildColumnMappings creates mappings between columns and row ranges based on column indexes
func (m *Materializer) buildColumnMappings(rr []RowRange, columnIndexes []parquet.Value) (map[int][]RowRange, map[RowRange]int, error) {
	columnToRowRanges := make(map[int][]RowRange, 10)
	rowRangeToStartIndex := make(map[RowRange]int, len(rr))

	columnIndexPos := 0
	resultIndex := 0

	for _, rowRange := range rr {
		rowRangeToStartIndex[rowRange] = resultIndex
		seenColumns := util.NewBitmap(len(m.b.LabelsFile().ColumnIndexes()))

		// Process each row in the current range
		for rowInRange := int64(0); rowInRange < rowRange.Count; rowInRange++ {
			columnIds, err := schema.DecodeUintSlice(columnIndexes[columnIndexPos].ByteArray())
			columnIndexPos++

			if err != nil {
				return nil, nil, err
			}

			// Track which columns are needed for this row range
			for _, columnId := range columnIds {
				if !seenColumns.Get(columnId) {
					columnToRowRanges[columnId] = append(columnToRowRanges[columnId], rowRange)
					seenColumns.Set(columnId)
				}
			}
			resultIndex++
		}
	}

	return columnToRowRanges, rowRangeToStartIndex, nil
}

func totalRows(rr []RowRange) int64 {
	res := int64(0)
	for _, r := range rr {
		res += r.Count
	}
	return res
}

func (m *Materializer) MaterializeChunks(ctx context.Context, rgi int, mint, maxt int64, rr []RowRange) (ChunksIteratorIterator, error) {
	var err error
	ctx, span := tracer.Start(ctx, "Materializer.MaterializeChunks")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	span.SetAttributes(
		attribute.Int("row_group_index", rgi),
		attribute.Int64("mint", mint),
		attribute.Int64("maxt", maxt),
		attribute.Int("row_ranges_count", len(rr)),
	)
	minDataCol := m.s.DataColumIdx(mint)
	maxDataCol := m.s.DataColumIdx(maxt)
	rg := m.b.ChunksFile().RowGroups()[rgi]

	span.SetAttributes(
		attribute.Int("min_data_col", minDataCol),
		attribute.Int("max_data_col", maxDataCol),
		attribute.Int("total_rows", int(totalRows(rr))),
	)

	var columnValueIterators []*ColumnValueIterator
	for i := minDataCol; i <= min(maxDataCol, len(m.dataColToIndex)-1); i++ {
		cc := rg.ColumnChunks()[m.dataColToIndex[i]]
		columnValueIter, err := m.materializeColumnIter(ctx, m.b.ChunksFile(), rgi, rr, cc, true)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create column value iterator")
		}
		columnValueIterators = append(columnValueIterators, columnValueIter)
	}
	return &MultiColumnChunksDecodingIterator{
		mint:                 mint,
		maxt:                 maxt,
		columnValueIterators: columnValueIterators,
		d:                    m.d,
	}, nil
}

func (m *Materializer) materializeColumnIter(
	ctx context.Context,
	file storage.ParquetFileView,
	rgi int,
	rr []RowRange,
	cc parquet.ColumnChunk,
	chunkColumn bool,
) (*ColumnValueIterator, error) {
	pageRanges, err := m.GetPageRangesForColummn(cc, file, rgi, rr, chunkColumn)
	if err != nil {
		return nil, err
	}

	dictOff, dictSz := file.DictionaryPageBounds(rgi, cc.Column())

	rowRangesValueIterators := make([]*RowRangesValueIterator, len(pageRanges))
	for i, pageRange := range pageRanges {
		rowRangesValueIter, err := newRowRangesValueIterator(
			ctx, file, cc, pageRange, dictOff, dictSz,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create row value iterator for page range")
		}
		rowRangesValueIterators[i] = rowRangesValueIter
	}
	return &ColumnValueIterator{
		rowRangesIterators: rowRangesValueIterators,
	}, nil
}

func (m *Materializer) materializeColumnSlice(
	ctx context.Context,
	file storage.ParquetFileView,
	rgi int,
	rr []RowRange,
	cc parquet.ColumnChunk,
	chunkColumn bool,
) ([]parquet.Value, error) {
	ctx, span := tracer.Start(ctx, "Materializer.materializeColumnSlice")
	var err error
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	span.SetAttributes(
		attribute.Int("row_group_index", rgi),
		attribute.Int("row_ranges_count", len(rr)),
		attribute.Int64("total_rows", totalRows(rr)),
		attribute.Bool("chunk_column", chunkColumn),
		attribute.Int("concurrency", m.concurrency),
	)

	pageRanges, err := m.GetPageRangesForColummn(cc, file, rgi, rr, chunkColumn)
	if err != nil || len(pageRanges) == 0 {
		return nil, err
	}

	span.SetAttributes(
		attribute.Int("page_ranges_after_coalesce", len(pageRanges)),
	)

	pageRangesValues := make([][]parquet.Value, len(pageRanges))

	dictOff, dictSz := file.DictionaryPageBounds(rgi, cc.Column())
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(m.concurrency)

	span.SetAttributes(attribute.Int("goroutine_pool_limit", m.concurrency))

	for i, pageRange := range pageRanges {
		errGroup.Go(func() error {
			valuesIter, err := newRowRangesValueIterator(ctx, file, cc, pageRange, dictOff, dictSz)
			if err != nil {
				return errors.Wrap(err, "failed to create row values iterator for page")
			}
			defer func() { _ = valuesIter.Close() }()

			iterValues := make([]parquet.Value, 0, totalRows(pageRange.rows))
			for valuesIter.Next() {
				iterValues = append(iterValues, valuesIter.At())
			}
			if err = valuesIter.Err(); err != nil {
				return err
			}

			pageRangesValues[i] = iterValues
			return nil
		})
	}
	err = errGroup.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "failed to materialize columns")
	}

	valuesFlattened := make([]parquet.Value, 0, totalRows(rr))
	for _, pageRangeValues := range pageRangesValues {
		valuesFlattened = append(valuesFlattened, pageRangeValues...)
	}

	span.SetAttributes(attribute.Int("materialized_values_count", len(valuesFlattened)))
	return valuesFlattened, nil
}

type PageToReadWithRow struct {
	PageToRead
	rows []RowRange
}

func NewPageToReadWithRow(p PageToRead, rows []RowRange) PageToReadWithRow {
	return PageToReadWithRow{
		PageToRead: p,
		rows:       rows,
	}
}

func (pr *PageToReadWithRow) Rows() []RowRange {
	return pr.rows
}

func (m *Materializer) GetPageRangesForColummn(cc parquet.ColumnChunk, file storage.ParquetFileView, rgi int, rr []RowRange, chunkColumn bool) ([]PageToReadWithRow, error) {
	if len(rr) == 0 {
		return nil, nil
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get offset index")
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column index")
	}

	group := file.RowGroups()[rgi]
	pagesToRowsMap := make(map[int][]RowRange, len(rr))

	for i := 0; i < cidx.NumPages(); i++ {
		pageRowRange := RowRange{
			From: oidx.FirstRowIndex(i),
		}
		pageRowRange.Count = group.NumRows()

		if i < oidx.NumPages()-1 {
			pageRowRange.Count = oidx.FirstRowIndex(i+1) - pageRowRange.From
		}

		for _, r := range rr {
			if pageRowRange.Overlaps(r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], pageRowRange.Intersection(r))
			}
		}
	}
	if err := m.checkBytesQuota(maps.Keys(pagesToRowsMap), oidx, chunkColumn); err != nil {
		return nil, err
	}

	pageRanges := m.CoalescePageRanges(pagesToRowsMap, oidx)
	return pageRanges, nil
}

// CoalescePageRanges merges nearby pages to enable efficient sequential reads.
// Pages that are not close to each other will be scheduled for concurrent reads.
func (m *Materializer) CoalescePageRanges(pagedIdx map[int][]RowRange, offset parquet.OffsetIndex) []PageToReadWithRow {
	_, span := tracer.Start(context.Background(), "Materializer.CoalescePageRanges")
	defer span.End()

	span.SetAttributes(
		attribute.Int("input_pages_count", len(pagedIdx)),
	)

	if len(pagedIdx) == 0 {
		return []PageToReadWithRow{}
	}
	idxs := make([]int, 0, len(pagedIdx))
	for idx := range pagedIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := m.partitioner.Partition(len(idxs), func(i int) (int, int) {
		return int(offset.Offset(idxs[i])), int(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]PageToReadWithRow, 0, len(parts))
	totalBytes := int64(0)
	for _, part := range parts {
		var rows []RowRange
		for i := part.ElemRng[0]; i < part.ElemRng[1]; i++ {
			rows = append(rows, pagedIdx[idxs[i]]...)
		}

		pageToReadWithRow := NewPageToReadWithRow(
			NewPageToRead(
				0,
				int64(part.ElemRng[0]),
				int64(part.ElemRng[1]),
				int64(part.Start),
				int64(part.End-part.Start),
			),
			rows,
		)

		r = append(r, pageToReadWithRow)
		totalBytes += pageToReadWithRow.CompressedSize()
	}

	span.SetAttributes(
		attribute.Int("output_page_ranges_count", len(r)),
		attribute.Int("partitions_count", len(parts)),
		attribute.Int64("total_bytes_to_read", totalBytes),
	)

	return r
}

func (m *Materializer) checkRowCountQuota(rr []RowRange) error {
	if err := m.rowCountQuota.Reserve(totalRows(rr)); err != nil {
		return fmt.Errorf("would fetch too many rows: %w", err)
	}
	return nil
}

func (m *Materializer) checkBytesQuota(pages iter.Seq[int], oidx parquet.OffsetIndex, chunkColumn bool) error {
	total := totalBytes(pages, oidx)
	if chunkColumn {
		if err := m.chunkBytesQuota.Reserve(total); err != nil {
			return fmt.Errorf("would fetch too many chunk bytes: %w", err)
		}
	}
	if err := m.dataBytesQuota.Reserve(total); err != nil {
		return fmt.Errorf("would fetch too many data bytes: %w", err)
	}
	return nil
}

func totalBytes(pages iter.Seq[int], oidx parquet.OffsetIndex) int64 {
	res := int64(0)
	for i := range pages {
		res += oidx.CompressedPageSize(i)
	}
	return res
}
