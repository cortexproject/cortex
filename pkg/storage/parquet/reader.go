package storage

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/thanos-io/thanos/pkg/store"

	"github.com/efficientgo/core/errors"

	"github.com/opentracing/opentracing-go"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
)

const (
	defaultReadBufferSize = 100 * 1024 // 100k
)

type CreateReadAtWithContext func(ctx context.Context) io.ReaderAt

type bufferedReaderAt struct {
	r      io.ReaderAt
	b      []byte
	offset int64
}

func (b bufferedReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= b.offset && off < b.offset+int64(len(b.b)) {
		diff := off - b.offset
		n := copy(p, b.b[diff:])
		return n, nil
	}

	return b.r.ReadAt(p, off)
}

func newBufferedReaderAt(r io.ReaderAt, minOffset, maxOffset int64) io.ReaderAt {
	if minOffset < maxOffset {
		b := make([]byte, maxOffset-minOffset)
		n, err := r.ReadAt(b, minOffset)
		if err == nil {
			return &bufferedReaderAt{r: r, b: b[:n], offset: minOffset}
		}
	}
	return r
}

type ParquetReader struct {
	//r io.ReaderAt

	size           int64
	Rows           int64
	f              *parquet.File
	columnIndexMap map[string]int
	sortedColumn   string
	dataColsSize   int
	asyncRead      bool // Add this field

	pagesSkippedPageIndex *prometheus.CounterVec
	projectionPushdown    prometheus.Counter

	dictionaryCache *Cache[parquet.Dictionary]
	readerFactory   CreateReadAtWithContext
}

func NewParquetReader(readerFactory CreateReadAtWithContext, size int64, asyncRead bool, cacheMetrics *CacheMetrics, dictionaryCacheSize int, pagesSkippedPageIndex *prometheus.CounterVec, projectionPushdown prometheus.Counter) (*ParquetReader, error) {
	options := []parquet.FileOption{
		parquet.SkipBloomFilters(true), // we don't use bloom filters
		parquet.ReadBufferSize(1024 * 512),
		parquet.OptimisticRead(true),
		parquet.SkipMagicBytes(true),
	}

	// Set read mode based on the flag
	if asyncRead {
		options = append(options, parquet.FileReadMode(parquet.ReadModeAsync))
	}

	// Create a reader with initial file loading.
	f, err := parquet.OpenFile(readerFactory(context.Background()), size, options...)
	if err != nil {
		return nil, err
	}
	columnIndexMap := make(map[string]int)
	for i, c := range f.Schema().Columns() {
		columnIndexMap[c[0]] = i
	}

	sortedColumn := ""
	numDataCols := 0

	for _, md := range f.Metadata().KeyValueMetadata {
		switch md.Key {
		case SortedColMetadataKey:
			sortedColumn = md.Value
		case NumberOfDataColumnsMetadataKey:
			numDataCols, _ = strconv.Atoi(md.Value)
		}
	}

	cache, err := NewCache[parquet.Dictionary]("dictionary", dictionaryCacheSize, cacheMetrics)
	if err != nil {
		return nil, err
	}

	return &ParquetReader{
		readerFactory:         readerFactory,
		f:                     f,
		size:                  size,
		Rows:                  f.NumRows(),
		columnIndexMap:        columnIndexMap,
		sortedColumn:          sortedColumn,
		dataColsSize:          numDataCols,
		asyncRead:             asyncRead,
		pagesSkippedPageIndex: pagesSkippedPageIndex,
		projectionPushdown:    projectionPushdown,
		dictionaryCache:       cache,
	}, nil
}

func (pr *ParquetReader) ColumnExists(name string) bool {
	if _, ok := pr.columnIndexMap[name]; ok {
		return true
	}
	return false
}

func (pr *ParquetReader) ColumnNames() []string {
	names := make([]string, 0, len(pr.columnIndexMap))
	for colName := range pr.columnIndexMap {
		if colName != ColHash && colName != ColIndexes && !pr.isColData(colName) {
			names = append(names, colName)
		}
	}
	sort.Strings(names)
	return names
}

func (pr *ParquetReader) SearchRows(ctx context.Context, colName, colValue string, sts *Stats) ([][]int64, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "ParquetReader.SearchRows")
	defer span.Finish()
	if colName != pr.sortedColumn {
		return nil, fmt.Errorf("cannot search column %v as its not sorted. Sorted column %v", colName, pr.sortedColumn)
	}

	ci, ok := pr.columnIndexMap[colName]
	results := make([][]int64, len(pr.f.RowGroups()))

	if !ok {
		return results, nil
	}

	pagesRead := 0
	pagesSizeBytes := int64(0)
NextRG:
	for rgi, rg := range pr.f.RowGroups() {
		ch, err := rg.ColumnChunks()[ci].ColumnIndex()
		if err != nil {
			return nil, err
		}

		found := parquet.Search(ch, parquet.ValueOf(colValue), parquet.ByteArrayType)

		pagesToRead := []int{}
		for i := found; i < ch.NumPages(); i++ {
			minValue := ch.MinValue(i).String()
			if minValue > colValue {
				break
			}
			pagesToRead = append(pagesToRead, i)
		}

		if found < ch.NumPages() {
			offset, err := rg.ColumnChunks()[ci].OffsetIndex()
			if err != nil {
				return results, err
			}
			pages := pr.getPage(ctx, rgi, colName, offset, pagesToRead)
			pagesSizeBytes += getCompressedPageSizes(pagesToRead, offset)
			row := offset.FirstRowIndex(found)
			err = pages.SeekToRow(row)
			if err != nil {
				return results, err
			}
			currentRow := row
			for {
				page, err := pages.ReadPage()
				if err != nil {
					break
				}
				pagesRead++

				vr := page.Values()
				values := [100]parquet.Value{}

				for {
					n, err := vr.ReadValues(values[:])
					for _, value := range values[:n] {
						c := strings.Compare(colValue, yoloString(value.ByteArray()))
						switch c {
						case 0:
							results[rgi] = append(results[rgi], currentRow)
						case -1:
							pr.releasePage(pages)
							continue NextRG
						}

						currentRow++
					}
					if err != nil {
						break
					}
				}
			}
			pr.releasePage(pages)
		}
	}

	sts.AddPagesSearchScanned(colName, pagesRead)
	sts.AddPagesSearchSizeBytes(colName, pagesSizeBytes)
	return results, nil
}

func (pr *ParquetReader) DataColsSize() int {
	return pr.dataColsSize
}

func (pr *ParquetReader) ScanRows(ctx context.Context, rows [][]int64, full bool, m *labels.Matcher, sts *Stats) ([][]int64, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "ParquetReader.ScanRows")
	defer span.Finish()

	mtx := sync.Mutex{}
	results := make([][]int64, len(pr.f.RowGroups()))

	ci, ok := pr.columnIndexMap[m.Name]
	if !ok {
		return results, nil
	}

	var pagesSkippedCounter prometheus.Counter
	if pr.pagesSkippedPageIndex != nil {
		pagesSkippedCounter = pr.pagesSkippedPageIndex.WithLabelValues(m.Name)
	}

	errGroup := &errgroup.Group{}

	loadPageIndex, matchNull, matchNotNull := usePageIndex(m)

	for rgi, rg := range pr.f.RowGroups() {
		if !full && len(rows[rgi]) == 0 {
			continue
		}

		errGroup.Go(func() error {
			var (
				pageSkipped int
			)
			offsetIndex, err := rg.ColumnChunks()[ci].OffsetIndex()
			if err != nil {
				return err
			}
			// Only use page Index if it is a full scan and equal matcher.
			var columnIndex parquet.ColumnIndex
			if loadPageIndex {
				columnIndex, err = rg.ColumnChunks()[ci].ColumnIndex()
				if err != nil {
					return err
				}
			}

			pagesToRowsMap := map[int][]int64{}

			if full {
				for i := 0; i < offsetIndex.NumPages(); i++ {
					if loadPageIndex {
						if !filterPageWithPageIndex(m, columnIndex, i, matchNull, matchNotNull) {
							pageSkipped++
							if pagesSkippedCounter != nil {
								pagesSkippedCounter.Inc()
							}
							continue
						}
					}

					firstRow := offsetIndex.FirstRowIndex(i)
					lastRow := rg.NumRows()
					if i < offsetIndex.NumPages()-1 {
						lastRow = offsetIndex.FirstRowIndex(i+1) - 1
					}
					pagesToRowsMap[i] = append(pagesToRowsMap[i], firstRow)
					pagesToRowsMap[i] = append(pagesToRowsMap[i], lastRow)
				}
			} else {
				pagesSkipped := make(map[int]bool)
				for _, v := range rows[rgi] {
					idx := sort.Search(offsetIndex.NumPages(), func(i int) bool {
						return offsetIndex.FirstRowIndex(i) > v
					}) - 1
					if loadPageIndex {
						if _, ok := pagesSkipped[idx]; !ok {
							skipPage := !filterPageWithPageIndex(m, columnIndex, idx, matchNull, matchNotNull)
							pagesSkipped[idx] = skipPage
						}
						if pagesSkipped[idx] {
							pageSkipped++
							if pagesSkippedCounter != nil {
								pagesSkippedCounter.Inc()
							}
							continue
						}
					}
					pagesToRowsMap[idx] = append(pagesToRowsMap[idx], v)
				}
			}
			sts.AddPagesSkipped(m.Name, pageSkipped)
			for _, pagesToRead := range mergePagesIdx(pagesToRowsMap, offsetIndex) {
				errGroup.Go(func() error {
					var (
						pageScanned    int
						pagesSizeBytes int64
					)
					defer func() {
						sts.AddPagesSearchScanned(m.Name, pageScanned)
						sts.AddPagesSearchSizeBytes(m.Name, pagesSizeBytes)
					}()
					offset, err := rg.ColumnChunks()[ci].OffsetIndex()
					if err != nil {
						return err
					}
					pages := pr.getPage(ctx, rgi, m.Name, offset, pagesToRead.pages)
					pagesSizeBytes += getCompressedPageSizes(pagesToRead.pages, offset)
					defer pr.releasePage(pages)

					next := pagesToRead.rows[0]
					maxRow := pagesToRead.rows[len(pagesToRead.rows)-1]
					currentRow := next
					err = pages.SeekToRow(next)
					if err != nil {
						return err
					}

					for {
						page, err := pages.ReadPage()
						if err != nil {
							if err == io.EOF {
								return nil
							}
							return err
						}
						pageScanned++
						vr := page.Values()
						values := [100]parquet.Value{}

						for {
							n, err := vr.ReadValues(values[:])
							for _, value := range values[:n] {
								if currentRow == next {
									// TODO: Lets run the match using dictionary or cache the values already matched,
									// so we prevent running the same matcher multiple times. (Regex match can be quite resource intensive).
									if m.Matches(yoloString(value.ByteArray())) {
										mtx.Lock()
										results[rgi] = append(results[rgi], currentRow)
										mtx.Unlock()
									}
									if !full {
										pagesToRead.rows = pagesToRead.rows[1:]
										if len(pagesToRead.rows) == 0 {
											parquet.Release(page)
											return nil
										}
										next = pagesToRead.rows[0]
									} else {
										next = next + 1
									}
								}
								currentRow++
								if currentRow > maxRow {
									parquet.Release(page)
									return nil
								}
							}
							if err != nil {
								parquet.Release(page)
								break
							}
						}
					}
				})
			}
			return nil
		})
	}

	err := errGroup.Wait()

	for _, result := range results {
		slices.Sort(result)
	}

	return results, err
}

func (pr *ParquetReader) ColumnValues(ctx context.Context, c string) ([]string, error) {
	errGroup := &errgroup.Group{}

	r := make(map[string]struct{}, 128)
	rMtx := sync.Mutex{}

	for rgi := range pr.f.RowGroups() {
		errGroup.Go(func() error {
			pages := pr.getPage(ctx, rgi, c, nil, nil)
			dic, err := pages.ReadDictionary()
			if err != nil {
				return err
			}
			for i := 0; i < dic.Len(); i++ {
				rMtx.Lock()
				r[dic.Index(int32(i)).String()] = struct{}{}
				rMtx.Unlock()
			}
			return nil
		})
	}

	err := errGroup.Wait()

	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(r))
	for v := range r {
		results = append(results, v)
	}
	sort.Strings(results)
	return results, nil
}

func (pr *ParquetReader) Materialize(ctx context.Context, rows [][]int64, dataColsIdx []int, grouping []string, by bool, sts *Stats) ([]ParquetRow, error) {
	start := time.Now()
	defer func() {
		sts.AddMaterializeWallTime(time.Since(start).Milliseconds())
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "ParquetReader.Materialize")
	defer span.Finish()

	if by {
		if pr.projectionPushdown != nil {
			pr.projectionPushdown.Inc()
		}
		return pr.materializeBy(ctx, rows, dataColsIdx, grouping, sts)
	}

	// TODO: skip fetching hash if not needed.
	columns := []string{ColHash, ColIndexes}
	cValues, err := pr.MaterializeColumn(ctx, rows, sts, columns...)
	if err != nil {
		return nil, err
	}

	results := make([]ParquetRow, len(cValues[0]))
	colsMap := make(map[string]struct{}, len(cValues[0]))

	for i, v := range cValues[0] {
		results[i] = ParquetRow{
			Hash:    v.Uint64(),
			Columns: map[string]string{},
		}

		if i < len(cValues[1]) {
			colIndexes, err := DecodeUintSlice(cValues[1][i].Bytes())
			if err != nil {
				return nil, errors.Wrapf(err, "failed to decode column indexes: len %d", len(cValues[1][i].Bytes()))
			}
			for _, idx := range colIndexes {
				if _, ok := colsMap[pr.f.Schema().Columns()[idx][0]]; !ok {
					colsMap[pr.f.Schema().Columns()[idx][0]] = struct{}{}
				}
			}
		}
	}

	cols := make([]string, 0, len(colsMap)+len(dataColsIdx))
	for _, idx := range dataColsIdx {
		c := fmt.Sprintf("%v_%v", ColData, idx)
		cols = append(cols, c)
	}
	// Subtract not needed groups.
	for _, group := range grouping {
		delete(colsMap, group)
	}
	if len(grouping) > 0 && pr.projectionPushdown != nil {
		pr.projectionPushdown.Inc()
	}
	for k := range colsMap {
		cols = append(cols, k)
	}

	cValues, err = pr.MaterializeColumn(ctx, rows, sts, cols...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to materialize column values")
	}

	for i, colValues := range cValues {
		for j, v := range colValues {
			if !v.IsNull() {
				if pr.isColData(cols[i]) {
					if len(v.ByteArray()) > 0 {
						results[j].Data = append(results[j].Data, v.ByteArray())
					}
				} else {
					results[j].Columns[cols[i]] = v.String()
				}
			}
		}
	}

	span.SetTag("cols", len(dataColsIdx))
	span.SetTag("by", by)
	span.SetTag("results", len(results))

	return results, nil
}

func (pr *ParquetReader) materializeBy(ctx context.Context, rows [][]int64, dataColsIdx []int, grouping []string, sts *Stats) ([]ParquetRow, error) {
	columns := []string{ColHash}
	for _, group := range grouping {
		if _, ok := pr.columnIndexMap[group]; ok {
			columns = append(columns, group)
		}
	}

	for _, idx := range dataColsIdx {
		c := fmt.Sprintf("%v_%v", ColData, idx)
		columns = append(columns, c)
	}

	cValues, err := pr.MaterializeColumn(ctx, rows, sts, columns...)
	if err != nil {
		return nil, err
	}

	results := make([]ParquetRow, len(cValues[0]))
	for i, v := range cValues[0] {
		results[i] = ParquetRow{
			Hash:    v.Uint64(),
			Columns: map[string]string{},
		}
	}

	for i, colValues := range cValues[1:] {
		for j, v := range colValues {
			if !v.IsNull() {
				if pr.isColData(columns[i+1]) {
					if len(v.ByteArray()) > 0 {
						results[j].Data = append(results[j].Data, v.ByteArray())
					}
				} else {
					results[j].Columns[columns[i+1]] = v.String()
				}
			}
		}
	}

	return results, nil
}

func (pr *ParquetReader) MaterializeColumn(ctx context.Context, rows [][]int64, sts *Stats, columns ...string) ([][]parquet.Value, error) {
	mtx := sync.Mutex{}
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(runtime.GOMAXPROCS(0))

	// Pre allocate results slices
	results := make([][]parquet.Value, len(columns))
	totalResults := 0
	for _, r := range rows {
		totalResults += len(r)
	}

	for i := range columns {
		results[i] = make([]parquet.Value, totalResults)
	}

	for cIndex, c := range columns {
		ci, ok := pr.columnIndexMap[c]
		if !ok {
			continue
		}
		sts.AddColumnMaterialized(c)
		resultIdx := 0
		for rgi, rg := range pr.f.RowGroups() {
			rowsResultsIdx := make(map[int64]int, len(results[cIndex]))
			if len(rows[rgi]) == 0 {
				continue
			}
			offsetIndex, err := rg.ColumnChunks()[ci].OffsetIndex()
			if err != nil {
				return nil, err
			}

			pagesToRowsMap := make(map[int][]int64, len(rows[rgi]))
			for _, v := range rows[rgi] {
				idx := sort.Search(offsetIndex.NumPages(), func(i int) bool {
					return offsetIndex.FirstRowIndex(i) > v
				}) - 1
				pagesToRowsMap[idx] = append(pagesToRowsMap[idx], v)
				mtx.Lock()
				rowsResultsIdx[v] = resultIdx
				mtx.Unlock()
				resultIdx++
			}

			for _, pagesToRead := range mergePagesIdx(pagesToRowsMap, offsetIndex) {
				errGroup.Go(func() error {
					var (
						pagesScanned   int
						pagesSizeBytes int64
					)
					defer func() {
						sts.AddPagesMaterializeScanned(c, pagesScanned)
						sts.AddPagesMaterializeSizeBytes(c, pagesSizeBytes)
					}()
					pages := pr.getPage(ctx, rgi, c, offsetIndex, pagesToRead.pages)
					pagesSizeBytes += getCompressedPageSizes(pagesToRead.pages, offsetIndex)

					defer pr.releasePage(pages)
					next := pagesToRead.rows[0]
					currentRow := next
					err := pages.SeekToRow(next)
					if err != nil {
						return err
					}
					for {
						page, err := pages.ReadPage()
						if err != nil {
							return err
						}
						pagesScanned++

						vr := page.Values()
						for {
							values := [1024]parquet.Value{}
							n, err := vr.ReadValues(values[:])
							if err != nil && err != io.EOF {
								parquet.Release(page)
								return err
							}
							for _, value := range values[:n] {
								if currentRow == next {
									mtx.Lock()
									results[cIndex][rowsResultsIdx[currentRow]] = value.Clone()
									mtx.Unlock()
									pagesToRead.rows = pagesToRead.rows[1:]
									if len(pagesToRead.rows) == 0 {
										parquet.Release(page)
										return nil
									}
									next = pagesToRead.rows[0]
								}
								currentRow++
								if currentRow > next {
									parquet.Release(page)
									return errors.Newf("did not find rows: next %d current %d remaining %d", next, currentRow, len(pagesToRead.rows))
								}
							}
							if err == io.EOF {
								parquet.Release(page)
								break
							}
						}
					}
				})
			}
		}
	}
	err := errGroup.Wait()
	return results, err
}

func (pr *ParquetReader) MaterializeColumnNames(ctx context.Context, rows [][]int64, sts *Stats) ([]string, error) {
	cValues, err := pr.MaterializeColumn(ctx, rows, sts, ColIndexes)
	if err != nil {
		return nil, err
	}

	colsMap := make(map[string]struct{}, len(pr.columnIndexMap))
	for _, v := range cValues[0] {
		colIndexes, err := DecodeUintSlice(v.Bytes())
		if err != nil {
			return nil, err
		}
		for _, idx := range colIndexes {
			if _, ok := colsMap[pr.f.Schema().Columns()[idx][0]]; !ok {
				colsMap[pr.f.Schema().Columns()[idx][0]] = struct{}{}
			}
		}
	}
	cols := make([]string, 0, len(colsMap))
	for col := range colsMap {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols, nil
}

func (pr *ParquetReader) isColData(c string) bool {
	return strings.HasPrefix(c, ColData)
}

func (pr *ParquetReader) getPage(ctx context.Context, rgi int, c string, offset parquet.OffsetIndex, pagesToRead []int) *parquet.FilePages {
	ci, ok := pr.columnIndexMap[c]
	if !ok {
		return nil
	}

	key := fmt.Sprintf("%d_%d", rgi, ci)
	dic := pr.dictionaryCache.Get(key)
	var minOffset, maxOffset int64
	if len(pagesToRead) > 0 && offset != nil {
		minOffset = offset.Offset(pagesToRead[0])
		maxOffset = offset.Offset(pagesToRead[len(pagesToRead)-1]) + offset.CompressedPageSize(pagesToRead[len(pagesToRead)-1])
	}

	colChunk := pr.f.RowGroups()[rgi].ColumnChunks()[ci].(*parquet.FileColumnChunk)
	bufferedReader := newBufferedReaderAt(pr.readerFactory(ctx), minOffset, maxOffset)
	pages := colChunk.PagesFrom(bufferedReader, defaultReadBufferSize)

	if dic != nil {
		pages.SetDictionary(dic)
	} else {
		d, _ := pages.ReadDictionary()
		pr.dictionaryCache.Set(key, d)
	}

	return pages
}

func (pr *ParquetReader) releasePage(p parquet.Pages) {
	p.Close()
}

func usePageIndex(m *labels.Matcher) (useIndex bool, matchNull bool, matchNotNull bool) {
	if matchNullMatcher(m) {
		return true, true, false
	}
	if matchNotNullMatcher(m) {
		return true, false, true
	}
	switch m.Type {
	case labels.MatchEqual, labels.MatchNotEqual:
		return true, false, false
	case labels.MatchRegexp:
		return len(m.SetMatches()) > 0 || len(m.Prefix()) > 0, false, false
	case labels.MatchNotRegexp:
		return len(m.SetMatches()) == 1, false, false
	}
	return false, false, false
}

func matchNullMatcher(matcher *labels.Matcher) bool {
	// If the matcher is ="" or =~"" or !~".+", it is the same as removing all values for the label.
	// Match null value only.
	if (matcher.Value == "" && (matcher.Type == labels.MatchEqual || matcher.Type == labels.MatchRegexp)) ||
		(matcher.Type == labels.MatchNotRegexp && matcher.Value == ".+") {
		return true
	}
	return false
}

func matchNotNullMatcher(matcher *labels.Matcher) bool {
	// If the matcher is !="" or !~"" or =~".+", it is the same as adding all values for the label.
	// Filter out null value.
	if (matcher.Value == "" && (matcher.Type == labels.MatchNotEqual || matcher.Type == labels.MatchNotRegexp)) ||
		(matcher.Type == labels.MatchRegexp && matcher.Value == ".+") {
		return true
	}
	return false
}

func filterPageWithPageIndex(m *labels.Matcher, columnIndex parquet.ColumnIndex, pageId int, matchNull, matchNotNull bool) bool {
	if matchNotNull {
		// If the matcher asks for not null values, skip any null page.
		return !columnIndex.NullPage(pageId)
	}
	if matchNull {
		// If the matcher asks for null values, skip any page that doesn't have null values.
		return columnIndex.NullCount(pageId) > 0
	}
	// For any other matcher scenario, filter out any page that is null.
	if columnIndex.NullPage(pageId) {
		return false
	}
	minVal := columnIndex.MinValue(pageId)
	maxVal := columnIndex.MaxValue(pageId)
	cmp := parquet.CompareNullsLast(parquet.ByteArrayType.Compare)
	var valuesToMatch []string
	switch m.Type {
	case labels.MatchEqual:
		valuesToMatch = []string{m.Value}
	case labels.MatchNotEqual:
		if cmp(minVal, maxVal) == 0 && cmp(minVal, parquet.ValueOf(m.Value)) == 0 {
			return false
		}
		return true
	case labels.MatchNotRegexp:
		// Only scenario we can skip page for not regexp is not regexp
		// has only 1 match which is the same as not equal.
		if cmp(minVal, maxVal) == 0 && cmp(minVal, parquet.ValueOf(m.SetMatches()[0])) == 0 {
			return false
		}
		return true
	case labels.MatchRegexp:
		if len(m.SetMatches()) > 0 {
			valuesToMatch = m.SetMatches()
		} else if len(m.Prefix()) > 0 {
			prefix := m.Prefix()
			// MaxValue shouldn't be null as we already skipped null page above.
			maxValStr := maxVal.String()
			if minVal.IsNull() {
				return prefix <= maxValStr[:min(len(prefix), len(maxValStr))]
			}
			minValStr := minVal.String()
			return prefix >= minValStr[:max(len(prefix), len(minValStr))] &&
				prefix <= maxValStr[:max(len(prefix), len(maxValStr))]
		}
	}

	for _, v := range valuesToMatch {
		// As long as a single value that matches, we don't filter out the page.
		if cmp(parquet.ValueOf(v), minVal) >= 0 && cmp(parquet.ValueOf(v), maxVal) <= 0 {
			return true
		}
	}
	return false
}

type pageEntryRead struct {
	pages []int
	rows  []int64
}

func mergePagesIdx(pagedIdx map[int][]int64, offset parquet.OffsetIndex) []pageEntryRead {
	partitioner := store.NewGapBasedPartitioner(10 * 1024)
	if len(pagedIdx) == 0 {
		return []pageEntryRead{}
	}
	idxs := make([]int, 0, len(pagedIdx))
	for idx := range pagedIdx {
		idxs = append(idxs, idx)
	}
	slices.Sort(idxs)

	parts := partitioner.Partition(len(idxs), func(i int) (uint64, uint64) {
		return uint64(offset.Offset(idxs[i])), uint64(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]pageEntryRead, 0, len(parts))
	for _, part := range parts {
		pagesToRead := pageEntryRead{}
		for i := part.ElemRng[0]; i < part.ElemRng[1]; i++ {
			pagesToRead.pages = append(pagesToRead.pages, idxs[i])
			pagesToRead.rows = append(pagesToRead.rows, pagedIdx[idxs[i]]...)
		}
		r = append(r, pagesToRead)
	}

	return r
}

func getCompressedPageSizes(pagesToRead []int, offset parquet.OffsetIndex) int64 {
	if len(pagesToRead) > 0 && offset != nil {
		minOffset := offset.Offset(pagesToRead[0])
		maxOffset := offset.Offset(pagesToRead[len(pagesToRead)-1]) + offset.CompressedPageSize(pagesToRead[len(pagesToRead)-1])
		return maxOffset - minOffset
	}
	return 0
}

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}
