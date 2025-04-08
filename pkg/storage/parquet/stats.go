package storage

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type Stats struct {
	Mtx sync.Mutex
	// Number of series we overfetched due to chunks time range.
	FilesScanned           int
	ParquetReaderCacheMiss int
	RowsFetched            int
	OverfetchedRows        int
	// Key is column name and value is how many rows after searching the column
	ColumnsSearched           map[string]int
	ColumnsMaterialize        map[string]struct{}
	PagesSearchScanned        map[string]int
	PagesMaterializeScanned   map[string]int
	PagesSkipped              map[string]int
	PagesSearchSizeBytes      map[string]int64
	PagesMaterializeSizeBytes map[string]int64

	RequestDuration           int64
	OpenParquetReaderDuration int64
	SearchWallTime            int64
	MaterializeWallTime       int64
	CreateSeriesSetWallTime   int64
	StorageWallTime           int64
}

func setToStrings(m map[string]struct{}) string {
	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}
	sort.Strings(res)
	return strings.Join(res, ",")
}

func mapToString(m map[string]int) string {
	type kv struct {
		key   string
		value int64
	}
	kvs := make([]kv, 0, len(m))
	for k, v := range m {
		kvs = append(kvs, kv{key: k, value: int64(v)})
	}
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].value > kvs[j].value
	})
	sb := strings.Builder{}
	for i, kv := range kvs {
		sb.WriteString(kv.key)
		sb.WriteString(":")
		sb.WriteString(strconv.FormatInt(kv.value, 10))
		if i != len(kvs)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func mapToStringInt64(m map[string]int64) string {
	type kv struct {
		key   string
		value int64
	}
	kvs := make([]kv, 0, len(m))
	for k, v := range m {
		kvs = append(kvs, kv{key: k, value: v})
	}
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].value > kvs[j].value
	})
	sb := strings.Builder{}
	for i, kv := range kvs {
		sb.WriteString(kv.key)
		sb.WriteString(":")
		sb.WriteString(strconv.FormatInt(kv.value, 10))
		if i != len(kvs)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func NewStats() *Stats {
	return &Stats{
		ColumnsSearched:           make(map[string]int),
		ColumnsMaterialize:        make(map[string]struct{}),
		PagesSearchScanned:        make(map[string]int),
		PagesMaterializeScanned:   make(map[string]int),
		PagesSkipped:              make(map[string]int),
		PagesSearchSizeBytes:      make(map[string]int64),
		PagesMaterializeSizeBytes: make(map[string]int64),
	}
}

func (s *Stats) Display(logger log.Logger, extraFields ...interface{}) {
	pagesSkippedTotal := 0
	for _, n := range s.PagesSkipped {
		pagesSkippedTotal += n
	}
	pagesSearchSizeBytesTotal := int64(0)
	for _, n := range s.PagesSearchSizeBytes {
		pagesSearchSizeBytesTotal += n
	}
	pagesMaterializeSizeBytesTotal := int64(0)
	for _, n := range s.PagesMaterializeSizeBytes {
		pagesMaterializeSizeBytesTotal += n
	}
	fields := []interface{}{
		"msg", "stats",
		"filesScanned", s.FilesScanned,
		"rowsFetched", s.RowsFetched,
		"overfetchedRows", s.OverfetchedRows,
		"columnsMaterialize", setToStrings(s.ColumnsMaterialize),
		"columnsSearched", mapToString(s.ColumnsSearched),
		"pagesSearchScanned", mapToString(s.PagesSearchScanned),
		"pagesMaterializeScanned", mapToString(s.PagesMaterializeScanned),
		"pagesSkipped", mapToString(s.PagesSkipped),
		"totalPagesSkipped", pagesSkippedTotal,
		"pagesSearchSizeBytes", mapToStringInt64(s.PagesSearchSizeBytes),
		"pagesSearchSizeBytesTotal", pagesSearchSizeBytesTotal,
		"pagesMaterializeSizeBytes", mapToStringInt64(s.PagesMaterializeSizeBytes),
		"pagesMaterializeSizeBytesTotal", pagesMaterializeSizeBytesTotal,
		"pagesSizeBytesTotal", pagesSearchSizeBytesTotal + pagesMaterializeSizeBytesTotal,
		"parquetReaderCacheMiss", s.ParquetReaderCacheMiss,
		"duration", s.RequestDuration,
		"openParquetReaderDuration", s.OpenParquetReaderDuration,
		"searchWallTime", s.SearchWallTime,
		"materializeWallTime", s.MaterializeWallTime,
		"createSeriesSetWallTime", s.CreateSeriesSetWallTime,
		"storageWallTime", s.StorageWallTime,
	}
	fields = append(fields, extraFields...)
	level.Info(logger).Log(fields...)
}

func (s *Stats) AddRowsFetched(rowsFetched int) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.RowsFetched += rowsFetched
}

func (s *Stats) AddFilesScanned(filesScanned int) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.FilesScanned += filesScanned
}

func (s *Stats) AddParquetReaderCacheMiss(miss int) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.ParquetReaderCacheMiss += miss
}

func (s *Stats) AddOverfetchedRows(overfetchedRows int) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.OverfetchedRows += overfetchedRows
}

func (s *Stats) AddPagesSearchScanned(col string, pages int) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.PagesSearchScanned[col] += pages
}

func (s *Stats) AddPagesMaterializeScanned(col string, pages int) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.PagesMaterializeScanned[col] += pages
}

func (s *Stats) AddPagesSkipped(col string, pages int) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.PagesSkipped[col] += pages
}

func (s *Stats) AddPagesSearchSizeBytes(col string, size int64) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.PagesSearchSizeBytes[col] += size
}

func (s *Stats) AddPagesMaterializeSizeBytes(col string, size int64) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.PagesMaterializeSizeBytes[col] += size
}

func (s *Stats) AddColumnSearched(col string, rows int) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.ColumnsSearched[col] += rows
}

func (s *Stats) AddColumnMaterialized(col string) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.ColumnsMaterialize[col] = struct{}{}
}

func (s *Stats) AddRequestDuration(d int64) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.RequestDuration += d
}

func (s *Stats) AddOpenParquetReaderDuration(d int64) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.OpenParquetReaderDuration += d
}

func (s *Stats) AddSearchWallTime(d int64) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.SearchWallTime += d
}

func (s *Stats) AddMaterializeWallTime(d int64) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.MaterializeWallTime += d
}

func (s *Stats) AddCreateSeriesSetWallTime(d int64) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.CreateSeriesSetWallTime += d
}

func (s *Stats) AddStorageWallTime(d int64) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.StorageWallTime += d
}
