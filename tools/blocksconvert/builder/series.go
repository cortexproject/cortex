package builder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/errors"
)

type seriesLabels []string

func seriesLabelsLess(a, b seriesLabels) bool {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

type series struct {
	Metric  seriesLabels
	Chunks  []seriesChunk
	MinTime int64
	MaxTime int64
	Samples uint64
}

func (s series) Labels() labels.Labels {
	var result []labels.Label

	for ix := 0; ix < len(s.Metric)-1; ix += 2 {
		result = append(result, labels.Label{
			Name:  s.Metric[ix],
			Value: s.Metric[ix+1],
		})
	}

	return result
}

func (s series) ChunksMetas() []chunks.Meta {
	var result []chunks.Meta

	for _, c := range s.Chunks {
		result = append(result, chunks.Meta{
			Ref:     c.Ref,
			MinTime: c.MinTime,
			MaxTime: c.MaxTime,
		})
	}

	return result
}

type seriesChunk struct {
	Ref     uint64 `json:"ref"`
	MinTime int64  `json:"min"`
	MaxTime int64  `json:"max"`
}

// Keeps series in memory until limit is reached. Then series are sorted, and written to the file.
// Each batch goes to different file.
// When series are iterated, all files are merged (which is each to do, as they are already sorted).
// Symbols are written to different set of files.
type seriesList struct {
	limit int
	dir   string

	mu           sync.Mutex
	sers         []series
	seriesFiles  []string
	symbolsFiles []string
}

func newSeriesList(limit int, dir string) *seriesList {
	return &seriesList{
		limit: limit,
		dir:   dir,
	}
}

func (sl *seriesList) addSeries(m labels.Labels, cs []chunks.Meta, samples uint64, minTime, maxTime int64) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	scs := make([]seriesChunk, 0, len(cs))
	for _, c := range cs {
		scs = append(scs, seriesChunk{
			Ref:     c.Ref,
			MinTime: c.MinTime,
			MaxTime: c.MaxTime,
		})
	}

	metric := make([]string, 0, 2*len(m))
	for _, l := range m {
		metric = append(metric, l.Name, l.Value)
	}

	sl.sers = append(sl.sers, series{
		Metric:  metric,
		Chunks:  scs,
		MinTime: minTime,
		MaxTime: maxTime,
		Samples: samples,
	})

	return sl.flushSeriesNoLock(false)
}

func (sl *seriesList) flushSeries() error {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	return sl.flushSeriesNoLock(true)
}

func (sl *seriesList) flushSeriesNoLock(force bool) error {
	if !force && len(sl.sers) < sl.limit {
		return nil
	}

	// Sort series by labels first.
	sort.Slice(sl.sers, func(i, j int) bool {
		return seriesLabelsLess(sl.sers[i].Metric, sl.sers[j].Metric)
	})

	seriesFile := filepath.Join(sl.dir, fmt.Sprintf("series_%d", len(sl.seriesFiles)))
	symbols, err := writeSeries(seriesFile, sl.sers)
	if err != nil {
		return err
	}

	sl.sers = nil
	sl.seriesFiles = append(sl.seriesFiles, seriesFile)

	// No error so far, let's write symbols too.
	sortedSymbols := make([]string, 0, len(symbols))
	for k, _ := range symbols {
		sortedSymbols = append(sortedSymbols, k)
	}

	sort.Strings(sortedSymbols)

	symbolsFile := filepath.Join(sl.dir, fmt.Sprintf("symbols_%d", len(sl.symbolsFiles)))
	err = writeSymbols(symbolsFile, sortedSymbols)
	if err == nil {
		sl.symbolsFiles = append(sl.symbolsFiles, symbolsFile)
	}

	return err
}

func writeSymbols(filename string, symbols []string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	sn := snappy.NewBufferedWriter(f)
	enc := json.NewEncoder(sn)

	errs := errors.MultiError{}

	for _, s := range symbols {
		err := enc.Encode(s)
		if err != nil {
			errs.Add(err)
			break
		}
	}

	errs.Add(sn.Flush())
	errs.Add(f.Close())
	return errs.Err()
}

func writeSeries(filename string, sers []series) (map[string]struct{}, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}

	errs := errors.MultiError{}

	sn := snappy.NewBufferedWriter(f)
	enc := json.NewEncoder(sn)

	// Write each series as a separate json object, so that we can read them back individually.
	for _, ser := range sers {
		for _, sym := range ser.Metric {
			symbols[sym] = struct{}{}
		}

		err := enc.Encode(ser)
		if err != nil {
			errs.Add(err)
			break
		}
	}

	errs.Add(sn.Flush())
	errs.Add(f.Close())

	return symbols, errs.Err()
}

// Returns iterator over sorted list of symbols. Each symbol is returned once.
func (sl *seriesList) symbolsIterator() (*symbolsIterator, error) {
	sl.mu.Lock()
	filenames := append([]string(nil), sl.symbolsFiles...)
	sl.mu.Unlock()

	files, err := openFiles(filenames)
	if err != nil {
		return nil, err
	}

	var result []*symbolsFile
	for _, f := range files {
		result = append(result, newSymbolsFile(f))
	}

	return newSymbolsIterator(result), nil
}

// Returns iterator over sorted list of series.
func (sl *seriesList) seriesIterator() (*seriesIterator, error) {
	sl.mu.Lock()
	filenames := append([]string(nil), sl.seriesFiles...)
	sl.mu.Unlock()

	files, err := openFiles(filenames)
	if err != nil {
		return nil, err
	}

	var result []*seriesFile
	for _, f := range files {
		result = append(result, newSeriesFile(f))
	}

	return newSeriesIterator(result), nil
}

func openFiles(filenames []string) ([]*os.File, error) {
	var result []*os.File

	for _, fn := range filenames {
		f, err := os.Open(fn)

		if err != nil {
			// Close opened files so far.
			for _, sf := range result {
				_ = sf.Close()
			}
			return nil, err
		}

		result = append(result, f)
	}
	return result, nil
}
