package builder

import (
	"encoding/gob"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

type seriesIterator struct {
	files []*seriesFile
	errs  []error
}

func newSeriesIterator(files []*seriesFile) *seriesIterator {
	si := &seriesIterator{
		files: files,
	}
	si.buildHeap()
	return si
}

func (sit *seriesIterator) buildHeap() {
	// All files on the heap must have at least one element, so that "heapify" can order them.
	// Here we verify that, and remove files with no more elements.
	for ix := 0; ix < len(sit.files); {
		f := sit.files[ix]
		next, err := f.hasNext()

		if err != nil {
			sit.errs = append(sit.errs, err)
			return
		}

		if !next {
			if err := f.close(); err != nil {
				sit.errs = append(sit.errs, err)
			}
			sit.files = append(sit.files[:ix], sit.files[ix+1:]...)
			continue
		}

		ix++
	}

	// Build heap, start with leaf nodes, and work towards to root. See comment at heapify for more details.
	for ix := len(sit.files) - 1; ix >= 0; ix-- {
		heapifySeries(sit.files, ix)
	}
}

// Next advances iterator forward, and returns next element. If there is no next element, returns false.
func (sit *seriesIterator) Next() (series, bool) {
	if len(sit.errs) > 0 {
		return series{}, false
	}

	if len(sit.files) == 0 {
		return series{}, false
	}

	result := sit.files[0].pop()

	hasNext, err := sit.files[0].hasNext()
	if err != nil {
		sit.errs = append(sit.errs, err)
	}

	if !hasNext {
		if err := sit.files[0].close(); err != nil {
			sit.errs = append(sit.errs, err)
		}

		// Move last file to the front, and heapify from there.
		sit.files[0] = sit.files[len(sit.files)-1]
		sit.files = sit.files[:len(sit.files)-1]
	}

	heapifySeries(sit.files, 0)

	return result, true
}

func (sit *seriesIterator) Error() error {
	return tsdb_errors.NewMulti(sit.errs...).Err()
}

func (sit *seriesIterator) Close() error {
	errs := tsdb_errors.NewMulti()
	for _, f := range sit.files {
		errs.Add(f.close())
	}
	return errs.Err()
}

func heapifySeries(files []*seriesFile, ix int) {
	heapify(len(files), ix, func(i, j int) bool {
		return labels.Compare(files[i].peek().Metric, files[j].peek().Metric) < 0
	}, func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})
}

type seriesFile struct {
	f   *os.File
	dec *gob.Decoder

	next       bool
	nextSeries series
}

func newSeriesFile(f *os.File) *seriesFile {
	sn := snappy.NewReader(f)
	dec := gob.NewDecoder(sn)

	return &seriesFile{
		f:   f,
		dec: dec,
	}
}

func (sf *seriesFile) close() error {
	return sf.f.Close()
}

func (sf *seriesFile) hasNext() (bool, error) {
	if sf.next {
		return true, nil
	}

	var s series
	err := sf.dec.Decode(&s)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}

	sf.next = true
	sf.nextSeries = s
	return true, nil
}

func (sf *seriesFile) peek() series {
	if !sf.next {
		panic("no next symbol")
	}

	return sf.nextSeries
}

func (sf *seriesFile) pop() series {
	if !sf.next {
		panic("no next symbol")
	}

	sf.next = false
	return sf.nextSeries
}
