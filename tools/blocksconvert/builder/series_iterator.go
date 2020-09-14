package builder

import (
	"bufio"
	"encoding/json"
	"io"
	"os"

	"github.com/prometheus/prometheus/tsdb/errors"
)

type seriesIterator struct {
	files []*seriesFile
	errs  errors.MultiError
}

func newSeriesIterator(files []*seriesFile) *seriesIterator {
	si := &seriesIterator{
		files: files,
	}
	si.buildHeap()
	return si
}

func (sit *seriesIterator) buildHeap() {
	for ix := 0; ix < len(sit.files); {
		f := sit.files[ix]
		next, err := f.hasNext()

		if err != nil {
			sit.errs.Add(err)
		}

		if !next {
			sit.errs.Add(f.close())
			sit.files = append(sit.files[:ix], sit.files[ix+1:]...)
			continue
		}

		ix++
	}

	for ix := len(sit.files) - 1; ix >= 0; ix-- {
		heapifySeries(sit.files, ix)
	}
}

// Advances iterator forward, and returns next element. If there is no next element, returns false.
func (sit *seriesIterator) Next() (series, bool) {
	if sit.errs.Err() != nil {
		return series{}, false
	}

	if len(sit.files) == 0 {
		return series{}, false
	}

	result := sit.files[0].advance()

	hasNext, err := sit.files[0].hasNext()
	sit.errs.Add(err)

	if hasNext {
		heapifySeries(sit.files, 0)
	} else {
		sit.errs.Add(sit.files[0].close())
		sit.files = sit.files[1:]

		// heapify everything again, to make sure heap is correct.
		for ix := len(sit.files) - 1; ix >= 0; ix-- {
			heapifySeries(sit.files, ix)
		}
	}

	return result, true
}

func (sit *seriesIterator) Error() error {
	return sit.errs.Err()
}

func (sit *seriesIterator) Close() error {
	var errs errors.MultiError
	for _, f := range sit.files {
		errs.Add(f.close())
	}
	return errs.Err()
}

// heapify series at given index. root is the minimum (only labels are compared)
func heapifySeries(files []*seriesFile, ix int) {
	smallest := ix
	left := 2*ix + 1
	right := 2*ix + 2

	if left < len(files) && seriesLabelsLess(files[left].peek().Metric, files[smallest].peek().Metric) {
		smallest = left
	}

	if right < len(files) && seriesLabelsLess(files[right].peek().Metric, files[smallest].peek().Metric) {
		smallest = right
	}

	if smallest != ix {
		files[ix], files[smallest] = files[smallest], files[ix]
		heapifySeries(files, smallest)
	}
}

type seriesFile struct {
	f   *os.File
	dec *json.Decoder

	next bool
	ser  series
}

func newSeriesFile(f *os.File) *seriesFile {
	buf := bufio.NewReader(f)
	dec := json.NewDecoder(buf)

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
	sf.ser = s
	return true, nil
}

func (sf *seriesFile) peek() series {
	if !sf.next {
		panic("no next symbol")
	}

	return sf.ser
}

func (sf *seriesFile) advance() series {
	if !sf.next {
		panic("no next symbol")
	}

	sf.next = false
	return sf.ser
}
