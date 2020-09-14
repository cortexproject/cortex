package builder

import (
	"bufio"
	"encoding/json"
	"io"
	"os"

	"github.com/prometheus/prometheus/tsdb/errors"
)

type symbolsIterator struct {
	files []*symbolsFile
	errs  errors.MultiError

	// To avoid returning duplicates, we remember last returned symbol.
	last     string
	returned bool // Used to avoid skipping first "" symbol.
}

func newSymbolsIterator(files []*symbolsFile) *symbolsIterator {
	si := &symbolsIterator{
		files: files,
	}
	si.buildHeap()
	return si
}

func (sit *symbolsIterator) buildHeap() {
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
		heapifySymbols(sit.files, ix)
	}
}

// Advances iterator forward, and returns next element. If there is no next element, returns false.
func (sit *symbolsIterator) Next() (string, bool) {
again:
	if sit.errs.Err() != nil {
		return "", false
	}

	if len(sit.files) == 0 {
		return "", false
	}

	result := sit.files[0].advance()

	hasNext, err := sit.files[0].hasNext()
	sit.errs.Add(err)

	if hasNext {
		heapifySymbols(sit.files, 0)
	} else {
		sit.errs.Add(sit.files[0].close())
		sit.files = sit.files[1:]

		// heapify everything again, to make sure heap is correct.
		for ix := len(sit.files) - 1; ix >= 0; ix-- {
			heapifySymbols(sit.files, ix)
		}
	}

	if !sit.returned || sit.last != result {
		sit.returned = true
		sit.last = result
		return result, true
	}

	// Otherwise try again.
	goto again
}

func (sit *symbolsIterator) Error() error {
	return sit.errs.Err()
}

func (sit *symbolsIterator) Close() error {
	var errs errors.MultiError
	for _, f := range sit.files {
		errs.Add(f.close())
	}
	return errs.Err()
}

// heapify symbols at given index. root is the minimum.
func heapifySymbols(files []*symbolsFile, ix int) {
	smallest := ix
	left := 2*ix + 1
	right := 2*ix + 2

	if left < len(files) && files[left].peek() < files[smallest].peek() {
		smallest = left
	}

	if right < len(files) && files[right].peek() < files[smallest].peek() {
		smallest = right
	}

	if smallest != ix {
		files[ix], files[smallest] = files[smallest], files[ix]
		heapifySymbols(files, smallest)
	}
}

type symbolsFile struct {
	f   *os.File
	dec *json.Decoder

	next   bool
	symbol string
}

func newSymbolsFile(f *os.File) *symbolsFile {
	buf := bufio.NewReader(f)
	dec := json.NewDecoder(buf)

	return &symbolsFile{
		f:   f,
		dec: dec,
	}
}

func (sf *symbolsFile) close() error {
	return sf.f.Close()
}

func (sf *symbolsFile) hasNext() (bool, error) {
	if sf.next {
		return true, nil
	}

	var s string
	err := sf.dec.Decode(&s)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}

	sf.next = true
	sf.symbol = s
	return true, nil
}

func (sf *symbolsFile) peek() string {
	if !sf.next {
		panic("no next symbol")
	}

	return sf.symbol
}

func (sf *symbolsFile) advance() string {
	if !sf.next {
		panic("no next symbol")
	}

	sf.next = false
	return sf.symbol
}
