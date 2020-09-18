package builder

import (
	"encoding/gob"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/tsdb/errors"
)

type symbolsIterator struct {
	files []*symbolsFile
	errs  errors.MultiError

	// To avoid returning duplicates, we remember last returned symbol.
	lastReturned *string
}

func newSymbolsIterator(files []*symbolsFile) *symbolsIterator {
	si := &symbolsIterator{
		files: files,
	}
	si.buildHeap()
	return si
}

func (sit *symbolsIterator) buildHeap() {
	// All files on the heap must have at least one element, so that "heapify" can order them.
	// Here we verify that, and remove files with no more elements.
	for ix := 0; ix < len(sit.files); {
		f := sit.files[ix]
		next, err := f.hasNext()

		if err != nil {
			sit.errs.Add(err)
			return
		}

		if !next {
			sit.errs.Add(f.close())
			sit.files = append(sit.files[:ix], sit.files[ix+1:]...)
			continue
		}

		ix++
	}

	// Build heap, start with leaf nodes, and work towards to root. See comment at heapify for more details.
	for ix := len(sit.files) - 1; ix >= 0; ix-- {
		heapifySymbols(sit.files, ix)
	}
}

// Next advances iterator forward, and returns next element. If there is no next element, returns false.
func (sit *symbolsIterator) Next() (string, bool) {
again:
	if sit.errs.Err() != nil {
		return "", false
	}

	if len(sit.files) == 0 {
		return "", false
	}

	result := sit.files[0].pop()

	hasNext, err := sit.files[0].hasNext()
	sit.errs.Add(err)

	if !hasNext {
		sit.errs.Add(sit.files[0].close())

		// Move last file to the front, and heapify from there.
		sit.files[0] = sit.files[len(sit.files)-1]
		sit.files = sit.files[:len(sit.files)-1]
	}

	heapifySymbols(sit.files, 0)

	if sit.lastReturned == nil || *sit.lastReturned != result {
		sit.lastReturned = &result
		return result, true
	}

	// Duplicate symbol, try next one.
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

func heapifySymbols(files []*symbolsFile, ix int) {
	heapify(len(files), ix, func(i, j int) bool {
		return files[i].peek() < files[j].peek()
	}, func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})
}

type symbolsFile struct {
	f   *os.File
	dec *gob.Decoder

	next       bool
	nextSymbol string
}

func newSymbolsFile(f *os.File) *symbolsFile {
	sn := snappy.NewReader(f)
	dec := gob.NewDecoder(sn)

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
	sf.nextSymbol = s
	return true, nil
}

func (sf *symbolsFile) peek() string {
	if !sf.next {
		panic("no next symbol")
	}

	return sf.nextSymbol
}

func (sf *symbolsFile) pop() string {
	if !sf.next {
		panic("no next symbol")
	}

	sf.next = false
	return sf.nextSymbol
}
