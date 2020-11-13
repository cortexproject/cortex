package scanner

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

type file struct {
	mu   sync.Mutex
	file *os.File
	comp io.WriteCloser
	enc  *json.Encoder
}

// Provides serialized access to writing entries.
type openFiles struct {
	mu    sync.Mutex
	files map[string]*file

	openFiles prometheus.Gauge
}

func newOpenFiles(openFilesGauge prometheus.Gauge) *openFiles {
	of := &openFiles{
		files:     map[string]*file{},
		openFiles: openFilesGauge,
	}

	return of
}

func (of *openFiles) appendJSONEntryToFile(dir, filename string, data interface{}, headerFn func() interface{}) error {
	f, err := of.getFile(dir, filename, headerFn)
	if err != nil {
		return err
	}

	// To avoid mixed output from different writes, make sure to serialize access to the file.
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.enc.Encode(data)
}

func (of *openFiles) getFile(dir, filename string, headerFn func() interface{}) (*file, error) {
	of.mu.Lock()
	defer of.mu.Unlock()

	name := filepath.Join(dir, filename+".snappy")

	f := of.files[name]
	if f == nil {
		err := os.MkdirAll(dir, os.FileMode(0700))
		if err != nil {
			return nil, err
		}

		fl, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}

		comp := snappy.NewBufferedWriter(fl)
		enc := json.NewEncoder(comp)
		enc.SetEscapeHTML(false)

		if headerFn != nil {
			err := enc.Encode(headerFn())
			if err != nil {
				_ = fl.Close()
			}
		}

		f = &file{
			file: fl,
			comp: comp,
			enc:  enc,
		}
		of.files[name] = f
		of.openFiles.Inc()
	}

	return f, nil
}

func (of *openFiles) closeAllFiles(footerFn func() interface{}) error {
	of.mu.Lock()
	defer of.mu.Unlock()

	errs := tsdb_errors.NewMulti()

	for fn, f := range of.files {
		delete(of.files, fn)
		of.openFiles.Dec()

		if footerFn != nil {
			errs.Add(f.enc.Encode(footerFn()))
		}

		errs.Add(f.comp.Close())
		errs.Add(f.file.Close())
	}

	return errs.Err()
}
