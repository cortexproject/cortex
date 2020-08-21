package blocksconvert

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type file struct {
	mu  sync.Mutex
	f   *os.File
	buf *bufio.Writer
	enc *json.Encoder
}

// Provides serialized access to writing entries.
type openFiles struct {
	bufferSize int

	mu    sync.Mutex
	files map[string]*file

	openFiles prometheus.Gauge
}

func newOpenFiles(bufferSize int, openFilesGauge prometheus.Gauge) *openFiles {
	of := &openFiles{
		bufferSize: bufferSize,
		files:      map[string]*file{},

		openFiles: openFilesGauge,
	}

	return of
}

func (of *openFiles) appendJsonEntryToFile(dir, filename string, data interface{}, headerFn func() interface{}) error {
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

	name := filepath.Join(dir, filename)

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

		buf := bufio.NewWriterSize(fl, of.bufferSize)

		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)

		if headerFn != nil {
			err := enc.Encode(headerFn())
			if err != nil {
				_ = fl.Close()
			}
		}

		f = &file{
			f:   fl,
			buf: buf,
			enc: enc,
		}
		of.files[name] = f
		of.openFiles.Inc()
	}

	return f, nil
}

func (of *openFiles) closeAllFiles(footerFn func() interface{}) []error {
	of.mu.Lock()
	defer of.mu.Unlock()

	var errors []error

	for fn, f := range of.files {
		delete(of.files, fn)
		of.openFiles.Dec()

		if footerFn != nil {
			err := f.enc.Encode(footerFn())
			if err != nil {
				errors = append(errors, err)
			}
		}

		err := f.buf.Flush()
		if err != nil {
			errors = append(errors, err)
		}

		err = f.f.Close()
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}
