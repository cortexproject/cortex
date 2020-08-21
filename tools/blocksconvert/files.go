package blocksconvert

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	gauge prometheus.GaugeFunc
}

func newOpenFiles(bufferSize int, reg prometheus.Registerer) *openFiles {
	of := &openFiles{
		bufferSize: bufferSize,
		files:      map[string]*file{},
	}

	of.gauge = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "open_files",
		Help: "Number of open filed",
	}, func() float64 {
		of.mu.Lock()
		defer of.mu.Unlock()

		return float64(len(of.files))
	})

	return of
}

func (of *openFiles) appendJsonEntryToFile(dir, filename string, data interface{}, initialEntry func() interface{}) error {
	f, err := of.getFile(dir, filename, initialEntry)
	if err != nil {
		return err
	}

	// To avoid mixed output from different writes, make sure to serialize access to the file.
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.enc.Encode(data)
}

func (of *openFiles) getFile(dir, filename string, initialEntry func() interface{}) (*file, error) {
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

		if initialEntry != nil {
			err := enc.Encode(initialEntry())
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
	}

	return f, nil
}

func (of *openFiles) closeAllFiles(footer func() interface{}) []error {
	of.mu.Lock()
	defer of.mu.Unlock()

	var errors []error

	for fn, f := range of.files {
		delete(of.files, fn)

		if footer != nil {
			err := f.enc.Encode(footer())
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
