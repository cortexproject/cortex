package util

import (
	"fmt"
	"os"
	"time"
)

// Reloader periodically loads the given file and calls the assigned callback function.
type Reloader struct {
	filename       string
	interval       time.Duration
	reloadCallback func(file *os.File, err error)
	quit           chan struct{}
}

// NewReloader returns a *Reloader and starts the reloading loop.
func NewReloader(filename string, interval time.Duration, reloadCallback func(file *os.File, err error)) (*Reloader, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("interval should be >0, got %d", interval)
	}
	if reloadCallback == nil {
		return nil, fmt.Errorf("callback function should not be nil")
	}
	reloader := &Reloader{
		filename:       filename,
		interval:       interval,
		reloadCallback: reloadCallback,
		quit:           make(chan struct{}),
	}
	go reloader.loop()
	return reloader, nil
}

func (r *Reloader) loop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f, err := os.Open(r.filename)
			r.reloadCallback(f, err)
			if err != nil {
				f.Close()
			}
		case <-r.quit:
			return
		}
	}
}

// Stop stops the reloader.
func (r *Reloader) Stop() {
	close(r.quit)
}
