package heap

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/prometheus/procfs"
)

// Heap will check every checkEvery for memory obtained from the system, by the process
// - using the metric Sys at https://golang.org/pkg/runtime/#MemStats -
// whether it reached or exceeds the threshold and take a memory profile to the path directory,
// but no more often than every minTimeDiff seconds
// any errors will be sent to the errors channel
type Heap struct {
	cfg           Config
	lastTriggered time.Time
	Errors        chan error

	proc *procfs.Proc
}

// Config is the config for triggering profile
type Config struct {
	Path           string
	AllocThreshold int
	RSSThreshold   int
	MinTimeDiff    time.Duration
	CheckEvery     time.Duration
}

// New creates a new Heap trigger. use a nil channel if you don't care about any errors
func New(cfg Config, errors chan error) (*Heap, error) {
	heap := Heap{
		cfg:           cfg,
		lastTriggered: time.Now().Add(-cfg.MinTimeDiff),
		Errors:        errors,
	}
	proc, err := procfs.Self()
	if err != nil {
		heap.logError(err)
	} else {
		heap.proc = &proc
	}

	return &heap, nil
}

func (heap Heap) logError(err error) {
	if heap.Errors != nil {
		select {
		case heap.Errors <- err:
		default:
		}
	}
}

// Run runs the trigger. encountered errors go to the configured channel (if any).
// you probably want to run this in a new goroutine.
func (heap Heap) Run() {
	cfg := heap.cfg
	tick := time.NewTicker(cfg.CheckEvery)

	for ts := range tick.C {
		if heap.shouldProfile(ts) {
			f, err := os.Create(fmt.Sprintf("%s/%d.profile-heap", cfg.Path, ts.Unix()))
			if err != nil {
				heap.logError(err)
				continue
			}
			err = pprof.WriteHeapProfile(f)
			if err != nil {
				heap.logError(err)
			}
			heap.lastTriggered = ts
			err = f.Close()
			if err != nil {
				heap.logError(err)
			}
		}
	}
}

func (heap Heap) shouldProfile(ts time.Time) bool {
	cfg := heap.cfg

	if ts.Before(heap.lastTriggered.Add(cfg.MinTimeDiff)) {
		return false
	}

	// Check RSS.
	if cfg.RSSThreshold != 0 && heap.proc != nil {
		stat, err := heap.proc.NewStat()
		if err != nil {
			heap.logError(err)
		} else if stat.ResidentMemory() >= cfg.RSSThreshold {
			return true
		}
	}

	// Check HeapAlloc
	if cfg.AllocThreshold != 0 {
		m := &runtime.MemStats{}
		runtime.ReadMemStats(m)

		if m.HeapAlloc >= uint64(cfg.AllocThreshold) {
			return true
		}
	}

	return false
}
