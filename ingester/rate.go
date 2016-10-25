package ingester

import (
	"sync"
	"sync/atomic"
	"time"
)

type ewmaRate struct {
	newEvents int64
	alpha     float64
	interval  time.Duration
	lastRate  float64
	init      bool
	mutex     sync.Mutex
}

func newEWMARate(alpha float64, interval time.Duration) *ewmaRate {
	return &ewmaRate{
		alpha:    alpha,
		interval: interval,
	}
}

func (r *ewmaRate) rate() float64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastRate
}

func (r *ewmaRate) tick() {
	newEvents := atomic.LoadInt64(&r.newEvents)
	atomic.AddInt64(&r.newEvents, -newEvents)
	instantRate := float64(newEvents) / r.interval.Seconds()

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.init {
		r.lastRate += r.alpha * (instantRate - r.lastRate)
	} else {
		r.init = true
		r.lastRate = instantRate
	}
}

func (r *ewmaRate) inc() {
	atomic.AddInt64(&r.newEvents, 1)
}
