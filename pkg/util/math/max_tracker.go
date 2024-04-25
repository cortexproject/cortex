package math

import "go.uber.org/atomic"

type MaxTracker struct {
	current atomic.Int64
	old     atomic.Int64
}

func (m *MaxTracker) Track(max int64) {
	if l := m.current.Load(); l < max {
		m.current.CompareAndSwap(l, max)
	}
}

func (m *MaxTracker) Tick() {
	m.old.Store(m.current.Load())
	m.current.Store(0)
}

func (m *MaxTracker) Load() int64 {
	c := m.current.Load()
	o := m.old.Load()
	return max(c, o)
}
