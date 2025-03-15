package distributor

import (
	"go.uber.org/atomic"
)

type WriteStats struct {
	// Samples represents X-Prometheus-Remote-Write-Written-Samples
	Samples atomic.Int64
	// Histograms represents X-Prometheus-Remote-Write-Written-Histograms
	Histograms atomic.Int64
	// Exemplars represents X-Prometheus-Remote-Write-Written-Exemplars
	Exemplars atomic.Int64
}

func (w *WriteStats) SetSamples(samples int64) {
	if w == nil {
		return
	}

	w.Samples.Store(samples)
}

func (w *WriteStats) SetHistograms(histograms int64) {
	if w == nil {
		return
	}

	w.Histograms.Store(histograms)
}

func (w *WriteStats) SetExemplars(exemplars int64) {
	if w == nil {
		return
	}

	w.Exemplars.Store(exemplars)
}

func (w *WriteStats) LoadSamples() int64 {
	if w == nil {
		return 0
	}

	return w.Samples.Load()
}

func (w *WriteStats) LoadHistogram() int64 {
	if w == nil {
		return 0
	}

	return w.Histograms.Load()
}

func (w *WriteStats) LoadExemplars() int64 {
	if w == nil {
		return 0
	}

	return w.Exemplars.Load()
}
