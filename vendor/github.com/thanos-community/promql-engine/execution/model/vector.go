// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

type Series struct {
	// ID is a numerical, zero-based identifier for a series.
	// It allows using slices instead of maps for fast lookups.
	ID     uint64
	Metric labels.Labels
}

type StepVector struct {
	T         int64
	SampleIDs []uint64
	Samples   []float64

	HistogramIDs []uint64
	Histograms   []*histogram.FloatHistogram
}

func (s *StepVector) AppendSample(pool *VectorPool, id uint64, val float64) {
	if s.Samples == nil {
		s.SampleIDs, s.Samples = pool.getSampleBuffers()
	}
	s.SampleIDs = append(s.SampleIDs, id)
	s.Samples = append(s.Samples, val)
}

func (s *StepVector) AppendSamples(pool *VectorPool, ids []uint64, vals []float64) {
	if len(ids) == 0 && len(vals) == 0 {
		return
	}
	if s.Samples == nil {
		s.SampleIDs, s.Samples = pool.getSampleBuffers()
	}
	s.SampleIDs = append(s.SampleIDs, ids...)
	s.Samples = append(s.Samples, vals...)
}

func (s *StepVector) RemoveSample(index int) {
	s.Samples = append(s.Samples[:index], s.Samples[index+1:]...)
	s.SampleIDs = append(s.SampleIDs[:index], s.SampleIDs[index+1:]...)
}

func (s *StepVector) AppendHistogram(pool *VectorPool, histogramID uint64, h *histogram.FloatHistogram) {
	if s.Histograms == nil {
		s.HistogramIDs, s.Histograms = pool.getHistogramBuffers()
	}
	s.HistogramIDs = append(s.HistogramIDs, histogramID)
	s.Histograms = append(s.Histograms, h)
}

func (s *StepVector) AppendHistograms(pool *VectorPool, histogramIDs []uint64, hs []*histogram.FloatHistogram) {
	if len(histogramIDs) == 0 && len(hs) == 0 {
		return
	}
	if s.Histograms == nil {
		s.HistogramIDs, s.Histograms = pool.getHistogramBuffers()
	}
	s.HistogramIDs = append(s.HistogramIDs, histogramIDs...)
	s.Histograms = append(s.Histograms, hs...)
}

func (s *StepVector) RemoveHistogram(index int) {
	s.Histograms = append(s.Histograms[:index], s.Histograms[index+1:]...)
	s.HistogramIDs = append(s.HistogramIDs[:index], s.HistogramIDs[index+1:]...)
}
