// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"slices"

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

// Reset resets the StepVector to the given timestamp while preserving slice capacity.
func (s *StepVector) Reset(t int64) {
	s.T = t
	if s.SampleIDs != nil {
		s.SampleIDs = s.SampleIDs[:0]
	}
	if s.Samples != nil {
		s.Samples = s.Samples[:0]
	}
	if s.HistogramIDs != nil {
		s.HistogramIDs = s.HistogramIDs[:0]
	}
	if s.Histograms != nil {
		s.Histograms = s.Histograms[:0]
	}
}

func (s *StepVector) AppendSample(id uint64, val float64) {
	s.SampleIDs = append(s.SampleIDs, id)
	s.Samples = append(s.Samples, val)
}

// AppendSampleWithSizeHint appends a sample and lazily pre-allocates capacity if needed.
// Use this when you know the expected number of samples to avoid repeated slice growth.
func (s *StepVector) AppendSampleWithSizeHint(id uint64, val float64, hint int) {
	if s.SampleIDs == nil || cap(s.SampleIDs) < hint {
		newSampleIDs := make([]uint64, len(s.SampleIDs), hint)
		copy(newSampleIDs, s.SampleIDs)
		s.SampleIDs = newSampleIDs

		newSamples := make([]float64, len(s.Samples), hint)
		copy(newSamples, s.Samples)
		s.Samples = newSamples
	}
	s.SampleIDs = append(s.SampleIDs, id)
	s.Samples = append(s.Samples, val)
}

func (s *StepVector) AppendSamples(ids []uint64, vals []float64) {
	if len(ids) == 0 && len(vals) == 0 {
		return
	}
	s.SampleIDs = append(s.SampleIDs, ids...)
	s.Samples = append(s.Samples, vals...)
}

func (s *StepVector) RemoveSample(index int) {
	s.Samples = slices.Delete(s.Samples, index, index+1)
	s.SampleIDs = slices.Delete(s.SampleIDs, index, index+1)
}

func (s *StepVector) AppendHistogram(histogramID uint64, h *histogram.FloatHistogram) {
	s.HistogramIDs = append(s.HistogramIDs, histogramID)
	s.Histograms = append(s.Histograms, h)
}

// AppendHistogramWithSizeHint appends a histogram and lazily pre-allocates capacity if needed.
// Use this when you know the expected number of histograms to avoid repeated slice growth.
func (s *StepVector) AppendHistogramWithSizeHint(histogramID uint64, h *histogram.FloatHistogram, hint int) {
	if s.HistogramIDs == nil || cap(s.HistogramIDs) < hint {
		newHistogramIDs := make([]uint64, len(s.HistogramIDs), hint)
		copy(newHistogramIDs, s.HistogramIDs)
		s.HistogramIDs = newHistogramIDs

		newHistograms := make([]*histogram.FloatHistogram, len(s.Histograms), hint)
		copy(newHistograms, s.Histograms)
		s.Histograms = newHistograms
	}
	s.HistogramIDs = append(s.HistogramIDs, histogramID)
	s.Histograms = append(s.Histograms, h)
}

func (s *StepVector) AppendHistograms(histogramIDs []uint64, hs []*histogram.FloatHistogram) {
	if len(histogramIDs) == 0 && len(hs) == 0 {
		return
	}
	s.HistogramIDs = append(s.HistogramIDs, histogramIDs...)
	s.Histograms = append(s.Histograms, hs...)
}

func (s *StepVector) RemoveHistogram(index int) {
	s.Histograms = slices.Delete(s.Histograms, index, index+1)
	s.HistogramIDs = slices.Delete(s.HistogramIDs, index, index+1)
}
