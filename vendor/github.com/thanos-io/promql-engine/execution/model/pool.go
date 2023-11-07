// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"sync"

	"github.com/prometheus/prometheus/model/histogram"
)

type VectorPool struct {
	vectors sync.Pool

	stepSize   int
	samples    sync.Pool
	sampleIDs  sync.Pool
	histograms sync.Pool
}

func NewVectorPoolWithSize(stepsBatch, size int) *VectorPool {
	pool := NewVectorPool(stepsBatch)
	pool.SetStepSize(size)
	return pool
}

func NewVectorPool(stepsBatch int) *VectorPool {
	pool := &VectorPool{}
	pool.vectors = sync.Pool{
		New: func() any {
			sv := make([]StepVector, 0, stepsBatch)
			return &sv
		},
	}
	pool.samples = sync.Pool{
		New: func() any {
			samples := make([]float64, 0, pool.stepSize)
			return &samples
		},
	}
	pool.sampleIDs = sync.Pool{
		New: func() any {
			sampleIDs := make([]uint64, 0, pool.stepSize)
			return &sampleIDs
		},
	}
	pool.histograms = sync.Pool{
		New: func() any {
			histograms := make([]*histogram.FloatHistogram, pool.stepSize)[:0]
			return &histograms
		},
	}

	return pool
}

func (p *VectorPool) GetVectorBatch() []StepVector {
	return *p.vectors.Get().(*[]StepVector)
}

func (p *VectorPool) PutVectors(vector []StepVector) {
	vector = vector[:0]
	p.vectors.Put(&vector)
}

func (p *VectorPool) GetStepVector(t int64) StepVector {
	return StepVector{T: t}
}

func (p *VectorPool) getSampleBuffers() ([]uint64, []float64) {
	return *p.sampleIDs.Get().(*[]uint64), *p.samples.Get().(*[]float64)
}

func (p *VectorPool) getHistogramBuffers() ([]uint64, []*histogram.FloatHistogram) {
	return *p.sampleIDs.Get().(*[]uint64), *p.histograms.Get().(*[]*histogram.FloatHistogram)
}

func (p *VectorPool) PutStepVector(v StepVector) {
	if v.SampleIDs != nil {
		v.SampleIDs = v.SampleIDs[:0]
		p.sampleIDs.Put(&v.SampleIDs)

		v.Samples = v.Samples[:0]
		p.samples.Put(&v.Samples)
	}

	if v.HistogramIDs != nil {
		v.Histograms = v.Histograms[:0]
		p.histograms.Put(&v.Histograms)

		v.HistogramIDs = v.HistogramIDs[:0]
		p.sampleIDs.Put(&v.HistogramIDs)
	}
}

func (p *VectorPool) SetStepSize(n int) {
	p.stepSize = n
}
