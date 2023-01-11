// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"sync"
)

type VectorPool struct {
	vectors sync.Pool

	stepSize  int
	samples   sync.Pool
	sampleIDs sync.Pool
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
	return StepVector{
		T:         t,
		SampleIDs: *p.sampleIDs.Get().(*[]uint64),
		Samples:   *p.samples.Get().(*[]float64),
	}
}

func (p *VectorPool) PutStepVector(v StepVector) {
	v.SampleIDs = v.SampleIDs[:0]
	v.Samples = v.Samples[:0]
	p.sampleIDs.Put(&v.SampleIDs)
	p.samples.Put(&v.Samples)
}

func (p *VectorPool) SetStepSize(n int) {
	p.stepSize = n
}
