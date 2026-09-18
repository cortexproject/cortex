// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package ringbuffer

import (
	"context"
	"math"

	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/prometheus/prometheus/model/histogram"
)

// Buffer owns sample admission for a range window. Reset establishes the
// window for an evaluation step, then Push decides whether each candidate
// belongs in that window.
type Buffer interface {
	MaxT() int64
	Push(t int64, v Value)
	Reset(mint int64, evalt int64)
	Eval(ctx context.Context, _, _ float64) (float64, *histogram.FloatHistogram, bool, warnings.Warnings, error)
	SampleCount() int
}

func Empty(b Buffer) bool { return b.MaxT() == math.MinInt64 }

type Value struct {
	F float64
	H *histogram.FloatHistogram
}

type Sample struct {
	T int64
	V Value
}

type GenericRingBuffer struct {
	ctx   context.Context
	items []Sample
	tail  []Sample

	currentStep       int64
	currentRangeStart int64
	offset            int64
	selectRange       int64
	call              FunctionCall
}

func New(ctx context.Context, size int, selectRange, offset int64, call FunctionCall) *GenericRingBuffer {
	return &GenericRingBuffer{
		ctx:         ctx,
		items:       make([]Sample, 0, size),
		selectRange: selectRange,
		offset:      offset,
		call:        call,
	}
}

func (r *GenericRingBuffer) SampleCount() int {
	c := 0
	for _, s := range r.items {
		if s.V.H != nil {
			c += telemetry.CalculateHistogramSampleCount(s.V.H)
			continue
		}
		c++
	}
	return c
}

// MaxT returns the maximum timestamp of the ring buffer.
// If the ring buffer is empty, it returns math.MinInt64.
func (r *GenericRingBuffer) MaxT() int64 {
	if len(r.items) == 0 {
		return math.MinInt64
	}
	return r.items[len(r.items)-1].T
}

// Push considers a sample for the current range.
func (r *GenericRingBuffer) Push(t int64, v Value) {
	if t <= r.currentRangeStart {
		return
	}
	r.push(t, v)
}

// push adds a sample without applying the current range boundary.
func (r *GenericRingBuffer) push(t int64, v Value) {
	n := len(r.items)
	if n < cap(r.items) {
		r.items = r.items[:n+1]
	} else {
		r.items = append(r.items, Sample{})
	}
	setSample(&r.items[n], t, v)
}

func setSample(dst *Sample, t int64, v Value) {
	dst.T = t
	dst.V.F = v.F
	if v.H == nil {
		dst.V.H = nil
		return
	}
	if dst.V.H == nil {
		dst.V.H = v.H.Copy()
		return
	}
	v.H.CopyTo(dst.V.H)
}

func (r *GenericRingBuffer) Reset(mint int64, evalt int64) {
	r.currentStep = evalt
	r.currentRangeStart = mint
	if len(r.items) == 0 || r.items[len(r.items)-1].T < mint {
		r.items = r.items[:0]
		return
	}
	var drop int
	for drop = 0; drop < len(r.items) && r.items[drop].T <= mint; drop++ {
	}
	r.drop(drop)
}

func (r *GenericRingBuffer) drop(drop int) {
	keep := len(r.items) - drop
	r.tail = resize(r.tail, drop)
	copy(r.tail, r.items[:drop])
	copy(r.items, r.items[drop:])
	copy(r.items[keep:], r.tail)
	r.items = r.items[:keep]
}

func (r *GenericRingBuffer) Eval(ctx context.Context, scalarArg float64, scalarArg2 float64) (float64, *histogram.FloatHistogram, bool, warnings.Warnings, error) {
	return r.eval(scalarArg, scalarArg2, math.MinInt64)
}

func (r *GenericRingBuffer) eval(scalarArg float64, scalarArg2 float64, metricAppearedTs int64) (float64, *histogram.FloatHistogram, bool, warnings.Warnings, error) {
	return r.call(FunctionArgs{
		Samples:          r.items,
		StepTime:         r.currentStep,
		SelectRange:      r.selectRange,
		Offset:           r.offset,
		ScalarPoint:      scalarArg,
		ScalarPoint2:     scalarArg2, // only for double_exponential_smoothing
		MetricAppearedTs: metricAppearedTs,
	})
}

func resize(s []Sample, n int) []Sample {
	if cap(s) >= n {
		return s[:n]
	}
	return make([]Sample, n)
}
