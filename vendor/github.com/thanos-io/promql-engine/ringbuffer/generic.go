// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package ringbuffer

import (
	"context"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
)

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

	currentStep int64
	offset      int64
	selectRange int64
	extLookback int64
	call        FunctionCall
}

func New(ctx context.Context, size int, selectRange, offset int64, call FunctionCall) *GenericRingBuffer {
	return NewWithExtLookback(ctx, size, selectRange, offset, 0, call)
}

func NewWithExtLookback(ctx context.Context, size int, selectRange, offset, extLookback int64, call FunctionCall) *GenericRingBuffer {
	return &GenericRingBuffer{
		ctx:         ctx,
		items:       make([]Sample, 0, size),
		selectRange: selectRange,
		offset:      offset,
		extLookback: extLookback,
		call:        call,
	}
}

func (r *GenericRingBuffer) Len() int { return len(r.items) }

// MaxT returns the maximum timestamp of the ring buffer.
// If the ring buffer is empty, it returns math.MinInt64.
func (r *GenericRingBuffer) MaxT() int64 {
	if len(r.items) == 0 {
		return math.MinInt64
	}
	return r.items[len(r.items)-1].T
}

// ReadIntoLast reads a sample into the last slot in the buffer, replacing the existing sample.
func (r *GenericRingBuffer) ReadIntoLast(f func(*Sample)) {
	f(&r.items[len(r.items)-1])
}

// Push adds a new sample to the buffer.
func (r *GenericRingBuffer) Push(t int64, v Value) {
	n := len(r.items)
	if n < cap(r.items) {
		r.items = r.items[:n+1]
	} else {
		r.items = append(r.items, Sample{})
	}

	r.items[n].T = t
	r.items[n].V.F = v.F
	if v.H != nil {
		if r.items[n].V.H == nil {
			r.items[n].V.H = v.H.Copy()
		} else {
			v.H.CopyTo(r.items[n].V.H)
		}
	} else {
		r.items[n].V.H = nil
	}
}

func (r *GenericRingBuffer) Reset(mint int64, evalt int64) {
	r.currentStep = evalt
	if len(r.items) == 0 || r.items[len(r.items)-1].T < mint {
		r.items = r.items[:0]
		return
	}
	var drop int
	for drop = 0; drop < len(r.items) && r.items[drop].T <= mint; drop++ {
	}
	if r.extLookback > 0 && drop > 0 && r.items[drop-1].T >= mint-r.extLookback {
		drop--
	}

	keep := len(r.items) - drop
	r.tail = resize(r.tail, drop)
	copy(r.tail, r.items[:drop])
	copy(r.items, r.items[drop:])
	copy(r.items[keep:], r.tail)
	r.items = r.items[:keep]
}

func (r *GenericRingBuffer) Eval(ctx context.Context, scalarArg float64, scalarArg2 float64, metricAppearedTs *int64) (float64, *histogram.FloatHistogram, bool, error) {
	return r.call(FunctionArgs{
		ctx:              ctx,
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
