// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package ringbuffer

import (
	"context"
	"math"
	"slices"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/thanos-io/promql-engine/query"
)

type Buffer interface {
	Len() int
	MaxT() int64
	Push(t int64, v Value)
	Reset(mint int64, evalt int64)
	Eval(ctx context.Context, _, _ float64, _ *int64) (float64, *histogram.FloatHistogram, bool, error)
	ReadIntoLast(f func(*Sample))
}

// RateBuffer is a Buffer which can calculate rate, increase and delta for a
// series in a streaming manner, calculating the value incrementally for each
// step where the sample is used.
type RateBuffer struct {
	ctx context.Context
	// stepRanges contain the bounds and number of samples for each evaluation step.
	stepRanges []stepRange
	// firstSamples contains the first sample for each evaluation step.
	firstSamples []Sample
	// resets contains all samples which are detected as a counter reset.
	resets []Sample
	// rateBuffer is the buffer passed to the rate function. This is a scratch buffer
	// used to avoid allocating a new slice each time we need to calculate the rate.
	rateBuffer []Sample
	// last is the last sample in the current evaluation step.
	last Sample

	currentMint int64
	selectRange int64
	step        int64
	offset      int64
	isCounter   bool
	isRate      bool

	evalTs int64
}

type stepRange struct {
	mint       int64
	maxt       int64
	numSamples int
}

// NewRateBuffer creates a new RateBuffer.
func NewRateBuffer(ctx context.Context, opts query.Options, isCounter, isRate bool, selectRange, offset int64) *RateBuffer {
	var (
		step     = max(1, opts.Step.Milliseconds())
		numSteps = min(
			(selectRange-1)/step+1,
			querySteps(opts),
		)

		current      = opts.Start.UnixMilli()
		firstSamples = make([]Sample, 0, numSteps)
		stepRanges   = make([]stepRange, 0, numSteps)
	)
	for i := 0; i < int(numSteps); i++ {
		var (
			maxt = current - offset
			mint = maxt - selectRange
		)
		stepRanges = append(stepRanges, stepRange{mint: mint, maxt: maxt})
		firstSamples = append(firstSamples, Sample{T: math.MaxInt64})
		current += step
	}

	return &RateBuffer{
		ctx:          ctx,
		isCounter:    isCounter,
		isRate:       isRate,
		selectRange:  selectRange,
		step:         step,
		offset:       offset,
		stepRanges:   stepRanges,
		firstSamples: firstSamples,
		last:         Sample{T: math.MinInt64},
		currentMint:  math.MaxInt64,
	}
}

func (r *RateBuffer) Len() int { return r.stepRanges[0].numSamples }

func (r *RateBuffer) MaxT() int64 { return r.last.T }

func (r *RateBuffer) Push(t int64, v Value) {
	// Detect resets and store the current and previous sample so that
	// the rate is properly adjusted.
	if r.last.T >= r.currentMint && v.H != nil && r.last.V.H != nil {
		if v.H.DetectReset(r.last.V.H) {
			r.resets = append(r.resets, Sample{
				T: r.last.T,
				V: Value{H: r.last.V.H.Copy()},
			})
			r.resets = append(r.resets, Sample{
				T: t,
				V: Value{H: v.H.Copy()},
			})
		}
	} else if r.last.T >= r.currentMint && r.last.V.F > v.F {
		r.resets = append(r.resets, Sample{T: r.last.T, V: Value{F: r.last.V.F}})
		r.resets = append(r.resets, Sample{T: t, V: Value{F: v.F}})
	}

	// Set the last sample for the current evaluation step.
	r.last.T, r.last.V.F = t, v.F
	if v.H != nil {
		if r.last.V.H == nil {
			r.last.V.H = v.H.Copy()
		} else {
			v.H.CopyTo(r.last.V.H)
		}
	} else {
		r.last.V.H = nil
	}

	// Set the first sample for each evaluation step where the currently read sample is used.
	for i := 0; i < len(r.stepRanges) && t > r.stepRanges[i].mint && t <= r.stepRanges[i].maxt; i++ {
		r.stepRanges[i].numSamples++
		sample := &r.firstSamples[i]
		if t >= sample.T {
			continue
		}
		sample.T, sample.V.F = t, v.F
		if v.H != nil {
			if sample.V.H == nil {
				sample.V.H = v.H.Copy()
			} else {
				v.H.CopyTo(sample.V.H)
			}
		} else {
			sample.V.H = nil
		}
	}
}

func (r *RateBuffer) Reset(mint int64, evalt int64) {
	r.currentMint, r.evalTs = mint, evalt
	if r.stepRanges[0].mint == mint {
		return
	}
	dropResets := 0
	for ; dropResets < len(r.resets) && r.resets[dropResets].T <= mint; dropResets++ {
	}
	r.resets = r.resets[dropResets:]

	last := len(r.stepRanges) - 1
	var (
		nextMint = r.stepRanges[last].mint + r.step
		nextMaxt = r.stepRanges[last].maxt + r.step
	)
	copy(r.stepRanges, r.stepRanges[1:])
	r.stepRanges[last] = stepRange{mint: nextMint, maxt: nextMaxt}

	nextSample := r.firstSamples[0]
	copy(r.firstSamples, r.firstSamples[1:])
	r.firstSamples[last] = nextSample
	r.firstSamples[last].T = math.MaxInt64
}

func (r *RateBuffer) Eval(ctx context.Context, _, _ float64, _ *int64) (float64, *histogram.FloatHistogram, bool, error) {
	if r.firstSamples[0].T == math.MaxInt64 || r.firstSamples[0].T == r.last.T {
		return 0, nil, false, nil
	}

	r.rateBuffer = append(append(
		append(r.rateBuffer[:0], r.firstSamples[0]),
		r.resets...),
		r.last,
	)
	r.rateBuffer = slices.CompactFunc(r.rateBuffer, func(s1 Sample, s2 Sample) bool { return s1.T == s2.T })
	numSamples := r.stepRanges[0].numSamples
	return extrapolatedRate(ctx, r.rateBuffer, numSamples, r.isCounter, r.isRate, r.evalTs, r.selectRange, r.offset)
}

func (r *RateBuffer) ReadIntoLast(func(*Sample)) {}

func querySteps(o query.Options) int64 {
	// Instant evaluation is executed as a range evaluation with one step.
	if o.Step.Milliseconds() == 0 {
		return 1
	}

	return (o.End.UnixMilli()-o.Start.UnixMilli())/o.Step.Milliseconds() + 1
}
