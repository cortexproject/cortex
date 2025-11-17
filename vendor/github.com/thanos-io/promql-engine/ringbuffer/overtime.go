// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package ringbuffer

import (
	"context"
	"math"

	"github.com/thanos-io/promql-engine/compute"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"
)

// OverTimeBuffer is a Buffer which can calculate [agg]_over_time for a series in a
// streaming manner, calculating the value incrementally for each step where the sample is used.
type OverTimeBuffer struct {
	// stepRanges contain the bounds and number of samples for each evaluation step.
	stepRanges []stepRange
	// stepStates contains the aggregation state for the corresponding stepRange
	stepStates []stepState

	// firstTimestamps contains the timestamp of the first sample for each evaluation step.
	firstTimestamps []int64

	// lastTimestamp is the timestamp of the lsat sample in the current evaluation step
	lastTimestamp int64

	step int64
}

type stepState struct {
	acc  compute.Accumulator
	warn error
}

func newOverTimeBuffer(opts query.Options, selectRange, offset int64, accMaker func() compute.Accumulator) *OverTimeBuffer {
	var (
		step     = max(1, opts.Step.Milliseconds())
		numSteps = min(
			(selectRange-1)/step+1,
			querySteps(opts),
		)

		current         = opts.Start.UnixMilli()
		firstTimestamps = make([]int64, 0, numSteps)
		stepRanges      = make([]stepRange, 0, numSteps)
		stepStates      = make([]stepState, 0, numSteps)
	)
	for range int(numSteps) {
		var (
			maxt = current - offset
			mint = maxt - selectRange
		)
		stepRanges = append(stepRanges, stepRange{mint: mint, maxt: maxt})
		stepStates = append(stepStates, stepState{acc: accMaker()})
		firstTimestamps = append(firstTimestamps, math.MaxInt64)

		current += step
	}

	return &OverTimeBuffer{
		step:            step,
		stepRanges:      stepRanges,
		stepStates:      stepStates,
		firstTimestamps: firstTimestamps,
		lastTimestamp:   math.MinInt64,
	}
}

// NewCountOverTimeBuffer creates a new OverTimeBuffer for the count_over_time function.
func NewCountOverTimeBuffer(opts query.Options, selectRange, offset int64) *OverTimeBuffer {
	return newOverTimeBuffer(opts, selectRange, offset, func() compute.Accumulator { return compute.NewCountAcc() })
}

// NewMaxOverTimeBuffer creates a new OverTimeBuffer for the max_over_time function.
func NewMaxOverTimeBuffer(opts query.Options, selectRange, offset int64) *OverTimeBuffer {
	return newOverTimeBuffer(opts, selectRange, offset, func() compute.Accumulator { return compute.NewMaxAcc() })
}

// NewMinOverTime creates a new OverTimeBuffer for the min_over_time function.
func NewMinOverTimeBuffer(opts query.Options, selectRange, offset int64) *OverTimeBuffer {
	return newOverTimeBuffer(opts, selectRange, offset, func() compute.Accumulator { return compute.NewMinAcc() })
}

// NewSumOverTime creates a new OverTimeBuffer for the sum_over_time function.
func NewSumOverTimeBuffer(opts query.Options, selectRange, offset int64) *OverTimeBuffer {
	return newOverTimeBuffer(opts, selectRange, offset, func() compute.Accumulator { return compute.NewSumAcc() })
}

func (r *OverTimeBuffer) SampleCount() int {
	return r.stepRanges[0].sampleCount
}

func (r *OverTimeBuffer) MaxT() int64 { return r.lastTimestamp }

func (r *OverTimeBuffer) Push(t int64, v Value) {
	// Set the lastSample sample for the current evaluation step.
	r.lastTimestamp = t

	// Set the first sample for each evaluation step where the currently read sample is used.
	for i := 0; i < len(r.stepRanges) && t > r.stepRanges[i].mint && t <= r.stepRanges[i].maxt; i++ {
		r.stepRanges[i].numSamples++
		if v.H != nil {
			r.stepRanges[i].sampleCount += telemetry.CalculateHistogramSampleCount(v.H)
		} else {
			r.stepRanges[i].sampleCount++
		}

		// Aggregate the sample to the current step
		if err := r.stepStates[i].acc.Add(v.F, v.H); err != nil {
			r.stepStates[i].warn = err
			continue
		}

		if fts := r.firstTimestamps[i]; t >= fts {
			continue
		}
		r.firstTimestamps[i] = t
	}
}

func (r *OverTimeBuffer) Reset(mint int64, evalt int64) {
	if r.stepRanges[0].mint == mint {
		return
	}

	lastSample := len(r.stepRanges) - 1
	var (
		nextMint = r.stepRanges[lastSample].mint + r.step
		nextMaxt = r.stepRanges[lastSample].maxt + r.step
	)
	nextStepRange := r.stepRanges[0]
	copy(r.stepRanges, r.stepRanges[1:])
	r.stepRanges[lastSample] = nextStepRange
	r.stepRanges[lastSample].mint = nextMint
	r.stepRanges[lastSample].maxt = nextMaxt
	r.stepRanges[lastSample].sampleCount = 0
	r.stepRanges[lastSample].numSamples = 0

	nextFirstState := r.stepStates[0]
	copy(r.stepStates, r.stepStates[1:])
	r.stepStates[lastSample] = nextFirstState
	r.stepStates[lastSample].acc.Reset(0)
	r.stepStates[lastSample].warn = nil

	nextFirstTimestamp := r.firstTimestamps[0]
	copy(r.firstTimestamps, r.firstTimestamps[1:])
	r.firstTimestamps[lastSample] = nextFirstTimestamp
	r.firstTimestamps[lastSample] = math.MaxInt64
}

func (r *OverTimeBuffer) ReadIntoLast(func(*Sample)) {}

func (r *OverTimeBuffer) Eval(ctx context.Context, _, _ float64, _ int64) (float64, *histogram.FloatHistogram, bool, error) {
	if r.stepStates[0].warn != nil {
		warnings.AddToContext(r.stepStates[0].warn, ctx)
	}

	if r.firstTimestamps[0] == math.MaxInt64 {
		return 0, nil, false, nil
	}

	f, h := r.stepStates[0].acc.Value()

	if r.stepStates[0].acc.ValueType() == compute.MixedTypeValue {
		warnings.AddToContext(annotations.MixedFloatsHistogramsWarning, ctx)
		return 0, nil, false, nil
	}
	return f, h, r.stepStates[0].acc.ValueType() == compute.SingleTypeValue, nil
}
