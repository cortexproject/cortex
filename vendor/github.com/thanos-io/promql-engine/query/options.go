// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"time"
)

type Options struct {
	Start                    time.Time
	End                      time.Time
	Step                     time.Duration
	StepsBatch               int
	LookbackDelta            time.Duration
	EnablePerStepStats       bool
	ExtLookbackDelta         time.Duration
	NoStepSubqueryIntervalFn func(time.Duration) time.Duration
	EnableAnalysis           bool
	DecodingConcurrency      int
}

func (o *Options) NumSteps() int {
	// Instant evaluation is executed as a range evaluation with one step.
	if o.Step.Milliseconds() == 0 {
		return 1
	}

	totalSteps := (o.End.UnixMilli()-o.Start.UnixMilli())/o.Step.Milliseconds() + 1
	if int64(o.StepsBatch) < totalSteps {
		return o.StepsBatch
	}
	return int(totalSteps)
}

func (o *Options) IsInstantQuery() bool {
	return o.NumSteps() == 1
}

func (o *Options) WithEndTime(end time.Time) *Options {
	result := *o
	result.End = end
	return &result
}

func NestedOptionsForSubquery(opts *Options, step, queryRange, offset time.Duration) *Options {
	nOpts := &Options{
		End:                      opts.End.Add(-offset),
		LookbackDelta:            opts.LookbackDelta,
		StepsBatch:               opts.StepsBatch,
		ExtLookbackDelta:         opts.ExtLookbackDelta,
		NoStepSubqueryIntervalFn: opts.NoStepSubqueryIntervalFn,
		EnableAnalysis:           opts.EnableAnalysis,
		DecodingConcurrency:      opts.DecodingConcurrency,
	}
	if step != 0 {
		nOpts.Step = step
	} else {
		nOpts.Step = opts.NoStepSubqueryIntervalFn(queryRange)
	}
	nOpts.Start = time.UnixMilli(nOpts.Step.Milliseconds() * (opts.Start.Add(-offset-queryRange).UnixMilli() / nOpts.Step.Milliseconds()))
	if nOpts.Start.Before(opts.Start.Add(-offset - queryRange)) {
		nOpts.Start = nOpts.Start.Add(nOpts.Step)
	}
	return nOpts
}
