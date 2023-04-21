// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"time"
)

type Options struct {
	Start            time.Time
	End              time.Time
	Step             time.Duration
	LookbackDelta    time.Duration
	ExtLookbackDelta time.Duration

	StepsBatch int64
}

func (o *Options) NumSteps() int {
	// Instant evaluation is executed as a range evaluation with one step.
	if o.Step.Milliseconds() == 0 {
		return 1
	}

	totalSteps := (o.End.UnixMilli()-o.Start.UnixMilli())/o.Step.Milliseconds() + 1
	if o.StepsBatch < totalSteps {
		return int(o.StepsBatch)
	}
	return int(totalSteps)
}

func (o *Options) WithEndTime(end time.Time) *Options {
	result := *o
	result.End = end
	return &result
}
