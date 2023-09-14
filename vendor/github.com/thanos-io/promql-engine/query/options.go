// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"time"

	"github.com/thanos-io/promql-engine/parser"
)

type Options struct {
	Context                  context.Context
	Start                    time.Time
	End                      time.Time
	Step                     time.Duration
	StepsBatch               int
	LookbackDelta            time.Duration
	ExtLookbackDelta         time.Duration
	NoStepSubqueryIntervalFn func(time.Duration) time.Duration
	EnableSubqueries         bool
	EnableAnalysis           bool
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

func NestedOptionsForSubquery(opts *Options, t *parser.SubqueryExpr) *Options {
	nOpts := &Options{
		Context:                  opts.Context,
		End:                      opts.End.Add(-t.Offset),
		LookbackDelta:            opts.LookbackDelta,
		StepsBatch:               opts.StepsBatch,
		ExtLookbackDelta:         opts.ExtLookbackDelta,
		NoStepSubqueryIntervalFn: opts.NoStepSubqueryIntervalFn,
		EnableSubqueries:         opts.EnableSubqueries,
	}
	if t.Step != 0 {
		nOpts.Step = t.Step
	} else {
		nOpts.Step = opts.NoStepSubqueryIntervalFn(t.Range)
	}
	nOpts.Start = time.UnixMilli(nOpts.Step.Milliseconds() * (opts.Start.Add(-t.Offset-t.Range).UnixMilli() / nOpts.Step.Milliseconds()))
	if nOpts.Start.Before(opts.Start.Add(-t.Offset - t.Range)) {
		nOpts.Start = nOpts.Start.Add(nOpts.Step)
	}
	return nOpts
}
