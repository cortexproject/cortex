// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package telemetry

import (
	"fmt"
	"time"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/util/stats"
)

type OperatorTelemetry interface {
	fmt.Stringer

	AddExecutionTimeTaken(time.Duration)
	ExecutionTimeTaken() time.Duration
	IncrementSamplesAtTimestamp(samples int, t int64)
	Samples() *stats.QuerySamples
	LogicalNode() logicalplan.Node
}

func NewTelemetry(operator fmt.Stringer, opts *query.Options) OperatorTelemetry {
	if opts.EnableAnalysis {
		return NewTrackedTelemetry(operator, opts, nil)
	}
	return NewNoopTelemetry(operator)
}

func NewSubqueryTelemetry(operator fmt.Stringer, opts *query.Options) OperatorTelemetry {
	if opts.EnableAnalysis {
		return NewTrackedTelemetry(operator, opts, &logicalplan.Subquery{})
	}
	return NewNoopTelemetry(operator)
}

func NewStepInvariantTelemetry(operator fmt.Stringer, opts *query.Options) OperatorTelemetry {
	if opts.EnableAnalysis {
		return NewTrackedTelemetry(operator, opts, &logicalplan.StepInvariantExpr{})
	}
	return NewNoopTelemetry(operator)
}

type NoopTelemetry struct {
	fmt.Stringer
}

func NewNoopTelemetry(operator fmt.Stringer) *NoopTelemetry {
	return &NoopTelemetry{Stringer: operator}
}

func (tm *NoopTelemetry) AddExecutionTimeTaken(t time.Duration) {}

func (tm *NoopTelemetry) ExecutionTimeTaken() time.Duration {
	return time.Duration(0)
}

func (tm *NoopTelemetry) IncrementSamplesAtTimestamp(_ int, _ int64) {}

func (tm *NoopTelemetry) Samples() *stats.QuerySamples { return nil }

func (tm *NoopTelemetry) LogicalNode() logicalplan.Node {
	return nil
}

type TrackedTelemetry struct {
	fmt.Stringer

	ExecutionTime time.Duration
	LoadedSamples *stats.QuerySamples
	logicalNode   logicalplan.Node
}

func NewTrackedTelemetry(operator fmt.Stringer, opts *query.Options, logicalPlanNode logicalplan.Node) *TrackedTelemetry {
	ss := stats.NewQuerySamples(opts.EnablePerStepStats)
	ss.InitStepTracking(opts.Start.UnixMilli(), opts.End.UnixMilli(), StepTrackingInterval(opts.Step))
	return &TrackedTelemetry{
		Stringer:      operator,
		LoadedSamples: ss,
		logicalNode:   logicalPlanNode,
	}
}

func StepTrackingInterval(step time.Duration) int64 {
	if step == 0 {
		return 1
	}
	return int64(step / (time.Millisecond / time.Nanosecond))
}

func (ti *TrackedTelemetry) AddExecutionTimeTaken(t time.Duration) { ti.ExecutionTime += t }

func (ti *TrackedTelemetry) ExecutionTimeTaken() time.Duration {
	return ti.ExecutionTime
}

func (ti *TrackedTelemetry) IncrementSamplesAtTimestamp(samples int, t int64) {
	ti.updatePeak(samples)
	ti.LoadedSamples.IncrementSamplesAtTimestamp(t, int64(samples))
}

func (ti *TrackedTelemetry) LogicalNode() logicalplan.Node {
	return ti.logicalNode
}

func (ti *TrackedTelemetry) updatePeak(samples int) {
	ti.LoadedSamples.UpdatePeak(samples)
}

func (ti *TrackedTelemetry) Samples() *stats.QuerySamples { return ti.LoadedSamples }

type ObservableVectorOperator interface {
	model.VectorOperator
	OperatorTelemetry
}
