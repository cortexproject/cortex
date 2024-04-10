// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/stats"
)

type OperatorTelemetry interface {
	fmt.Stringer

	AddExecutionTimeTaken(time.Duration)
	ExecutionTimeTaken() time.Duration
	IncrementSamplesAtStep(samples int, step int)
	Samples() *stats.QuerySamples
}

func NewTelemetry(operator fmt.Stringer, enabled bool) OperatorTelemetry {
	if enabled {
		return NewTrackedTelemetry(operator)
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

func (tm *NoopTelemetry) IncrementSamplesAtStep(_, _ int) {}

func (tm *NoopTelemetry) Samples() *stats.QuerySamples { return nil }

type TrackedTelemetry struct {
	fmt.Stringer

	ExecutionTime time.Duration
	LoadedSamples *stats.QuerySamples
}

func NewTrackedTelemetry(operator fmt.Stringer) *TrackedTelemetry {
	return &TrackedTelemetry{
		Stringer: operator,
	}
}

func (ti *TrackedTelemetry) AddExecutionTimeTaken(t time.Duration) { ti.ExecutionTime += t }

func (ti *TrackedTelemetry) ExecutionTimeTaken() time.Duration {
	return ti.ExecutionTime
}

func (ti *TrackedTelemetry) IncrementSamplesAtStep(samples, step int) {
	ti.updatePeak(samples)
	if ti.LoadedSamples == nil {
		ti.LoadedSamples = stats.NewQuerySamples(false)
	}
	ti.LoadedSamples.IncrementSamplesAtStep(step, int64(samples))
}

func (ti *TrackedTelemetry) updatePeak(samples int) {
	if ti.LoadedSamples == nil {
		ti.LoadedSamples = stats.NewQuerySamples(false)
	}
	ti.LoadedSamples.UpdatePeak(samples)
}

func (ti *TrackedTelemetry) Samples() *stats.QuerySamples { return ti.LoadedSamples }

type ObservableVectorOperator interface {
	VectorOperator
	OperatorTelemetry
}

// VectorOperator performs operations on series in step by step fashion.
type VectorOperator interface {
	// Next yields vectors of samples from all series for one or more execution steps.
	Next(ctx context.Context) ([]StepVector, error)

	// Series returns all series that the operator will process during Next results.
	// The result can be used by upstream operators to allocate output tables and buffers
	// before starting to process samples.
	Series(ctx context.Context) ([]labels.Labels, error)

	// GetPool returns pool of vectors that can be shared across operators.
	GetPool() *VectorPool

	// Explain returns human-readable explanation of the current operator and optional nested operators.
	Explain() (next []VectorOperator)

	fmt.Stringer
}
