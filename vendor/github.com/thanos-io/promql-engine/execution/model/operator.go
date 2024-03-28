// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

type OperatorTelemetry interface {
	AddExecutionTimeTaken(time.Duration)
	ExecutionTimeTaken() time.Duration
	fmt.Stringer
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

type TrackedTelemetry struct {
	name          string
	ExecutionTime time.Duration
	fmt.Stringer
}

func NewTrackedTelemetry(operator fmt.Stringer) *TrackedTelemetry {
	return &TrackedTelemetry{Stringer: operator}
}

func (ti *TrackedTelemetry) Name() string {
	return ti.name
}

func (ti *TrackedTelemetry) AddExecutionTimeTaken(t time.Duration) { ti.ExecutionTime += t }

func (ti *TrackedTelemetry) ExecutionTimeTaken() time.Duration {
	return ti.ExecutionTime
}

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
