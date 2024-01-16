// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

type OperatorTelemetry interface {
	AddExecutionTimeTaken(time.Duration)
	ExecutionTimeTaken() time.Duration
	Name() string
}

func NewTelemetry(name string, enabled bool) OperatorTelemetry {
	if enabled {
		return NewTrackedTelemetry(name)
	}
	return NewNoopTelemetry(name)

}

type NoopTelemetry struct {
	name string
}

func NewNoopTelemetry(name string) *NoopTelemetry {
	return &NoopTelemetry{name: name}
}

func (tm *NoopTelemetry) Name() string { return tm.name }

func (tm *NoopTelemetry) AddExecutionTimeTaken(t time.Duration) {}

func (tm *NoopTelemetry) ExecutionTimeTaken() time.Duration {
	return time.Duration(0)
}

type TrackedTelemetry struct {
	name          string
	ExecutionTime time.Duration
}

func NewTrackedTelemetry(name string) *TrackedTelemetry {
	return &TrackedTelemetry{name: name}
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
	Explain() (me string, next []VectorOperator)
}
