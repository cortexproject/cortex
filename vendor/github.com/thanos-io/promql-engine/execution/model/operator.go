// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

type NoopTelemetry struct{}

type TrackedTelemetry struct {
	name          string
	ExecutionTime time.Duration
}

func (ti *NoopTelemetry) AddExecutionTimeTaken(t time.Duration) {}

func (ti *TrackedTelemetry) AddExecutionTimeTaken(t time.Duration) {
	ti.ExecutionTime += t
}

func (ti *TrackedTelemetry) Name() string {
	return ti.name
}

func (ti *TrackedTelemetry) SetName(operatorName string) {
	ti.name = operatorName
}

func (ti *NoopTelemetry) Name() string {
	return ""
}

func (ti *NoopTelemetry) SetName(operatorName string) {}

type OperatorTelemetry interface {
	AddExecutionTimeTaken(time.Duration)
	ExecutionTimeTaken() time.Duration
	SetName(string)
	Name() string
}

func (ti *NoopTelemetry) ExecutionTimeTaken() time.Duration {
	return time.Duration(0)
}

func (ti *TrackedTelemetry) ExecutionTimeTaken() time.Duration {
	return ti.ExecutionTime
}

type ObservableVectorOperator interface {
	VectorOperator
	OperatorTelemetry
	Analyze() (OperatorTelemetry, []ObservableVectorOperator)
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
