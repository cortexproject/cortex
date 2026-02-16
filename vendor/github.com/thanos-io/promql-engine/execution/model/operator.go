// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
)

// VectorOperator performs operations on series in step by step fashion.
type VectorOperator interface {
	// Next yields vectors of samples from all series for one or more execution steps.
	// The caller provides a buffer (buf) to be filled with StepVectors.
	// Returns the number of StepVectors written to buf and any error encountered.
	// A return value of 0 indicates no more data is available.
	Next(ctx context.Context, buf []StepVector) (int, error)

	// Series returns all series that the operator will process during Next results.
	// The result can be used by upstream operators to allocate output tables and buffers
	// before starting to process samples.
	Series(ctx context.Context) ([]labels.Labels, error)

	// Explain returns human-readable explanation of the current operator and optional nested operators.
	Explain() (next []VectorOperator)

	fmt.Stringer
}
