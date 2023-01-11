// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import "github.com/prometheus/prometheus/model/labels"

type Series struct {
	// ID is a numerical, zero-based identifier for a series.
	// It allows using slices instead of maps for fast lookups.
	ID     uint64
	Metric labels.Labels
}

type StepVector struct {
	T         int64
	SampleIDs []uint64
	Samples   []float64
}
