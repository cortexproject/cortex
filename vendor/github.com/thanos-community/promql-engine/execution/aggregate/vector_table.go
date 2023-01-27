// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"fmt"

	"github.com/efficientgo/core/errors"

	"github.com/prometheus/prometheus/promql/parser"
	"gonum.org/v1/gonum/floats"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
)

type vectorAccumulator func([]float64) float64

type vectorTable struct {
	timestamp   int64
	value       float64
	hasValue    bool
	accumulator vectorAccumulator
}

func newVectorizedTables(stepsBatch int, a parser.ItemType) ([]aggregateTable, error) {
	tables := make([]aggregateTable, stepsBatch)
	for i := 0; i < len(tables); i++ {
		accumulator, err := newVectorAccumulator(a)
		if err != nil {
			return nil, err
		}
		tables[i] = newVectorizedTable(accumulator)
	}

	return tables, nil
}

func newVectorizedTable(a vectorAccumulator) *vectorTable {
	return &vectorTable{
		accumulator: a,
	}
}

func (t *vectorTable) aggregate(_ float64, vector model.StepVector) {
	if len(vector.SampleIDs) == 0 {
		t.hasValue = false
		return
	}
	t.hasValue = true
	t.timestamp = vector.T
	t.value = t.accumulator(vector.Samples)
}

func (t *vectorTable) toVector(pool *model.VectorPool) model.StepVector {
	result := pool.GetStepVector(t.timestamp)
	if !t.hasValue {
		return result
	}

	result.T = t.timestamp
	result.SampleIDs = append(result.SampleIDs, 0)
	result.Samples = append(result.Samples, t.value)
	return result
}

func (t *vectorTable) size() int {
	return 1
}

func newVectorAccumulator(expr parser.ItemType) (vectorAccumulator, error) {
	t := parser.ItemTypeStr[expr]
	switch t {
	case "sum":
		return floats.Sum, nil
	case "max":
		return floats.Max, nil
	case "min":
		return floats.Min, nil
	case "count":
		return func(in []float64) float64 {
			return float64(len(in))
		}, nil
	case "avg":
		return func(in []float64) float64 {
			return floats.Sum(in) / float64(len(in))
		}, nil
	case "group":
		return func(in []float64) float64 {
			return 1
		}, nil
	}
	msg := fmt.Sprintf("unknown aggregation function %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}
