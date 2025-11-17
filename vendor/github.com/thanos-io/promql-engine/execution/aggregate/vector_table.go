// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"fmt"
	"math"

	"github.com/thanos-io/promql-engine/compute"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
)

type vectorTable struct {
	ts          int64
	accumulator compute.VectorAccumulator
}

func newVectorizedTables(stepsBatch int, a parser.ItemType) ([]aggregateTable, error) {
	tables := make([]aggregateTable, stepsBatch)
	for i := range tables {
		acc, err := newVectorAccumulator(a)
		if err != nil {
			return nil, err
		}
		tables[i] = newVectorizedTable(acc)
	}

	return tables, nil
}

func newVectorizedTable(a compute.VectorAccumulator) *vectorTable {
	return &vectorTable{
		ts:          math.MinInt64,
		accumulator: a,
	}
}

func (t *vectorTable) timestamp() int64 {
	return t.ts
}

func (t *vectorTable) aggregate(vector model.StepVector) error {
	t.ts = vector.T
	return t.accumulator.AddVector(vector.Samples, vector.Histograms)
}

func (t *vectorTable) toVector(ctx context.Context, pool *model.VectorPool) model.StepVector {
	result := pool.GetStepVector(t.ts)
	switch t.accumulator.ValueType() {
	case compute.NoValue:
		return result
	case compute.SingleTypeValue:
		v, h := t.accumulator.Value()
		if h == nil {
			result.AppendSample(pool, 0, v)
		} else {
			result.AppendHistogram(pool, 0, h)
		}
	case compute.MixedTypeValue:
		warnings.AddToContext(annotations.NewMixedFloatsHistogramsAggWarning(posrange.PositionRange{}), ctx)
	}
	return result
}

func (t *vectorTable) reset(p float64) {
	t.ts = math.MinInt64
	t.accumulator.Reset(p)
}

func newVectorAccumulator(expr parser.ItemType) (compute.VectorAccumulator, error) {
	t := parser.ItemTypeStr[expr]
	switch t {
	case "sum":
		return compute.NewSumAcc(), nil
	case "max":
		return compute.NewMaxAcc(), nil
	case "min":
		return compute.NewMinAcc(), nil
	case "count":
		return compute.NewCountAcc(), nil
	case "avg":
		return compute.NewAvgAcc(), nil
	case "group":
		return compute.NewGroupAcc(), nil
	}
	msg := fmt.Sprintf("unknown aggregation function %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}
