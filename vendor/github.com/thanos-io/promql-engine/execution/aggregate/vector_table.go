// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"fmt"
	"math"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
)

type vectorTable struct {
	ts          int64
	accumulator vectorAccumulator
}

func newVectorizedTables(stepsBatch int, a parser.ItemType) ([]aggregateTable, error) {
	tables := make([]aggregateTable, stepsBatch)
	for i := 0; i < len(tables); i++ {
		acc, err := newVectorAccumulator(a)
		if err != nil {
			return nil, err
		}
		tables[i] = newVectorizedTable(acc)
	}

	return tables, nil
}

func newVectorizedTable(a vectorAccumulator) *vectorTable {
	return &vectorTable{
		ts:          math.MinInt64,
		accumulator: a,
	}
}

func (t *vectorTable) timestamp() int64 {
	return t.ts
}

func (t *vectorTable) aggregate(vector model.StepVector) {
	t.ts = vector.T
	t.accumulator.AddVector(vector.Samples, vector.Histograms)
}

func (t *vectorTable) toVector(pool *model.VectorPool) model.StepVector {
	result := pool.GetStepVector(t.ts)
	if !t.accumulator.HasValue() {
		return result
	}
	v, h := t.accumulator.Value()
	if h == nil {
		result.AppendSample(pool, 0, v)
	} else {
		result.AppendHistogram(pool, 0, h)
	}
	return result
}

func (t *vectorTable) reset(p float64) {
	t.ts = math.MinInt64
	t.accumulator.Reset(p)
}

func newVectorAccumulator(expr parser.ItemType) (vectorAccumulator, error) {
	t := parser.ItemTypeStr[expr]
	switch t {
	case "sum":
		return newSumAcc(), nil
	case "max":
		return newMaxAcc(), nil
	case "min":
		return newMinAcc(), nil
	case "count":
		return newCountAcc(), nil
	case "avg":
		return newAvgAcc(), nil
	case "group":
		return newGroupAcc(), nil
	}
	msg := fmt.Sprintf("unknown aggregation function %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}

func histogramSum(current *histogram.FloatHistogram, histograms []*histogram.FloatHistogram) *histogram.FloatHistogram {
	if len(histograms) == 0 {
		return current
	}
	if current == nil && len(histograms) == 1 {
		return histograms[0].Copy()
	}
	var histSum *histogram.FloatHistogram
	if current != nil {
		histSum = current.Copy()
	} else {
		histSum = histograms[0].Copy()
		histograms = histograms[1:]
	}

	for i := 0; i < len(histograms); i++ {
		if histograms[i].Schema >= histSum.Schema {
			histSum = histSum.Add(histograms[i])
		} else {
			t := histograms[i].Copy()
			t.Add(histSum)
			histSum = t
		}
	}
	return histSum
}
