// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/efficientgo/core/errors"

	"gonum.org/v1/gonum/floats"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
)

type vectorAccumulator func([]float64, []*histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool)

type vectorTable struct {
	timestamp   int64
	histValue   *histogram.FloatHistogram
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
	t.timestamp = vector.T

	if len(vector.SampleIDs) == 0 && len(vector.Histograms) == 0 {
		t.hasValue = false
		return
	}
	t.hasValue = true

	var ok bool
	t.value, t.histValue, ok = t.accumulator(vector.Samples, vector.Histograms)
	if !ok {
		t.hasValue = false
	}
}

func (t *vectorTable) toVector(pool *model.VectorPool) model.StepVector {
	result := pool.GetStepVector(t.timestamp)
	if !t.hasValue {
		return result
	}
	if t.histValue == nil {
		result.AppendSample(pool, 0, t.value)
	} else {
		result.AppendHistogram(pool, 0, t.histValue)
	}
	return result
}

func (t *vectorTable) size() int {
	return 1
}

func newVectorAccumulator(expr parser.ItemType) (vectorAccumulator, error) {
	t := parser.ItemTypeStr[expr]
	switch t {
	case "sum":
		return func(float64s []float64, histograms []*histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool) {
			// Summing up mixed types is not defined.
			if len(float64s) != 0 && len(histograms) != 0 {
				return 0, nil, false
			}
			if len(float64s) > 0 {
				return floats.Sum(float64s), nil, true
			}
			return 0, histogramSum(histograms), true
		}, nil
	case "max":
		return func(float64s []float64, hs []*histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool) {
			if len(float64s) > 0 {
				return floats.Max(float64s), nil, true
			}
			return 0, nil, false
		}, nil
	case "min":
		return func(float64s []float64, hs []*histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool) {
			if len(float64s) > 0 {
				return floats.Min(float64s), nil, true
			}
			return 0, nil, false
		}, nil
	case "count":
		return func(in []float64, hs []*histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool) {
			return float64(len(in)) + float64(len(hs)), nil, true
		}, nil
	case "avg":
		return func(float64s []float64, histograms []*histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool) {
			if len(float64s) > 0 {
				return floats.Sum(float64s) / float64(len(float64s)), nil, true
			}
			return 0, nil, false
		}, nil
	case "group":
		return func(float64s []float64, histograms []*histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool) {
			return 1, nil, true
		}, nil
	}
	msg := fmt.Sprintf("unknown aggregation function %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}

func histogramSum(histograms []*histogram.FloatHistogram) *histogram.FloatHistogram {
	if len(histograms) == 1 {
		return histograms[0].Copy()
	}

	histSum := histograms[0].Copy()
	for i := 1; i < len(histograms); i++ {
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
