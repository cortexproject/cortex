// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"fmt"
	"math"
	"sort"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
)

// aggregateTable is a table that aggregates input samples into
// output samples for a single step.
type aggregateTable interface {
	// timestamp returns the timestamp of the table.
	// If the table is empty, it returns math.MinInt64.
	timestamp() int64
	// aggregate aggregates the given vector into the table.
	aggregate(vector model.StepVector)
	// toVector writes out the accumulated result to the given vector and
	// resets the table.
	toVector(pool *model.VectorPool) model.StepVector
	// reset resets the table with a new aggregation argument.
	// The argument is currently used for quantile aggregation.
	reset(arg float64)
}

type scalarTable struct {
	ts           int64
	inputs       []uint64
	outputs      []*model.Series
	accumulators []accumulator
}

func newScalarTables(stepsBatch int, inputCache []uint64, outputCache []*model.Series, aggregation parser.ItemType) ([]aggregateTable, error) {
	tables := make([]aggregateTable, stepsBatch)
	for i := 0; i < len(tables); i++ {
		table, err := newScalarTable(inputCache, outputCache, aggregation)
		if err != nil {
			return nil, err
		}
		tables[i] = table
	}
	return tables, nil
}

func (t *scalarTable) timestamp() int64 {
	return t.ts
}

func newScalarTable(inputSampleIDs []uint64, outputs []*model.Series, aggregation parser.ItemType) (*scalarTable, error) {
	accumulators := make([]accumulator, len(outputs))
	for i := 0; i < len(accumulators); i++ {
		acc, err := newScalarAccumulator(aggregation)
		if err != nil {
			return nil, err
		}
		accumulators[i] = acc
	}
	return &scalarTable{
		ts:           math.MinInt64,
		inputs:       inputSampleIDs,
		outputs:      outputs,
		accumulators: accumulators,
	}, nil
}

func (t *scalarTable) aggregate(vector model.StepVector) {
	t.ts = vector.T

	for i := range vector.Samples {
		t.addSample(vector.SampleIDs[i], vector.Samples[i])
	}
	for i := range vector.Histograms {
		t.addHistogram(vector.HistogramIDs[i], vector.Histograms[i])
	}
}

func (t *scalarTable) addSample(sampleID uint64, sample float64) {
	outputSampleID := t.inputs[sampleID]
	output := t.outputs[outputSampleID]

	t.accumulators[output.ID].Add(sample, nil)
}

func (t *scalarTable) addHistogram(sampleID uint64, h *histogram.FloatHistogram) {
	outputSampleID := t.inputs[sampleID]
	output := t.outputs[outputSampleID]

	t.accumulators[output.ID].Add(0, h)
}

func (t *scalarTable) reset(arg float64) {
	for i := range t.outputs {
		t.accumulators[i].Reset(arg)
	}
	t.ts = math.MinInt64
}

func (t *scalarTable) toVector(pool *model.VectorPool) model.StepVector {
	result := pool.GetStepVector(t.ts)
	for i, v := range t.outputs {
		if t.accumulators[i].HasValue() {
			f, h := t.accumulators[i].Value()
			if h == nil {
				result.AppendSample(pool, v.ID, f)
			} else {
				result.AppendHistogram(pool, v.ID, h)
			}
		}
	}
	return result
}

func hashMetric(
	builder labels.ScratchBuilder,
	metric labels.Labels,
	without bool,
	grouping []string,
	groupingSet map[string]struct{},
	buf []byte,
) (uint64, labels.Labels) {
	buf = buf[:0]
	builder.Reset()

	if without {
		metric.Range(func(lbl labels.Label) {
			if lbl.Name == labels.MetricName {
				return
			}
			if _, ok := groupingSet[lbl.Name]; ok {
				return
			}
			builder.Add(lbl.Name, lbl.Value)
		})
		key, _ := metric.HashWithoutLabels(buf, grouping...)
		return key, builder.Labels()
	}

	if len(grouping) == 0 {
		return 0, labels.Labels{}
	}

	metric.Range(func(lbl labels.Label) {
		if _, ok := groupingSet[lbl.Name]; !ok {
			return
		}
		builder.Add(lbl.Name, lbl.Value)
	})
	key, _ := metric.HashForLabels(buf, grouping...)
	return key, builder.Labels()
}

func newScalarAccumulator(expr parser.ItemType) (accumulator, error) {
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
	case "stddev":
		return newStdDevAcc(), nil
	case "stdvar":
		return newStdVarAcc(), nil
	case "quantile":
		return newQuantileAcc(), nil
	}
	msg := fmt.Sprintf("unknown aggregation function %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}

func Quantile(q float64, points []float64) float64 {
	if len(points) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sort.Float64s(points)

	n := float64(len(points))
	// When the quantile lies between two samples,
	// we use a weighted average of the two samples.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	return points[int(lowerIndex)]*(1-weight) + points[int(upperIndex)]*weight
}
