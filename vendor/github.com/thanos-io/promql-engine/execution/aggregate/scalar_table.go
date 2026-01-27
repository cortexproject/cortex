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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

// aggregateTable is a table that aggregates input samples into
// output samples for a single step.
type aggregateTable interface {
	// timestamp returns the timestamp of the table.
	// If the table is empty, it returns math.MinInt64.
	timestamp() int64
	// aggregate aggregates the given vector into the table.
	aggregate(ctx context.Context, vector model.StepVector)
	// toVector writes out the accumulated result to the given vector and
	// resets the table.
	toVector(ctx context.Context, pool *model.VectorPool) model.StepVector
	// reset resets the table with a new aggregation argument.
	// The argument is currently used for quantile aggregation.
	reset(arg float64)
}

type scalarTable struct {
	ts           int64
	inputs       []uint64
	outputs      []*model.Series
	accumulators []compute.Accumulator
}

func newScalarTables(stepsBatch int, inputCache []uint64, outputCache []*model.Series, aggregation parser.ItemType) ([]aggregateTable, error) {
	tables := make([]aggregateTable, stepsBatch)
	for i := range tables {
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
	accumulators := make([]compute.Accumulator, len(outputs))
	for i := range accumulators {
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

func (t *scalarTable) aggregate(ctx context.Context, vector model.StepVector) {
	t.ts = vector.T

	for i := range vector.Samples {
		outputSampleID := t.inputs[vector.SampleIDs[i]]
		output := t.outputs[outputSampleID]
		if err := t.accumulators[output.ID].Add(vector.Samples[i], nil); err != nil {
			warnings.AddToContext(err, ctx)
		}
	}
	for i := range vector.Histograms {
		outputSampleID := t.inputs[vector.HistogramIDs[i]]
		output := t.outputs[outputSampleID]
		if err := t.accumulators[output.ID].Add(0, vector.Histograms[i]); err != nil {
			warnings.AddToContext(err, ctx)
		}
	}
}

func (t *scalarTable) reset(arg float64) {
	for i := range t.outputs {
		t.accumulators[i].Reset(arg)
	}
	t.ts = math.MinInt64
}

func (t *scalarTable) toVector(ctx context.Context, pool *model.VectorPool) model.StepVector {
	result := pool.GetStepVector(t.ts)
	for i, v := range t.outputs {
		acc := t.accumulators[i]
		if acc.HasIgnoredHistograms() {
			warnings.AddToContext(annotations.HistogramIgnoredInAggregationInfo, ctx)
		}
		switch acc.ValueType() {
		case compute.NoValue:
			continue
		case compute.SingleTypeValue:
			f, h := acc.Value()
			if h == nil {
				result.AppendSample(pool, v.ID, f)
			} else {
				result.AppendHistogram(pool, v.ID, h)
			}
		case compute.MixedTypeValue:
			warnings.AddToContext(warnings.MixedFloatsHistogramsAggWarning, ctx)
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

// doing it the prometheus way
// https://github.com/prometheus/prometheus/blob/f379e2eac7134dea12ae1d93ebdcb8109db3a5ef/promql/engine.go#L3809C1-L3833C2
// if ratioLimit > 0 and sampleOffset turns out to be < ratioLimit add sample to the result
// else if ratioLimit < 0 then do ratioLimit+1(switch to positive axis), therefore now we will be taking those samples whose sampleOffset >= 1+ratioLimit (inverting the logic from previous case).
func addRatioSample(ratioLimit float64, series labels.Labels) bool {
	sampleOffset := float64(series.Hash()) / float64(math.MaxUint64)

	return (ratioLimit >= 0 && sampleOffset < ratioLimit) ||
		(ratioLimit < 0 && sampleOffset >= (1.0+ratioLimit))
}

func newScalarAccumulator(expr parser.ItemType) (compute.Accumulator, error) {
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
	case "stddev":
		return compute.NewStdDevAcc(), nil
	case "stdvar":
		return compute.NewStdVarAcc(), nil
	case "quantile":
		return compute.NewQuantileAcc(), nil
	case "histogram_avg":
		return compute.NewHistogramAvgAcc(), nil
	}

	msg := fmt.Sprintf("unknown aggregation function %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}
