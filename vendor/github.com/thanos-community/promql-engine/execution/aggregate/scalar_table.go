// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"fmt"
	"math"
	"sort"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/execution/function"
	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
)

type aggregateTable interface {
	aggregate(arg float64, vector model.StepVector)
	toVector(pool *model.VectorPool) model.StepVector
	size() int
}

type scalarTable struct {
	timestamp    int64
	inputs       []uint64
	outputs      []*model.Series
	accumulators []*accumulator
}

func newScalarTables(stepsBatch int, inputCache []uint64, outputCache []*model.Series, newAccumulator newAccumulatorFunc) []aggregateTable {
	tables := make([]aggregateTable, stepsBatch)
	for i := 0; i < len(tables); i++ {
		tables[i] = newScalarTable(inputCache, outputCache, newAccumulator)
	}
	return tables
}

func newScalarTable(inputSampleIDs []uint64, outputs []*model.Series, newAccumulator newAccumulatorFunc) *scalarTable {
	accumulators := make([]*accumulator, len(outputs))
	for i := 0; i < len(accumulators); i++ {
		accumulators[i] = newAccumulator()
	}
	return &scalarTable{
		inputs:       inputSampleIDs,
		outputs:      outputs,
		accumulators: accumulators,
	}
}

func (t *scalarTable) aggregate(arg float64, vector model.StepVector) {
	t.reset(arg)

	for i := range vector.Samples {
		t.addSample(vector.T, vector.SampleIDs[i], vector.Samples[i])
	}
}

func (t *scalarTable) addSample(ts int64, sampleID uint64, sample float64) {
	outputSampleID := t.inputs[sampleID]
	output := t.outputs[outputSampleID]

	t.timestamp = ts
	t.accumulators[output.ID].AddFunc(sample)
}

func (t *scalarTable) reset(arg float64) {
	for i := range t.outputs {
		t.accumulators[i].Reset(arg)
	}
}

func (t *scalarTable) toVector(pool *model.VectorPool) model.StepVector {
	result := pool.GetStepVector(t.timestamp)
	for i, v := range t.outputs {
		if t.accumulators[i].HasValue() {
			result.SampleIDs = append(result.SampleIDs, v.ID)
			result.Samples = append(result.Samples, t.accumulators[i].ValueFunc())
		}
	}
	return result
}

func (t *scalarTable) size() int {
	return len(t.outputs)
}

func hashMetric(metric labels.Labels, without bool, grouping []string, buf []byte) (uint64, string, labels.Labels) {
	buf = buf[:0]
	if without {
		lb := labels.NewBuilder(metric)
		lb.Del(grouping...)
		key, bytes := metric.HashWithoutLabels(buf, grouping...)
		return key, string(bytes), lb.Labels(nil)
	}

	if len(grouping) == 0 {
		return 0, "", labels.Labels{}
	}

	lb := labels.NewBuilder(metric)
	lb.Keep(grouping...)
	key, bytes := metric.HashForLabels(buf, grouping...)
	return key, string(bytes), lb.Labels(nil)
}

type newAccumulatorFunc func() *accumulator

type accumulator struct {
	AddFunc   func(v float64)
	ValueFunc func() float64
	HasValue  func() bool
	Reset     func(arg float64)
}

func makeAccumulatorFunc(expr parser.ItemType) (newAccumulatorFunc, error) {
	t := parser.ItemTypeStr[expr]
	switch t {
	case "sum":
		return func() *accumulator {
			var value float64
			var hasValue bool

			return &accumulator{
				AddFunc: func(v float64) {
					hasValue = true
					value += v
				},
				ValueFunc: func() float64 { return value },
				HasValue:  func() bool { return hasValue },
				Reset: func(_ float64) {
					hasValue = false
					value = 0
				},
			}
		}, nil
	case "max":
		return func() *accumulator {
			var value float64
			var hasValue bool

			return &accumulator{
				AddFunc: func(v float64) {
					if !hasValue {
						value = v
					} else {
						value = math.Max(value, v)
					}
					hasValue = true
				},
				ValueFunc: func() float64 { return value },
				HasValue:  func() bool { return hasValue },
				Reset: func(_ float64) {
					hasValue = false
					value = 0
				},
			}
		}, nil
	case "min":
		return func() *accumulator {
			var value float64
			var hasValue bool

			return &accumulator{
				AddFunc: func(v float64) {
					if !hasValue {
						value = v
					} else {
						value = math.Min(value, v)
					}
					hasValue = true
				},
				ValueFunc: func() float64 { return value },
				HasValue:  func() bool { return hasValue },
				Reset: func(_ float64) {
					hasValue = false
					value = 0
				},
			}
		}, nil
	case "count":
		return func() *accumulator {
			var value float64
			var hasValue bool

			return &accumulator{
				AddFunc: func(v float64) {
					hasValue = true
					value += 1
				},
				ValueFunc: func() float64 { return value },
				HasValue:  func() bool { return hasValue },
				Reset: func(_ float64) {
					hasValue = false
					value = 0
				},
			}
		}, nil
	case "avg":
		return func() *accumulator {
			var count, sum float64
			var hasValue bool

			return &accumulator{
				AddFunc: func(v float64) {
					hasValue = true
					count += 1
					sum += v
				},
				ValueFunc: func() float64 { return sum / count },
				HasValue:  func() bool { return hasValue },
				Reset: func(_ float64) {
					hasValue = false
					sum = 0
					count = 0
				},
			}
		}, nil
	case "group":
		return func() *accumulator {
			var hasValue bool
			return &accumulator{
				AddFunc: func(v float64) {
					hasValue = true
				},
				ValueFunc: func() float64 { return 1 },
				HasValue:  func() bool { return hasValue },
				Reset: func(_ float64) {
					hasValue = false
				},
			}
		}, nil
	case "stddev":
		return func() *accumulator {
			var count float64
			var mean, cMean float64
			var aux, cAux float64
			var hasValue bool
			return &accumulator{
				AddFunc: func(v float64) {
					hasValue = true
					count++
					delta := v - (mean + cMean)
					mean, cMean = function.KahanSumInc(delta/count, mean, cMean)
					aux, cAux = function.KahanSumInc(delta*(v-(mean+cMean)), aux, cAux)
				},
				ValueFunc: func() float64 { return math.Sqrt((aux + cAux) / count) },
				HasValue:  func() bool { return hasValue },
				Reset: func(_ float64) {
					hasValue = false
					count = 0
					mean = 0
					cMean = 0
					aux = 0
					cAux = 0
				},
			}
		}, nil
	case "stdvar":
		return func() *accumulator {
			var count float64
			var mean, cMean float64
			var aux, cAux float64
			var hasValue bool
			return &accumulator{
				AddFunc: func(v float64) {
					hasValue = true
					count++
					delta := v - (mean + cMean)
					mean, cMean = function.KahanSumInc(delta/count, mean, cMean)
					aux, cAux = function.KahanSumInc(delta*(v-(mean+cMean)), aux, cAux)
				},
				ValueFunc: func() float64 { return (aux + cAux) / count },
				HasValue:  func() bool { return hasValue },
				Reset: func(_ float64) {
					hasValue = false
					count = 0
					mean = 0
					cMean = 0
					aux = 0
					cAux = 0
				},
			}
		}, nil
	case "quantile":
		return func() *accumulator {
			var hasValue bool
			var arg float64
			points := make([]float64, 0)
			return &accumulator{
				AddFunc: func(v float64) {
					hasValue = true
					points = append(points, v)
				},
				ValueFunc: func() float64 {
					return quantile(arg, points)
				},
				HasValue: func() bool { return hasValue },
				Reset: func(a float64) {
					hasValue = false
					arg = a
					points = points[:0]
				},
			}
		}, nil
	}
	msg := fmt.Sprintf("unknown aggregation function %s", t)
	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}

func quantile(q float64, points []float64) float64 {
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
