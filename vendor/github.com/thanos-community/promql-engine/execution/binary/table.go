// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"math"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
)

type binOpSide string

const (
	lhBinOpSide binOpSide = "left"
	rhBinOpSide binOpSide = "right"
)

type errManyToManyMatch struct {
	sampleID          uint64
	duplicateSampleID uint64
	side              binOpSide
}

func newManyToManyMatchError(sampleID, duplicateSampleID uint64, side binOpSide) *errManyToManyMatch {
	return &errManyToManyMatch{
		sampleID:          sampleID,
		duplicateSampleID: duplicateSampleID,
		side:              side,
	}
}

type outputSample struct {
	lhT        int64
	rhT        int64
	lhSampleID uint64
	rhSampleID uint64
	v          float64
}

type table struct {
	pool *model.VectorPool

	operation operation
	card      parser.VectorMatchCardinality

	outputValues []outputSample
	// highCardOutputIndex is a mapping from series ID of the high cardinality
	// operator to an output series ID.
	// During joins, each high cardinality series that has a matching
	// low cardinality series will map to exactly one output series.
	highCardOutputIndex outputIndex
	// lowCardOutputIndex is a mapping from series ID of the low cardinality
	// operator to an output series ID.
	// Each series from the low cardinality operator can join with many
	// series of the high cardinality operator.
	lowCardOutputIndex outputIndex
}

func newTable(
	pool *model.VectorPool,
	card parser.VectorMatchCardinality,
	operation operation,
	outputValues []outputSample,
	highCardOutputCache outputIndex,
	lowCardOutputCache outputIndex,
) *table {
	for i := range outputValues {
		outputValues[i].lhT = -1
		outputValues[i].rhT = -1
	}
	return &table{
		pool: pool,
		card: card,

		operation:           operation,
		outputValues:        outputValues,
		highCardOutputIndex: highCardOutputCache,
		lowCardOutputIndex:  lowCardOutputCache,
	}
}

func (t *table) execBinaryOperation(lhs model.StepVector, rhs model.StepVector, returnBool bool) (model.StepVector, *errManyToManyMatch) {
	ts := lhs.T
	step := t.pool.GetStepVector(ts)

	lhsIndex, rhsIndex := t.highCardOutputIndex, t.lowCardOutputIndex
	if t.card == parser.CardOneToMany {
		lhsIndex, rhsIndex = rhsIndex, lhsIndex
	}

	for i, sampleID := range lhs.SampleIDs {
		lhsVal := lhs.Samples[i]
		outputSampleIDs := lhsIndex.outputSamples(sampleID)
		for _, outputSampleID := range outputSampleIDs {
			if t.card != parser.CardManyToOne && t.outputValues[outputSampleID].lhT == ts {
				prevSampleID := t.outputValues[outputSampleID].lhSampleID
				return model.StepVector{}, newManyToManyMatchError(prevSampleID, sampleID, lhBinOpSide)
			}

			t.outputValues[outputSampleID].lhSampleID = sampleID
			t.outputValues[outputSampleID].lhT = lhs.T
			t.outputValues[outputSampleID].v = lhsVal
		}
	}

	for i, sampleID := range rhs.SampleIDs {
		rhVal := rhs.Samples[i]
		outputSampleIDs := rhsIndex.outputSamples(sampleID)
		for _, outputSampleID := range outputSampleIDs {
			outputSample := t.outputValues[outputSampleID]
			if rhs.T != outputSample.lhT {
				continue
			}
			if t.card != parser.CardOneToMany && outputSample.rhT == rhs.T {
				prevSampleID := t.outputValues[outputSampleID].rhSampleID
				return model.StepVector{}, newManyToManyMatchError(prevSampleID, sampleID, rhBinOpSide)
			}
			t.outputValues[outputSampleID].rhSampleID = sampleID
			t.outputValues[outputSampleID].rhT = rhs.T

			outputVal, keep := t.operation([2]float64{outputSample.v, rhVal}, 0)
			if returnBool {
				outputVal = 0
				if keep {
					outputVal = 1
				}
			} else if !keep {
				continue
			}
			step.AppendSample(t.pool, outputSampleID, outputVal)
		}
	}

	return step, nil
}

// operands is a length 2 array which contains lhs and rhs.
// valueIdx is used in vector comparison operator to decide
// which operand value we should return.
type operation func(operands [2]float64, valueIdx int) (float64, bool)

var operations = map[string]operation{
	"+": func(operands [2]float64, valueIdx int) (float64, bool) { return operands[0] + operands[1], true },
	"-": func(operands [2]float64, valueIdx int) (float64, bool) { return operands[0] - operands[1], true },
	"*": func(operands [2]float64, valueIdx int) (float64, bool) { return operands[0] * operands[1], true },
	"/": func(operands [2]float64, valueIdx int) (float64, bool) { return operands[0] / operands[1], true },
	"^": func(operands [2]float64, valueIdx int) (float64, bool) {
		return math.Pow(operands[0], operands[1]), true
	},
	"%": func(operands [2]float64, valueIdx int) (float64, bool) {
		return math.Mod(operands[0], operands[1]), true
	},
	"==": func(operands [2]float64, valueIdx int) (float64, bool) { return btof(operands[0] == operands[1]), true },
	"!=": func(operands [2]float64, valueIdx int) (float64, bool) { return btof(operands[0] != operands[1]), true },
	">":  func(operands [2]float64, valueIdx int) (float64, bool) { return btof(operands[0] > operands[1]), true },
	"<":  func(operands [2]float64, valueIdx int) (float64, bool) { return btof(operands[0] < operands[1]), true },
	">=": func(operands [2]float64, valueIdx int) (float64, bool) { return btof(operands[0] >= operands[1]), true },
	"<=": func(operands [2]float64, valueIdx int) (float64, bool) { return btof(operands[0] <= operands[1]), true },
	"atan2": func(operands [2]float64, valueIdx int) (float64, bool) {
		return math.Atan2(operands[0], operands[1]), true
	},
}

// For vector, those operations are handled differently to check whether to keep
// the value or not. https://github.com/prometheus/prometheus/blob/main/promql/engine.go#L2229
var vectorBinaryOperations = map[string]operation{
	"==": func(operands [2]float64, valueIdx int) (float64, bool) {
		return operands[valueIdx], operands[0] == operands[1]
	},
	"!=": func(operands [2]float64, valueIdx int) (float64, bool) {
		return operands[valueIdx], operands[0] != operands[1]
	},
	">": func(operands [2]float64, valueIdx int) (float64, bool) {
		return operands[valueIdx], operands[0] > operands[1]
	},
	"<": func(operands [2]float64, valueIdx int) (float64, bool) {
		return operands[valueIdx], operands[0] < operands[1]
	},
	">=": func(operands [2]float64, valueIdx int) (float64, bool) {
		return operands[valueIdx], operands[0] >= operands[1]
	},
	"<=": func(operands [2]float64, valueIdx int) (float64, bool) {
		return operands[valueIdx], operands[0] <= operands[1]
	},
}

func newOperation(expr parser.ItemType, vectorBinOp bool) (operation, error) {
	t := parser.ItemTypeStr[expr]
	if expr.IsComparisonOperator() && vectorBinOp {
		if o, ok := vectorBinaryOperations[t]; ok {
			return o, nil
		}
		return nil, parse.UnsupportedOperationErr(expr)
	}
	if o, ok := operations[t]; ok {
		return o, nil
	}
	return nil, parse.UnsupportedOperationErr(expr)
}

// btof returns 1 if b is true, 0 otherwise.
func btof(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func shouldDropMetricName(op parser.ItemType, returnBool bool) bool {
	switch op.String() {
	case "+", "-", "*", "/", "%", "^":
		return true
	}

	return op.IsComparisonOperator() && returnBool
}
