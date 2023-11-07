// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/parse"
)

type binOpSide string

const (
	lhBinOpSide binOpSide = "left"
	rhBinOpSide binOpSide = "right"
)

type errManyToManyMatch struct {
	matching *parser.VectorMatching
	side     binOpSide

	original, duplicate labels.Labels
}

func newManyToManyMatchError(matching *parser.VectorMatching, original, duplicate labels.Labels, side binOpSide) *errManyToManyMatch {
	return &errManyToManyMatch{
		original:  original,
		duplicate: duplicate,
		matching:  matching,
		side:      side,
	}
}

func (e *errManyToManyMatch) Error() string {
	group := e.original.MatchLabels(e.matching.On, e.matching.MatchingLabels...)
	msg := "found duplicate series for the match group %s on the %s hand-side of the operation: [%s, %s]" +
		";many-to-many matching not allowed: matching labels must be unique on one side"
	return fmt.Sprintf(msg, group, e.side, e.original.String(), e.duplicate.String())
}

// operands is a length 2 array which contains lhs and rhs.
// valueIdx is used in vector comparison operator to decide
// which operand value we should return.
type operation func(operands [2]float64, valueIdx int) (float64, bool)

var operations = map[string]operation{
	"+":  func(operands [2]float64, _ int) (float64, bool) { return operands[0] + operands[1], true },
	"-":  func(operands [2]float64, _ int) (float64, bool) { return operands[0] - operands[1], true },
	"*":  func(operands [2]float64, _ int) (float64, bool) { return operands[0] * operands[1], true },
	"/":  func(operands [2]float64, _ int) (float64, bool) { return operands[0] / operands[1], true },
	"^":  func(operands [2]float64, _ int) (float64, bool) { return math.Pow(operands[0], operands[1]), true },
	"%":  func(operands [2]float64, _ int) (float64, bool) { return math.Mod(operands[0], operands[1]), true },
	"==": func(operands [2]float64, _ int) (float64, bool) { return btof(operands[0] == operands[1]), true },
	"!=": func(operands [2]float64, _ int) (float64, bool) { return btof(operands[0] != operands[1]), true },
	">":  func(operands [2]float64, _ int) (float64, bool) { return btof(operands[0] > operands[1]), true },
	"<":  func(operands [2]float64, _ int) (float64, bool) { return btof(operands[0] < operands[1]), true },
	">=": func(operands [2]float64, _ int) (float64, bool) { return btof(operands[0] >= operands[1]), true },
	"<=": func(operands [2]float64, _ int) (float64, bool) { return btof(operands[0] <= operands[1]), true },
	"atan2": func(operands [2]float64, _ int) (float64, bool) {
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
	if expr.IsSetOperator() && vectorBinOp {
		// handled in the operator
		return nil, nil
	}
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

// histogramFloatOperation is an operation defined one histogram and one float.
type histogramFloatOperation func(lhsHist *histogram.FloatHistogram, rhsFloat float64) *histogram.FloatHistogram

func undefinedHistogramOp(_ *histogram.FloatHistogram, _ float64) *histogram.FloatHistogram {
	return nil
}

var lhsHistogramOperations = map[string]histogramFloatOperation{
	"*": func(hist *histogram.FloatHistogram, float float64) *histogram.FloatHistogram {
		return hist.Copy().Mul(float)
	},
	"/": func(hist *histogram.FloatHistogram, float float64) *histogram.FloatHistogram {
		return hist.Copy().Div(float)
	},
}

var rhsHistogramOperations = map[string]histogramFloatOperation{
	"*": func(hist *histogram.FloatHistogram, float float64) *histogram.FloatHistogram {
		return hist.Copy().Mul(float)
	},
}

func getHistogramFloatOperation(expr parser.ItemType, scalarSide ScalarSide) histogramFloatOperation {
	t := parser.ItemTypeStr[expr]
	var operation histogramFloatOperation
	if scalarSide == ScalarSideLeft {
		operation = rhsHistogramOperations[t]
	} else {
		operation = lhsHistogramOperations[t]
	}
	if operation != nil {
		return operation
	}
	return undefinedHistogramOp
}

// btof returns 1 if b is true, 0 otherwise.
func btof(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func shouldDropMetricName(op parser.ItemType, returnBool bool) bool {
	switch op {
	case parser.ADD, parser.SUB, parser.MUL, parser.DIV, parser.MOD, parser.POW, parser.ATAN2:
		return true
	default:
		return op.IsComparisonOperator() && returnBool
	}
}
