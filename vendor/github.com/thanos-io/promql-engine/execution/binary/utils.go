// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"math"

	"github.com/thanos-io/promql-engine/warnings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
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

func shouldDropMetricName(op parser.ItemType, returnBool bool) bool {
	switch op {
	case parser.ADD, parser.SUB, parser.MUL, parser.DIV, parser.MOD, parser.POW, parser.ATAN2:
		return true
	default:
		return op.IsComparisonOperator() && returnBool
	}
}

// binOp evaluates a binary operation between two values.
// Returns: value, histogram, keep, warnings, error.
func binOp(op parser.ItemType, lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, warnings.Warnings, error) {
	switch {
	case hlhs == nil && hrhs == nil:
		{
			switch op {
			case parser.ADD:
				return lhs + rhs, nil, true, 0, nil
			case parser.SUB:
				return lhs - rhs, nil, true, 0, nil
			case parser.MUL:
				return lhs * rhs, nil, true, 0, nil
			case parser.DIV:
				return lhs / rhs, nil, true, 0, nil
			case parser.POW:
				return math.Pow(lhs, rhs), nil, true, 0, nil
			case parser.MOD:
				return math.Mod(lhs, rhs), nil, true, 0, nil
			case parser.EQLC:
				return lhs, nil, lhs == rhs, 0, nil
			case parser.NEQ:
				return lhs, nil, lhs != rhs, 0, nil
			case parser.GTR:
				return lhs, nil, lhs > rhs, 0, nil
			case parser.LSS:
				return lhs, nil, lhs < rhs, 0, nil
			case parser.GTE:
				return lhs, nil, lhs >= rhs, 0, nil
			case parser.LTE:
				return lhs, nil, lhs <= rhs, 0, nil
			case parser.ATAN2:
				return math.Atan2(lhs, rhs), nil, true, 0, nil
			}
		}
	case hlhs == nil && hrhs != nil:
		{
			switch op {
			case parser.MUL:
				return 0, hrhs.Copy().Mul(lhs).Compact(0), true, 0, nil
			case parser.ADD, parser.SUB, parser.DIV, parser.POW, parser.MOD, parser.EQLC, parser.NEQ, parser.GTR, parser.LSS, parser.GTE, parser.LTE, parser.ATAN2:
				return 0, nil, false, warnings.WarnIncompatibleTypesInBinOp, nil
			}
		}
	case hlhs != nil && hrhs == nil:
		{
			switch op {
			case parser.MUL:
				return 0, hlhs.Copy().Mul(rhs).Compact(0), true, 0, nil
			case parser.DIV:
				return 0, hlhs.Copy().Div(rhs).Compact(0), true, 0, nil
			case parser.ADD, parser.SUB, parser.POW, parser.MOD, parser.EQLC, parser.NEQ, parser.GTR, parser.LSS, parser.GTE, parser.LTE, parser.ATAN2:
				return 0, nil, false, warnings.WarnIncompatibleTypesInBinOp, nil
			}
		}
	case hlhs != nil && hrhs != nil:
		{
			switch op {
			case parser.ADD:
				res, counterResetCollision, nhcbBoundsReconciled, err := hlhs.Copy().Add(hrhs)
				if err != nil {
					return 0, nil, false, 0, err
				}
				var warn warnings.Warnings
				if counterResetCollision {
					warn |= warnings.WarnCounterResetCollision
				}
				if nhcbBoundsReconciled {
					warn |= warnings.WarnNHCBBoundsReconciled
				}
				return 0, res.Compact(0), true, warn, nil
			case parser.SUB:
				res, counterResetCollision, nhcbBoundsReconciled, err := hlhs.Copy().Sub(hrhs)
				if err != nil {
					return 0, nil, false, 0, err
				}
				var warn warnings.Warnings
				if counterResetCollision {
					warn |= warnings.WarnCounterResetCollision
				}
				if nhcbBoundsReconciled {
					warn |= warnings.WarnNHCBBoundsReconciled
				}
				return 0, res.Compact(0), true, warn, nil
			case parser.EQLC:
				// This operation expects that both histograms are compacted.
				return 0, hlhs, hlhs.Equals(hrhs), 0, nil
			case parser.NEQ:
				// This operation expects that both histograms are compacted.
				return 0, hlhs, !hlhs.Equals(hrhs), 0, nil
			case parser.MUL, parser.DIV, parser.POW, parser.MOD, parser.GTR, parser.LSS, parser.GTE, parser.LTE, parser.ATAN2:
				return 0, nil, false, warnings.WarnIncompatibleTypesInBinOp, nil
			}
		}
	}
	return 0, nil, false, 0, nil
}

// emitBinaryOpWarnings emits warnings for binary operation side effects.
func emitBinaryOpWarnings(ctx context.Context, warn warnings.Warnings, opType parser.ItemType) {
	if warn == 0 {
		return
	}
	if warn&warnings.WarnMixedExponentialCustomBuckets != 0 {
		warnings.AddToContext(annotations.NewMixedExponentialCustomHistogramsWarning("", posrange.PositionRange{}), ctx)
	}
	if warn&warnings.WarnCounterResetCollision != 0 {
		var op annotations.HistogramOperation
		switch opType {
		case parser.ADD:
			op = annotations.HistogramAdd
		case parser.SUB:
			op = annotations.HistogramSub
		default:
			return
		}
		warnings.AddToContext(annotations.NewHistogramCounterResetCollisionWarning(posrange.PositionRange{}, op), ctx)
	}
	if warn&warnings.WarnNHCBBoundsReconciled != 0 {
		var op annotations.HistogramOperation
		switch opType {
		case parser.ADD:
			op = annotations.HistogramAdd
		case parser.SUB:
			op = annotations.HistogramSub
		default:
			return
		}
		warnings.AddToContext(annotations.NewMismatchedCustomBucketsHistogramsInfo(posrange.PositionRange{}, op), ctx)
	}
	if warn&warnings.WarnIncompatibleTypesInBinOp != 0 {
		warnings.AddToContext(annotations.IncompatibleTypesInBinOpInfo, ctx)
	}
}
