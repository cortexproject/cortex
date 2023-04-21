// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package execution

import (
	"runtime"
	"sort"
	"time"

	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-community/promql-engine/execution/noop"
	"github.com/thanos-community/promql-engine/execution/remote"

	"github.com/efficientgo/core/errors"

	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-community/promql-engine/execution/aggregate"
	"github.com/thanos-community/promql-engine/execution/binary"
	"github.com/thanos-community/promql-engine/execution/exchange"
	"github.com/thanos-community/promql-engine/execution/function"
	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
	"github.com/thanos-community/promql-engine/execution/scan"
	"github.com/thanos-community/promql-engine/execution/step_invariant"
	engstore "github.com/thanos-community/promql-engine/execution/storage"
	"github.com/thanos-community/promql-engine/execution/unary"
	"github.com/thanos-community/promql-engine/logicalplan"
	"github.com/thanos-community/promql-engine/query"
)

const stepsBatch = 10

// New creates new physical query execution for a given query expression which represents logical plan.
// TODO(bwplotka): Add definition (could be parameters for each execution operator) we can optimize - it would represent physical plan.
func New(expr parser.Expr, queryable storage.Queryable, mint, maxt time.Time, step, lookbackDelta, extLookbackDelta time.Duration) (model.VectorOperator, error) {
	opts := &query.Options{
		Start:            mint,
		End:              maxt,
		Step:             step,
		LookbackDelta:    lookbackDelta,
		StepsBatch:       stepsBatch,
		ExtLookbackDelta: extLookbackDelta,
	}
	selectorPool := engstore.NewSelectorPool(queryable)
	hints := storage.SelectHints{
		Start: mint.UnixMilli(),
		End:   maxt.UnixMilli(),
		// TODO(fpetkovski): Adjust the step for sub-queries once they are supported.
		Step: step.Milliseconds(),
	}
	return newOperator(expr, selectorPool, opts, hints)
}

func newOperator(expr parser.Expr, storage *engstore.SelectorPool, opts *query.Options, hints storage.SelectHints) (model.VectorOperator, error) {
	switch e := expr.(type) {
	case *parser.NumberLiteral:
		return scan.NewNumberLiteralSelector(model.NewVectorPool(stepsBatch), opts, e.Val), nil

	case *parser.VectorSelector:
		start, end := getTimeRangesForVectorSelector(e, opts, 0)
		hints.Start = start
		hints.End = end
		filter := storage.GetSelector(start, end, opts.Step.Milliseconds(), e.LabelMatchers, hints)
		return newShardedVectorSelector(filter, opts, e.Offset)

	case *logicalplan.FilteredSelector:
		start, end := getTimeRangesForVectorSelector(e.VectorSelector, opts, 0)
		hints.Start = start
		hints.End = end
		selector := storage.GetFilteredSelector(start, end, opts.Step.Milliseconds(), e.LabelMatchers, e.Filters, hints)
		return newShardedVectorSelector(selector, opts, e.Offset)

	case *parser.Call:
		hints.Func = e.Func.Name
		hints.Grouping = nil
		hints.By = false

		if e.Func.Name == "histogram_quantile" {
			nextOperators := make([]model.VectorOperator, len(e.Args))
			for i := range e.Args {
				next, err := newOperator(e.Args[i], storage, opts, hints)
				if err != nil {
					return nil, err
				}
				nextOperators[i] = next
			}

			return function.NewHistogramOperator(model.NewVectorPool(stepsBatch), e.Args, nextOperators, stepsBatch)
		}

		// TODO(saswatamcode): Tracked in https://github.com/thanos-community/promql-engine/issues/23
		// Based on the category we can create an apt query plan.
		call, err := function.NewFunctionCall(e.Func)
		if err != nil {
			return nil, err
		}

		// TODO(saswatamcode): Range vector result might need new operator
		// before it can be non-nested. https://github.com/thanos-community/promql-engine/issues/39
		for i := range e.Args {
			switch t := e.Args[i].(type) {
			case *parser.MatrixSelector:
				if call == nil {
					return nil, parse.ErrNotImplemented
				}

				vs, filters, err := unpackVectorSelector(t)
				if err != nil {
					return nil, err
				}

				milliSecondRange := t.Range.Milliseconds()
				if function.IsExtFunction(hints.Func) {
					milliSecondRange += opts.ExtLookbackDelta.Milliseconds()
				}

				start, end := getTimeRangesForVectorSelector(vs, opts, milliSecondRange)
				hints.Start = start
				hints.End = end
				hints.Range = milliSecondRange
				filter := storage.GetFilteredSelector(start, end, opts.Step.Milliseconds(), vs.LabelMatchers, filters, hints)

				numShards := runtime.GOMAXPROCS(0) / 2
				if numShards < 1 {
					numShards = 1
				}

				operators := make([]model.VectorOperator, 0, numShards)
				for i := 0; i < numShards; i++ {
					operator := exchange.NewConcurrent(
						scan.NewMatrixSelector(model.NewVectorPool(stepsBatch), filter, call, e, opts, t.Range, vs.Offset, i, numShards),
						2,
					)
					operators = append(operators, operator)
				}

				return exchange.NewCoalesce(model.NewVectorPool(stepsBatch), operators...), nil
			}
		}

		// Does not have matrix arg so create functionOperator normally.
		nextOperators := make([]model.VectorOperator, len(e.Args))
		for i := range e.Args {
			next, err := newOperator(e.Args[i], storage, opts, hints)
			if err != nil {
				return nil, err
			}
			nextOperators[i] = next
		}

		return function.NewFunctionOperator(e, call, nextOperators, stepsBatch, opts)

	case *parser.AggregateExpr:
		hints.Func = e.Op.String()
		hints.Grouping = e.Grouping
		hints.By = !e.Without
		var paramOp model.VectorOperator

		next, err := newOperator(e.Expr, storage, opts, hints)
		if err != nil {
			return nil, err
		}

		if e.Param != nil {
			paramOp, err = newOperator(e.Param, storage, opts, hints)
			if err != nil {
				return nil, err
			}
		}

		if e.Op == parser.TOPK || e.Op == parser.BOTTOMK {
			next, err = aggregate.NewKHashAggregate(model.NewVectorPool(stepsBatch), next, paramOp, e.Op, !e.Without, e.Grouping, stepsBatch)
		} else {
			next, err = aggregate.NewHashAggregate(model.NewVectorPool(stepsBatch), next, paramOp, e.Op, !e.Without, e.Grouping, stepsBatch)
		}

		if err != nil {
			return nil, err
		}

		return exchange.NewConcurrent(next, 2), nil

	case *parser.BinaryExpr:
		if e.LHS.Type() == parser.ValueTypeScalar || e.RHS.Type() == parser.ValueTypeScalar {
			return newScalarBinaryOperator(e, storage, opts, hints)
		}

		return newVectorBinaryOperator(e, storage, opts, hints)

	case *parser.ParenExpr:
		return newOperator(e.Expr, storage, opts, hints)

	case *parser.StringLiteral:
		// TODO(saswatamcode): This requires separate model with strings.
		return nil, errors.Wrapf(parse.ErrNotImplemented, "got: %s", e)

	case *parser.UnaryExpr:
		next, err := newOperator(e.Expr, storage, opts, hints)
		if err != nil {
			return nil, err
		}
		switch e.Op {
		case parser.ADD:
			return next, nil
		case parser.SUB:
			return unary.NewUnaryNegation(next, stepsBatch)
		default:
			// This shouldn't happen as Op was validated when parsing already
			// https://github.com/prometheus/prometheus/blob/v2.38.0/promql/parser/parse.go#L573.
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "got: %s", e)
		}

	case *parser.StepInvariantExpr:
		switch t := e.Expr.(type) {
		case *parser.NumberLiteral:
			return scan.NewNumberLiteralSelector(model.NewVectorPool(stepsBatch), opts, t.Val), nil
		}
		next, err := newOperator(e.Expr, storage, opts.WithEndTime(opts.Start), hints)
		if err != nil {
			return nil, err
		}
		return step_invariant.NewStepInvariantOperator(model.NewVectorPool(stepsBatch), next, e.Expr, opts, stepsBatch)

	case logicalplan.Deduplicate:
		// The Deduplicate operator will deduplicate samples using a last-sample-wins strategy.
		// Sorting engines by MaxT ensures that samples produced due to
		// staleness will be overwritten and corrected by samples coming from
		// engines with a higher max time.
		sort.Slice(e.Expressions, func(i, j int) bool {
			return e.Expressions[i].Engine.MaxT() < e.Expressions[j].Engine.MaxT()
		})

		operators := make([]model.VectorOperator, len(e.Expressions))
		for i, expr := range e.Expressions {
			operator, err := newOperator(expr, storage, opts, hints)
			if err != nil {
				return nil, err
			}
			operators[i] = operator
		}
		coalesce := exchange.NewCoalesce(model.NewVectorPool(stepsBatch), operators...)
		dedup := exchange.NewDedupOperator(model.NewVectorPool(stepsBatch), coalesce)
		return exchange.NewConcurrent(dedup, 2), nil

	case logicalplan.RemoteExecution:
		// Create a new remote query scoped to the calculated start time.
		qry, err := e.Engine.NewRangeQuery(&promql.QueryOpts{LookbackDelta: opts.LookbackDelta}, e.Query, e.QueryRangeStart, opts.End, opts.Step)
		if err != nil {
			return nil, err
		}

		// The selector uses the original query time to make sure that steps from different
		// operators have the same timestamps.
		// We need to set the lookback for the selector to 0 since the remote query already applies one lookback.
		selectorOpts := *opts
		selectorOpts.LookbackDelta = 0
		remoteExec := remote.NewExecution(qry, model.NewVectorPool(stepsBatch), &selectorOpts)
		return exchange.NewConcurrent(remoteExec, 2), nil
	case logicalplan.Noop:
		return noop.NewOperator(), nil
	default:
		return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "got: %s", e)
	}
}

func unpackVectorSelector(t *parser.MatrixSelector) (*parser.VectorSelector, []*labels.Matcher, error) {
	switch t := t.VectorSelector.(type) {
	case *parser.VectorSelector:
		return t, nil, nil
	case *logicalplan.FilteredSelector:
		return t.VectorSelector, t.Filters, nil
	default:
		return nil, nil, parse.ErrNotSupportedExpr
	}
}

func newShardedVectorSelector(selector engstore.SeriesSelector, opts *query.Options, offset time.Duration) (model.VectorOperator, error) {
	numShards := runtime.GOMAXPROCS(0) / 2
	if numShards < 1 {
		numShards = 1
	}
	operators := make([]model.VectorOperator, 0, numShards)
	for i := 0; i < numShards; i++ {
		operator := exchange.NewConcurrent(
			scan.NewVectorSelector(
				model.NewVectorPool(stepsBatch), selector, opts, offset, i, numShards), 2)
		operators = append(operators, operator)
	}

	return exchange.NewCoalesce(model.NewVectorPool(stepsBatch), operators...), nil
}

func newVectorBinaryOperator(e *parser.BinaryExpr, selectorPool *engstore.SelectorPool, opts *query.Options, hints storage.SelectHints) (model.VectorOperator, error) {
	leftOperator, err := newOperator(e.LHS, selectorPool, opts, hints)
	if err != nil {
		return nil, err
	}
	rightOperator, err := newOperator(e.RHS, selectorPool, opts, hints)
	if err != nil {
		return nil, err
	}
	return binary.NewVectorOperator(model.NewVectorPool(stepsBatch), leftOperator, rightOperator, e.VectorMatching, e.Op, e.ReturnBool)
}

func newScalarBinaryOperator(e *parser.BinaryExpr, selectorPool *engstore.SelectorPool, opts *query.Options, hints storage.SelectHints) (model.VectorOperator, error) {
	lhs, err := newOperator(e.LHS, selectorPool, opts, hints)
	if err != nil {
		return nil, err
	}
	rhs, err := newOperator(e.RHS, selectorPool, opts, hints)
	if err != nil {
		return nil, err
	}

	scalarSide := binary.ScalarSideRight
	if e.LHS.Type() == parser.ValueTypeScalar && e.RHS.Type() == parser.ValueTypeScalar {
		scalarSide = binary.ScalarSideBoth
	} else if e.LHS.Type() == parser.ValueTypeScalar {
		rhs, lhs = lhs, rhs
		scalarSide = binary.ScalarSideLeft
	}

	return binary.NewScalar(model.NewVectorPool(stepsBatch), lhs, rhs, e.Op, scalarSide, e.ReturnBool)
}

// Copy from https://github.com/prometheus/prometheus/blob/v2.39.1/promql/engine.go#L791.
func getTimeRangesForVectorSelector(n *parser.VectorSelector, opts *query.Options, evalRange int64) (int64, int64) {
	start := opts.Start.UnixMilli()
	end := opts.End.UnixMilli()
	if n.Timestamp != nil {
		start = *n.Timestamp
		end = *n.Timestamp
	}
	if evalRange == 0 {
		start -= opts.LookbackDelta.Milliseconds()
	} else {
		start -= evalRange
	}
	offset := n.OriginalOffset.Milliseconds()
	return start - offset, end - offset
}
