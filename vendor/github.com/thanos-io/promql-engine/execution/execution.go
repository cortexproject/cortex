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

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/aggregate"
	"github.com/thanos-io/promql-engine/execution/binary"
	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/function"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/noop"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/execution/remote"
	"github.com/thanos-io/promql-engine/execution/scan"
	"github.com/thanos-io/promql-engine/execution/step_invariant"
	engstore "github.com/thanos-io/promql-engine/execution/storage"
	"github.com/thanos-io/promql-engine/execution/unary"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

// New creates new physical query execution for a given query expression which represents logical plan.
// TODO(bwplotka): Add definition (could be parameters for each execution operator) we can optimize - it would represent physical plan.
func New(expr parser.Expr, queryable storage.Queryable, opts *query.Options) (model.VectorOperator, error) {
	selectorPool := engstore.NewSelectorPool(queryable)
	hints := storage.SelectHints{
		Start: opts.Start.UnixMilli(),
		End:   opts.End.UnixMilli(),
		Step:  opts.Step.Milliseconds(),
	}

	return newOperator(expr, selectorPool, opts, hints)
}

func newOperator(expr parser.Expr, storage *engstore.SelectorPool, opts *query.Options, hints storage.SelectHints) (model.VectorOperator, error) {
	switch e := expr.(type) {
	case *parser.NumberLiteral:
		return scan.NewNumberLiteralSelector(model.NewVectorPool(opts.StepsBatch), opts, e.Val), nil

	case *parser.VectorSelector:
		start, end := getTimeRangesForVectorSelector(e, opts, 0)
		hints.Start = start
		hints.End = end
		filter := storage.GetSelector(start, end, opts.Step.Milliseconds(), e.LabelMatchers, hints)
		return newShardedVectorSelector(filter, opts, e.Offset, 0)

	case *logicalplan.VectorSelector:
		start, end := getTimeRangesForVectorSelector(e.VectorSelector, opts, 0)
		hints.Start = start
		hints.End = end
		selector := storage.GetFilteredSelector(start, end, opts.Step.Milliseconds(), e.LabelMatchers, e.Filters, hints)
		return newShardedVectorSelector(selector, opts, e.Offset, e.BatchSize)

	case *parser.Call:
		hints.Func = e.Func.Name
		hints.Grouping = nil
		hints.By = false

		// TODO(saswatamcode): Range vector result might need new operator
		// before it can be non-nested. https://github.com/thanos-io/promql-engine/issues/39
		for i := range e.Args {
			switch t := e.Args[i].(type) {
			case *parser.SubqueryExpr:
				if !opts.EnableSubqueries {
					return nil, parse.ErrNotImplemented
				}
				return newSubqueryFunction(e, t, storage, opts, hints)
			case *parser.MatrixSelector:
				return newRangeVectorFunction(e, t, storage, opts, hints)
			}
		}
		return newInstantVectorFunction(e, storage, opts, hints)

	case *parser.AggregateExpr:
		hints.Func = e.Op.String()
		hints.Grouping = e.Grouping
		hints.By = !e.Without
		var paramOp model.VectorOperator

		next, err := newOperator(e.Expr, storage, opts, hints)
		if err != nil {
			return nil, err
		}

		if e.Param != nil && e.Param.Type() != parser.ValueTypeString {
			paramOp, err = newOperator(e.Param, storage, opts, hints)
			if err != nil {
				return nil, err
			}
		}

		if e.Op == parser.TOPK || e.Op == parser.BOTTOMK {
			next, err = aggregate.NewKHashAggregate(model.NewVectorPool(opts.StepsBatch), next, paramOp, e.Op, !e.Without, e.Grouping, opts)
		} else {
			next, err = aggregate.NewHashAggregate(model.NewVectorPool(opts.StepsBatch), next, paramOp, e.Op, !e.Without, e.Grouping, opts)
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

	case *parser.UnaryExpr:
		next, err := newOperator(e.Expr, storage, opts, hints)
		if err != nil {
			return nil, err
		}
		switch e.Op {
		case parser.ADD:
			return next, nil
		case parser.SUB:
			return unary.NewUnaryNegation(next, opts.StepsBatch)
		default:
			// This shouldn't happen as Op was validated when parsing already
			// https://github.com/prometheus/prometheus/blob/v2.38.0/promql/parser/parse.go#L573.
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "got: %s", e)
		}

	case *parser.StepInvariantExpr:
		switch t := e.Expr.(type) {
		case *parser.NumberLiteral:
			return scan.NewNumberLiteralSelector(model.NewVectorPool(opts.StepsBatch), opts, t.Val), nil
		}
		next, err := newOperator(e.Expr, storage, opts.WithEndTime(opts.Start), hints)
		if err != nil {
			return nil, err
		}
		return step_invariant.NewStepInvariantOperator(model.NewVectorPoolWithSize(opts.StepsBatch, 1), next, e.Expr, opts)

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
		coalesce := exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, 0, operators...)
		dedup := exchange.NewDedupOperator(model.NewVectorPool(opts.StepsBatch), coalesce)
		return exchange.NewConcurrent(dedup, 2), nil

	case logicalplan.RemoteExecution:
		// Create a new remote query scoped to the calculated start time.
		qry, err := e.Engine.NewRangeQuery(opts.Context, promql.NewPrometheusQueryOpts(false, opts.LookbackDelta), e.Query, e.QueryRangeStart, opts.End, opts.Step)
		if err != nil {
			return nil, err
		}

		// The selector uses the original query time to make sure that steps from different
		// operators have the same timestamps.
		// We need to set the lookback for the selector to 0 since the remote query already applies one lookback.
		selectorOpts := *opts
		selectorOpts.LookbackDelta = 0
		remoteExec := remote.NewExecution(qry, model.NewVectorPool(opts.StepsBatch), e.QueryRangeStart, &selectorOpts)
		return exchange.NewConcurrent(remoteExec, 2), nil
	case logicalplan.Noop:
		return noop.NewOperator(), nil
	case logicalplan.UserDefinedExpr:
		return e.MakeExecutionOperator(model.NewVectorPool(opts.StepsBatch), storage, opts, hints)
	default:
		return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "got: %s", e)
	}
}

func unpackVectorSelector(t *parser.MatrixSelector) (int64, *parser.VectorSelector, []*labels.Matcher, error) {
	switch t := t.VectorSelector.(type) {
	case *parser.VectorSelector:
		return 0, t, nil, nil
	case *logicalplan.VectorSelector:
		return t.BatchSize, t.VectorSelector, t.Filters, nil
	default:
		return 0, nil, nil, parse.ErrNotSupportedExpr
	}
}

func newRangeVectorFunction(e *parser.Call, t *parser.MatrixSelector, storage *engstore.SelectorPool, opts *query.Options, hints storage.SelectHints) (model.VectorOperator, error) {
	// TODO(saswatamcode): Range vector result might need new operator
	// before it can be non-nested. https://github.com/thanos-io/promql-engine/issues/39
	batchSize, vs, filters, err := unpackVectorSelector(t)
	if err != nil {
		return nil, err
	}

	milliSecondRange := t.Range.Milliseconds()
	if function.IsExtFunction(e.Func.Name) {
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
		operator, err := scan.NewMatrixSelector(model.NewVectorPool(opts.StepsBatch), filter, e, opts, t.Range, vs.Offset, batchSize, i, numShards)
		if err != nil {
			return nil, err
		}
		operators = append(operators, exchange.NewConcurrent(operator, 2))
	}

	return exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, batchSize*int64(numShards), operators...), nil
}

func newSubqueryFunction(e *parser.Call, t *parser.SubqueryExpr, storage *engstore.SelectorPool, opts *query.Options, hints storage.SelectHints) (model.VectorOperator, error) {
	// TODO: We dont implement ext functions
	if parse.IsExtFunction(e.Func.Name) {
		return nil, parse.ErrNotImplemented
	}
	// TODO: only instant queries for now.
	if !opts.IsInstantQuery() {
		return nil, parse.ErrNotImplemented
	}
	nOpts := query.NestedOptionsForSubquery(opts, t)

	hints.Start = nOpts.Start.UnixMilli()
	hints.End = nOpts.End.UnixMilli()
	hints.Step = nOpts.Step.Milliseconds()

	inner, err := newOperator(t.Expr, storage, nOpts, hints)
	if err != nil {
		return nil, err
	}
	return scan.NewSubqueryOperator(model.NewVectorPool(opts.StepsBatch), inner, opts, e, t)
}

func newInstantVectorFunction(e *parser.Call, storage *engstore.SelectorPool, opts *query.Options, hints storage.SelectHints) (model.VectorOperator, error) {
	nextOperators := make([]model.VectorOperator, 0, len(e.Args))
	for i := range e.Args {
		// Strings don't need an operator
		if e.Args[i].Type() == parser.ValueTypeString {
			continue
		}
		next, err := newOperator(e.Args[i], storage, opts, hints)
		if err != nil {
			return nil, err
		}
		nextOperators = append(nextOperators, next)
	}

	return function.NewFunctionOperator(e, nextOperators, opts.StepsBatch, opts)
}

func newShardedVectorSelector(selector engstore.SeriesSelector, opts *query.Options, offset time.Duration, batchSize int64) (model.VectorOperator, error) {
	numShards := runtime.GOMAXPROCS(0) / 2
	if numShards < 1 {
		numShards = 1
	}
	operators := make([]model.VectorOperator, 0, numShards)
	for i := 0; i < numShards; i++ {
		operator := exchange.NewConcurrent(
			scan.NewVectorSelector(
				model.NewVectorPool(opts.StepsBatch), selector, opts, offset, batchSize, i, numShards), 2)
		operators = append(operators, operator)
	}

	return exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, batchSize*int64(numShards), operators...), nil
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
	return binary.NewVectorOperator(model.NewVectorPool(opts.StepsBatch), leftOperator, rightOperator, e.VectorMatching, e.Op, e.ReturnBool, opts)
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

	return binary.NewScalar(model.NewVectorPoolWithSize(opts.StepsBatch, 1), lhs, rhs, e.Op, scalarSide, e.ReturnBool, opts)
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
