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
	"sort"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	promstorage "github.com/prometheus/prometheus/storage"

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
	"github.com/thanos-io/promql-engine/execution/unary"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/storage"
)

// New creates new physical query execution for a given query expression which represents logical plan.
// TODO(bwplotka): Add definition (could be parameters for each execution operator) we can optimize - it would represent physical plan.
func New(expr parser.Expr, storage storage.Scanners, opts *query.Options) (model.VectorOperator, error) {
	hints := promstorage.SelectHints{
		Start: opts.Start.UnixMilli(),
		End:   opts.End.UnixMilli(),
		Step:  opts.Step.Milliseconds(),
	}
	return newOperator(expr, storage, opts, hints)
}

func newOperator(expr parser.Expr, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	switch e := expr.(type) {
	case *logicalplan.NumberLiteral:
		return scan.NewNumberLiteralSelector(model.NewVectorPool(opts.StepsBatch), opts, e.Val), nil
	case *logicalplan.VectorSelector:
		return newVectorSelector(e, storage, opts, hints)
	case *parser.Call:
		return newCall(e, storage, opts, hints)
	case *parser.AggregateExpr:
		return newAggregateExpression(e, storage, opts, hints)
	case *parser.BinaryExpr:
		return newBinaryExpression(e, storage, opts, hints)
	case *parser.ParenExpr:
		return newOperator(e.Expr, storage, opts, hints)
	case *parser.UnaryExpr:
		return newUnaryExpression(e, storage, opts, hints)
	case *logicalplan.StepInvariantExpr:
		return newStepInvariantExpression(e, storage, opts, hints)
	case logicalplan.Deduplicate:
		return newDeduplication(e, storage, opts, hints)
	case logicalplan.RemoteExecution:
		return newRemoteExecution(e, opts, hints)
	case logicalplan.CheckDuplicateLabels:
		return newDuplicateLabelCheck(e, storage, opts, hints)
	case logicalplan.Noop:
		return noop.NewOperator(), nil
	case logicalplan.UserDefinedExpr:
		return e.MakeExecutionOperator(model.NewVectorPool(opts.StepsBatch), opts, hints)
	default:
		return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "got: %s (%T)", e, e)
	}
}

func newVectorSelector(e *logicalplan.VectorSelector, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	start, end := getTimeRangesForVectorSelector(e, opts, 0)
	hints.Start = start
	hints.End = end
	return scanners.NewVectorSelector(opts, hints, *e)
}

func newCall(e *parser.Call, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	hints.Func = e.Func.Name
	hints.Grouping = nil
	hints.By = false

	if e.Func.Name == "absent_over_time" {
		return newAbsentOverTimeOperator(e, scanners, opts, hints)
	}
	if e.Func.Name == "timestamp" {
		switch arg := e.Args[0].(type) {
		case *logicalplan.VectorSelector:
			arg.SelectTimestamp = true
			return newVectorSelector(arg, scanners, opts, hints)
		case *parser.StepInvariantExpr:
			// Step invariant expressions on vector selectors need to be unwrapped so that we
			// can return the original timestamp rather than the step invariant one.
			switch vs := arg.Expr.(type) {
			case *logicalplan.VectorSelector:
				// Prometheus weirdness.
				if vs.Timestamp != nil {
					vs.OriginalOffset = 0
				}
				vs.SelectTimestamp = true
				return newVectorSelector(vs, scanners, opts, hints)
			}
			return newInstantVectorFunction(e, scanners, opts, hints)
		}
		return newInstantVectorFunction(e, scanners, opts, hints)
	}

	// TODO(saswatamcode): Range vector result might need new operator
	// before it can be non-nested. https://github.com/thanos-io/promql-engine/issues/39
	for i := range e.Args {
		switch t := e.Args[i].(type) {
		case *parser.SubqueryExpr:
			return newSubqueryFunction(e, t, scanners, opts, hints)
		case *logicalplan.MatrixSelector:
			return newRangeVectorFunction(e, t, scanners, opts, hints)
		}
	}
	return newInstantVectorFunction(e, scanners, opts, hints)
}

func newAbsentOverTimeOperator(call *parser.Call, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	switch arg := call.Args[0].(type) {
	case *parser.SubqueryExpr:
		matrixCall := &parser.Call{
			Func: &parser.Function{Name: "last_over_time"},
		}
		argOp, err := newSubqueryFunction(matrixCall, arg, scanners, opts, hints)
		if err != nil {
			return nil, err
		}
		f := &parser.Call{
			Func: &parser.Function{Name: "absent"},
			Args: []parser.Expr{matrixCall},
		}
		return function.NewFunctionOperator(f, []model.VectorOperator{argOp}, opts.StepsBatch, opts)
	case *logicalplan.MatrixSelector:
		matrixCall := &parser.Call{
			Func: &parser.Function{Name: "last_over_time"},
			Args: call.Args,
		}
		argOp, err := newRangeVectorFunction(matrixCall, arg, scanners, opts, hints)
		if err != nil {
			return nil, err
		}
		f := &parser.Call{
			Func: &parser.Function{Name: "absent"},
			Args: []parser.Expr{&logicalplan.MatrixSelector{
				VectorSelector: arg.VectorSelector,
				Range:          arg.Range,
				OriginalString: arg.String(),
			}},
		}
		return function.NewFunctionOperator(f, []model.VectorOperator{argOp}, opts.StepsBatch, opts)
	default:
		return nil, parse.ErrNotSupportedExpr
	}
}

func newRangeVectorFunction(e *parser.Call, t *logicalplan.MatrixSelector, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	// TODO(saswatamcode): Range vector result might need new operator
	// before it can be non-nested. https://github.com/thanos-io/promql-engine/issues/39
	milliSecondRange := t.Range.Milliseconds()
	if function.IsExtFunction(e.Func.Name) {
		milliSecondRange += opts.ExtLookbackDelta.Milliseconds()
	}

	start, end := getTimeRangesForVectorSelector(t.VectorSelector.(*logicalplan.VectorSelector), opts, milliSecondRange)
	hints.Start = start
	hints.End = end
	hints.Range = milliSecondRange
	return scanners.NewMatrixSelector(opts, hints, *t, *e)
}

func newSubqueryFunction(e *parser.Call, t *parser.SubqueryExpr, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	// TODO: We dont implement ext functions
	if parse.IsExtFunction(e.Func.Name) {
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

	outerOpts := *opts
	if t.Timestamp != nil {
		outerOpts.Start = time.UnixMilli(*t.Timestamp)
		outerOpts.End = time.UnixMilli(*t.Timestamp)
	}
	return scan.NewSubqueryOperator(model.NewVectorPool(opts.StepsBatch), inner, &outerOpts, e, t)
}

func newInstantVectorFunction(e *parser.Call, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
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

func newAggregateExpression(e *parser.AggregateExpr, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	hints.Func = e.Op.String()
	hints.Grouping = e.Grouping
	hints.By = !e.Without
	var paramOp model.VectorOperator

	next, err := newOperator(e.Expr, scanners, opts, hints)
	if err != nil {
		return nil, err
	}

	if e.Param != nil && e.Param.Type() != parser.ValueTypeString {
		paramOp, err = newOperator(e.Param, scanners, opts, hints)
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

	return exchange.NewConcurrent(next, 2, opts), nil

}

func newBinaryExpression(e *parser.BinaryExpr, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	if e.LHS.Type() == parser.ValueTypeScalar || e.RHS.Type() == parser.ValueTypeScalar {
		return newScalarBinaryOperator(e, scanners, opts, hints)
	}
	return newVectorBinaryOperator(e, scanners, opts, hints)
}

func newVectorBinaryOperator(e *parser.BinaryExpr, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	leftOperator, err := newOperator(e.LHS, storage, opts, hints)
	if err != nil {
		return nil, err
	}
	rightOperator, err := newOperator(e.RHS, storage, opts, hints)
	if err != nil {
		return nil, err
	}
	return binary.NewVectorOperator(model.NewVectorPool(opts.StepsBatch), leftOperator, rightOperator, e.VectorMatching, e.Op, e.ReturnBool, opts)
}

func newScalarBinaryOperator(e *parser.BinaryExpr, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	lhs, err := newOperator(e.LHS, storage, opts, hints)
	if err != nil {
		return nil, err
	}
	rhs, err := newOperator(e.RHS, storage, opts, hints)
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

func newUnaryExpression(e *parser.UnaryExpr, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	next, err := newOperator(e.Expr, scanners, opts, hints)
	if err != nil {
		return nil, err
	}
	switch e.Op {
	case parser.ADD:
		return next, nil
	case parser.SUB:
		return unary.NewUnaryNegation(next, opts)
	default:
		// This shouldn't happen as Op was validated when parsing already
		// https://github.com/prometheus/prometheus/blob/v2.38.0/promql/parser/parse.go#L573.
		return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "got: %s", e)
	}
}

func newStepInvariantExpression(e *logicalplan.StepInvariantExpr, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	switch t := e.Expr.(type) {
	case *logicalplan.NumberLiteral:
		return scan.NewNumberLiteralSelector(model.NewVectorPool(opts.StepsBatch), opts, t.Val), nil
	}
	next, err := newOperator(e.Expr, scanners, opts.WithEndTime(opts.Start), hints)
	if err != nil {
		return nil, err
	}
	return step_invariant.NewStepInvariantOperator(model.NewVectorPoolWithSize(opts.StepsBatch, 1), next, e.Expr, opts)
}

func newDeduplication(e logicalplan.Deduplicate, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	// The Deduplicate operator will deduplicate samples using a last-sample-wins strategy.
	// Sorting engines by MaxT ensures that samples produced due to
	// staleness will be overwritten and corrected by samples coming from
	// engines with a higher max time.
	sort.Slice(e.Expressions, func(i, j int) bool {
		return e.Expressions[i].Engine.MaxT() < e.Expressions[j].Engine.MaxT()
	})

	operators := make([]model.VectorOperator, len(e.Expressions))
	for i, expr := range e.Expressions {
		operator, err := newOperator(expr, scanners, opts, hints)
		if err != nil {
			return nil, err
		}
		operators[i] = operator
	}
	coalesce := exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, 0, operators...)
	dedup := exchange.NewDedupOperator(model.NewVectorPool(opts.StepsBatch), coalesce, opts)
	return exchange.NewConcurrent(dedup, 2, opts), nil
}

func newRemoteExecution(e logicalplan.RemoteExecution, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
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
	remoteExec := remote.NewExecution(qry, model.NewVectorPool(opts.StepsBatch), e.QueryRangeStart, &selectorOpts, hints)
	return exchange.NewConcurrent(remoteExec, 2, opts), nil
}

func newDuplicateLabelCheck(e logicalplan.CheckDuplicateLabels, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	op, err := newOperator(e.Expr, storage, opts, hints)
	if err != nil {
		return nil, err
	}
	return exchange.NewDuplicateLabelCheck(op, opts), nil
}

// Copy from https://github.com/prometheus/prometheus/blob/v2.39.1/promql/engine.go#L791.
func getTimeRangesForVectorSelector(n *logicalplan.VectorSelector, opts *query.Options, evalRange int64) (int64, int64) {
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
