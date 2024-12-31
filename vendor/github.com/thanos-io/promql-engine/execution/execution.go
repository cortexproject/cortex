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
	"context"
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
func New(ctx context.Context, expr logicalplan.Node, storage storage.Scanners, opts *query.Options) (model.VectorOperator, error) {
	hints := promstorage.SelectHints{
		Start: opts.Start.UnixMilli(),
		End:   opts.End.UnixMilli(),
		Step:  opts.Step.Milliseconds(),
	}
	return newOperator(ctx, expr, storage, opts, hints)
}

func newOperator(ctx context.Context, expr logicalplan.Node, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	switch e := expr.(type) {
	case *logicalplan.NumberLiteral:
		return scan.NewNumberLiteralSelector(model.NewVectorPool(opts.StepsBatch), opts, e.Val), nil
	case *logicalplan.VectorSelector:
		return newVectorSelector(ctx, e, storage, opts, hints)
	case *logicalplan.FunctionCall:
		return newCall(ctx, e, storage, opts, hints)
	case *logicalplan.Aggregation:
		return newAggregateExpression(ctx, e, storage, opts, hints)
	case *logicalplan.Binary:
		return newBinaryExpression(ctx, e, storage, opts, hints)
	case *logicalplan.Parens:
		return newOperator(ctx, e.Expr, storage, opts, hints)
	case *logicalplan.Unary:
		return newUnaryExpression(ctx, e, storage, opts, hints)
	case *logicalplan.StepInvariantExpr:
		return newStepInvariantExpression(ctx, e, storage, opts, hints)
	case logicalplan.Deduplicate:
		return newDeduplication(ctx, e, storage, opts, hints)
	case logicalplan.RemoteExecution:
		return newRemoteExecution(ctx, e, opts, hints)
	case *logicalplan.CheckDuplicateLabels:
		return newDuplicateLabelCheck(ctx, e, storage, opts, hints)
	case logicalplan.Noop:
		return noop.NewOperator(opts), nil
	case logicalplan.UserDefinedExpr:
		return e.MakeExecutionOperator(ctx, model.NewVectorPool(opts.StepsBatch), opts, hints)
	default:
		return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "got: %s (%T)", e, e)
	}
}

func newVectorSelector(ctx context.Context, e *logicalplan.VectorSelector, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	start, end := getTimeRangesForVectorSelector(e, opts, 0)
	hints.Start = start
	hints.End = end
	return scanners.NewVectorSelector(ctx, opts, hints, *e)
}

func newCall(ctx context.Context, e *logicalplan.FunctionCall, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	hints.Func = e.Func.Name
	hints.Grouping = nil
	hints.By = false

	if e.Func.Name == "absent_over_time" {
		return newAbsentOverTimeOperator(ctx, e, scanners, opts, hints)
	}
	if e.Func.Name == "timestamp" {
		switch arg := e.Args[0].(type) {
		case *logicalplan.VectorSelector:
			arg.SelectTimestamp = true
			return newVectorSelector(ctx, arg, scanners, opts, hints)
		case *logicalplan.StepInvariantExpr:
			// Step invariant expressions on vector selectors need to be unwrapped so that we
			// can return the original timestamp rather than the step invariant one.
			switch vs := arg.Expr.(type) {
			case *logicalplan.VectorSelector:
				// Prometheus weirdness.
				if vs.Timestamp != nil {
					vs.OriginalOffset = 0
				}
				vs.SelectTimestamp = true
				return newVectorSelector(ctx, vs, scanners, opts, hints)
			}
			return newInstantVectorFunction(ctx, e, scanners, opts, hints)
		}
		return newInstantVectorFunction(ctx, e, scanners, opts, hints)
	}

	// TODO(saswatamcode): Range vector result might need new operator
	// before it can be non-nested. https://github.com/thanos-io/promql-engine/issues/39
	for i := range e.Args {
		switch t := e.Args[i].(type) {
		case *logicalplan.Subquery:
			return newSubqueryFunction(ctx, e, t, scanners, opts, hints)
		case *logicalplan.MatrixSelector:
			return newRangeVectorFunction(ctx, e, t, scanners, opts, hints)
		}
	}
	return newInstantVectorFunction(ctx, e, scanners, opts, hints)
}

func newAbsentOverTimeOperator(ctx context.Context, call *logicalplan.FunctionCall, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	switch arg := call.Args[0].(type) {
	case *logicalplan.Subquery:
		matrixCall := &logicalplan.FunctionCall{
			Func: parser.Function{Name: "last_over_time"},
		}
		argOp, err := newSubqueryFunction(ctx, matrixCall, arg, scanners, opts, hints)
		if err != nil {
			return nil, err
		}
		f := &logicalplan.FunctionCall{
			Func: parser.Function{Name: "absent"},
			Args: []logicalplan.Node{matrixCall},
		}
		return function.NewFunctionOperator(f, []model.VectorOperator{argOp}, opts.StepsBatch, opts)
	case *logicalplan.MatrixSelector:
		matrixCall := &logicalplan.FunctionCall{
			Func: parser.Function{Name: "last_over_time"},
			Args: call.Args,
		}
		argOp, err := newRangeVectorFunction(ctx, matrixCall, arg, scanners, opts, hints)
		if err != nil {
			return nil, err
		}
		f := &logicalplan.FunctionCall{
			Func: parser.Function{Name: "absent"},
			Args: []logicalplan.Node{&logicalplan.MatrixSelector{
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

func newRangeVectorFunction(ctx context.Context, e *logicalplan.FunctionCall, t *logicalplan.MatrixSelector, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	// TODO(saswatamcode): Range vector result might need new operator
	// before it can be non-nested. https://github.com/thanos-io/promql-engine/issues/39
	milliSecondRange := t.Range.Milliseconds()
	if function.IsExtFunction(e.Func.Name) {
		milliSecondRange += opts.ExtLookbackDelta.Milliseconds()
	}

	start, end := getTimeRangesForVectorSelector(t.VectorSelector, opts, milliSecondRange)
	hints.Start = start
	hints.End = end
	hints.Range = milliSecondRange
	return scanners.NewMatrixSelector(ctx, opts, hints, *t, *e)
}

func newSubqueryFunction(ctx context.Context, e *logicalplan.FunctionCall, t *logicalplan.Subquery, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	// TODO: We dont implement ext functions
	if parse.IsExtFunction(e.Func.Name) {
		return nil, parse.ErrNotImplemented
	}

	nOpts := query.NestedOptionsForSubquery(opts, t.Step, t.Range, t.Offset)

	hints.Start = nOpts.Start.UnixMilli()
	hints.End = nOpts.End.UnixMilli()
	hints.Step = nOpts.Step.Milliseconds()

	inner, err := newOperator(ctx, t.Expr, storage, nOpts, hints)
	if err != nil {
		return nil, err
	}

	outerOpts := *opts
	if t.Timestamp != nil {
		outerOpts.Start = time.UnixMilli(*t.Timestamp)
		outerOpts.End = time.UnixMilli(*t.Timestamp)
	}

	var scalarArg model.VectorOperator
	switch e.Func.Name {
	case "quantile_over_time":
		// quantile_over_time(scalar, range-vector)
		scalarArg, err = newOperator(ctx, e.Args[0], storage, opts, hints)
		if err != nil {
			return nil, err
		}
	case "predict_linear":
		// predict_linear(range-vector, scalar)
		scalarArg, err = newOperator(ctx, e.Args[1], storage, opts, hints)
		if err != nil {
			return nil, err
		}
	}

	return scan.NewSubqueryOperator(model.NewVectorPool(opts.StepsBatch), inner, scalarArg, &outerOpts, e, t)
}

func newInstantVectorFunction(ctx context.Context, e *logicalplan.FunctionCall, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	nextOperators := make([]model.VectorOperator, 0, len(e.Args))
	for i := range e.Args {
		// Strings don't need an operator
		if e.Args[i].ReturnType() == parser.ValueTypeString {
			continue
		}
		next, err := newOperator(ctx, e.Args[i], storage, opts, hints)
		if err != nil {
			return nil, err
		}
		nextOperators = append(nextOperators, next)
	}

	return function.NewFunctionOperator(e, nextOperators, opts.StepsBatch, opts)
}

func newAggregateExpression(ctx context.Context, e *logicalplan.Aggregation, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	hints.Func = e.Op.String()
	hints.Grouping = e.Grouping
	hints.By = !e.Without

	next, err := newOperator(ctx, e.Expr, scanners, opts, hints)
	if err != nil {
		return nil, err
	}
	if e.Op == parser.COUNT_VALUES {
		param := logicalplan.UnsafeUnwrapString(e.Param)
		return aggregate.NewCountValues(model.NewVectorPool(opts.StepsBatch), next, param, !e.Without, e.Grouping, opts), nil
	}

	// parameter is only required for count_values, quantile, topk and bottomk.
	var paramOp model.VectorOperator
	switch e.Op {
	case parser.QUANTILE, parser.TOPK, parser.BOTTOMK:
		paramOp, err = newOperator(ctx, e.Param, scanners, opts, hints)
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

func newBinaryExpression(ctx context.Context, e *logicalplan.Binary, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	if e.LHS.ReturnType() == parser.ValueTypeScalar || e.RHS.ReturnType() == parser.ValueTypeScalar {
		return newScalarBinaryOperator(ctx, e, scanners, opts, hints)
	}
	return newVectorBinaryOperator(ctx, e, scanners, opts, hints)
}

func newVectorBinaryOperator(ctx context.Context, e *logicalplan.Binary, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	leftOperator, err := newOperator(ctx, e.LHS, storage, opts, hints)
	if err != nil {
		return nil, err
	}
	rightOperator, err := newOperator(ctx, e.RHS, storage, opts, hints)
	if err != nil {
		return nil, err
	}
	return binary.NewVectorOperator(model.NewVectorPool(opts.StepsBatch), leftOperator, rightOperator, e.VectorMatching, e.Op, e.ReturnBool, opts)
}

func newScalarBinaryOperator(ctx context.Context, e *logicalplan.Binary, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	lhs, err := newOperator(ctx, e.LHS, storage, opts, hints)
	if err != nil {
		return nil, err
	}
	rhs, err := newOperator(ctx, e.RHS, storage, opts, hints)
	if err != nil {
		return nil, err
	}

	scalarSide := binary.ScalarSideRight
	if e.LHS.ReturnType() == parser.ValueTypeScalar && e.RHS.ReturnType() == parser.ValueTypeScalar {
		scalarSide = binary.ScalarSideBoth
	} else if e.LHS.ReturnType() == parser.ValueTypeScalar {
		rhs, lhs = lhs, rhs
		scalarSide = binary.ScalarSideLeft
	}

	return binary.NewScalar(model.NewVectorPoolWithSize(opts.StepsBatch, 1), lhs, rhs, e.Op, scalarSide, e.ReturnBool, opts)
}

func newUnaryExpression(ctx context.Context, e *logicalplan.Unary, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	next, err := newOperator(ctx, e.Expr, scanners, opts, hints)
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

func newStepInvariantExpression(ctx context.Context, e *logicalplan.StepInvariantExpr, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	switch t := e.Expr.(type) {
	case *logicalplan.NumberLiteral:
		return scan.NewNumberLiteralSelector(model.NewVectorPool(opts.StepsBatch), opts, t.Val), nil
	}
	next, err := newOperator(ctx, e.Expr, scanners, opts.WithEndTime(opts.Start), hints)
	if err != nil {
		return nil, err
	}
	return step_invariant.NewStepInvariantOperator(model.NewVectorPoolWithSize(opts.StepsBatch, 1), next, e.Expr, opts)
}

func newDeduplication(ctx context.Context, e logicalplan.Deduplicate, scanners storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	// The Deduplicate operator will deduplicate samples using a last-sample-wins strategy.
	// Sorting engines by MaxT ensures that samples produced due to
	// staleness will be overwritten and corrected by samples coming from
	// engines with a higher max time.
	sort.Slice(e.Expressions, func(i, j int) bool {
		return e.Expressions[i].Engine.MaxT() < e.Expressions[j].Engine.MaxT()
	})

	operators := make([]model.VectorOperator, len(e.Expressions))
	for i, expr := range e.Expressions {
		operator, err := newOperator(ctx, expr, scanners, opts, hints)
		if err != nil {
			return nil, err
		}
		operators[i] = operator
	}
	coalesce := exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, 0, operators...)
	dedup := exchange.NewDedupOperator(model.NewVectorPool(opts.StepsBatch), coalesce, opts)
	return exchange.NewConcurrent(dedup, 2, opts), nil
}

func newRemoteExecution(ctx context.Context, e logicalplan.RemoteExecution, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	// Create a new remote query scoped to the calculated start time.
	qry, err := e.Engine.NewRangeQuery(ctx, promql.NewPrometheusQueryOpts(false, opts.LookbackDelta), e.Query, e.QueryRangeStart, opts.End, opts.Step)
	if err != nil {
		return nil, err
	}

	// The selector uses the original query time to make sure that steps from different
	// operators have the same timestamps.
	// We need to set the lookback for the selector to 0 since the remote query already applies one lookback.
	selectorOpts := *opts
	selectorOpts.LookbackDelta = 0
	remoteExec := remote.NewExecution(qry, model.NewVectorPool(opts.StepsBatch), e.QueryRangeStart, e.Engine.LabelSets(), &selectorOpts, hints)
	return exchange.NewConcurrent(remoteExec, 2, opts), nil
}

func newDuplicateLabelCheck(ctx context.Context, e *logicalplan.CheckDuplicateLabels, storage storage.Scanners, opts *query.Options, hints promstorage.SelectHints) (model.VectorOperator, error) {
	op, err := newOperator(ctx, e.Expr, storage, opts, hints)
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
