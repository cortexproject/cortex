// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"runtime"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

type prometheusScanners struct {
	selectors *SelectorPool
}

func NewPrometheusScanners(queryable storage.Queryable) *prometheusScanners {
	return &prometheusScanners{selectors: NewSelectorPool(queryable)}
}

func (p prometheusScanners) NewVectorSelector(
	opts *query.Options,
	hints storage.SelectHints,
	logicalNode logicalplan.VectorSelector,
) (model.VectorOperator, error) {
	numShards := runtime.GOMAXPROCS(0) / 2
	if numShards < 1 {
		numShards = 1
	}
	selector := p.selectors.GetFilteredSelector(hints.Start, hints.End, opts.Step.Milliseconds(), logicalNode.VectorSelector.LabelMatchers, logicalNode.Filters, hints)

	operators := make([]model.VectorOperator, 0, numShards)
	for i := 0; i < numShards; i++ {
		operator := exchange.NewConcurrent(
			NewVectorSelector(
				model.NewVectorPool(opts.StepsBatch),
				selector,
				opts,
				logicalNode.Offset,
				logicalNode.BatchSize,
				logicalNode.SelectTimestamp,
				i,
				numShards,
			), 2, opts)
		operators = append(operators, operator)
	}

	return exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, logicalNode.BatchSize*int64(numShards), operators...), nil
}

func (p prometheusScanners) NewMatrixSelector(
	opts *query.Options,
	hints storage.SelectHints,
	logicalNode logicalplan.MatrixSelector,
	call logicalplan.FunctionCall,
) (model.VectorOperator, error) {
	numShards := runtime.GOMAXPROCS(0) / 2
	if numShards < 1 {
		numShards = 1
	}

	arg := 0.0
	if call.Func.Name == "quantile_over_time" {
		unwrap, err := logicalplan.UnwrapFloat(call.Args[0])
		if err != nil {
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "quantile_over_time with expression as first argument is not supported")
		}
		arg = unwrap
	}

	vs := logicalNode.VectorSelector
	filter := p.selectors.GetFilteredSelector(hints.Start, hints.End, opts.Step.Milliseconds(), vs.LabelMatchers, vs.Filters, hints)

	operators := make([]model.VectorOperator, 0, numShards)
	for i := 0; i < numShards; i++ {
		operator, err := NewMatrixSelector(
			model.NewVectorPool(opts.StepsBatch),
			filter,
			call.Func.Name,
			arg,
			opts,
			logicalNode.Range,
			vs.Offset,
			vs.BatchSize,
			i,
			numShards,
		)
		if err != nil {
			return nil, err
		}
		operators = append(operators, exchange.NewConcurrent(operator, 2, opts))
	}

	return exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, vs.BatchSize*int64(numShards), operators...), nil
}
