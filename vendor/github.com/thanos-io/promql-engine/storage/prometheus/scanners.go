// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"math"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/execution/warnings"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

type Scanners struct {
	selectors *SelectorPool

	querier storage.Querier
}

func (s *Scanners) Close() error {
	return s.querier.Close()
}

func NewPrometheusScanners(queryable storage.Queryable, qOpts *query.Options, lplan logicalplan.Plan) (*Scanners, error) {
	var min, max int64
	if lplan != nil {
		min, max = lplan.MinMaxTime(qOpts)
	} else {
		min, max = qOpts.Start.UnixMilli(), qOpts.End.UnixMilli()
	}

	querier, err := queryable.Querier(min, max)
	if err != nil {
		return nil, errors.Wrap(err, "create querier")
	}
	return &Scanners{querier: querier, selectors: NewSelectorPool(querier)}, nil
}

func (p Scanners) NewVectorSelector(
	_ context.Context,
	opts *query.Options,
	hints storage.SelectHints,
	logicalNode logicalplan.VectorSelector,
) (model.VectorOperator, error) {
	selector := p.selectors.GetFilteredSelector(hints.Start, hints.End, opts.Step.Milliseconds(), logicalNode.VectorSelector.LabelMatchers, logicalNode.Filters, hints)
	if logicalNode.DecodeNativeHistogramStats {
		selector = newHistogramStatsSelector(selector)
	}

	operators := make([]model.VectorOperator, 0, opts.DecodingConcurrency)
	for i := 0; i < opts.DecodingConcurrency; i++ {
		operator := exchange.NewConcurrent(
			NewVectorSelector(
				model.NewVectorPool(opts.StepsBatch),
				selector,
				opts,
				logicalNode.Offset,
				logicalNode.BatchSize,
				logicalNode.SelectTimestamp,
				i,
				opts.DecodingConcurrency,
			), 2, opts)
		operators = append(operators, operator)
	}

	return exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, logicalNode.BatchSize*int64(opts.DecodingConcurrency), operators...), nil
}

func (p Scanners) NewMatrixSelector(
	ctx context.Context,
	opts *query.Options,
	hints storage.SelectHints,
	logicalNode logicalplan.MatrixSelector,
	call logicalplan.FunctionCall,
) (model.VectorOperator, error) {
	arg := 0.0
	switch call.Func.Name {
	case "quantile_over_time":
		unwrap, err := logicalplan.UnwrapFloat(call.Args[0])
		if err != nil {
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "quantile_over_time with expression as first argument is not supported")
		}
		arg = unwrap
		if math.IsNaN(unwrap) || unwrap < 0 || unwrap > 1 {
			warnings.AddToContext(annotations.NewInvalidQuantileWarning(unwrap, posrange.PositionRange{}), ctx)
		}
	case "predict_linear":
		unwrap, err := logicalplan.UnwrapFloat(call.Args[1])
		if err != nil {
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "predict_linear with expression as second argument is not supported")
		}
		arg = unwrap
	}

	vs := logicalNode.VectorSelector
	selector := p.selectors.GetFilteredSelector(hints.Start, hints.End, opts.Step.Milliseconds(), vs.LabelMatchers, vs.Filters, hints)
	if logicalNode.VectorSelector.DecodeNativeHistogramStats {
		selector = newHistogramStatsSelector(selector)
	}

	operators := make([]model.VectorOperator, 0, opts.DecodingConcurrency)
	for i := 0; i < opts.DecodingConcurrency; i++ {
		operator, err := NewMatrixSelector(
			model.NewVectorPool(opts.StepsBatch),
			selector,
			call.Func.Name,
			arg,
			opts,
			logicalNode.Range,
			vs.Offset,
			vs.BatchSize,
			i,
			opts.DecodingConcurrency,
		)
		if err != nil {
			return nil, err
		}
		operators = append(operators, exchange.NewConcurrent(operator, 2, opts))
	}

	return exchange.NewCoalesce(model.NewVectorPool(opts.StepsBatch), opts, vs.BatchSize*int64(opts.DecodingConcurrency), operators...), nil
}

type histogramStatsSelector struct {
	SeriesSelector
}

func newHistogramStatsSelector(seriesSelector SeriesSelector) histogramStatsSelector {
	return histogramStatsSelector{SeriesSelector: seriesSelector}
}

func (h histogramStatsSelector) GetSeries(ctx context.Context, shard, numShards int) ([]SignedSeries, error) {
	series, err := h.SeriesSelector.GetSeries(ctx, shard, numShards)
	if err != nil {
		return nil, err
	}
	for i := range series {
		series[i].Series = newHistogramStatsSeries(series[i].Series)
	}
	return series, nil
}

type histogramStatsSeries struct {
	storage.Series
}

func newHistogramStatsSeries(series storage.Series) histogramStatsSeries {
	return histogramStatsSeries{Series: series}
}

func (h histogramStatsSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	return NewHistogramStatsIterator(h.Series.Iterator(it))
}
