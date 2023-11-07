// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"
)

// TODO: only instant subqueries for now.
type subqueryOperator struct {
	next        model.VectorOperator
	pool        *model.VectorPool
	call        FunctionCall
	mint        int64
	maxt        int64
	currentStep int64

	funcExpr *parser.Call
	subQuery *parser.SubqueryExpr

	onceSeries sync.Once
	series     []labels.Labels
	acc        [][]Sample
}

func NewSubqueryOperator(pool *model.VectorPool, next model.VectorOperator, opts *query.Options, funcExpr *parser.Call, subQuery *parser.SubqueryExpr) (model.VectorOperator, error) {
	call, err := NewRangeVectorFunc(funcExpr.Func.Name)
	if err != nil {
		return nil, err
	}
	return &subqueryOperator{
		next:        next,
		call:        call,
		pool:        pool,
		funcExpr:    funcExpr,
		subQuery:    subQuery,
		mint:        opts.Start.UnixMilli(),
		maxt:        opts.End.UnixMilli(),
		currentStep: opts.Start.UnixMilli(),
	}, nil
}

func (o *subqueryOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*subqueryOperator] %v()", o.funcExpr.Func.Name), []model.VectorOperator{o.next}
}

func (o *subqueryOperator) GetPool() *model.VectorPool { return o.pool }

func (o *subqueryOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if o.currentStep > o.maxt {
		return nil, nil
	}
	if err := o.initSeries(ctx); err != nil {
		return nil, err
	}

ACC:
	for {
		vectors, err := o.next.Next(ctx)
		if err != nil {
			return nil, err
		}
		if len(vectors) == 0 {
			break ACC
		}
		for _, vector := range vectors {
			for j, s := range vector.Samples {
				o.acc[vector.SampleIDs[j]] = append(o.acc[vector.SampleIDs[j]], Sample{T: vector.T, F: s})
			}
			o.next.GetPool().PutStepVector(vector)
		}
		o.next.GetPool().PutVectors(vectors)
	}

	res := o.pool.GetVectorBatch()
	sv := o.pool.GetStepVector(o.currentStep)
	for sampleId, rangeSamples := range o.acc {
		f, h, ok := o.call(FunctionArgs{
			Samples:     rangeSamples,
			StepTime:    o.currentStep,
			SelectRange: o.subQuery.Range.Milliseconds(),
			Offset:      o.subQuery.Offset.Milliseconds(),
		})
		if ok {
			if h != nil {
				sv.AppendHistogram(o.pool, uint64(sampleId), h)
			} else {
				sv.AppendSample(o.pool, uint64(sampleId), f)
			}
		}
	}
	res = append(res, sv)

	o.currentStep++
	return res, nil
}

func (o *subqueryOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.initSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *subqueryOperator) initSeries(ctx context.Context) error {
	var err error
	o.onceSeries.Do(func() {
		var series []labels.Labels
		series, err = o.next.Series(ctx)
		if err != nil {
			return
		}

		o.series = make([]labels.Labels, len(series))
		o.acc = make([][]Sample, len(series))
		var b labels.ScratchBuilder
		for i, s := range series {
			lbls := s
			if o.funcExpr.Func.Name != "last_over_time" {
				lbls, _ = extlabels.DropMetricName(s, b)
			}
			o.series[i] = lbls
		}
	})
	return err
}
