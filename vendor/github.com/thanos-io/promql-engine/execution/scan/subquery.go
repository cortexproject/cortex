// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/ringbuffer"
)

type subqueryOperator struct {
	model.OperatorTelemetry

	next        model.VectorOperator
	pool        *model.VectorPool
	call        FunctionCall
	mint        int64
	maxt        int64
	currentStep int64
	step        int64
	stepsBatch  int

	scalarArgs []float64
	funcExpr   *parser.Call
	subQuery   *parser.SubqueryExpr

	onceSeries sync.Once
	series     []labels.Labels

	lastVectors   []model.StepVector
	lastCollected int
	buffers       []*ringbuffer.RingBuffer[Value]
}

func NewSubqueryOperator(pool *model.VectorPool, next model.VectorOperator, opts *query.Options, funcExpr *parser.Call, subQuery *parser.SubqueryExpr) (model.VectorOperator, error) {
	call, err := NewRangeVectorFunc(funcExpr.Func.Name)
	if err != nil {
		return nil, err
	}
	step := opts.Step.Milliseconds()
	if step == 0 {
		step = 1
	}

	arg := 0.0
	if funcExpr.Func.Name == "quantile_over_time" {
		unwrap, err := logicalplan.UnwrapFloat(funcExpr.Args[0])
		if err != nil {
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "quantile_over_time with expression as first argument is not supported")
		}
		arg = unwrap
	}

	return &subqueryOperator{
		OperatorTelemetry: model.NewTelemetry("[subquery]", opts.EnableAnalysis),

		next:          next,
		call:          call,
		pool:          pool,
		scalarArgs:    []float64{arg},
		funcExpr:      funcExpr,
		subQuery:      subQuery,
		mint:          opts.Start.UnixMilli(),
		maxt:          opts.End.UnixMilli(),
		currentStep:   opts.Start.UnixMilli(),
		step:          step,
		stepsBatch:    opts.StepsBatch,
		lastCollected: -1,
	}, nil
}

func (o *subqueryOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[subquery] %v()", o.funcExpr.Func.Name), []model.VectorOperator{o.next}
}

func (o *subqueryOperator) GetPool() *model.VectorPool { return o.pool }

func (o *subqueryOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { o.OperatorTelemetry.AddExecutionTimeTaken(time.Since(start)) }()

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

	res := o.pool.GetVectorBatch()
	for i := 0; o.currentStep <= o.maxt && i < o.stepsBatch; i++ {
		mint := o.currentStep - o.subQuery.Range.Milliseconds() - o.subQuery.OriginalOffset.Milliseconds()
		maxt := o.currentStep - o.subQuery.OriginalOffset.Milliseconds()
		for _, b := range o.buffers {
			b.DropBefore(mint)
		}
		if len(o.lastVectors) > 0 {
			for _, v := range o.lastVectors[o.lastCollected+1:] {
				if v.T > maxt {
					break
				}
				o.collect(v, mint)
				o.lastCollected++
			}
			if o.lastCollected == len(o.lastVectors)-1 {
				o.next.GetPool().PutVectors(o.lastVectors)
				o.lastVectors = nil
				o.lastCollected = -1
			}
		}

	ACC:
		for len(o.lastVectors) == 0 {
			vectors, err := o.next.Next(ctx)
			if err != nil {
				return nil, err
			}
			if len(vectors) == 0 {
				break ACC
			}
			for j, vector := range vectors {
				if vector.T > maxt {
					o.lastVectors = vectors
					o.lastCollected = j - 1
					break ACC
				}
				o.collect(vector, mint)
			}
			o.next.GetPool().PutVectors(vectors)
		}

		sv := o.pool.GetStepVector(o.currentStep)
		for sampleId, rangeSamples := range o.buffers {
			f, h, ok := o.call(FunctionArgs{
				ScalarPoints: o.scalarArgs,
				Samples:      rangeSamples.Samples(),
				StepTime:     maxt,
				SelectRange:  o.subQuery.Range.Milliseconds(),
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

		o.currentStep += o.step
	}
	return res, nil
}

func (o *subqueryOperator) collect(v model.StepVector, mint int64) {
	if v.T < mint {
		o.next.GetPool().PutStepVector(v)
		return
	}
	for i, s := range v.Samples {
		buffer := o.buffers[v.SampleIDs[i]]
		if buffer.Len() > 0 && v.T <= buffer.MaxT() {
			continue
		}
		buffer.Push(v.T, Value{F: s})
	}
	for i, s := range v.Histograms {
		buffer := o.buffers[v.HistogramIDs[i]]
		if buffer.Len() > 0 && v.T < buffer.MaxT() {
			continue
		}
		buffer.Push(v.T, Value{H: s})
	}
	o.next.GetPool().PutStepVector(v)
}

func (o *subqueryOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { o.OperatorTelemetry.AddExecutionTimeTaken(time.Since(start)) }()

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
		o.buffers = make([]*ringbuffer.RingBuffer[Value], len(series))
		for i := range o.buffers {
			o.buffers[i] = ringbuffer.New[Value](8)
		}
		var b labels.ScratchBuilder
		for i, s := range series {
			lbls := s
			if o.funcExpr.Func.Name != "last_over_time" {
				lbls, _ = extlabels.DropMetricName(s, b)
			}
			o.series[i] = lbls
		}
		o.pool.SetStepSize(len(o.series))
	})
	return err
}
