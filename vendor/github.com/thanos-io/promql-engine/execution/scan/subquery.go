// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/thanos-io/promql-engine/execution/telemetry"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/ringbuffer"
)

type subqueryOperator struct {
	telemetry.OperatorTelemetry

	next    model.VectorOperator
	paramOp model.VectorOperator

	pool        *model.VectorPool
	call        ringbuffer.FunctionCall
	mint        int64
	maxt        int64
	currentStep int64
	step        int64
	stepsBatch  int
	opts        *query.Options

	funcExpr *logicalplan.FunctionCall
	subQuery *logicalplan.Subquery

	onceSeries sync.Once
	series     []labels.Labels

	lastVectors   []model.StepVector
	lastCollected int
	buffers       []*ringbuffer.GenericRingBuffer

	// params holds the function parameter for each step.
	params []float64
}

func NewSubqueryOperator(pool *model.VectorPool, next, paramOp model.VectorOperator, opts *query.Options, funcExpr *logicalplan.FunctionCall, subQuery *logicalplan.Subquery) (model.VectorOperator, error) {
	call, err := ringbuffer.NewRangeVectorFunc(funcExpr.Func.Name)
	if err != nil {
		return nil, err
	}
	step := opts.Step.Milliseconds()
	if step == 0 {
		step = 1
	}

	o := &subqueryOperator{
		next:          next,
		paramOp:       paramOp,
		call:          call,
		pool:          pool,
		funcExpr:      funcExpr,
		subQuery:      subQuery,
		opts:          opts,
		mint:          opts.Start.UnixMilli(),
		maxt:          opts.End.UnixMilli(),
		currentStep:   opts.Start.UnixMilli(),
		step:          step,
		stepsBatch:    opts.StepsBatch,
		lastCollected: -1,
		params:        make([]float64, opts.StepsBatch),
	}
	o.OperatorTelemetry = telemetry.NewSubqueryTelemetry(o, opts)

	return o, nil
}

func (o *subqueryOperator) String() string {
	return fmt.Sprintf("[subquery] %v()", o.funcExpr.Func.Name)
}

func (o *subqueryOperator) Explain() (next []model.VectorOperator) {
	switch o.funcExpr.Func.Name {
	case "quantile_over_time", "predict_linear":
		return []model.VectorOperator{o.paramOp, o.next}
	default:
		return []model.VectorOperator{o.next}
	}
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

	if o.paramOp != nil {
		args, err := o.paramOp.Next(ctx)
		if err != nil {
			return nil, err
		}
		for i := range args {
			o.params[i] = math.NaN()
			if len(args[i].Samples) == 1 {
				o.params[i] = args[i].Samples[0]
			}
			o.paramOp.GetPool().PutStepVector(args[i])
		}
		o.paramOp.GetPool().PutVectors(args)
	}

	res := o.pool.GetVectorBatch()
	for i := 0; o.currentStep <= o.maxt && i < o.stepsBatch; i++ {
		mint := o.currentStep - o.subQuery.Range.Milliseconds() - o.subQuery.OriginalOffset.Milliseconds()
		maxt := o.currentStep - o.subQuery.OriginalOffset.Milliseconds()
		for _, b := range o.buffers {
			b.Reset(mint, maxt+o.subQuery.Offset.Milliseconds())
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
			f, h, ok, err := rangeSamples.Eval(o.params[i], nil)
			if err != nil {
				return nil, err
			}
			if ok {
				if h != nil {
					sv.AppendHistogram(o.pool, uint64(sampleId), h)
				} else {
					sv.AppendSample(o.pool, uint64(sampleId), f)
				}
			}
			o.IncrementSamplesAtTimestamp(rangeSamples.Len(), sv.T)
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
		buffer.Push(v.T, ringbuffer.Value{F: s})
	}
	for i, s := range v.Histograms {
		buffer := o.buffers[v.HistogramIDs[i]]
		if buffer.Len() > 0 && v.T < buffer.MaxT() {
			continue
		}
		buffer.Push(v.T, ringbuffer.Value{H: s})
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
		o.buffers = make([]*ringbuffer.GenericRingBuffer, len(series))
		for i := range o.buffers {
			o.buffers[i] = ringbuffer.New(8, o.subQuery.Range.Milliseconds(), o.subQuery.Offset.Milliseconds(), o.call)
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
