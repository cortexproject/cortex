// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/efficientgo/core/errors"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
	"github.com/thanos-community/promql-engine/parser"
	"github.com/thanos-community/promql-engine/query"
)

// functionOperator returns []model.StepVector after processing input with desired function.
type functionOperator struct {
	funcExpr *parser.Call
	series   []labels.Labels
	once     sync.Once

	vectorIndex int
	nextOps     []model.VectorOperator

	call         FunctionCall
	scalarPoints [][]float64
	sampleBuf    []promql.Sample
}

type noArgFunctionOperator struct {
	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	stepsBatch  int
	funcExpr    *parser.Call
	call        FunctionCall
	vectorPool  *model.VectorPool
	series      []labels.Labels
	sampleIDs   []uint64
}

func (o *noArgFunctionOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*noArgFunctionOperator] %v()", o.funcExpr.Func.Name), []model.VectorOperator{}
}

func (o *noArgFunctionOperator) Series(_ context.Context) ([]labels.Labels, error) {
	return o.series, nil
}

func (o *noArgFunctionOperator) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *noArgFunctionOperator) Next(_ context.Context) ([]model.StepVector, error) {
	if o.currentStep > o.maxt {
		return nil, nil
	}
	ret := o.vectorPool.GetVectorBatch()
	for i := 0; i < o.stepsBatch && o.currentStep <= o.maxt; i++ {
		sv := o.vectorPool.GetStepVector(o.currentStep)
		result := o.call(FunctionArgs{
			StepTime: o.currentStep,
		})
		sv.Samples = []float64{result.F}
		sv.SampleIDs = o.sampleIDs

		ret = append(ret, sv)
		o.currentStep += o.step
	}

	return ret, nil
}

func NewFunctionOperator(funcExpr *parser.Call, call FunctionCall, nextOps []model.VectorOperator, stepsBatch int, opts *query.Options) (model.VectorOperator, error) {
	// Short-circuit functions that take no args. Their only input is the step's timestamp.
	if len(nextOps) == 0 {
		interval := opts.Step.Milliseconds()
		// We set interval to be at least 1.
		if interval == 0 {
			interval = 1
		}

		op := &noArgFunctionOperator{
			currentStep: opts.Start.UnixMilli(),
			mint:        opts.Start.UnixMilli(),
			maxt:        opts.End.UnixMilli(),
			step:        interval,
			stepsBatch:  stepsBatch,
			funcExpr:    funcExpr,
			call:        call,
			vectorPool:  model.NewVectorPool(stepsBatch),
		}

		switch funcExpr.Func.Name {
		case "pi", "time", "scalar":
			op.sampleIDs = []uint64{0}
		default:
			// Other functions require non-nil labels.
			op.series = []labels.Labels{{}}
			op.sampleIDs = []uint64{0}
		}

		return op, nil
	}
	scalarPoints := make([][]float64, stepsBatch)
	for i := 0; i < stepsBatch; i++ {
		scalarPoints[i] = make([]float64, len(nextOps)-1)
	}
	f := &functionOperator{
		nextOps:      nextOps,
		call:         call,
		funcExpr:     funcExpr,
		vectorIndex:  0,
		scalarPoints: scalarPoints,
		sampleBuf:    make([]promql.Sample, 1),
	}

	for i := range funcExpr.Args {
		if funcExpr.Args[i].Type() == parser.ValueTypeVector {
			f.vectorIndex = i
			break
		}
	}

	// Check selector type.
	// TODO(saswatamcode): Add support for matrix.
	switch funcExpr.Args[f.vectorIndex].Type() {
	case parser.ValueTypeVector, parser.ValueTypeScalar:
		return f, nil
	default:
		return nil, errors.Wrapf(parse.ErrNotImplemented, "got %s:", funcExpr.String())
	}
}

func (o *functionOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*functionOperator] %v(%v)", o.funcExpr.Func.Name, o.funcExpr.Args), o.nextOps
}

func (o *functionOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}

	return o.series, nil
}

func (o *functionOperator) GetPool() *model.VectorPool {
	return o.nextOps[o.vectorIndex].GetPool()
}

func (o *functionOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}

	// Process non-variadic single/multi-arg instant vector and scalar input functions.
	// Call next on vector input.
	vectors, err := o.nextOps[o.vectorIndex].Next(ctx)
	if err != nil {
		return nil, err
	}

	if len(vectors) == 0 {
		return nil, nil
	}
	if o.funcExpr.Func.Name == "label_join" {
		return vectors, nil
	}

	scalarIndex := 0
	for i := range o.nextOps {
		if i == o.vectorIndex {
			continue
		}

		scalarVectors, err := o.nextOps[i].Next(ctx)
		if err != nil {
			return nil, err
		}

		for batchIndex := range vectors {
			val := math.NaN()
			if len(scalarVectors) > 0 && len(scalarVectors[batchIndex].Samples) > 0 {
				val = scalarVectors[batchIndex].Samples[0]
				o.nextOps[i].GetPool().PutStepVector(scalarVectors[batchIndex])
			}
			o.scalarPoints[batchIndex][scalarIndex] = val
		}
		o.nextOps[i].GetPool().PutVectors(scalarVectors)
		scalarIndex++
	}

	for batchIndex, vector := range vectors {
		// scalar() depends on number of samples per vector and returns NaN if len(samples) != 1.
		// So need to handle this separately here, instead of going via call which is per point.
		// TODO(fpetkovski): make this decision once in the constructor and create a new operator.
		if o.funcExpr.Func.Name == "scalar" {
			if len(vector.Samples) == 0 {
				vectors[batchIndex].SampleIDs = []uint64{0}
				vectors[batchIndex].Samples = []float64{math.NaN()}
				continue
			}

			vectors[batchIndex].SampleIDs = vector.SampleIDs[:1]
			vectors[batchIndex].SampleIDs[0] = 0
			if len(vector.Samples) > 1 {
				vectors[batchIndex].Samples = vector.Samples[:1]
				vectors[batchIndex].Samples[0] = math.NaN()
			}
			continue
		}

		i := 0
		for i < len(vectors[batchIndex].Samples) {
			o.sampleBuf[0].H = nil
			o.sampleBuf[0].F = vector.Samples[i]
			result := o.call(o.newFunctionArgs(vector, batchIndex))

			if result.T != InvalidSample.T {
				vector.Samples[i] = result.F
				i++
			} else {
				// This operator modifies samples directly in the input vector to avoid allocations.
				// In case of an invalid output sample, we need to do an in-place removal of the input sample.
				vectors[batchIndex].RemoveSample(i)
			}
		}

		i = 0
		for i < len(vectors[batchIndex].Histograms) {
			o.sampleBuf[0].H = vector.Histograms[i]
			result := o.call(o.newFunctionArgs(vector, batchIndex))

			// This operator modifies samples directly in the input vector to avoid allocations.
			// All current functions for histograms produce a float64 sample. It's therefore safe to
			// always remove the input histogram so that it does not propagate to the output.
			sampleID := vectors[batchIndex].HistogramIDs[i]
			vectors[batchIndex].RemoveHistogram(i)
			if result.T != InvalidSample.T {
				vectors[batchIndex].AppendSample(o.GetPool(), sampleID, result.F)
			}
		}
	}

	return vectors, nil
}

func (o *functionOperator) newFunctionArgs(vector model.StepVector, batchIndex int) FunctionArgs {
	return FunctionArgs{
		Labels:       o.series[0],
		Samples:      o.sampleBuf,
		StepTime:     vector.T,
		ScalarPoints: o.scalarPoints[batchIndex],
	}
}

func (o *functionOperator) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {
		if o.funcExpr.Func.Name == "vector" {
			o.series = []labels.Labels{labels.New()}
			return
		}

		if o.funcExpr.Func.Name == "scalar" {
			o.series = []labels.Labels{}
			return
		}

		series, loadErr := o.nextOps[o.vectorIndex].Series(ctx)
		if loadErr != nil {
			err = loadErr
			return
		}

		o.series = make([]labels.Labels, len(series))

		var labelJoinDst string
		var labelJoinSep string
		var labelJoinSrcLabels []string
		if o.funcExpr.Func.Name == "label_join" {
			l := len(o.funcExpr.Args)
			labelJoinDst = o.funcExpr.Args[1].(*parser.StringLiteral).Val
			if !prommodel.LabelName(labelJoinDst).IsValid() {
				err = errors.Newf("invalid destination label name in label_join: %s", labelJoinDst)
				return
			}
			labelJoinSep = o.funcExpr.Args[2].(*parser.StringLiteral).Val
			for j := 3; j < l; j++ {
				labelJoinSrcLabels = append(labelJoinSrcLabels, o.funcExpr.Args[j].(*parser.StringLiteral).Val)
			}
		}
		for i, s := range series {
			lbls := s
			switch o.funcExpr.Func.Name {
			case "last_over_time":
			case "label_join":
				srcVals := make([]string, len(labelJoinSrcLabels))

				for j, src := range labelJoinSrcLabels {
					srcVals[j] = lbls.Get(src)
				}
				lb := labels.NewBuilder(lbls)

				strval := strings.Join(srcVals, labelJoinSep)
				if strval == "" {
					lb.Del(labelJoinDst)
				} else {
					lb.Set(labelJoinDst, strval)
				}

				lbls = lb.Labels()
			default:
				lbls, _ = DropMetricName(s.Copy())
			}
			o.series[i] = lbls
		}
	})

	return err
}

func DropMetricName(l labels.Labels) (labels.Labels, labels.Label) {
	return dropLabel(l, labels.MetricName)
}

// dropLabel removes the label with name from l and returns the dropped label.
func dropLabel(l labels.Labels, name string) (labels.Labels, labels.Label) {
	if len(l) == 0 {
		return l, labels.Label{}
	}

	if len(l) == 1 {
		if l[0].Name == name {
			return l[:0], l[0]

		}
		return l, labels.Label{}
	}

	for i := range l {
		if l[i].Name == name {
			lbl := l[i]
			return append(l[:i], l[i+1:]...), lbl
		}
	}

	return l, labels.Label{}
}
