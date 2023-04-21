// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/exp/slices"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
	"github.com/thanos-community/promql-engine/worker"
)

type aggregate struct {
	next    model.VectorOperator
	paramOp model.VectorOperator
	// params holds the aggregate parameter for each step.
	params []float64

	vectorPool *model.VectorPool

	by          bool
	labels      []string
	aggregation parser.ItemType

	once           sync.Once
	tables         []aggregateTable
	series         []labels.Labels
	newAccumulator newAccumulatorFunc
	stepsBatch     int
	workers        worker.Group
}

func NewHashAggregate(
	points *model.VectorPool,
	next model.VectorOperator,
	paramOp model.VectorOperator,
	aggregation parser.ItemType,
	by bool,
	labels []string,
	stepsBatch int,
) (model.VectorOperator, error) {
	newAccumulator, err := makeAccumulatorFunc(aggregation)
	if err != nil {
		return nil, err
	}

	// Grouping labels need to be sorted in order for metric hashing to work.
	// https://github.com/prometheus/prometheus/blob/8ed39fdab1ead382a354e45ded999eb3610f8d5f/model/labels/labels.go#L162-L181
	slices.Sort(labels)
	a := &aggregate{
		next:           next,
		paramOp:        paramOp,
		params:         make([]float64, stepsBatch),
		vectorPool:     points,
		by:             by,
		aggregation:    aggregation,
		labels:         labels,
		stepsBatch:     stepsBatch,
		newAccumulator: newAccumulator,
	}
	a.workers = worker.NewGroup(stepsBatch, a.workerTask)

	return a, nil
}

func (a *aggregate) Explain() (me string, next []model.VectorOperator) {
	var ops []model.VectorOperator

	if a.paramOp != nil {
		ops = append(ops, a.paramOp)
	}
	ops = append(ops, a.next)

	if a.by {
		return fmt.Sprintf("[*aggregate] %v by (%v)", a.aggregation.String(), a.labels), ops
	}
	return fmt.Sprintf("[*aggregate] %v without (%v)", a.aggregation.String(), a.labels), ops
}

func (a *aggregate) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	a.once.Do(func() { err = a.initializeTables(ctx) })
	if err != nil {
		return nil, err
	}

	return a.series, nil
}

func (a *aggregate) GetPool() *model.VectorPool {
	return a.vectorPool
}

func (a *aggregate) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	in, err := a.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}
	defer a.next.GetPool().PutVectors(in)

	a.once.Do(func() { err = a.initializeTables(ctx) })
	if err != nil {
		return nil, err
	}

	if a.paramOp != nil {
		args, err := a.paramOp.Next(ctx)
		if err != nil {
			return nil, err
		}
		for i := range a.params {
			a.params[i] = math.NaN()
			if i < len(args) && len(args[i].Samples) > 0 {
				a.params[i] = args[i].Samples[0]
				a.paramOp.GetPool().PutStepVector(args[i])
			}
		}
		a.paramOp.GetPool().PutVectors(args)
	}

	result := a.vectorPool.GetVectorBatch()
	for i, vector := range in {
		if err = a.workers[i].Send(a.params[i], vector); err != nil {
			return nil, err
		}
	}

	for i, vector := range in {
		output, err := a.workers[i].GetOutput()
		if err != nil {
			return nil, err
		}
		result = append(result, output)
		a.next.GetPool().PutStepVector(vector)
	}

	return result, nil
}

func (a *aggregate) initializeTables(ctx context.Context) error {
	var (
		tables []aggregateTable
		series []labels.Labels
		err    error
	)

	if a.by && len(a.labels) == 0 {
		tables, series, err = a.initializeVectorizedTables(ctx)
	} else {
		tables, series, err = a.initializeScalarTables(ctx)
	}
	if err != nil {
		return err
	}
	a.tables = tables
	a.series = series
	a.workers.Start(ctx)

	return nil
}

func (a *aggregate) workerTask(workerID int, arg float64, vector model.StepVector) model.StepVector {
	table := a.tables[workerID]
	table.aggregate(arg, vector)
	return table.toVector(a.vectorPool)
}

func (a *aggregate) initializeVectorizedTables(ctx context.Context) ([]aggregateTable, []labels.Labels, error) {
	tables, err := newVectorizedTables(a.stepsBatch, a.aggregation)
	if errors.Is(err, parse.ErrNotSupportedExpr) {
		return a.initializeScalarTables(ctx)
	}

	if err != nil {
		return nil, nil, err
	}

	return tables, []labels.Labels{{}}, nil
}

func (a *aggregate) initializeScalarTables(ctx context.Context) ([]aggregateTable, []labels.Labels, error) {
	series, err := a.next.Series(ctx)
	if err != nil {
		return nil, nil, err
	}

	inputCache := make([]uint64, len(series))
	outputMap := make(map[uint64]*model.Series)
	outputCache := make([]*model.Series, 0)
	buf := make([]byte, 1024)
	for i := 0; i < len(series); i++ {
		hash, _, lbls := hashMetric(series[i], !a.by, a.labels, buf)
		output, ok := outputMap[hash]
		if !ok {
			output = &model.Series{
				Metric: lbls,
				ID:     uint64(len(outputCache)),
			}
			outputMap[hash] = output
			outputCache = append(outputCache, output)
		}

		inputCache[i] = output.ID
	}
	a.vectorPool.SetStepSize(len(outputCache))
	tables := newScalarTables(a.stepsBatch, inputCache, outputCache, a.newAccumulator)

	series = make([]labels.Labels, len(outputCache))
	for i := 0; i < len(outputCache); i++ {
		series[i] = outputCache[i].Metric
	}

	return tables, series, nil
}
