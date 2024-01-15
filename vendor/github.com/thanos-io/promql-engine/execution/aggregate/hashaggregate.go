// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/query"
)

type aggregate struct {
	model.OperatorTelemetry

	next    model.VectorOperator
	paramOp model.VectorOperator
	// params holds the aggregate parameter for each step.
	params    []float64
	lastBatch []model.StepVector

	vectorPool *model.VectorPool

	by          bool
	labels      []string
	aggregation parser.ItemType

	once       sync.Once
	tables     []aggregateTable
	series     []labels.Labels
	stepsBatch int
}

func NewHashAggregate(
	points *model.VectorPool,
	next model.VectorOperator,
	paramOp model.VectorOperator,
	aggregation parser.ItemType,
	by bool,
	labels []string,
	opts *query.Options,
) (model.VectorOperator, error) {
	// Verify that the aggregation is supported.
	if _, err := newScalarAccumulator(aggregation); err != nil {
		return nil, err
	}

	// Grouping labels need to be sorted in order for metric hashing to work.
	// https://github.com/prometheus/prometheus/blob/8ed39fdab1ead382a354e45ded999eb3610f8d5f/model/labels/labels.go#L162-L181
	slices.Sort(labels)
	return &aggregate{
		OperatorTelemetry: model.NewTelemetry("[aggregate]", opts.EnableAnalysis),

		next:        next,
		paramOp:     paramOp,
		params:      make([]float64, opts.StepsBatch),
		vectorPool:  points,
		by:          by,
		aggregation: aggregation,
		labels:      labels,
		stepsBatch:  opts.StepsBatch,
	}, nil
}

func (a *aggregate) Explain() (me string, next []model.VectorOperator) {
	var ops []model.VectorOperator
	if a.paramOp != nil {
		ops = append(ops, a.paramOp)
	}
	ops = append(ops, a.next)
	if a.by {
		return fmt.Sprintf("[aggregate] %v by (%v)", a.aggregation.String(), a.labels), ops
	}
	return fmt.Sprintf("[aggregate] %v without (%v)", a.aggregation.String(), a.labels), ops
}

func (a *aggregate) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { a.AddExecutionTimeTaken(time.Since(start)) }()

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
	start := time.Now()
	defer func() { a.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var err error
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

	for i, p := range a.params {
		a.tables[i].reset(p)
	}
	if a.lastBatch != nil {
		a.aggregate(a.lastBatch)
		a.lastBatch = nil
	}
	for {
		next, err := a.next.Next(ctx)
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
		// Keep aggregating samples as long as timestamps of batches are equal.
		currentTs := a.tables[0].timestamp()
		if currentTs == math.MinInt64 || next[0].T == currentTs {
			a.aggregate(next)
			continue
		}
		a.lastBatch = next
		break
	}

	if a.tables[0].timestamp() == math.MinInt64 {
		return nil, nil
	}

	result := a.vectorPool.GetVectorBatch()
	for i := range a.tables {
		if a.tables[i].timestamp() == math.MinInt64 {
			break
		}
		result = append(result, a.tables[i].toVector(a.vectorPool))
	}
	return result, nil
}

func (a *aggregate) aggregate(in []model.StepVector) {
	for i, vector := range in {
		a.tables[i].aggregate(vector)
		a.next.GetPool().PutStepVector(vector)
	}
	a.next.GetPool().PutVectors(in)
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
	a.vectorPool.SetStepSize(len(a.series))

	return nil
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
	var (
		// inputCache is an index from input seriesID to output seriesID.
		inputCache = make([]uint64, len(series))
		// outputMap is used to map from the hash of an input series to an output series.
		outputMap = make(map[uint64]*model.Series)
		// outputCache is an index from output seriesID to output series.
		outputCache = make([]*model.Series, 0)
		// hashingBuf is a reusable buffer for hashing input series.
		hashingBuf = make([]byte, 1024)
		// builder is a reusable labels builder for output series.
		builder labels.ScratchBuilder
	)
	labelsMap := make(map[string]struct{})
	for _, lblName := range a.labels {
		labelsMap[lblName] = struct{}{}
	}
	for i := 0; i < len(series); i++ {
		hash, lbls := hashMetric(builder, series[i], !a.by, a.labels, labelsMap, hashingBuf)
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
	tables, err := newScalarTables(a.stepsBatch, inputCache, outputCache, a.aggregation)
	if err != nil {
		return nil, nil, err
	}

	series = make([]labels.Labels, len(outputCache))
	for i := 0; i < len(outputCache); i++ {
		series[i] = outputCache[i].Metric
	}

	return tables, series, nil
}
