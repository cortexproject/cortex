// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/exp/slices"
)

type aggregate struct {
	next        model.VectorOperator
	paramOp     model.VectorOperator
	by          bool
	labels      []string
	aggregation parser.ItemType
	stepsBatch  int

	once   sync.Once
	series []labels.Labels
	tables []aggregateTable
	params []float64

	lastBatch        []model.StepVector
	tempBuf          []model.StepVector
	paramBuf         []model.StepVector
	lastBatchBuf     []model.StepVector
	inputSeriesCount int
}

func NewHashAggregate(
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
	a := &aggregate{
		next:        next,
		paramOp:     paramOp,
		by:          by,
		labels:      labels,
		aggregation: aggregation,
		stepsBatch:  opts.StepsBatch,
		params:      make([]float64, opts.StepsBatch),
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(a, opts), a), nil
}

func (a *aggregate) String() string {
	if a.by {
		return fmt.Sprintf("[aggregate] %v by (%v)", a.aggregation.String(), a.labels)
	}
	return fmt.Sprintf("[aggregate] %v without (%v)", a.aggregation.String(), a.labels)
}

func (a *aggregate) Explain() (next []model.VectorOperator) {
	switch a.aggregation {
	case parser.QUANTILE:
		return []model.VectorOperator{a.paramOp, a.next}
	default:
		return []model.VectorOperator{a.next}
	}
}

func (a *aggregate) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	a.once.Do(func() { err = a.initializeTables(ctx) })
	if err != nil {
		return nil, err
	}
	return a.series, nil
}

func (a *aggregate) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	var err error
	a.once.Do(func() { err = a.initializeTables(ctx) })
	if err != nil {
		return 0, err
	}

	if a.paramOp != nil {
		n, err := a.paramOp.Next(ctx, a.paramBuf)
		if err != nil {
			return 0, err
		}
		for i := range n {
			a.params[i] = a.paramBuf[i].Samples[0]
			if sample := a.params[i]; math.IsNaN(sample) || sample < 0 || sample > 1 {
				warnings.AddToContext(annotations.NewInvalidQuantileWarning(sample, posrange.PositionRange{}), ctx)
			}
		}
	}

	for i, p := range a.params {
		a.tables[i].reset(p)
	}

	// Track how many tables are populated during aggregation.
	numTables := 0
	if a.lastBatch != nil {
		numTables = len(a.lastBatch)
		if warn := a.aggregate(a.lastBatch); warn != nil {
			warnings.AddToContext(warn, ctx)
		}
		a.lastBatch = nil
	}

	for {
		n, err := a.next.Next(ctx, a.tempBuf)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			break
		}
		next := a.tempBuf[:n]
		// Keep aggregating samples as long as timestamps of batches are equal.
		currentTs := a.tables[0].timestamp()
		if currentTs == math.MinInt64 || next[0].T == currentTs {
			numTables = n
			if warn := a.aggregate(next); warn != nil {
				warnings.AddToContext(warn, ctx)
			}
			continue
		}
		a.lastBatch = a.lastBatchBuf[:n]
		copy(a.lastBatch, next)
		break
	}

	n := min(numTables, len(buf))
	for i := range n {
		buf[i].Reset(a.tables[i].timestamp())
		a.tables[i].populateVector(ctx, &buf[i])
	}
	return n, nil
}

func (a *aggregate) aggregate(in []model.StepVector) error {
	var err error
	for i, vector := range in {
		err = warnings.Coalesce(err, a.tables[i].aggregate(vector))
	}
	return err
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

	// Allocate outer slice for buffers; inner slices will be allocated by child operators
	// or grow on demand. This avoids over-allocation when aggregating many series to few.
	a.tempBuf = make([]model.StepVector, a.stepsBatch)
	a.lastBatchBuf = make([]model.StepVector, a.stepsBatch)
	if a.paramOp != nil {
		a.paramBuf = make([]model.StepVector, len(a.params))
	}

	return nil
}

func (a *aggregate) initializeVectorizedTables(ctx context.Context) ([]aggregateTable, []labels.Labels, error) {
	// perform initialization of the underlying operator even if we are aggregating the labels away
	series, err := a.next.Series(ctx)
	if err != nil {
		return nil, nil, err
	}
	a.inputSeriesCount = len(series)
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
	a.inputSeriesCount = len(series)
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
	for i := range series {
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
	for i := range outputCache {
		series[i] = outputCache[i].Metric
	}

	return tables, series, nil
}
