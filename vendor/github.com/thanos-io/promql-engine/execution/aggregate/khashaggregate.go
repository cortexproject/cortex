// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/query"
)

type kAggregate struct {
	model.OperatorTelemetry

	next    model.VectorOperator
	paramOp model.VectorOperator
	// params holds the aggregate parameter for each step.
	params []float64

	vectorPool *model.VectorPool

	by          bool
	labels      []string
	aggregation parser.ItemType

	once        sync.Once
	series      []labels.Labels
	inputToHeap []*samplesHeap
	heaps       []*samplesHeap
	compare     func(float64, float64) bool
}

func NewKHashAggregate(
	points *model.VectorPool,
	next model.VectorOperator,
	paramOp model.VectorOperator,
	aggregation parser.ItemType,
	by bool,
	labels []string,
	opts *query.Options,
) (model.VectorOperator, error) {
	var compare func(float64, float64) bool

	if aggregation == parser.TOPK {
		compare = func(f float64, s float64) bool {
			return f < s
		}
	} else {
		compare = func(f float64, s float64) bool {
			return s < f
		}
	}
	// Grouping labels need to be sorted in order for metric hashing to work.
	// https://github.com/prometheus/prometheus/blob/8ed39fdab1ead382a354e45ded999eb3610f8d5f/model/labels/labels.go#L162-L181
	slices.Sort(labels)

	op := &kAggregate{
		next:        next,
		vectorPool:  points,
		by:          by,
		aggregation: aggregation,
		labels:      labels,
		paramOp:     paramOp,
		compare:     compare,
		params:      make([]float64, opts.StepsBatch),
	}

	op.OperatorTelemetry = model.NewTelemetry(op, opts)

	return op, nil
}

func (a *kAggregate) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { a.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	in, err := a.next.Next(ctx)
	if err != nil {
		return nil, err
	}

	args, err := a.paramOp.Next(ctx)
	if err != nil {
		return nil, err
	}

	for i := range args {
		a.params[i] = args[i].Samples[0]
		a.paramOp.GetPool().PutStepVector(args[i])

		val := a.params[i]
		if val > math.MaxInt64 || val < math.MinInt64 || math.IsNaN(val) {
			return nil, errors.Newf("Scalar value %v overflows int64", val)
		}
	}
	a.paramOp.GetPool().PutVectors(args)

	if in == nil {
		return nil, nil
	}

	a.once.Do(func() { err = a.init(ctx) })
	if err != nil {
		return nil, err
	}

	result := a.vectorPool.GetVectorBatch()
	for i, vector := range in {
		// Skip steps where the argument is less than or equal to 0.
		if int(a.params[i]) <= 0 {
			result = append(result, a.GetPool().GetStepVector(vector.T))
			continue
		}
		a.aggregate(vector.T, &result, int(a.params[i]), vector.SampleIDs, vector.Samples)
		a.next.GetPool().PutStepVector(vector)
	}
	a.next.GetPool().PutVectors(in)

	return result, nil
}

func (a *kAggregate) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { a.AddExecutionTimeTaken(time.Since(start)) }()

	var err error
	a.once.Do(func() { err = a.init(ctx) })
	if err != nil {
		return nil, err
	}

	return a.series, nil
}

func (a *kAggregate) GetPool() *model.VectorPool {
	return a.vectorPool
}

func (a *kAggregate) String() string {
	if a.by {
		return fmt.Sprintf("[kaggregate] %v by (%v)", a.aggregation.String(), a.labels)
	}
	return fmt.Sprintf("[kaggregate] %v without (%v)", a.aggregation.String(), a.labels)
}

func (a *kAggregate) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{a.paramOp, a.next}
}

func (a *kAggregate) init(ctx context.Context) error {
	series, err := a.next.Series(ctx)
	if err != nil {
		return err
	}
	var (
		// heapsHash is a map of hash of the series to output samples heap for that series.
		heapsHash = make(map[uint64]*samplesHeap)
		// hashingBuf is a buffer used for metric hashing.
		hashingBuf = make([]byte, 1024)
		// builder is a scratch builder used for creating output series.
		builder labels.ScratchBuilder
	)
	labelsMap := make(map[string]struct{})
	for _, lblName := range a.labels {
		labelsMap[lblName] = struct{}{}
	}
	for i := 0; i < len(series); i++ {
		hash, _ := hashMetric(builder, series[i], !a.by, a.labels, labelsMap, hashingBuf)
		h, ok := heapsHash[hash]
		if !ok {
			h = &samplesHeap{compare: a.compare}
			heapsHash[hash] = h
			a.heaps = append(a.heaps, h)
		}
		a.inputToHeap = append(a.inputToHeap, h)
	}
	a.vectorPool.SetStepSize(len(series))
	a.series = series
	return nil
}

func (a *kAggregate) aggregate(t int64, result *[]model.StepVector, k int, sampleIDs []uint64, samples []float64) {
	for i, sId := range sampleIDs {
		h := a.inputToHeap[sId]
		switch {
		case h.Len() < k:
			heap.Push(h, &entry{sId: sId, total: samples[i]})

		case h.compare(h.entries[0].total, samples[i]) || (math.IsNaN(h.entries[0].total) && !math.IsNaN(samples[i])):
			h.entries[0].sId = sId
			h.entries[0].total = samples[i]

			if k > 1 {
				heap.Fix(h, 0)
			}
		}
	}

	s := a.vectorPool.GetStepVector(t)
	for _, h := range a.heaps {
		// The heap keeps the lowest value on top, so reverse it.
		if len(h.entries) > 1 {
			sort.Sort(sort.Reverse(h))
		}

		for _, e := range h.entries {
			s.AppendSample(a.vectorPool, e.sId, e.total)
		}
		h.entries = h.entries[:0]
	}
	*result = append(*result, s)
}

type entry struct {
	sId   uint64
	total float64
}

type samplesHeap struct {
	entries []entry
	compare func(float64, float64) bool
}

func (s samplesHeap) Len() int {
	return len(s.entries)
}

func (s samplesHeap) Less(i, j int) bool {
	if math.IsNaN(s.entries[i].total) {
		return true
	}
	return s.compare(s.entries[i].total, s.entries[j].total)
}

func (s samplesHeap) Swap(i, j int) {
	s.entries[i], s.entries[j] = s.entries[j], s.entries[i]
}

func (s *samplesHeap) Push(x interface{}) {
	s.entries = append(s.entries, *(x.(*entry)))
}

func (s *samplesHeap) Pop() interface{} {
	old := (*s).entries
	n := len(old)
	el := old[n-1]
	(*s).entries = old[0 : n-1]
	return el
}
