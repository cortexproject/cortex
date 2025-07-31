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

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/execution/warnings"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/exp/slices"
)

type kAggregate struct {
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
	} else if aggregation == parser.BOTTOMK {
		compare = func(f float64, s float64) bool {
			return s < f
		}
	} else if aggregation != parser.LIMITK && aggregation != parser.LIMIT_RATIO {
		return nil, errors.Newf("Unsupported aggregate expression: %v", aggregation)
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

	return telemetry.NewOperator(telemetry.NewTelemetry(op, opts), op), nil
}

func (a *kAggregate) Next(ctx context.Context) ([]model.StepVector, error) {
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

		switch a.aggregation {
		case parser.TOPK, parser.BOTTOMK, parser.LIMITK:
			if math.IsNaN(val) {
				return nil, errors.New("Parameter value is NaN")
			}
			if val > math.MaxInt64 {
				return nil, errors.Newf("Scalar value %v overflows int64", val)
			}
			if val < math.MinInt64 {
				return nil, errors.Newf("Scalar value %v underflows int64", val)
			}
		case parser.LIMIT_RATIO:
			if math.IsNaN(val) {
				return nil, errors.Newf("Ratio value is NaN")
			}
			switch {
			case val < -1.0:
				val = -1.0
				warnings.AddToContext(annotations.NewInvalidRatioWarning(a.params[i], val, posrange.PositionRange{}), ctx)
			case val > 1.0:
				val = 1.0
				warnings.AddToContext(annotations.NewInvalidRatioWarning(a.params[i], val, posrange.PositionRange{}), ctx)
			}
			a.params[i] = val
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
		// Skip steps where the argument is less than or equal to 0, limit_ratio is an exception.
		if (a.aggregation != parser.LIMIT_RATIO && int(a.params[i]) <= 0) || (a.aggregation == parser.LIMIT_RATIO && a.params[i] == 0) {
			result = append(result, a.GetPool().GetStepVector(vector.T))
			continue
		}
		if a.aggregation != parser.LIMITK && a.aggregation != parser.LIMIT_RATIO && len(vector.Histograms) > 0 {
			warnings.AddToContext(annotations.NewHistogramIgnoredInAggregationInfo(a.aggregation.String(), posrange.PositionRange{}), ctx)
		}

		var k int
		var ratio float64

		if a.aggregation == parser.LIMIT_RATIO {
			ratio = a.params[i]
		} else {
			k = int(a.params[i])
		}

		a.aggregate(vector.T, &result, k, ratio, vector.SampleIDs, vector.Samples, vector.HistogramIDs, vector.Histograms)
		a.next.GetPool().PutStepVector(vector)
	}
	a.next.GetPool().PutVectors(in)

	return result, nil
}

func (a *kAggregate) Series(ctx context.Context) ([]labels.Labels, error) {
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
	for i := range series {
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

// aggregates based on the given parameter k (or ratio for limit_ratio) and timeseries, supported aggregation are
// topk: gives the 'k' largest element based on the sample values
// bottomk: gives the 'k' smallest element based on the sample values
// limitk: samples the first 'k' element from the given timeseries (has native histogram support)
// limit_ratio: deterministically samples out the 'ratio' amount of the samples from the given timeseries (also has native histogram support).
func (a *kAggregate) aggregate(t int64, result *[]model.StepVector, k int, ratio float64, sampleIDs []uint64, samples []float64, histogramIDs []uint64, histograms []*histogram.FloatHistogram) {
	groupsRemaining := len(a.heaps)

	switch a.aggregation {
	case parser.TOPK, parser.BOTTOMK:
		for i, sId := range sampleIDs {
			sampleHeap := a.inputToHeap[sId]
			switch {
			case sampleHeap.Len() < k:
				heap.Push(sampleHeap, &entry{sId: sId, total: samples[i]})

			case sampleHeap.compare(sampleHeap.entries[0].total, samples[i]) || (math.IsNaN(sampleHeap.entries[0].total) && !math.IsNaN(samples[i])):
				sampleHeap.entries[0].sId = sId
				sampleHeap.entries[0].total = samples[i]

				if k > 1 {
					heap.Fix(sampleHeap, 0)
				}
			}
		}

	case parser.LIMITK:
		if len(histogramIDs) == 0 {
			for i, sId := range sampleIDs {
				sampleHeap := a.inputToHeap[sId]
				if sampleHeap.Len() < k {
					heap.Push(sampleHeap, &entry{sId: sId, total: samples[i]})

					if sampleHeap.Len() == k {
						groupsRemaining--
					}

					if groupsRemaining == 0 {
						break
					}
				}
			}
		} else {
			histogramIndex := 0
			sampleIndex := 0

			// pick the first 'k' samples based on the increasing order of their ids
			for histogramIndex < len(histogramIDs) || sampleIndex < len(sampleIDs) {
				var currentID uint64
				haveSample := sampleIndex < len(sampleIDs)
				haveHistogram := histogramIndex < len(histogramIDs)

				if haveSample && haveHistogram {
					currentID = uint64(min(sampleIDs[sampleIndex], histogramIDs[histogramIndex]))
				} else if haveHistogram {
					currentID = histogramIDs[histogramIndex]
				} else {
					currentID = sampleIDs[sampleIndex]
				}

				sampleHeap := a.inputToHeap[currentID]

				if sampleHeap.Len() < k {
					if haveHistogram && histogramIDs[histogramIndex] == currentID {
						heap.Push(sampleHeap, &entry{histId: currentID, histogramSample: histograms[histogramIndex]})
						histogramIndex++
					} else if haveSample && sampleIDs[sampleIndex] == currentID {
						heap.Push(sampleHeap, &entry{sId: currentID, total: samples[sampleIndex]})
						sampleIndex++
					}

					if sampleHeap.Len() == k {
						groupsRemaining--
					}

					if groupsRemaining == 0 {
						break
					}
				} else {
					if haveHistogram && histogramIDs[histogramIndex] == currentID {
						histogramIndex++
					} else if haveSample && sampleIDs[sampleIndex] == currentID {
						sampleIndex++
					}
				}
			}
		}
	case parser.LIMIT_RATIO:
		for i, sId := range sampleIDs {
			sampleHeap := a.inputToHeap[sId]

			if addRatioSample(ratio, a.series[sId]) {
				heap.Push(sampleHeap, &entry{sId: sId, total: samples[i]})
			}
		}

		for i, histId := range histogramIDs {
			sampleHeap := a.inputToHeap[histId]

			if addRatioSample(ratio, a.series[histId]) {
				heap.Push(sampleHeap, &entry{histId: histId, histogramSample: histograms[i]})
			}
		}
	}

	s := a.vectorPool.GetStepVector(t)
	for _, sampleHeap := range a.heaps {
		// for topk and bottomk the heap keeps the lowest value on top, so reverse it.
		if a.aggregation == parser.TOPK || a.aggregation == parser.BOTTOMK {
			sort.Sort(sort.Reverse(sampleHeap))
		}
		sampleHeap.addSamplesToPool(a.vectorPool, &s)
	}

	*result = append(*result, s)
}

type entry struct {
	sId             uint64
	histId          uint64
	total           float64
	histogramSample *histogram.FloatHistogram
}

type samplesHeap struct {
	entries []entry
	compare func(float64, float64) bool
}

func (s samplesHeap) Len() int {
	return len(s.entries)
}

func (s *samplesHeap) addSamplesToPool(pool *model.VectorPool, stepVector *model.StepVector) {
	for _, e := range s.entries {
		if e.histogramSample == nil {
			stepVector.AppendSample(pool, e.sId, e.total)
		} else {
			stepVector.AppendHistogram(pool, e.histId, e.histogramSample)
		}
	}
	s.entries = s.entries[:0]
}

func (s samplesHeap) Less(i, j int) bool {
	if math.IsNaN(s.entries[i].total) {
		return true
	}
	if s.compare == nil { // this is case for limitk as it doesn't require any sorting logic
		return false
	}

	return s.compare(s.entries[i].total, s.entries[j].total)
}

func (s samplesHeap) Swap(i, j int) {
	s.entries[i], s.entries[j] = s.entries[j], s.entries[i]
}

func (s *samplesHeap) Push(x any) {
	s.entries = append(s.entries, *(x.(*entry)))
}

func (s *samplesHeap) Pop() any {
	old := (*s).entries
	n := len(old)
	el := old[n-1]
	(*s).entries = old[0 : n-1]
	return el
}
