// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-community/promql-engine/execution/model"
	engstore "github.com/thanos-community/promql-engine/execution/storage"
	"github.com/thanos-community/promql-engine/query"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"

	"github.com/prometheus/prometheus/storage"
)

type vectorScanner struct {
	labels    labels.Labels
	signature uint64
	samples   *storage.MemoizedSeriesIterator
}

type vectorSelector struct {
	storage  engstore.SeriesSelector
	scanners []vectorScanner
	series   []labels.Labels

	once       sync.Once
	vectorPool *model.VectorPool

	numSteps      int
	mint          int64
	maxt          int64
	lookbackDelta int64
	step          int64
	currentStep   int64
	offset        int64

	shard     int
	numShards int
}

// NewVectorSelector creates operator which selects vector of series.
func NewVectorSelector(
	pool *model.VectorPool,
	selector engstore.SeriesSelector,
	queryOpts *query.Options,
	offset time.Duration,
	shard, numShards int,
) model.VectorOperator {
	return &vectorSelector{
		storage:    selector,
		vectorPool: pool,

		mint:          queryOpts.Start.UnixMilli(),
		maxt:          queryOpts.End.UnixMilli(),
		step:          queryOpts.Step.Milliseconds(),
		currentStep:   queryOpts.Start.UnixMilli(),
		lookbackDelta: queryOpts.LookbackDelta.Milliseconds(),
		offset:        offset.Milliseconds(),
		numSteps:      queryOpts.NumSteps(),

		shard:     shard,
		numShards: numShards,
	}
}

func (o *vectorSelector) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*vectorSelector] {%v} %v mod %v", o.storage.Matchers(), o.shard, o.numShards), nil
}

func (o *vectorSelector) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *vectorSelector) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *vectorSelector) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return nil, nil
	}

	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}

	vectors := o.vectorPool.GetVectorBatch()
	ts := o.currentStep
	for i := 0; i < len(o.scanners); i++ {
		var (
			series   = o.scanners[i]
			seriesTs = ts
		)

		for currStep := 0; currStep < o.numSteps && seriesTs <= o.maxt; currStep++ {
			if len(vectors) <= currStep {
				vectors = append(vectors, o.vectorPool.GetStepVector(seriesTs))
			}
			_, v, h, ok, err := selectPoint(series.samples, seriesTs, o.lookbackDelta, o.offset)
			if err != nil {
				return nil, err
			}
			if ok {
				if h != nil {
					vectors[currStep].AppendHistogram(o.vectorPool, series.signature, h)
				} else {
					vectors[currStep].AppendSample(o.vectorPool, series.signature, v)
				}
			}
			seriesTs += o.step
		}
	}
	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if o.step == 0 {
		o.step = 1
	}
	o.currentStep += o.step * int64(o.numSteps)

	return vectors, nil
}

func (o *vectorSelector) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {
		series, loadErr := o.storage.GetSeries(ctx, o.shard, o.numShards)
		if loadErr != nil {
			err = loadErr
			return
		}

		o.scanners = make([]vectorScanner, len(series))
		o.series = make([]labels.Labels, len(series))
		for i, s := range series {
			o.scanners[i] = vectorScanner{
				labels:    s.Labels(),
				signature: s.Signature,
				samples:   storage.NewMemoizedIterator(s.Iterator(nil), o.lookbackDelta),
			}
			o.series[i] = s.Labels()
		}
		o.vectorPool.SetStepSize(len(series))
	})
	return err
}

// TODO(fpetkovski): Add max samples limit.
func selectPoint(it *storage.MemoizedSeriesIterator, ts, lookbackDelta, offset int64) (int64, float64, *histogram.FloatHistogram, bool, error) {
	refTime := ts - offset
	var t int64
	var v float64
	var fh *histogram.FloatHistogram

	valueType := it.Seek(refTime)
	switch valueType {
	case chunkenc.ValNone:
		if it.Err() != nil {
			return 0, 0, nil, false, it.Err()
		}
	case chunkenc.ValFloatHistogram, chunkenc.ValHistogram:
		t, fh = it.AtFloatHistogram()
	case chunkenc.ValFloat:
		t, v = it.At()
	default:
		panic(errors.Newf("unknown value type %v", valueType))
	}
	if valueType == chunkenc.ValNone || t > refTime {
		var ok bool
		t, v, _, fh, ok = it.PeekPrev()
		if !ok || t < refTime-lookbackDelta {
			return 0, 0, nil, false, nil
		}
	}
	if value.IsStaleNaN(v) || (fh != nil && value.IsStaleNaN(fh.Sum)) {
		return 0, 0, nil, false, nil
	}

	return t, v, fh, true, nil
}
