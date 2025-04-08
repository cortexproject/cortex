// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type vectorScanner struct {
	labels    labels.Labels
	signature uint64
	samples   *storage.MemoizedSeriesIterator
}

type vectorSelector struct {
	telemetry.OperatorTelemetry

	storage  SeriesSelector
	scanners []vectorScanner
	series   []labels.Labels

	once       sync.Once
	vectorPool *model.VectorPool

	numSteps        int
	mint            int64
	maxt            int64
	lookbackDelta   int64
	step            int64
	offset          int64
	seriesBatchSize int64

	currentSeries int64
	currentStep   int64

	shard     int
	numShards int

	selectTimestamp bool
}

// NewVectorSelector creates operator which selects vector of series.
func NewVectorSelector(
	pool *model.VectorPool,
	selector SeriesSelector,
	queryOpts *query.Options,
	offset time.Duration,
	batchSize int64,
	selectTimestamp bool,
	shard, numShards int,
) model.VectorOperator {
	o := &vectorSelector{
		storage:    selector,
		vectorPool: pool,

		mint:            queryOpts.Start.UnixMilli(),
		maxt:            queryOpts.End.UnixMilli(),
		step:            queryOpts.Step.Milliseconds(),
		currentStep:     queryOpts.Start.UnixMilli(),
		lookbackDelta:   queryOpts.LookbackDelta.Milliseconds(),
		offset:          offset.Milliseconds(),
		numSteps:        queryOpts.NumSteps(),
		seriesBatchSize: batchSize,

		shard:     shard,
		numShards: numShards,

		selectTimestamp: selectTimestamp,
	}
	o.OperatorTelemetry = telemetry.NewTelemetry(o, queryOpts)

	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if o.step == 0 {
		o.step = 1
	}

	return o
}

func (o *vectorSelector) String() string {
	return fmt.Sprintf("[vectorSelector] {%v} %v mod %v", o.storage.Matchers(), o.shard, o.numShards)
}

func (o *vectorSelector) Explain() (next []model.VectorOperator) {
	return nil
}

func (o *vectorSelector) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *vectorSelector) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *vectorSelector) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

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

	ts := o.currentStep
	vectors := o.vectorPool.GetVectorBatch()
	for currStep := 0; currStep < o.numSteps && ts <= o.maxt; currStep++ {
		vectors = append(vectors, o.vectorPool.GetStepVector(ts))
		ts += o.step
	}

	var currStepSamples uint64
	// Reset the current timestamp.
	ts = o.currentStep
	fromSeries := o.currentSeries
	for ; o.currentSeries-fromSeries < o.seriesBatchSize && o.currentSeries < int64(len(o.scanners)); o.currentSeries++ {
		var (
			series   = o.scanners[o.currentSeries]
			seriesTs = ts
		)
		for currStep := 0; currStep < o.numSteps && seriesTs <= o.maxt; currStep++ {
			currStepSamples = 0
			t, v, h, ok, err := selectPoint(series.samples, seriesTs, o.lookbackDelta, o.offset)
			if err != nil {
				return nil, err
			}
			if o.selectTimestamp {
				v = float64(t) / 1000
			}
			if ok {
				if h != nil && !o.selectTimestamp {
					vectors[currStep].AppendHistogram(o.vectorPool, series.signature, h)
				} else {
					vectors[currStep].AppendSample(o.vectorPool, series.signature, v)
				}
				currStepSamples++
			}
			o.IncrementSamplesAtTimestamp(int(currStepSamples), seriesTs)
			seriesTs += o.step
		}
	}
	if o.currentSeries == int64(len(o.scanners)) {
		o.currentStep += o.step * int64(o.numSteps)
		o.currentSeries = 0
	}
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

		b := labels.NewBuilder(labels.EmptyLabels())
		o.scanners = make([]vectorScanner, len(series))
		o.series = make([]labels.Labels, len(series))
		for i, s := range series {
			o.scanners[i] = vectorScanner{
				labels:    s.Labels(),
				signature: s.Signature,
				samples:   storage.NewMemoizedIterator(s.Iterator(nil), o.lookbackDelta),
			}
			b.Reset(s.Labels())
			// if we have pushed down a timestamp function into the scan we need to drop
			// the __name__ label
			if o.selectTimestamp {
				b.Del(labels.MetricName)
			}
			o.series[i] = b.Labels()
		}

		numSeries := int64(len(o.series))
		if o.seriesBatchSize == 0 || numSeries < o.seriesBatchSize {
			o.seriesBatchSize = numSeries
		}
		o.vectorPool.SetStepSize(int(o.seriesBatchSize))
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
		t, v, fh, ok = it.PeekPrev()
		if !ok || t <= refTime-lookbackDelta {
			return 0, 0, nil, false, nil
		}
	}
	if value.IsStaleNaN(v) || (fh != nil && value.IsStaleNaN(fh.Sum)) {
		return 0, 0, nil, false, nil
	}
	return t, v, fh, true, nil
}
