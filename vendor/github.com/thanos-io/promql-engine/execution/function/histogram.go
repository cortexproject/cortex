// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/extlabels"
)

type histogramSeries struct {
	outputID       int
	upperBound     float64
	hasBucketValue bool
}

// histogramOperator is a function operator that calculates percentiles.
type histogramOperator struct {
	pool *model.VectorPool

	funcArgs parser.Expressions

	once     sync.Once
	series   []labels.Labels
	scalarOp model.VectorOperator
	vectorOp model.VectorOperator

	// scalarPoints is a reusable buffer for points from the first argument of histogram_quantile.
	scalarPoints []float64

	// outputIndex is a mapping from input series ID to the output series ID and its upper boundary value
	// parsed from the le label.
	// If outputIndex[i] is nil then series[i] has no valid `le` label.
	outputIndex []*histogramSeries

	// seriesBuckets are the buckets for each individual conventional histogram series.
	seriesBuckets []buckets
	model.OperatorTelemetry
}

func (o *histogramOperator) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	o.SetName("[*functionOperator]")
	next := make([]model.ObservableVectorOperator, 0, 2)
	if obsScalarOp, ok := o.scalarOp.(model.ObservableVectorOperator); ok {
		next = append(next, obsScalarOp)
	}
	if obsVectorOp, ok := o.vectorOp.(model.ObservableVectorOperator); ok {
		next = append(next, obsVectorOp)
	}
	return o, next
}

func (o *histogramOperator) Explain() (me string, next []model.VectorOperator) {
	next = []model.VectorOperator{o.scalarOp, o.vectorOp}
	return fmt.Sprintf("[*functionOperator] histogram_quantile(%v)", o.funcArgs), next
}

func (o *histogramOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	return o.series, nil
}

func (o *histogramOperator) GetPool() *model.VectorPool {
	return o.pool
}

func (o *histogramOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	start := time.Now()
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	scalars, err := o.scalarOp.Next(ctx)
	if err != nil {
		return nil, err
	}

	if len(scalars) == 0 {
		return nil, nil
	}

	vectors, err := o.vectorOp.Next(ctx)
	if err != nil {
		return nil, err
	}

	o.scalarPoints = o.scalarPoints[:0]
	for _, scalar := range scalars {
		if len(scalar.Samples) > 0 {
			o.scalarPoints = append(o.scalarPoints, scalar.Samples[0])
		}
		o.scalarOp.GetPool().PutStepVector(scalar)
	}
	o.scalarOp.GetPool().PutVectors(scalars)
	o.AddExecutionTimeTaken(time.Since(start))

	return o.processInputSeries(vectors)
}

func (o *histogramOperator) processInputSeries(vectors []model.StepVector) ([]model.StepVector, error) {
	out := o.pool.GetVectorBatch()
	for stepIndex, vector := range vectors {
		o.resetBuckets()
		for i, seriesID := range vector.SampleIDs {
			outputSeries := o.outputIndex[seriesID]
			// This means that it has an invalid `le` label.
			if outputSeries == nil || !outputSeries.hasBucketValue {
				continue
			}

			outputSeriesID := outputSeries.outputID
			bucket := le{
				upperBound: outputSeries.upperBound,
				count:      vector.Samples[i],
			}
			o.seriesBuckets[outputSeriesID] = append(o.seriesBuckets[outputSeriesID], bucket)
		}

		step := o.pool.GetStepVector(vector.T)
		for i, seriesID := range vector.HistogramIDs {
			outputSeriesID := o.outputIndex[seriesID].outputID
			// We need to check if there is a conventional histogram mapped to this output series ID.
			// If that is the case, it means we have mixed data types for a single step and this behavior is undefined.
			// In that case, we reset the conventional buckets to avoid emitting a sample.
			// TODO(fpetkovski): Prometheus is looking to solve these conflicts through warnings: https://github.com/prometheus/prometheus/issues/10839.
			if len(o.seriesBuckets[outputSeriesID]) == 0 {
				value := histogramQuantile(o.scalarPoints[stepIndex], vector.Histograms[i])
				step.AppendSample(o.pool, uint64(outputSeriesID), value)
			} else {
				o.seriesBuckets[outputSeriesID] = o.seriesBuckets[outputSeriesID][:0]
			}
		}

		for i, stepBuckets := range o.seriesBuckets {
			// It could be zero if multiple input series map to the same output series ID.
			if len(stepBuckets) == 0 {
				continue
			}
			// If there is only bucket or if we are after how many
			// scalar points we have then it needs to be NaN.
			if len(stepBuckets) == 1 || stepIndex >= len(o.scalarPoints) {
				step.AppendSample(o.pool, uint64(i), math.NaN())
				continue
			}

			val := bucketQuantile(o.scalarPoints[stepIndex], stepBuckets)
			step.AppendSample(o.pool, uint64(i), val)
		}

		out = append(out, step)
		o.vectorOp.GetPool().PutStepVector(vector)
	}

	o.vectorOp.GetPool().PutVectors(vectors)
	return out, nil
}

func (o *histogramOperator) loadSeries(ctx context.Context) error {
	series, err := o.vectorOp.Series(ctx)
	if err != nil {
		return err
	}

	var (
		hashBuf      = make([]byte, 0, 256)
		hasher       = xxhash.New()
		seriesHashes = make(map[uint64]int, len(series))
	)

	o.series = make([]labels.Labels, 0)
	o.outputIndex = make([]*histogramSeries, len(series))
	b := labels.ScratchBuilder{}
	for i, s := range series {
		hasBucketValue := true
		lbls, bucketLabel := extlabels.DropBucketLabel(s, b)
		value, err := strconv.ParseFloat(bucketLabel.Value, 64)
		if err != nil {
			hasBucketValue = false
		}

		hasher.Reset()
		hashBuf = lbls.Bytes(hashBuf)
		if _, err := hasher.Write(hashBuf); err != nil {
			return err
		}

		// We check for duplicate series after dropped labels when
		// showing the result of the query. Series that are equal after
		// dropping name should not hash to the same bucket here.
		lbls, _ = extlabels.DropMetricName(lbls, b)

		seriesHash := hasher.Sum64()
		seriesID, ok := seriesHashes[seriesHash]
		if !ok {
			o.series = append(o.series, lbls)
			seriesID = len(o.series) - 1
			seriesHashes[seriesHash] = seriesID
		}

		o.outputIndex[i] = &histogramSeries{
			outputID:       seriesID,
			upperBound:     value,
			hasBucketValue: hasBucketValue,
		}
	}
	o.seriesBuckets = make([]buckets, len(o.series))
	o.pool.SetStepSize(len(o.series))
	return nil
}

func (o *histogramOperator) resetBuckets() {
	for i := range o.seriesBuckets {
		o.seriesBuckets[i] = o.seriesBuckets[i][:0]
	}
}
