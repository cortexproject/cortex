// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/execution/warnings"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
)

type histogramSeries struct {
	outputID       int
	upperBound     float64
	hasBucketValue bool
}

// histogramOperator is a function operator that calculates percentiles.
type histogramOperator struct {
	once   sync.Once
	series []labels.Labels

	pool      *model.VectorPool
	funcName  string
	funcArgs  logicalplan.Nodes
	vectorOp  model.VectorOperator
	scalar1Op model.VectorOperator
	scalar2Op model.VectorOperator

	// scalarPoints is a reusable buffer for points from the first argument of histogram_quantile.
	scalar1Points []float64
	scalar2Points []float64

	// outputIndex is a mapping from input series ID to the output series ID and its upper boundary value
	// parsed from the le label.
	// If outputIndex[i] is nil then series[i] has no valid `le` label.
	outputIndex []*histogramSeries

	// needed to compile warnings on mixed histograms
	inputSeriesNames []string

	// seriesBuckets are the buckets for each individual conventional histogram series.
	seriesBuckets []promql.Buckets
}

func newHistogramOperator(
	pool *model.VectorPool,
	call *logicalplan.FunctionCall,
	nextOps []model.VectorOperator,
	opts *query.Options,
) model.VectorOperator {
	o := &histogramOperator{
		pool:     pool,
		funcName: call.Func.Name,
		funcArgs: call.Args,
	}

	switch o.funcName {
	case "histogram_quantile":
		o.scalar1Op = nextOps[0]
		o.vectorOp = nextOps[1]
		o.scalar1Points = make([]float64, opts.StepsBatch)
	case "histogram_fraction":
		o.scalar1Op = nextOps[0]
		o.scalar2Op = nextOps[1]
		o.vectorOp = nextOps[2]
		o.scalar1Points = make([]float64, opts.StepsBatch)
		o.scalar2Points = make([]float64, opts.StepsBatch)
	default:
		panic("unsupported function passed")
	}
	return telemetry.NewOperator(telemetry.NewTelemetry(o, opts), o)
}

func (o *histogramOperator) String() string {
	return fmt.Sprintf("[%s](%v)", o.funcName, o.funcArgs)
}

func (o *histogramOperator) Explain() (next []model.VectorOperator) {
	switch o.funcName {
	case "histogram_quantile":
		return []model.VectorOperator{o.scalar1Op, o.vectorOp}
	case "histogram_fraction":
		return []model.VectorOperator{o.scalar1Op, o.scalar2Op, o.vectorOp}
	}
	return nil
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

	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	switch o.funcName {
	case "histogram_quantile":
		scalars1, err := o.scalar1Op.Next(ctx)
		if err != nil {
			return nil, err
		}

		if len(scalars1) == 0 {
			return nil, nil
		}

		o.scalar1Points = o.scalar1Points[:0]
		for _, scalar := range scalars1 {
			if len(scalar.Samples) > 0 {
				sample := scalar.Samples[0]
				if math.IsNaN(sample) || sample < 0 || sample > 1 {
					warnings.AddToContext(annotations.NewInvalidQuantileWarning(sample, posrange.PositionRange{}), ctx)
				}
				o.scalar1Points = append(o.scalar1Points, sample)
			}
			o.scalar1Op.GetPool().PutStepVector(scalar)
		}
		o.scalar1Op.GetPool().PutVectors(scalars1)
	case "histogram_fraction":
		scalars1, err := o.scalar1Op.Next(ctx)
		if err != nil {
			return nil, err
		}

		if len(scalars1) == 0 {
			return nil, nil
		}

		o.scalar1Points = o.scalar1Points[:0]
		for _, scalar := range scalars1 {
			if len(scalar.Samples) > 0 {
				sample := scalar.Samples[0]
				o.scalar1Points = append(o.scalar1Points, sample)
			}
			o.scalar1Op.GetPool().PutStepVector(scalar)
		}
		o.scalar1Op.GetPool().PutVectors(scalars1)

		scalars2, err := o.scalar2Op.Next(ctx)
		if err != nil {
			return nil, err
		}

		if len(scalars2) == 0 {
			return nil, nil
		}

		o.scalar2Points = o.scalar2Points[:0]
		for _, scalar := range scalars2 {
			if len(scalar.Samples) > 0 {
				sample := scalar.Samples[0]
				o.scalar2Points = append(o.scalar2Points, sample)
			}
			o.scalar2Op.GetPool().PutStepVector(scalar)
		}
		o.scalar2Op.GetPool().PutVectors(scalars2)
	}

	vectors, err := o.vectorOp.Next(ctx)
	if err != nil {
		return nil, err
	}

	return o.processInputSeries(ctx, vectors)
}

// nolint: unparam
func (o *histogramOperator) processInputSeries(ctx context.Context, vectors []model.StepVector) ([]model.StepVector, error) {
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
			bucket := promql.Bucket{
				UpperBound: outputSeries.upperBound,
				Count:      vector.Samples[i],
			}
			o.seriesBuckets[outputSeriesID] = append(o.seriesBuckets[outputSeriesID], bucket)
		}

		step := o.pool.GetStepVector(vector.T)
		for i, seriesID := range vector.HistogramIDs {
			outputSeriesID := o.outputIndex[seriesID].outputID
			// We need to check if there is a conventional histogram mapped to this output series ID.
			// If that is the case, it means we have mixed data types for a single step and this behavior is undefined.
			// In that case, we reset the conventional buckets to avoid emitting a sample.
			if len(o.seriesBuckets[outputSeriesID]) == 0 {
				var annos annotations.Annotations
				var v float64
				switch o.funcName {
				case "histogram_quantile":
					v, annos = promql.HistogramQuantile(o.scalar1Points[stepIndex], vector.Histograms[i], o.inputSeriesNames[seriesID], posrange.PositionRange{})
					step.AppendSample(o.pool, uint64(outputSeriesID), v)
				case "histogram_fraction":
					v, annos = promql.HistogramFraction(o.scalar1Points[stepIndex], o.scalar2Points[stepIndex], vector.Histograms[i], o.inputSeriesNames[seriesID], posrange.PositionRange{})
					step.AppendSample(o.pool, uint64(outputSeriesID), v)
				}
				warnings.MergeToContext(annos, ctx)
			} else {
				warnings.AddToContext(annotations.NewMixedClassicNativeHistogramsWarning(o.inputSeriesNames[seriesID], posrange.PositionRange{}), ctx)
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
			if len(stepBuckets) == 1 || stepIndex >= len(o.scalar1Points) {
				step.AppendSample(o.pool, uint64(i), math.NaN())
				continue
			}
			switch o.funcName {
			case "histogram_quantile":
				v, forcedMonotonicity, _ := promql.BucketQuantile(o.scalar1Points[stepIndex], stepBuckets)
				step.AppendSample(o.pool, uint64(i), v)
				if forcedMonotonicity {
					warnings.AddToContext(annotations.NewHistogramQuantileForcedMonotonicityInfo(o.inputSeriesNames[i], posrange.PositionRange{}), ctx)
				}
			case "histogram_fraction":
				v := promql.BucketFraction(o.scalar1Points[stepIndex], o.scalar2Points[stepIndex], stepBuckets)
				step.AppendSample(o.pool, uint64(i), v)
			}
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
	o.inputSeriesNames = make([]string, len(series))
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

		o.inputSeriesNames[i] = s.Get(labels.MetricName)
		o.outputIndex[i] = &histogramSeries{
			outputID:       seriesID,
			upperBound:     value,
			hasBucketValue: hasBucketValue,
		}
	}
	o.seriesBuckets = make([]promql.Buckets, len(o.series))
	o.pool.SetStepSize(len(o.series))
	return nil
}

func (o *histogramOperator) resetBuckets() {
	for i := range o.seriesBuckets {
		o.seriesBuckets[i] = o.seriesBuckets[i][:0]
	}
}
