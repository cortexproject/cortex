// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package ringbuffer

import (
	"context"
	"math"
	"sort"

	"github.com/thanos-io/promql-engine/execution/aggregate"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/execution/warnings"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"
	"gonum.org/v1/gonum/stat"
)

type SamplesBuffer GenericRingBuffer

type FunctionArgs struct {
	ctx              context.Context
	Samples          []Sample
	StepTime         int64
	SelectRange      int64
	Offset           int64
	MetricAppearedTs *int64

	// quantile_over_time and predict_linear use one, so we only use one here.
	ScalarPoint  float64
	ScalarPoint2 float64 // only for double_exponential_smoothing (trend factor)
}

type FunctionCall func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error)

func instantValue(samples []Sample, isRate bool) (float64, bool) {
	lastSample := samples[len(samples)-1]
	previousSample := samples[len(samples)-2]

	var resultValue float64
	if isRate && lastSample.V.F < previousSample.V.F {
		// Counter reset.
		resultValue = lastSample.V.F
	} else {
		resultValue = lastSample.V.F - previousSample.V.F
	}

	sampledInterval := lastSample.T - previousSample.T
	if sampledInterval == 0 {
		// Avoid dividing by 0.
		return 0, false
	}

	if isRate {
		// Convert to per-second.
		resultValue /= float64(sampledInterval) / 1000
	}

	return resultValue, true
}

var rangeVectorFuncs = map[string]FunctionCall{
	"sum_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		if f.Samples[0].V.H != nil {
			// histogram
			sum := f.Samples[0].V.H.Copy()
			for _, sample := range f.Samples[1:] {
				h := sample.V.H
				_, err := sum.Add(h)
				if err != nil {
					return 0, sum, true, err
				}
			}

			return 0, sum, true, nil
		}
		return sumOverTime(f.Samples), nil, true, nil
	},
	"mad_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return madOverTime(f.Samples), nil, true, nil
	},
	"max_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return maxOverTime(f.Samples), nil, true, nil
	},
	"min_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return minOverTime(f.Samples), nil, true, nil
	},
	"avg_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		if f.Samples[0].V.H != nil {
			// histogram
			count := 1
			mean := f.Samples[0].V.H.Copy()
			for _, sample := range f.Samples[1:] {
				count++
				left := sample.V.H.Copy().Div(float64(count))
				right := mean.Copy().Div(float64(count))
				toAdd, err := left.Sub(right)
				if err != nil {
					return 0, mean, true, err
				}
				_, err = mean.Add(toAdd)
				if err != nil {
					return 0, mean, true, err
				}
			}
			return 0, mean, true, nil
		}

		return avgOverTime(f.Samples), nil, true, nil
	},
	"stddev_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return stddevOverTime(f.Samples), nil, true, nil
	},
	"stdvar_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return stdvarOverTime(f.Samples), nil, true, nil
	},
	"count_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return countOverTime(f.Samples), nil, true, nil
	},
	"last_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		if f.Samples[0].V.H != nil {
			return 0, f.Samples[len(f.Samples)-1].V.H.Copy(), true, nil
		}
		return f.Samples[len(f.Samples)-1].V.F, nil, true, nil
	},
	"present_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return 1., nil, true, nil
	},
	"quantile_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		floats := make([]float64, len(f.Samples))
		for i, sample := range f.Samples {
			floats[i] = sample.V.F
		}
		return aggregate.Quantile(f.ScalarPoint, floats), nil, true, nil
	},
	"changes": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return changes(f.Samples), nil, true, nil
	},
	"resets": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		return resets(f.Samples), nil, true, nil
	},
	"deriv": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) < 2 {
			return 0., nil, false, nil
		}
		if f.Samples[0].V.H != nil {
			// deriv should ignore histograms.
			return 0, nil, false, nil
		}
		return deriv(f.Samples), nil, true, nil
	},
	"irate": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		f.Samples = filterFloatOnlySamples(f.Samples)
		if len(f.Samples) < 2 {
			return 0., nil, false, nil
		}
		val, ok := instantValue(f.Samples, true)
		if !ok {
			return 0., nil, false, nil
		}
		return val, nil, true, nil
	},
	"idelta": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		f.Samples = filterFloatOnlySamples(f.Samples)
		if len(f.Samples) < 2 {
			return 0., nil, false, nil
		}
		val, ok := instantValue(f.Samples, false)
		if !ok {
			return 0., nil, false, nil
		}
		return val, nil, true, nil
	},
	"rate": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) < 2 {
			return 0., nil, false, nil
		}
		return extrapolatedRate(f.ctx, f.Samples, len(f.Samples), true, true, f.StepTime, f.SelectRange, f.Offset)
	},
	"delta": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) < 2 {
			return 0., nil, false, nil
		}
		return extrapolatedRate(f.ctx, f.Samples, len(f.Samples), false, false, f.StepTime, f.SelectRange, f.Offset)
	},
	"increase": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) < 2 {
			return 0., nil, false, nil
		}
		return extrapolatedRate(f.ctx, f.Samples, len(f.Samples), true, false, f.StepTime, f.SelectRange, f.Offset)
	},
	"xrate": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		if f.MetricAppearedTs == nil {
			panic("BUG: we got some Samples but metric still hasn't appeared")
		}
		v, h, err := extendedRate(f.ctx, f.Samples, true, true, f.StepTime, f.SelectRange, f.Offset, *f.MetricAppearedTs)
		if err != nil {
			return 0, nil, false, err
		}
		return v, h, true, nil
	},
	"xdelta": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		if f.MetricAppearedTs == nil {
			panic("BUG: we got some Samples but metric still hasn't appeared")
		}
		v, h, err := extendedRate(f.ctx, f.Samples, false, false, f.StepTime, f.SelectRange, f.Offset, *f.MetricAppearedTs)
		if err != nil {
			return 0, nil, false, err
		}
		return v, h, true, nil
	},
	"xincrease": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) == 0 {
			return 0., nil, false, nil
		}
		if f.MetricAppearedTs == nil {
			panic("BUG: we got some Samples but metric still hasn't appeared")
		}
		v, h, err := extendedRate(f.ctx, f.Samples, true, false, f.StepTime, f.SelectRange, f.Offset, *f.MetricAppearedTs)
		if err != nil {
			return 0, nil, false, err
		}
		return v, h, true, nil
	},
	"predict_linear": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) < 2 {
			return 0., nil, false, nil
		}
		v := predictLinear(f.Samples, f.ScalarPoint, f.StepTime)
		return v, nil, true, nil
	},
	"double_exponential_smoothing": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool, error) {
		if len(f.Samples) < 2 {
			if len(f.Samples) == 1 && f.Samples[0].V.H != nil {
				warnings.AddToContext(annotations.MixedFloatsHistogramsWarning, f.ctx)
				return 0, nil, false, nil
			}
			return 0, nil, false, nil
		}

		// Annotate mix of float and histogram.
		for _, s := range f.Samples {
			if s.V.H != nil {
				warnings.AddToContext(annotations.MixedFloatsHistogramsWarning, f.ctx)
				return 0, nil, false, nil
			}
		}

		sf := f.ScalarPoint  // smoothing factor or alpha
		tf := f.ScalarPoint2 // trend factor argument or beta

		v, ok := doubleExponentialSmoothing(f.Samples, sf, tf)
		return v, nil, ok, nil
	},
}

func NewRangeVectorFunc(name string) (FunctionCall, error) {
	call, ok := rangeVectorFuncs[name]
	if !ok {
		return nil, parse.UnknownFunctionError(name)
	}
	return call, nil
}

// extrapolatedRate is a utility function for rate/increase/delta.
// It calculates the rate (allowing for counter resets if isCounter is true),
// extrapolates if the first/last sample is close to the boundary, and returns
// the result as either per-second (if isRate is true) or overall.
func extrapolatedRate(ctx context.Context, samples []Sample, numSamples int, isCounter, isRate bool, stepTime int64, selectRange int64, offset int64) (f float64, h *histogram.FloatHistogram, ok bool, err error) {
	var (
		rangeStart      = stepTime - (selectRange + offset)
		rangeEnd        = stepTime - offset
		resultValue     float64
		resultHistogram *histogram.FloatHistogram
	)

	if samples[0].V.H != nil {
		resultHistogram, err = histogramRate(ctx, samples, isCounter)
		if err != nil {
			return 0, nil, false, err
		}
	} else {
		resultValue = samples[len(samples)-1].V.F - samples[0].V.F
		if isCounter {
			var lastValue float64
			for _, sample := range samples {
				if sample.V.F < lastValue {
					resultValue += lastValue
				}
				lastValue = sample.V.F
			}
		}
	}

	// Duration between first/last Samples and boundary of range.
	durationToStart := float64(samples[0].T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-samples[len(samples)-1].T) / 1000

	sampledInterval := float64(samples[len(samples)-1].T-samples[0].T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(numSamples-1)

	// If samples are close enough to the (lower or upper) boundary of the
	// range, we extrapolate the rate all the way to the boundary in
	// question. "Close enough" is defined as "up to 10% more than the
	// average duration between samples within the range", see
	// extrapolationThreshold below. Essentially, we are assuming a more or
	// less regular spacing between samples, and if we don't see a sample
	// where we would expect one, we assume the series does not cover the
	// whole range, but starts and/or ends within the range. We still
	// extrapolate the rate in this case, but not all the way to the
	// boundary, but only by half of the average duration between samples
	// (which is our guess for where the series actually starts or ends).

	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart >= extrapolationThreshold {
		durationToStart = averageDurationBetweenSamples / 2
	}
	if isCounter && resultValue > 0 && samples[0].V.F >= 0 {
		// Counters cannot be negative. If we have any slope at
		// all (i.e. resultValue went up), we can extrapolate
		// the zero point of the counter. If the duration to the
		// zero point is shorter than the durationToStart, we
		// take the zero point as the start of the series,
		// thereby avoiding extrapolation to negative counter
		// values.
		// TODO(beorn7): Do this for histograms, too.
		durationToZero := sampledInterval * (samples[0].V.F / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}
	extrapolateToInterval += durationToStart

	if durationToEnd >= extrapolationThreshold {
		durationToEnd = averageDurationBetweenSamples / 2
	}
	extrapolateToInterval += durationToEnd

	factor := extrapolateToInterval / sampledInterval
	if isRate {
		factor /= float64(selectRange) / 1000
	}
	if resultHistogram == nil {
		resultValue *= factor
	} else {
		resultHistogram.Mul(factor)
	}

	if samples[0].V.H != nil && resultHistogram == nil {
		// to prevent appending sample with 0
		return 0, nil, false, nil
	}

	return resultValue, resultHistogram, true, nil
}

// extendedRate is a utility function for xrate/xincrease/xdelta.
// It calculates the rate (allowing for counter resets if isCounter is true),
// taking into account the last sample before the range start, and returns
// the result as either per-second (if isRate is true) or overall.
func extendedRate(ctx context.Context, samples []Sample, isCounter, isRate bool, stepTime int64, selectRange int64, offset int64, metricAppearedTs int64) (float64, *histogram.FloatHistogram, error) {
	var (
		rangeStart      = stepTime - (selectRange + offset)
		rangeEnd        = stepTime - offset
		resultValue     float64
		resultHistogram *histogram.FloatHistogram
	)

	if samples[0].V.H != nil {
		var err error
		// TODO - support extended rate for histograms
		resultHistogram, err = histogramRate(ctx, samples, isCounter)
		if err != nil {
			return 0, nil, err
		}

		return resultValue, resultHistogram, nil
	}

	sameVals := true
	for i := range samples {
		if i > 0 && samples[i-1].V.F != samples[i].V.F {
			sameVals = false
			break
		}
	}

	// This effectively injects a "zero" series for xincrease if we only have one sample.
	// Only do it for some time when the metric appears the first time.
	until := selectRange + metricAppearedTs
	if isCounter && !isRate && sameVals {
		// Make sure we are not at the end of the range.
		if stepTime-offset <= until {
			return samples[0].V.F, nil, nil
		}
	}

	sampledInterval := float64(samples[len(samples)-1].T - samples[0].T)
	averageDurationBetweenSamples := sampledInterval / float64(len(samples)-1)

	firstPoint := 0
	// Only do this for not xincrease
	if !(isCounter && !isRate) {
		// If the point before the range is too far from rangeStart, drop it.
		if float64(rangeStart-samples[0].T) > averageDurationBetweenSamples {
			if len(samples) < 3 {
				return resultValue, nil, nil
			}
			firstPoint = 1
			sampledInterval = float64(samples[len(samples)-1].T - samples[1].T)
			averageDurationBetweenSamples = sampledInterval / float64(len(samples)-2)
		}
	}

	var (
		counterCorrection float64
		lastValue         float64
	)
	if isCounter {
		for i := firstPoint; i < len(samples); i++ {
			sample := samples[i]
			if sample.V.F < lastValue {
				counterCorrection += lastValue
			}
			lastValue = sample.V.F
		}
	}
	resultValue = samples[len(samples)-1].V.F - samples[firstPoint].V.F + counterCorrection

	// Duration between last sample and boundary of range.
	durationToEnd := float64(rangeEnd - samples[len(samples)-1].T)
	// If the points cover the whole range (i.e. they start just before the
	// range start and end just before the range end) adjust the value from
	// the sampled range to the requested range.
	// Only do this for not xincrease.
	if !(isCounter && !isRate) {
		if samples[firstPoint].T <= rangeStart && durationToEnd < averageDurationBetweenSamples {
			adjustToRange := float64(selectRange / 1000)
			resultValue = resultValue * (adjustToRange / (sampledInterval / 1000))
		}
	}

	if isRate {
		resultValue = resultValue / float64(selectRange/1000)
	}

	return resultValue, nil, nil
}

// histogramRate is a helper function for extrapolatedRate. It requires
// points[0] to be a histogram. It returns nil if any other Point in points is
// not a histogram.
func histogramRate(ctx context.Context, points []Sample, isCounter bool) (*histogram.FloatHistogram, error) {
	// Calculating a rate on a single sample is not defined.
	if len(points) < 2 {
		return nil, nil
	}

	prev := points[0].V.H // We already know that this is a histogram.
	usingCustomBuckets := prev.UsesCustomBuckets()
	last := points[len(points)-1].V.H
	if last == nil {
		warnings.AddToContext(annotations.MixedFloatsHistogramsWarning, ctx)
		return nil, nil // Range contains a mix of histograms and floats.
	}
	minSchema := prev.Schema
	if last.Schema < minSchema {
		minSchema = last.Schema
	}

	if last.UsesCustomBuckets() != usingCustomBuckets {
		warnings.AddToContext(annotations.MixedExponentialCustomHistogramsWarning, ctx)
		return nil, nil
	}

	// We check for gauge type histograms in the loop below, but the loop below does not run on the first and last point,
	// so check the first and last point now.
	if isCounter && (prev.CounterResetHint == histogram.GaugeType || last.CounterResetHint == histogram.GaugeType) {
		warnings.AddToContext(annotations.NativeHistogramNotCounterWarning, ctx)
	}

	// https://github.com/prometheus/prometheus/blob/ccea61c7bf1e6bce2196ba8189a209945a204c5b/promql/functions.go#L183
	// First iteration to find out two things:
	// - What's the smallest relevant schema?
	// - Are all data points histograms?
	//   []FloatPoint and a []HistogramPoint separately.
	for _, currPoint := range points[1 : len(points)-1] {
		curr := currPoint.V.H
		if curr == nil {
			warnings.AddToContext(annotations.MixedFloatsHistogramsWarning, ctx)
			return nil, nil // Range contains a mix of histograms and floats.
		}
		if !isCounter {
			continue
		}
		if curr.CounterResetHint == histogram.GaugeType {
			warnings.AddToContext(annotations.NativeHistogramNotCounterWarning, ctx)
		}
		if curr.Schema < minSchema {
			minSchema = curr.Schema
		}
		if curr.UsesCustomBuckets() != usingCustomBuckets {
			warnings.AddToContext(annotations.MixedExponentialCustomHistogramsWarning, ctx)
			return nil, nil
		}
	}

	h := last.CopyToSchema(minSchema)
	if _, err := h.Sub(prev); err != nil {
		if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
			warnings.AddToContext(annotations.MixedExponentialCustomHistogramsWarning, ctx)
		} else if errors.Is(err, histogram.ErrHistogramsIncompatibleBounds) {
			warnings.AddToContext(annotations.IncompatibleCustomBucketsHistogramsWarning, ctx)
		}
		return nil, err
	}

	if isCounter {
		// Second iteration to deal with counter resets.
		for _, currPoint := range points[1:] {
			curr := currPoint.V.H
			if curr.DetectReset(prev) {
				if _, err := h.Add(prev); err != nil {
					if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
						warnings.AddToContext(annotations.MixedExponentialCustomHistogramsWarning, ctx)
					} else if errors.Is(err, histogram.ErrHistogramsIncompatibleBounds) {
						warnings.AddToContext(annotations.IncompatibleCustomBucketsHistogramsWarning, ctx)
					}
					return nil, err
				}
			}
			prev = curr
		}
	} else if points[0].V.H.CounterResetHint != histogram.GaugeType || points[len(points)-1].V.H.CounterResetHint != histogram.GaugeType {
		warnings.AddToContext(annotations.NativeHistogramNotGaugeWarning, ctx)
	}

	h.CounterResetHint = histogram.GaugeType
	return h.Compact(0), nil
}

func madOverTime(points []Sample) float64 {
	values := make([]float64, 0, len(points))
	for _, f := range points {
		values = append(values, f.V.F)
	}
	sort.Float64s(values)

	median := stat.Quantile(0.5, stat.LinInterp, values, nil)

	for i, f := range points {
		values[i] = math.Abs(f.V.F - median)
	}
	sort.Float64s(values)

	return stat.Quantile(0.5, stat.LinInterp, values, nil)
}

func maxOverTime(points []Sample) float64 {
	max := points[0].V.F
	for _, v := range points {
		if v.V.F > max || math.IsNaN(max) {
			max = v.V.F
		}
	}
	return max
}

func minOverTime(points []Sample) float64 {
	min := points[0].V.F
	for _, v := range points {
		if v.V.F < min || math.IsNaN(min) {
			min = v.V.F
		}
	}
	return min
}

func countOverTime(points []Sample) float64 {
	return float64(len(points))
}

func avgOverTime(points []Sample) float64 {
	var (
		sum, mean, count, kahanC float64
		incrementalMean          bool
	)
	for _, v := range points {
		count++
		if !incrementalMean {
			newSum, newC := kahanSumInc(v.V.F, sum, kahanC)
			// Perform regular mean calculation as long as
			// the sum doesn't overflow and (in any case)
			// for the first iteration (even if we start
			// with ±Inf) to not run into division-by-zero
			// problems below.
			if count == 1 || !math.IsInf(newSum, 0) {
				sum, kahanC = newSum, newC
				continue
			}
			// Handle overflow by reverting to incremental calculation of the mean value.
			incrementalMean = true
			mean = sum / (count - 1)
			kahanC /= count - 1
		}
		if math.IsInf(mean, 0) {
			if math.IsInf(v.V.F, 0) && (mean > 0) == (v.V.F > 0) {
				// The `mean` and `v.V.F` values are `Inf` of the same sign.  They
				// can't be subtracted, but the value of `mean` is correct
				// already.
				continue
			}
			if !math.IsInf(v.V.F, 0) && !math.IsNaN(v.V.F) {
				// At this stage, the mean is an infinite. If the added
				// value is neither an Inf or a Nan, we can keep that mean
				// value.
				// This is required because our calculation below removes
				// the mean value, which would look like Inf += x - Inf and
				// end up as a NaN.
				continue
			}
		}
		correctedMean := mean + kahanC
		mean, kahanC = kahanSumInc(v.V.F/count-correctedMean/count, mean, kahanC)
	}

	if incrementalMean {
		return mean + kahanC
	}
	return (sum + kahanC) / count
}

func sumOverTime(points []Sample) float64 {
	var sum, c float64
	for _, v := range points {
		sum, c = kahanSumInc(v.V.F, sum, c)
	}
	if math.IsInf(sum, 0) {
		return sum
	}
	return sum + c
}

func stddevOverTime(points []Sample) float64 {
	var count float64
	var mean, cMean float64
	var aux, cAux float64
	for _, v := range points {
		count++
		delta := v.V.F - (mean + cMean)
		mean, cMean = kahanSumInc(delta/count, mean, cMean)
		aux, cAux = kahanSumInc(delta*(v.V.F-(mean+cMean)), aux, cAux)
	}
	return math.Sqrt((aux + cAux) / count)
}

func stdvarOverTime(points []Sample) float64 {
	var count float64
	var mean, cMean float64
	var aux, cAux float64
	for _, v := range points {
		count++
		delta := v.V.F - (mean + cMean)
		mean, cMean = kahanSumInc(delta/count, mean, cMean)
		aux, cAux = kahanSumInc(delta*(v.V.F-(mean+cMean)), aux, cAux)
	}
	return (aux + cAux) / count
}

func changes(points []Sample) float64 {
	count := 0.

	prevSample := points[0]
	for _, curSample := range points[1:] {
		switch {
		case prevSample.V.H == nil && curSample.V.H == nil:
			if curSample.V.F != prevSample.V.F && !(math.IsNaN(curSample.V.F) && math.IsNaN(prevSample.V.F)) {
				count++
			}
		case prevSample.V.H != nil && curSample.V.H == nil, prevSample.V.H == nil && curSample.V.H != nil:
			count++
		case prevSample.V.H != nil && curSample.V.H != nil:
			if !curSample.V.H.Equals(prevSample.V.H) {
				count++
			}
		}
		prevSample = curSample
	}
	return count
}

func deriv(points []Sample) float64 {
	// We pass in an arbitrary timestamp that is near the values in use
	// to avoid floating point accuracy issues, see
	// https://github.com/prometheus/prometheus/issues/2674
	slope, _ := linearRegression(points, points[0].T)
	return slope
}

func predictLinear(points []Sample, duration float64, stepTime int64) float64 {
	slope, intercept := linearRegression(points, stepTime)
	return slope*duration + intercept
}

// Based on https://github.com/prometheus/prometheus/blob/8baad1a73e471bd3cf3175a1608199e27484f179/promql/functions.go#L438
// doubleExponentialSmoothing calculates the smoothed out value for the given series.
// It is similar to a weighted moving average, where historical data has exponentially less influence on the current data.
// It also accounts for trends in data. The smoothing factor (0 < sf < 1), aka "alpha", affects how historical data will affect the current data.
// A lower smoothing factor increases the influence of historical data.
// The trend factor (0 < tf < 1), aka "beta", affects how trends in historical data will affect the current data.
// A higher trend factor increases the influence of trends.
// Algorithm taken from https://en.wikipedia.org/wiki/Exponential_smoothing
func doubleExponentialSmoothing(points []Sample, sf, tf float64) (float64, bool) {
	// Check that the input parameters are valid
	if sf <= 0 || sf >= 1 || tf <= 0 || tf >= 1 {
		return 0, false
	}

	// Can't do the smoothing operation with less than two points
	if len(points) < 2 {
		return 0, false
	}

	// Check for histograms in the samples
	for _, s := range points {
		if s.V.H != nil {
			return 0, false
		}
	}

	var s0, s1, b float64
	// Set initial values
	s1 = points[0].V.F
	b = points[1].V.F - points[0].V.F

	// Run the smoothing operation
	for i := 1; i < len(points); i++ {
		// Scale the raw value against the smoothing factor
		x := sf * points[i].V.F
		// Scale the last smoothed value with the trend at this point
		b = calcTrendValue(i-1, tf, s0, s1, b)
		y := (1 - sf) * (s1 + b)
		s0, s1 = s1, x+y
	}

	return s1, true
}

// calcTrendValue calculates the trend value at the given index i.
// This is somewhat analogous to the slope of the trend at the given index.
// The argument "tf" is the trend factor.
// The argument "s0" is the previous smoothed value.
// The argument "s1" is the current smoothed value.
// The argument "b" is the previous trend value.
func calcTrendValue(i int, tf, s0, s1, b float64) float64 {
	if i == 0 {
		return b
	}
	x := tf * (s1 - s0)
	y := (1 - tf) * b
	return x + y
}

func resets(points []Sample) float64 {
	var histogramPoints []Sample
	var floatPoints []Sample

	for _, p := range points {
		if p.V.H != nil {
			histogramPoints = append(histogramPoints, p)
		} else {
			floatPoints = append(floatPoints, p)
		}
	}

	count := 0
	var prevSample, curSample Sample
	for iFloat, iHistogram := 0, 0; iFloat < len(floatPoints) || iHistogram < len(histogramPoints); {
		switch {
		// Process a float sample if no histogram sample remains or its timestamp is earlier.
		// Process a histogram sample if no float sample remains or its timestamp is earlier.
		case iHistogram >= len(histogramPoints) || iFloat < len(floatPoints) && floatPoints[iFloat].T < histogramPoints[iHistogram].T:
			curSample.V.F = floatPoints[iFloat].V.F
			curSample.V.H = nil
			iFloat++
		case iFloat >= len(floatPoints) || iHistogram < len(histogramPoints) && floatPoints[iFloat].T > histogramPoints[iHistogram].T:
			curSample.V.H = histogramPoints[iHistogram].V.H
			iHistogram++
		}
		// Skip the comparison for the first sample, just initialize prevSample.
		if iFloat+iHistogram == 1 {
			prevSample = curSample
			continue
		}
		switch {
		case prevSample.V.H == nil && curSample.V.H == nil:
			if curSample.V.F < prevSample.V.F {
				count++
			}
		case prevSample.V.H != nil && curSample.V.H == nil, prevSample.V.H == nil && curSample.V.H != nil:
			count++
		case prevSample.V.H != nil && curSample.V.H != nil:
			if curSample.V.H.DetectReset(prevSample.V.H) {
				count++
			}
		}
		prevSample = curSample
	}

	return float64(count)
}

func linearRegression(Samples []Sample, interceptTime int64) (slope, intercept float64) {
	var (
		n          float64
		sumX, cX   float64
		sumY, cY   float64
		sumXY, cXY float64
		sumX2, cX2 float64
		initY      float64
		constY     bool
	)
	initY = Samples[0].V.F
	constY = true
	for i, sample := range Samples {
		if sample.V.H != nil {
			// should ignore histograms
			continue
		}

		// Set constY to false if any new y values are encountered.
		if constY && i > 0 && sample.V.F != initY {
			constY = false
		}
		n += 1.0
		x := float64(sample.T-interceptTime) / 1e3
		sumX, cX = kahanSumInc(x, sumX, cX)
		sumY, cY = kahanSumInc(sample.V.F, sumY, cY)
		sumXY, cXY = kahanSumInc(x*sample.V.F, sumXY, cXY)
		sumX2, cX2 = kahanSumInc(x*x, sumX2, cX2)
	}
	if constY {
		if math.IsInf(initY, 0) {
			return math.NaN(), math.NaN()
		}
		return 0, initY
	}
	sumX = sumX + cX
	sumY = sumY + cY
	sumXY = sumXY + cXY
	sumX2 = sumX2 + cX2

	covXY := sumXY - sumX*sumY/n
	varX := sumX2 - sumX*sumX/n

	slope = covXY / varX
	intercept = sumY/n - slope*sumX/n
	return slope, intercept
}

func filterFloatOnlySamples(samples []Sample) []Sample {
	i := 0
	for _, sample := range samples {
		if sample.V.H == nil {
			samples[i] = sample
			i++
		}
	}
	samples = samples[:i]
	return samples
}

func kahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	t := sum + inc
	switch {
	case math.IsInf(t, 0):
		c = 0

	// Using Neumaier improvement, swap if next term larger than sum.
	case math.Abs(sum) >= math.Abs(inc):
		c += (sum - t) + inc
	default:
		c += (inc - t) + sum
	}
	return t, c
}
