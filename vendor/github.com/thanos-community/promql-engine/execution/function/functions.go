// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"fmt"
	"math"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"

	"github.com/thanos-community/promql-engine/execution/parse"
)

var InvalidSample = promql.Sample{T: -1, F: 0}

type FunctionArgs struct {
	Labels       labels.Labels
	Samples      []promql.Sample
	StepTime     int64
	SelectRange  int64
	ScalarPoints []float64
	Offset       int64
}

// FunctionCall represents functions as defined in https://prometheus.io/docs/prometheus/latest/querying/functions/
type FunctionCall func(f FunctionArgs) promql.Sample

func simpleFunc(f func(float64) float64) FunctionCall {
	return func(fa FunctionArgs) promql.Sample {
		if len(fa.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: fa.Labels,
			T:      fa.StepTime,
			F:      f(fa.Samples[0].F),
		}
	}

}

func filterFloatOnlySamples(samples []promql.Sample) []promql.Sample {
	i := 0
	for _, sample := range samples {
		if sample.H == nil {
			samples[i] = sample
			i++
		}
	}
	samples = samples[:i]
	return samples
}

// The engine handles sort and sort_desc when presenting the results. They are not needed here.
var Funcs = map[string]FunctionCall{
	"abs":   simpleFunc(math.Abs),
	"ceil":  simpleFunc(math.Ceil),
	"exp":   simpleFunc(math.Exp),
	"floor": simpleFunc(math.Floor),
	"sqrt":  simpleFunc(math.Sqrt),
	"ln":    simpleFunc(math.Log),
	"log2":  simpleFunc(math.Log2),
	"log10": simpleFunc(math.Log10),
	"sin":   simpleFunc(math.Sin),
	"cos":   simpleFunc(math.Cos),
	"tan":   simpleFunc(math.Tan),
	"asin":  simpleFunc(math.Asin),
	"acos":  simpleFunc(math.Acos),
	"atan":  simpleFunc(math.Atan),
	"sinh":  simpleFunc(math.Sinh),
	"cosh":  simpleFunc(math.Cosh),
	"tanh":  simpleFunc(math.Tanh),
	"asinh": simpleFunc(math.Asinh),
	"acosh": simpleFunc(math.Acosh),
	"atanh": simpleFunc(math.Atanh),
	"rad": simpleFunc(func(v float64) float64 {
		return v * math.Pi / 180
	}),
	"deg": simpleFunc(func(v float64) float64 {
		return v * 180 / math.Pi
	}),
	"sgn": simpleFunc(func(v float64) float64 {
		var sign float64
		if v > 0 {
			sign = 1
		} else if v < 0 {
			sign = -1
		}
		if math.IsNaN(v) {
			sign = math.NaN()
		}
		return sign
	}),
	"round": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) != 1 || len(f.ScalarPoints) > 1 {
			return InvalidSample
		}

		toNearest := 1.0
		if len(f.ScalarPoints) > 0 {
			toNearest = f.ScalarPoints[0]
		}
		toNearestInverse := 1.0 / toNearest
		return promql.Sample{
			T: f.StepTime,
			F: math.Floor(f.Samples[0].F*toNearestInverse+0.5) / toNearestInverse,
		}
	},
	"pi": func(f FunctionArgs) promql.Sample {
		return promql.Sample{
			T: f.StepTime,
			F: math.Pi,
		}
	},
	"sum_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      sumOverTime(f.Samples),
		}
	},
	"max_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      maxOverTime(f.Samples),
		}
	},
	"min_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      minOverTime(f.Samples),
		}
	},
	"avg_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      avgOverTime(f.Samples),
		}
	},
	"stddev_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      stddevOverTime(f.Samples),
		}
	},
	"stdvar_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      stdvarOverTime(f.Samples),
		}
	},
	"count_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      countOverTime(f.Samples),
		}
	},
	"last_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      f.Samples[len(f.Samples)-1].F,
		}
	},
	"present_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      1,
		}
	},
	"time": func(f FunctionArgs) promql.Sample {
		return promql.Sample{
			T: f.StepTime,
			F: float64(f.StepTime) / 1000,
		}
	},
	"changes": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      changes(f.Samples),
		}
	},
	"resets": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      resets(f.Samples),
		}
	},
	"deriv": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) < 2 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      deriv(f.Samples),
		}
	},
	"irate": func(f FunctionArgs) promql.Sample {
		f.Samples = filterFloatOnlySamples(f.Samples)
		if len(f.Samples) < 2 {
			return InvalidSample
		}
		val, ok := instantValue(f.Samples, true)
		if !ok {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      val,
		}
	},
	"idelta": func(f FunctionArgs) promql.Sample {
		f.Samples = filterFloatOnlySamples(f.Samples)
		if len(f.Samples) < 2 {
			return InvalidSample
		}
		val, ok := instantValue(f.Samples, false)
		if !ok {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      val,
		}
	},
	"vector": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      f.Samples[0].F,
		}
	},
	"scalar": func(f FunctionArgs) promql.Sample {
		// This is handled specially by operator.
		return promql.Sample{}
	},
	"rate": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) < 2 {
			return InvalidSample
		}
		v, h := extrapolatedRate(f.Samples, true, true, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      v,
			H:      h,
		}
	},
	"delta": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) < 2 {
			return InvalidSample
		}
		v, h := extrapolatedRate(f.Samples, false, false, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      v,
			H:      h,
		}
	},
	"increase": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) < 2 {
			return InvalidSample
		}
		v, h := extrapolatedRate(f.Samples, true, false, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      v,
			H:      h,
		}
	},
	"xrate": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		v, h := extendedRate(f.Samples, true, true, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      v,
			H:      h,
		}
	},
	"xdelta": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		v, h := extendedRate(f.Samples, false, false, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      v,
			H:      h,
		}
	},
	"xincrease": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 {
			return InvalidSample
		}
		v, h := extendedRate(f.Samples, true, false, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      v,
			H:      h,
		}
	},
	"clamp": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 || len(f.ScalarPoints) < 2 {
			return InvalidSample
		}

		v := f.Samples[0].F
		min := f.ScalarPoints[0]
		max := f.ScalarPoints[1]

		if max < min {
			return InvalidSample
		}

		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      math.Max(min, math.Min(max, v)),
		}
	},
	"clamp_min": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 || len(f.ScalarPoints) == 0 {
			return InvalidSample
		}

		v := f.Samples[0].F
		min := f.ScalarPoints[0]

		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      math.Max(min, v),
		}
	},
	"clamp_max": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 || len(f.ScalarPoints) == 0 {
			return InvalidSample
		}

		v := f.Samples[0].F
		max := f.ScalarPoints[0]

		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      math.Min(max, v),
		}
	},
	"histogram_sum": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 || f.Samples[0].H == nil {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      f.Samples[0].H.Sum,
		}
	},
	"histogram_count": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 || f.Samples[0].H == nil {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      f.Samples[0].H.Count,
		}
	},
	"histogram_fraction": func(f FunctionArgs) promql.Sample {
		if len(f.Samples) == 0 || f.Samples[0].H == nil {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			T:      f.StepTime,
			F:      histogramFraction(f.ScalarPoints[0], f.ScalarPoints[1], f.Samples[0].H),
		}
	},
	"days_in_month": func(f FunctionArgs) promql.Sample {
		return dateWrapper(f, func(t time.Time) float64 {
			return float64(32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC).Day())
		})
	},
	"day_of_month": func(f FunctionArgs) promql.Sample {
		return dateWrapper(f, func(t time.Time) float64 {
			return float64(t.Day())
		})
	},
	"day_of_week": func(f FunctionArgs) promql.Sample {
		return dateWrapper(f, func(t time.Time) float64 {
			return float64(t.Weekday())
		})
	},
	"day_of_year": func(f FunctionArgs) promql.Sample {
		return dateWrapper(f, func(t time.Time) float64 {
			return float64(t.YearDay())
		})
	},
	"hour": func(f FunctionArgs) promql.Sample {
		return dateWrapper(f, func(t time.Time) float64 {
			return float64(t.Hour())
		})
	},
	"minute": func(f FunctionArgs) promql.Sample {
		return dateWrapper(f, func(t time.Time) float64 {
			return float64(t.Minute())
		})
	},
	"month": func(f FunctionArgs) promql.Sample {
		return dateWrapper(f, func(t time.Time) float64 {
			return float64(t.Month())
		})
	},
	"year": func(f FunctionArgs) promql.Sample {
		return dateWrapper(f, func(t time.Time) float64 {
			return float64(t.Year())
		})
	},
}

func NewFunctionCall(f *parser.Function) (FunctionCall, error) {
	if call, ok := Funcs[f.Name]; ok {
		return call, nil
	}

	msg := fmt.Sprintf("unknown function: %s", f.Name)
	if _, ok := parser.Functions[f.Name]; ok {
		return nil, errors.Wrap(parse.ErrNotImplemented, msg)
	}

	return nil, errors.Wrap(parse.ErrNotSupportedExpr, msg)
}

// extrapolatedRate is a utility function for rate/increase/delta.
// It calculates the rate (allowing for counter resets if isCounter is true),
// extrapolates if the first/last sample is close to the boundary, and returns
// the result as either per-second (if isRate is true) or overall.
func extrapolatedRate(samples []promql.Sample, isCounter, isRate bool, stepTime int64, selectRange int64, offset int64) (float64, *histogram.FloatHistogram) {
	var (
		rangeStart      = stepTime - (selectRange + offset)
		rangeEnd        = stepTime - offset
		resultValue     float64
		resultHistogram *histogram.FloatHistogram
	)

	if samples[0].H != nil {
		resultHistogram = histogramRate(samples, isCounter)
	} else {
		resultValue = samples[len(samples)-1].F - samples[0].F
		if isCounter {
			var lastValue float64
			for _, sample := range samples {
				if sample.F < lastValue {
					resultValue += lastValue
				}
				lastValue = sample.F
			}
		}
	}

	// Duration between first/last samples and boundary of range.
	durationToStart := float64(samples[0].T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-samples[len(samples)-1].T) / 1000

	sampledInterval := float64(samples[len(samples)-1].T-samples[0].T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(len(samples)-1)

	if isCounter && resultValue > 0 && samples[0].F >= 0 {
		// Counters cannot be negative. If we have any slope at
		// all (i.e. resultValue went up), we can extrapolate
		// the zero point of the counter. If the duration to the
		// zero point is shorter than the durationToStart, we
		// take the zero point as the start of the series,
		// thereby avoiding extrapolation to negative counter
		// values.
		durationToZero := sampledInterval * (samples[0].F / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	// If the first/last samples are close to the boundaries of the range,
	// extrapolate the result. This is as we expect that another sample
	// will exist given the spacing between samples we've seen thus far,
	// with an allowance for noise.
	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart < extrapolationThreshold {
		extrapolateToInterval += durationToStart
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	if durationToEnd < extrapolationThreshold {
		extrapolateToInterval += durationToEnd
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	factor := extrapolateToInterval / sampledInterval
	if isRate {
		factor /= float64(selectRange / 1000)
	}
	if resultHistogram == nil {
		resultValue *= factor
	} else {
		resultHistogram.Scale(factor)

	}

	return resultValue, resultHistogram
}

// extendedRate is a utility function for xrate/xincrease/xdelta.
// It calculates the rate (allowing for counter resets if isCounter is true),
// taking into account the last sample before the range start, and returns
// the result as either per-second (if isRate is true) or overall.
func extendedRate(samples []promql.Sample, isCounter, isRate bool, stepTime int64, selectRange int64, offset int64) (float64, *histogram.FloatHistogram) {
	var (
		rangeStart      = stepTime - (selectRange + offset)
		rangeEnd        = stepTime - offset
		resultValue     float64
		resultHistogram *histogram.FloatHistogram
	)

	if samples[0].H != nil {
		// TODO - support extended rate for histograms
		resultHistogram = histogramRate(samples, isCounter)
		return resultValue, resultHistogram
	}

	sameVals := true
	for i := range samples {
		if i > 0 && samples[i-1].F != samples[i].F {
			sameVals = false
			break
		}
	}

	// This effectively injects a "zero" series for xincrease if we only have one sample.
	until := selectRange
	if isCounter && !isRate && sameVals {
		// Make sure we are not at the end of the range
		if stepTime-offset <= until {
			return samples[0].F, nil
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
				return resultValue, nil
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
			if sample.F < lastValue {
				counterCorrection += lastValue
			}
			lastValue = sample.F
		}
	}
	resultValue = samples[len(samples)-1].F - samples[firstPoint].F + counterCorrection

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

	return resultValue, nil
}

// histogramRate is a helper function for extrapolatedRate. It requires
// points[0] to be a histogram. It returns nil if any other Point in points is
// not a histogram.
func histogramRate(points []promql.Sample, isCounter bool) *histogram.FloatHistogram {
	prev := points[0].H // We already know that this is a histogram.
	last := points[len(points)-1].H
	if last == nil {
		return nil // Range contains a mix of histograms and floats.
	}
	minSchema := prev.Schema
	if last.Schema < minSchema {
		minSchema = last.Schema
	}

	// https://github.com/prometheus/prometheus/blob/ccea61c7bf1e6bce2196ba8189a209945a204c5b/promql/functions.go#L183
	// First iteration to find out two things:
	// - What's the smallest relevant schema?
	// - Are all data points histograms?
	//   []FloatPoint and a []HistogramPoint separately.
	for _, currPoint := range points[1 : len(points)-1] {
		curr := currPoint.H
		if curr == nil {
			return nil // Range contains a mix of histograms and floats.
		}
		if !isCounter {
			continue
		}
		if curr.Schema < minSchema {
			minSchema = curr.Schema
		}
	}

	h := last.CopyToSchema(minSchema)
	h.Sub(prev)

	if isCounter {
		// Second iteration to deal with counter resets.
		for _, currPoint := range points[1:] {
			curr := currPoint.H
			if curr.DetectReset(prev) {
				h.Add(prev)
			}
			prev = curr
		}
	}
	h.CounterResetHint = histogram.GaugeType
	return h.Compact(0)
}

func instantValue(samples []promql.Sample, isRate bool) (float64, bool) {
	lastSample := samples[len(samples)-1]
	previousSample := samples[len(samples)-2]

	var resultValue float64
	if isRate && lastSample.F < previousSample.F {
		// Counter reset.
		resultValue = lastSample.F
	} else {
		resultValue = lastSample.F - previousSample.F
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

func maxOverTime(points []promql.Sample) float64 {
	max := points[0].F
	for _, v := range points {
		if v.F > max || math.IsNaN(max) {
			max = v.F
		}
	}
	return max
}

func minOverTime(points []promql.Sample) float64 {
	min := points[0].F
	for _, v := range points {
		if v.F < min || math.IsNaN(min) {
			min = v.F
		}
	}
	return min
}

func countOverTime(points []promql.Sample) float64 {
	return float64(len(points))
}

func avgOverTime(points []promql.Sample) float64 {
	var mean, count, c float64
	for _, v := range points {
		count++
		if math.IsInf(mean, 0) {
			if math.IsInf(v.F, 0) && (mean > 0) == (v.F > 0) {
				// The `mean` and `v.F` values are `Inf` of the same sign.  They
				// can't be subtracted, but the value of `mean` is correct
				// already.
				continue
			}
			if !math.IsInf(v.F, 0) && !math.IsNaN(v.F) {
				// At this stage, the mean is an infinite. If the added
				// value is neither an Inf or a Nan, we can keep that mean
				// value.
				// This is required because our calculation below removes
				// the mean value, which would look like Inf += x - Inf and
				// end up as a NaN.
				continue
			}
		}
		mean, c = KahanSumInc(v.F/count-mean/count, mean, c)
	}

	if math.IsInf(mean, 0) {
		return mean
	}
	return mean + c
}

func sumOverTime(points []promql.Sample) float64 {
	var sum, c float64
	for _, v := range points {
		sum, c = KahanSumInc(v.F, sum, c)
	}
	if math.IsInf(sum, 0) {
		return sum
	}
	return sum + c
}

func stddevOverTime(points []promql.Sample) float64 {
	var count float64
	var mean, cMean float64
	var aux, cAux float64
	for _, v := range points {
		count++
		delta := v.F - (mean + cMean)
		mean, cMean = KahanSumInc(delta/count, mean, cMean)
		aux, cAux = KahanSumInc(delta*(v.F-(mean+cMean)), aux, cAux)
	}
	return math.Sqrt((aux + cAux) / count)
}

func stdvarOverTime(points []promql.Sample) float64 {
	var count float64
	var mean, cMean float64
	var aux, cAux float64
	for _, v := range points {
		count++
		delta := v.F - (mean + cMean)
		mean, cMean = KahanSumInc(delta/count, mean, cMean)
		aux, cAux = KahanSumInc(delta*(v.F-(mean+cMean)), aux, cAux)
	}
	return (aux + cAux) / count
}

func changes(points []promql.Sample) float64 {
	var count float64
	prev := points[0].F
	count = 0
	for _, sample := range points[1:] {
		current := sample.F
		if current != prev && !(math.IsNaN(current) && math.IsNaN(prev)) {
			count++
		}
		prev = current
	}
	return count
}

func deriv(points []promql.Sample) float64 {
	// We pass in an arbitrary timestamp that is near the values in use
	// to avoid floating point accuracy issues, see
	// https://github.com/prometheus/prometheus/issues/2674
	slope, _ := linearRegression(points, points[0].T)
	return slope
}

func resets(points []promql.Sample) float64 {
	count := 0
	prev := points[0].F
	for _, sample := range points[1:] {
		current := sample.F
		if current < prev {
			count++
		}
		prev = current
	}

	return float64(count)
}

func linearRegression(samples []promql.Sample, interceptTime int64) (slope, intercept float64) {
	var (
		n          float64
		sumX, cX   float64
		sumY, cY   float64
		sumXY, cXY float64
		sumX2, cX2 float64
		initY      float64
		constY     bool
	)
	initY = samples[0].F
	constY = true
	for i, sample := range samples {
		// Set constY to false if any new y values are encountered.
		if constY && i > 0 && sample.F != initY {
			constY = false
		}
		n += 1.0
		x := float64(sample.T-interceptTime) / 1e3
		sumX, cX = KahanSumInc(x, sumX, cX)
		sumY, cY = KahanSumInc(sample.F, sumY, cY)
		sumXY, cXY = KahanSumInc(x*sample.F, sumXY, cXY)
		sumX2, cX2 = KahanSumInc(x*x, sumX2, cX2)
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

func KahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	t := sum + inc
	// Using Neumaier improvement, swap if next term larger than sum.
	if math.Abs(sum) >= math.Abs(inc) {
		c += (sum - t) + inc
	} else {
		c += (inc - t) + sum
	}
	return t, c
}

// Common code for date related functions.
func dateWrapper(fa FunctionArgs, f func(time.Time) float64) promql.Sample {
	if len(fa.Samples) == 0 {
		return promql.Sample{
			Metric: labels.Labels{},
			F:      f(time.Unix(fa.StepTime/1000, 0).UTC()),
		}
	}
	t := time.Unix(int64(fa.Samples[0].F), 0).UTC()
	lbls, _ := DropMetricName(fa.Labels)
	return promql.Sample{
		Metric: lbls,
		F:      f(t),
	}
}

// IsExtFunction is a convenience function to determine whether extended range calculations are required.
func IsExtFunction(functionName string) bool {
	return functionName == "xincrease" || functionName == "xrate" || functionName == "xdelta"
}
