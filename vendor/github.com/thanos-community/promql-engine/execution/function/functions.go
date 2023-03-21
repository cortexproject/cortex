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
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/execution/parse"
)

var InvalidSample = promql.Sample{Point: promql.Point{T: -1, V: 0}}

type FunctionArgs struct {
	Labels       labels.Labels
	Points       []promql.Point
	StepTime     int64
	SelectRange  int64
	ScalarPoints []float64
	Offset       int64
}

// FunctionCall represents functions as defined in https://prometheus.io/docs/prometheus/latest/querying/functions/
type FunctionCall func(f FunctionArgs) promql.Sample

func simpleFunc(f func(float64) float64) FunctionCall {
	return func(fa FunctionArgs) promql.Sample {
		if len(fa.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: fa.Labels,
			Point: promql.Point{
				T: fa.StepTime,
				V: f(fa.Points[0].V),
			},
		}
	}

}

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
		return sign
	}),
	"timestamp": func(f FunctionArgs) promql.Sample {
		return promql.Sample{
			Point: promql.Point{
				T: f.StepTime,
				V: float64(f.Points[0].T) / 1000,
			},
		}
	},
	"pi": func(f FunctionArgs) promql.Sample {
		return promql.Sample{
			Point: promql.Point{
				T: f.StepTime,
				V: math.Pi,
			},
		}
	},
	"sum_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: sumOverTime(f.Points),
			},
		}
	},
	"max_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: maxOverTime(f.Points),
			},
		}
	},
	"min_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: minOverTime(f.Points),
			},
		}
	},
	"avg_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: avgOverTime(f.Points),
			},
		}
	},
	"stddev_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: stddevOverTime(f.Points),
			},
		}
	},
	"stdvar_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: stdvarOverTime(f.Points),
			},
		}
	},
	"count_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: countOverTime(f.Points),
			},
		}
	},
	"last_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: f.Points[len(f.Points)-1].V,
			},
		}
	},
	"present_over_time": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: 1,
			},
		}
	},
	"time": func(f FunctionArgs) promql.Sample {
		return promql.Sample{
			Point: promql.Point{
				T: f.StepTime,
				V: float64(f.StepTime) / 1000,
			},
		}
	},
	"changes": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: changes(f.Points),
			},
		}
	},
	"resets": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: resets(f.Points),
			},
		}
	},
	"deriv": func(f FunctionArgs) promql.Sample {
		if len(f.Points) < 2 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: deriv(f.Points),
			},
		}
	},
	"irate": func(f FunctionArgs) promql.Sample {
		if len(f.Points) < 2 {
			return InvalidSample
		}
		val, ok := instantValue(f.Points, true)
		if !ok {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: val,
			},
		}
	},
	"idelta": func(f FunctionArgs) promql.Sample {
		if len(f.Points) < 2 {
			return InvalidSample
		}
		val, ok := instantValue(f.Points, false)
		if !ok {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: val,
			},
		}
	},
	"vector": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: f.Points[0].V,
			},
		}
	},
	"scalar": func(f FunctionArgs) promql.Sample {
		// This is handled specially by operator.
		return promql.Sample{}
	},
	"rate": func(f FunctionArgs) promql.Sample {
		if len(f.Points) < 2 {
			return InvalidSample
		}
		v, h := extrapolatedRate(f.Points, true, true, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: v,
				H: h,
			},
		}
	},
	"delta": func(f FunctionArgs) promql.Sample {
		if len(f.Points) < 2 {
			return InvalidSample
		}
		v, h := extrapolatedRate(f.Points, false, false, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: v,
				H: h,
			},
		}
	},
	"increase": func(f FunctionArgs) promql.Sample {
		if len(f.Points) < 2 {
			return InvalidSample
		}
		v, h := extrapolatedRate(f.Points, true, false, f.StepTime, f.SelectRange, f.Offset)
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: v,
				H: h,
			},
		}
	},
	"clamp": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 || len(f.ScalarPoints) < 2 {
			return InvalidSample
		}

		v := f.Points[0].V
		min := f.ScalarPoints[0]
		max := f.ScalarPoints[1]

		if max < min {
			return InvalidSample
		}

		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: math.Max(min, math.Min(max, v)),
			},
		}
	},
	"clamp_min": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 || len(f.ScalarPoints) == 0 {
			return InvalidSample
		}

		v := f.Points[0].V
		min := f.ScalarPoints[0]

		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: math.Max(min, v),
			},
		}
	},
	"clamp_max": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 || len(f.ScalarPoints) == 0 {
			return InvalidSample
		}

		v := f.Points[0].V
		max := f.ScalarPoints[0]

		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: math.Min(max, v),
			},
		}
	},
	"histogram_sum": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 || f.Points[0].H == nil {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: f.Points[0].H.Sum,
			},
		}
	},
	"histogram_count": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 || f.Points[0].H == nil {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: f.Points[0].H.Count,
			},
		}
	},
	"histogram_fraction": func(f FunctionArgs) promql.Sample {
		if len(f.Points) == 0 || f.Points[0].H == nil {
			return InvalidSample
		}
		return promql.Sample{
			Metric: f.Labels,
			Point: promql.Point{
				T: f.StepTime,
				V: histogramFraction(f.ScalarPoints[0], f.ScalarPoints[1], f.Points[0].H),
			},
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
func extrapolatedRate(samples []promql.Point, isCounter, isRate bool, stepTime int64, selectRange int64, offset int64) (float64, *histogram.FloatHistogram) {
	var (
		rangeStart      = stepTime - (selectRange + offset)
		rangeEnd        = stepTime - offset
		resultValue     float64
		resultHistogram *histogram.FloatHistogram
	)

	if samples[0].H != nil {
		resultHistogram = histogramRate(samples, isCounter)
	} else {
		resultValue = samples[len(samples)-1].V - samples[0].V
		if isCounter {
			var lastValue float64
			for _, sample := range samples {
				if sample.V < lastValue {
					resultValue += lastValue
				}
				lastValue = sample.V
			}
		}
	}

	// Duration between first/last samples and boundary of range.
	durationToStart := float64(samples[0].T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-samples[len(samples)-1].T) / 1000

	sampledInterval := float64(samples[len(samples)-1].T-samples[0].T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(len(samples)-1)

	if isCounter && resultValue > 0 && samples[0].V >= 0 {
		// Counters cannot be negative. If we have any slope at
		// all (i.e. resultValue went up), we can extrapolate
		// the zero point of the counter. If the duration to the
		// zero point is shorter than the durationToStart, we
		// take the zero point as the start of the series,
		// thereby avoiding extrapolation to negative counter
		// values.
		durationToZero := sampledInterval * (samples[0].V / resultValue)
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

// histogramRate is a helper function for extrapolatedRate. It requires
// points[0] to be a histogram. It returns nil if any other Point in points is
// not a histogram.
func histogramRate(points []promql.Point, isCounter bool) *histogram.FloatHistogram {
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
	return h.Compact(0)
}

func instantValue(samples []promql.Point, isRate bool) (float64, bool) {
	lastSample := samples[len(samples)-1]
	previousSample := samples[len(samples)-2]

	var resultValue float64
	if isRate && lastSample.V < previousSample.V {
		// Counter reset.
		resultValue = lastSample.V
	} else {
		resultValue = lastSample.V - previousSample.V
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

func maxOverTime(points []promql.Point) float64 {
	max := points[0].V
	for _, v := range points {
		if v.V > max || math.IsNaN(max) {
			max = v.V
		}
	}
	return max
}

func minOverTime(points []promql.Point) float64 {
	min := points[0].V
	for _, v := range points {
		if v.V < min || math.IsNaN(min) {
			min = v.V
		}
	}
	return min
}

func countOverTime(points []promql.Point) float64 {
	return float64(len(points))
}

func avgOverTime(points []promql.Point) float64 {
	var mean, count, c float64
	for _, v := range points {
		count++
		if math.IsInf(mean, 0) {
			if math.IsInf(v.V, 0) && (mean > 0) == (v.V > 0) {
				// The `mean` and `v.V` values are `Inf` of the same sign.  They
				// can't be subtracted, but the value of `mean` is correct
				// already.
				continue
			}
			if !math.IsInf(v.V, 0) && !math.IsNaN(v.V) {
				// At this stage, the mean is an infinite. If the added
				// value is neither an Inf or a Nan, we can keep that mean
				// value.
				// This is required because our calculation below removes
				// the mean value, which would look like Inf += x - Inf and
				// end up as a NaN.
				continue
			}
		}
		mean, c = KahanSumInc(v.V/count-mean/count, mean, c)
	}

	if math.IsInf(mean, 0) {
		return mean
	}
	return mean + c
}

func sumOverTime(points []promql.Point) float64 {
	var sum, c float64
	for _, v := range points {
		sum, c = KahanSumInc(v.V, sum, c)
	}
	if math.IsInf(sum, 0) {
		return sum
	}
	return sum + c
}

func stddevOverTime(points []promql.Point) float64 {
	var count float64
	var mean, cMean float64
	var aux, cAux float64
	for _, v := range points {
		count++
		delta := v.V - (mean + cMean)
		mean, cMean = KahanSumInc(delta/count, mean, cMean)
		aux, cAux = KahanSumInc(delta*(v.V-(mean+cMean)), aux, cAux)
	}
	return math.Sqrt((aux + cAux) / count)
}

func stdvarOverTime(points []promql.Point) float64 {
	var count float64
	var mean, cMean float64
	var aux, cAux float64
	for _, v := range points {
		count++
		delta := v.V - (mean + cMean)
		mean, cMean = KahanSumInc(delta/count, mean, cMean)
		aux, cAux = KahanSumInc(delta*(v.V-(mean+cMean)), aux, cAux)
	}
	return (aux + cAux) / count
}

func changes(points []promql.Point) float64 {
	var count float64
	prev := points[0].V
	count = 0
	for _, sample := range points[1:] {
		current := sample.V
		if current != prev && !(math.IsNaN(current) && math.IsNaN(prev)) {
			count++
		}
		prev = current
	}
	return count
}

func deriv(points []promql.Point) float64 {
	// We pass in an arbitrary timestamp that is near the values in use
	// to avoid floating point accuracy issues, see
	// https://github.com/prometheus/prometheus/issues/2674
	slope, _ := linearRegression(points, points[0].T)
	return slope
}

func resets(points []promql.Point) float64 {
	count := 0
	prev := points[0].V
	for _, sample := range points[1:] {
		current := sample.V
		if current < prev {
			count++
		}
		prev = current
	}

	return float64(count)
}

func linearRegression(samples []promql.Point, interceptTime int64) (slope, intercept float64) {
	var (
		n          float64
		sumX, cX   float64
		sumY, cY   float64
		sumXY, cXY float64
		sumX2, cX2 float64
		initY      float64
		constY     bool
	)
	initY = samples[0].V
	constY = true
	for i, sample := range samples {
		// Set constY to false if any new y values are encountered.
		if constY && i > 0 && sample.V != initY {
			constY = false
		}
		n += 1.0
		x := float64(sample.T-interceptTime) / 1e3
		sumX, cX = KahanSumInc(x, sumX, cX)
		sumY, cY = KahanSumInc(sample.V, sumY, cY)
		sumXY, cXY = KahanSumInc(x*sample.V, sumXY, cXY)
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
	if len(fa.Points) == 0 {
		return promql.Sample{
			Metric: labels.Labels{},
			Point:  promql.Point{V: f(time.Unix(fa.StepTime/1000, 0).UTC())},
		}
	}
	t := time.Unix(int64(fa.Points[0].V), 0).UTC()
	lbls, _ := DropMetricName(fa.Labels)
	return promql.Sample{
		Metric: lbls,
		Point:  promql.Point{V: f(t)},
	}
}
