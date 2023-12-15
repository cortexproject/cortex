// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/thanos-io/promql-engine/execution/aggregate"
	"github.com/thanos-io/promql-engine/execution/parse"
)

type Sample struct {
	T int64
	F float64
	H *histogram.FloatHistogram
}

type FunctionArgs struct {
	Samples          []Sample
	StepTime         int64
	SelectRange      int64
	ScalarPoints     []float64
	Offset           int64
	MetricAppearedTs *int64
}

type FunctionCall func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool)

func instantValue(samples []Sample, isRate bool) (float64, bool) {
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

var rangeVectorFuncs = map[string]FunctionCall{
	"sum_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return sumOverTime(f.Samples), nil, true
	},
	"max_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return maxOverTime(f.Samples), nil, true
	},
	"min_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return minOverTime(f.Samples), nil, true
	},
	"avg_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return avgOverTime(f.Samples), nil, true
	},
	"stddev_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return stddevOverTime(f.Samples), nil, true
	},
	"stdvar_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return stdvarOverTime(f.Samples), nil, true
	},
	"count_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return countOverTime(f.Samples), nil, true
	},
	"last_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return f.Samples[len(f.Samples)-1].F, nil, true
	},
	"present_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return 1., nil, true
	},
	"quantile_over_time": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		floats := make([]float64, len(f.Samples))
		for i, v := range f.Samples {
			floats[i] = v.F
		}
		return aggregate.Quantile(f.ScalarPoints[0], floats), nil, true
	},
	"changes": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return changes(f.Samples), nil, true
	},
	"resets": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		return resets(f.Samples), nil, true
	},
	"deriv": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) < 2 {
			return 0., nil, false
		}
		return deriv(f.Samples), nil, true
	},
	"irate": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		f.Samples = filterFloatOnlySamples(f.Samples)
		if len(f.Samples) < 2 {
			return 0., nil, false
		}
		val, ok := instantValue(f.Samples, true)
		if !ok {
			return 0., nil, false
		}
		return val, nil, true
	},
	"idelta": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		f.Samples = filterFloatOnlySamples(f.Samples)
		if len(f.Samples) < 2 {
			return 0., nil, false
		}
		val, ok := instantValue(f.Samples, false)
		if !ok {
			return 0., nil, false
		}
		return val, nil, true
	},
	"rate": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) < 2 {
			return 0., nil, false
		}
		v, h := extrapolatedRate(f.Samples, true, true, f.StepTime, f.SelectRange, f.Offset)
		return v, h, true
	},
	"delta": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) < 2 {
			return 0., nil, false
		}
		v, h := extrapolatedRate(f.Samples, false, false, f.StepTime, f.SelectRange, f.Offset)
		return v, h, true
	},
	"increase": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) < 2 {
			return 0., nil, false
		}
		v, h := extrapolatedRate(f.Samples, true, false, f.StepTime, f.SelectRange, f.Offset)
		return v, h, true
	},
	"xrate": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		if f.MetricAppearedTs == nil {
			panic("BUG: we got some Samples but metric still hasn't appeared")
		}
		v, h := extendedRate(f.Samples, true, true, f.StepTime, f.SelectRange, f.Offset, *f.MetricAppearedTs)
		return v, h, true
	},
	"xdelta": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		if f.MetricAppearedTs == nil {
			panic("BUG: we got some Samples but metric still hasn't appeared")
		}
		v, h := extendedRate(f.Samples, false, false, f.StepTime, f.SelectRange, f.Offset, *f.MetricAppearedTs)
		return v, h, true
	},
	"xincrease": func(f FunctionArgs) (float64, *histogram.FloatHistogram, bool) {
		if len(f.Samples) == 0 {
			return 0., nil, false
		}
		if f.MetricAppearedTs == nil {
			panic("BUG: we got some Samples but metric still hasn't appeared")
		}
		v, h := extendedRate(f.Samples, true, false, f.StepTime, f.SelectRange, f.Offset, *f.MetricAppearedTs)
		return v, h, true
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
func extrapolatedRate(samples []Sample, isCounter, isRate bool, stepTime int64, selectRange int64, offset int64) (float64, *histogram.FloatHistogram) {
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

	// Duration between first/last Samples and boundary of range.
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

	// If the first/last Samples are close to the boundaries of the range,
	// extrapolate the result. This is as we expect that another sample
	// will exist given the spacing between Samples we've seen thus far,
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
		resultHistogram.Mul(factor)

	}

	return resultValue, resultHistogram
}

// extendedRate is a utility function for xrate/xincrease/xdelta.
// It calculates the rate (allowing for counter resets if isCounter is true),
// taking into account the last sample before the range start, and returns
// the result as either per-second (if isRate is true) or overall.
func extendedRate(samples []Sample, isCounter, isRate bool, stepTime int64, selectRange int64, offset int64, metricAppearedTs int64) (float64, *histogram.FloatHistogram) {
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
	// Only do it for some time when the metric appears the first time.
	until := selectRange + metricAppearedTs
	if isCounter && !isRate && sameVals {
		// Make sure we are not at the end of the range.
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
func histogramRate(points []Sample, isCounter bool) *histogram.FloatHistogram {
	// Calculating a rate on a single sample is not defined.
	if len(points) < 2 {
		return nil
	}

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

func maxOverTime(points []Sample) float64 {
	max := points[0].F
	for _, v := range points {
		if v.F > max || math.IsNaN(max) {
			max = v.F
		}
	}
	return max
}

func minOverTime(points []Sample) float64 {
	min := points[0].F
	for _, v := range points {
		if v.F < min || math.IsNaN(min) {
			min = v.F
		}
	}
	return min
}

func countOverTime(points []Sample) float64 {
	return float64(len(points))
}

func avgOverTime(points []Sample) float64 {
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
		mean, c = kahanSumInc(v.F/count-mean/count, mean, c)
	}

	if math.IsInf(mean, 0) {
		return mean
	}
	return mean + c
}

func sumOverTime(points []Sample) float64 {
	var sum, c float64
	for _, v := range points {
		sum, c = kahanSumInc(v.F, sum, c)
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
		delta := v.F - (mean + cMean)
		mean, cMean = kahanSumInc(delta/count, mean, cMean)
		aux, cAux = kahanSumInc(delta*(v.F-(mean+cMean)), aux, cAux)
	}
	return math.Sqrt((aux + cAux) / count)
}

func stdvarOverTime(points []Sample) float64 {
	var count float64
	var mean, cMean float64
	var aux, cAux float64
	for _, v := range points {
		count++
		delta := v.F - (mean + cMean)
		mean, cMean = kahanSumInc(delta/count, mean, cMean)
		aux, cAux = kahanSumInc(delta*(v.F-(mean+cMean)), aux, cAux)
	}
	return (aux + cAux) / count
}

func changes(points []Sample) float64 {
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

func deriv(points []Sample) float64 {
	// We pass in an arbitrary timestamp that is near the values in use
	// to avoid floating point accuracy issues, see
	// https://github.com/prometheus/prometheus/issues/2674
	slope, _ := linearRegression(points, points[0].T)
	return slope
}

func resets(points []Sample) float64 {
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
	initY = Samples[0].F
	constY = true
	for i, sample := range Samples {
		// Set constY to false if any new y values are encountered.
		if constY && i > 0 && sample.F != initY {
			constY = false
		}
		n += 1.0
		x := float64(sample.T-interceptTime) / 1e3
		sumX, cX = kahanSumInc(x, sumX, cX)
		sumY, cY = kahanSumInc(sample.F, sumY, cY)
		sumXY, cXY = kahanSumInc(x*sample.F, sumXY, cXY)
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
		if sample.H == nil {
			samples[i] = sample
			i++
		}
	}
	samples = samples[:i]
	return samples
}

func kahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	t := sum + inc
	// Using Neumaier improvement, swap if next term larger than sum.
	if math.Abs(sum) >= math.Abs(inc) {
		c += (sum - t) + inc
	} else {
		c += (inc - t) + sum
	}
	return t, c
}
