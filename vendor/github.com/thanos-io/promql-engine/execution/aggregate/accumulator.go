// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"context"
	"math"

	"github.com/efficientgo/core/errors"
	"gonum.org/v1/gonum/floats"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/promql-engine/execution/warnings"
)

type ValueType int

const (
	NoValue ValueType = iota
	SingleTypeValue
	MixedTypeValue
)

type accumulator interface {
	Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error
	Value() (float64, *histogram.FloatHistogram)
	ValueType() ValueType
	Reset(float64)
}

type vectorAccumulator interface {
	AddVector(ctx context.Context, vs []float64, hs []*histogram.FloatHistogram) error
	Value() (float64, *histogram.FloatHistogram)
	ValueType() ValueType
	Reset(float64)
}

type sumAcc struct {
	value       float64
	histSum     *histogram.FloatHistogram
	hasFloatVal bool
}

func newSumAcc() *sumAcc {
	return &sumAcc{}
}

func (s *sumAcc) AddVector(ctx context.Context, float64s []float64, histograms []*histogram.FloatHistogram) error {
	if len(float64s) > 0 {
		s.value += SumCompensated(float64s)
		s.hasFloatVal = true
	}

	var err error
	if len(histograms) > 0 {
		s.histSum, err = histogramSum(ctx, s.histSum, histograms)
	}
	return err
}

func (s *sumAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	if h == nil {
		s.hasFloatVal = true
		s.value += v
		return nil
	}
	if s.histSum == nil {
		s.histSum = h.Copy()
		return nil
	}
	// The histogram being added must have an equal or larger schema.
	// https://github.com/prometheus/prometheus/blob/57bcbf18880f7554ae34c5b341d52fc53f059a97/promql/engine.go#L2448-L2456
	var err error
	if h.Schema >= s.histSum.Schema {
		if s.histSum, err = s.histSum.Add(h); err != nil {
			if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
				warnings.AddToContext(histogram.ErrHistogramsIncompatibleSchema, ctx)
				return nil
			}
			if errors.Is(err, histogram.ErrHistogramsIncompatibleBounds) {
				warnings.AddToContext(histogram.ErrHistogramsIncompatibleBounds, ctx)
				return nil
			}
			return err
		}
	} else {
		t := h.Copy()
		if s.histSum, err = t.Add(s.histSum); err != nil {
			if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
				warnings.AddToContext(histogram.ErrHistogramsIncompatibleSchema, ctx)
				return nil
			}
			if errors.Is(err, histogram.ErrHistogramsIncompatibleBounds) {
				warnings.AddToContext(histogram.ErrHistogramsIncompatibleBounds, ctx)
				return nil
			}
			return err
		}
		s.histSum = t
	}
	return nil
}

func (s *sumAcc) Value() (float64, *histogram.FloatHistogram) {
	return s.value, s.histSum
}

func (s *sumAcc) ValueType() ValueType {
	if s.hasFloatVal && s.histSum != nil {
		return MixedTypeValue
	}
	if s.hasFloatVal || s.histSum != nil {
		return SingleTypeValue
	}
	return NoValue
}

func (s *sumAcc) Reset(_ float64) {
	s.histSum = nil
	s.hasFloatVal = false
	s.value = 0
}

func newMaxAcc() *maxAcc {
	return &maxAcc{}
}

type maxAcc struct {
	value    float64
	hasValue bool
}

func (c *maxAcc) AddVector(ctx context.Context, vs []float64, hs []*histogram.FloatHistogram) error {
	if len(hs) > 0 {
		warnings.AddToContext(annotations.NewHistogramIgnoredInAggregationInfo("max", posrange.PositionRange{}), ctx)
	}

	if len(vs) == 0 {
		return nil
	}

	fst, rem := vs[0], vs[1:]
	if err := c.Add(ctx, fst, nil); err != nil {
		return err
	}
	if len(rem) == 0 {
		return nil
	}
	return c.Add(ctx, floats.Max(rem), nil)
}

func (c *maxAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		return nil
	}

	if !c.hasValue {
		c.value = v
		c.hasValue = true
		return nil
	}
	if c.value < v || math.IsNaN(c.value) {
		c.value = v
	}
	return nil
}

func (c *maxAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *maxAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}

func (c *maxAcc) Reset(_ float64) {
	c.hasValue = false
	c.value = 0
}

func newMinAcc() *minAcc {
	return &minAcc{}
}

type minAcc struct {
	value    float64
	hasValue bool
}

func (c *minAcc) AddVector(ctx context.Context, vs []float64, hs []*histogram.FloatHistogram) error {
	if len(hs) > 0 {
		warnings.AddToContext(annotations.NewHistogramIgnoredInAggregationInfo("min", posrange.PositionRange{}), ctx)
	}

	if len(vs) == 0 {
		return nil
	}

	fst, rem := vs[0], vs[1:]
	if err := c.Add(ctx, fst, nil); err != nil {
		return err
	}
	if len(rem) == 0 {
		return nil
	}
	return c.Add(ctx, floats.Min(rem), nil)
}

func (c *minAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		return nil
	}

	if !c.hasValue {
		c.value = v
		c.hasValue = true
		return nil
	}
	if c.value > v || math.IsNaN(c.value) {
		c.value = v
	}
	return nil
}

func (c *minAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *minAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}

func (c *minAcc) Reset(_ float64) {
	c.hasValue = false
	c.value = 0
}

func newGroupAcc() *groupAcc {
	return &groupAcc{}
}

type groupAcc struct {
	value    float64
	hasValue bool
}

func (c *groupAcc) AddVector(ctx context.Context, vs []float64, hs []*histogram.FloatHistogram) error {
	if len(vs) == 0 && len(hs) == 0 {
		return nil
	}
	c.hasValue = true
	c.value = 1
	return nil
}

func (c *groupAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	c.hasValue = true
	c.value = 1
	return nil
}

func (c *groupAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *groupAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}

func (c *groupAcc) Reset(_ float64) {
	c.hasValue = false
	c.value = 0
}

type countAcc struct {
	value    float64
	hasValue bool
}

func newCountAcc() *countAcc {
	return &countAcc{}
}

func (c *countAcc) AddVector(ctx context.Context, vs []float64, hs []*histogram.FloatHistogram) error {

	if len(vs) > 0 || len(hs) > 0 {
		c.hasValue = true
		c.value += float64(len(vs)) + float64(len(hs))
	}
	return nil
}

func (c *countAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	c.hasValue = true
	c.value += 1
	return nil
}

func (c *countAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *countAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}
func (c *countAcc) Reset(_ float64) {
	c.hasValue = false
	c.value = 0
}

type avgAcc struct {
	kahanSum    float64
	kahanC      float64
	avg         float64
	incremental bool
	count       int64
	hasValue    bool

	histSum        *histogram.FloatHistogram
	histScratch    *histogram.FloatHistogram
	histSumScratch *histogram.FloatHistogram
	histCount      float64
}

func newAvgAcc() *avgAcc {
	return &avgAcc{}
}

func (a *avgAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		a.histCount++
		if a.histSum == nil {
			a.histSum = h.Copy()
			a.histScratch = &histogram.FloatHistogram{}
			a.histSumScratch = &histogram.FloatHistogram{}
			return nil
		}

		h.CopyTo(a.histScratch)
		left := a.histScratch.Div(a.histCount)
		a.histSum.CopyTo(a.histSumScratch)
		right := a.histSumScratch.Div(a.histCount)
		toAdd, err := left.Sub(right)
		if err != nil {
			return err
		}
		a.histSum, err = a.histSum.Add(toAdd)
		return err
	}

	a.count++
	if !a.hasValue {
		a.hasValue = true
		a.kahanSum = v
		return nil
	}

	a.hasValue = true

	if !a.incremental {
		newSum, newC := KahanSumInc(v, a.kahanSum, a.kahanC)

		if !math.IsInf(newSum, 0) {
			// The sum doesn't overflow, so we propagate it to the
			// group struct and continue with the regular
			// calculation of the mean value.
			a.kahanSum, a.kahanC = newSum, newC
			return nil
		}

		// If we are here, we know that the sum _would_ overflow. So
		// instead of continue to sum up, we revert to incremental
		// calculation of the mean value from here on.
		a.incremental = true
		a.avg = a.kahanSum / float64(a.count-1)
		a.kahanC /= float64(a.count) - 1
	}

	if math.IsInf(a.avg, 0) {
		if math.IsInf(v, 0) && (a.avg > 0) == (v > 0) {
			// The `floatMean` and `s.F` values are `Inf` of the same sign.  They
			// can't be subtracted, but the value of `floatMean` is correct
			// already.
			return nil
		}
		if !math.IsInf(v, 0) && !math.IsNaN(v) {
			// At this stage, the mean is an infinite. If the added
			// value is neither an Inf or a Nan, we can keep that mean
			// value.
			// This is required because our calculation below removes
			// the mean value, which would look like Inf += x - Inf and
			// end up as a NaN.
			return nil
		}
	}
	currentMean := a.avg + a.kahanC
	a.avg, a.kahanC = KahanSumInc(
		// Divide each side of the `-` by `group.groupCount` to avoid float64 overflows.
		v/float64(a.count)-currentMean/float64(a.count),
		a.avg,
		a.kahanC,
	)
	return nil
}

func (a *avgAcc) AddVector(ctx context.Context, vs []float64, hs []*histogram.FloatHistogram) error {
	for _, v := range vs {
		if err := a.Add(ctx, v, nil); err != nil {
			return err
		}
	}
	for _, h := range hs {
		if err := a.Add(ctx, 0, h); err != nil {
			if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
				// to make valueType NoValue
				a.histSum = nil
				a.histCount = 0
				warnings.AddToContext(annotations.NewMixedExponentialCustomHistogramsWarning("", posrange.PositionRange{}), ctx)
				return nil
			}
			if errors.Is(err, histogram.ErrHistogramsIncompatibleBounds) {
				// to make valueType NoValue
				a.histSum = nil
				a.histCount = 0
				warnings.AddToContext(annotations.NewIncompatibleCustomBucketsHistogramsWarning("", posrange.PositionRange{}), ctx)
				return nil
			}
			return err
		}
	}
	return nil
}

func (a *avgAcc) Value() (float64, *histogram.FloatHistogram) {
	if a.incremental {
		return a.avg + a.kahanC, a.histSum
	}
	return (a.kahanSum + a.kahanC) / float64(a.count), a.histSum
}

func (a *avgAcc) ValueType() ValueType {
	hasFloat := a.count > 0
	hasHist := a.histCount > 0

	if hasFloat && hasHist {
		return MixedTypeValue
	}
	if hasFloat || hasHist {
		return SingleTypeValue
	}
	return NoValue
}

func (a *avgAcc) Reset(_ float64) {
	a.hasValue = false
	a.count = 0

	a.histCount = 0
	a.histSum = nil
}

type statAcc struct {
	count    float64
	mean     float64
	value    float64
	hasValue bool
}

func (s *statAcc) ValueType() ValueType {
	if s.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}
func (s *statAcc) Reset(_ float64) {
	s.hasValue = false
	s.count = 0
	s.mean = 0
	s.value = 0
}

type stdDevAcc struct {
	statAcc
}

func newStdDevAcc() *stdDevAcc {
	return &stdDevAcc{}
}

func (s *stdDevAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		// ignore native histogram for STDDEV.
		warnings.AddToContext(annotations.NewHistogramIgnoredInAggregationInfo("stddev", posrange.PositionRange{}), ctx)
		return nil
	}

	s.hasValue = true
	s.count++

	if math.IsNaN(v) || math.IsInf(v, 0) {
		s.value = math.NaN()
	} else {
		delta := v - s.mean
		s.mean += delta / s.count
		s.value += delta * (v - s.mean)
	}
	return nil
}

func (s *stdDevAcc) Value() (float64, *histogram.FloatHistogram) {
	if math.IsNaN(s.value) {
		return math.NaN(), nil
	}

	if s.count == 1 {
		return 0, nil
	}
	return math.Sqrt(s.value / s.count), nil
}

type stdVarAcc struct {
	statAcc
}

func newStdVarAcc() *stdVarAcc {
	return &stdVarAcc{}
}

func (s *stdVarAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		// ignore native histogram for STDVAR.
		warnings.AddToContext(annotations.NewHistogramIgnoredInAggregationInfo("stdvar", posrange.PositionRange{}), ctx)
		return nil
	}

	s.hasValue = true
	s.count++

	if math.IsNaN(v) || math.IsInf(v, 0) {
		s.value = math.NaN()
	} else {
		delta := v - s.mean
		s.mean += delta / s.count
		s.value += delta * (v - s.mean)
	}
	return nil
}

func (s *stdVarAcc) Value() (float64, *histogram.FloatHistogram) {
	if math.IsNaN(s.value) {
		return math.NaN(), nil
	}

	if s.count == 1 {
		return 0, nil
	}
	return s.value / s.count, nil
}

type quantileAcc struct {
	arg      float64
	points   []float64
	hasValue bool
}

func newQuantileAcc() accumulator {
	return &quantileAcc{}
}

func (q *quantileAcc) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		warnings.AddToContext(annotations.NewHistogramIgnoredInAggregationInfo("quantile", posrange.PositionRange{}), ctx)
		return nil
	}

	q.hasValue = true
	q.points = append(q.points, v)
	return nil
}

func (q *quantileAcc) Value() (float64, *histogram.FloatHistogram) {
	return Quantile(q.arg, q.points), nil
}

func (q *quantileAcc) ValueType() ValueType {
	if q.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}

func (q *quantileAcc) Reset(f float64) {
	q.hasValue = false
	q.arg = f
	q.points = q.points[:0]
}

type histogramAvg struct {
	sum      *histogram.FloatHistogram
	count    int64
	hasFloat bool
}

func newHistogramAvg() *histogramAvg {
	return &histogramAvg{
		sum: &histogram.FloatHistogram{},
	}
}

func (acc *histogramAvg) Add(ctx context.Context, v float64, h *histogram.FloatHistogram) error {
	if h == nil {
		acc.hasFloat = true
	}
	if acc.count == 0 {
		h.CopyTo(acc.sum)
	}
	var err error
	if h.Schema >= acc.sum.Schema {
		if acc.sum, err = acc.sum.Add(h); err != nil {
			return err
		}
	} else {
		t := h.Copy()
		if _, err = t.Add(acc.sum); err != nil {
			return err
		}
		acc.sum = t
	}
	acc.count++
	return nil
}

func (acc *histogramAvg) Value() (float64, *histogram.FloatHistogram) {
	return 0, acc.sum.Mul(1 / float64(acc.count))
}

func (acc *histogramAvg) ValueType() ValueType {
	if acc.count > 0 && !acc.hasFloat {
		return SingleTypeValue
	}
	return NoValue
}

func (acc *histogramAvg) Reset(f float64) {
	acc.count = 0
}

// SumCompensated returns the sum of the elements of the slice calculated with greater
// accuracy than Sum at the expense of additional computation.
func SumCompensated(s []float64) float64 {
	// SumCompensated uses an improved version of Kahan's compensated
	// summation algorithm proposed by Neumaier.
	// See https://en.wikipedia.org/wiki/Kahan_summation_algorithm for details.
	var sum, c float64
	for _, x := range s {
		// This type conversion is here to prevent a sufficiently smart compiler
		// from optimizing away these operations.
		t := sum + x
		switch {
		case math.IsInf(t, 0):
			c = 0

		// Using Neumaier improvement, swap if next term larger than sum.
		case math.Abs(sum) >= math.Abs(x):
			c += (sum - t) + x
		default:
			c += (x - t) + sum
		}
		sum = t
	}
	return sum + c
}

// KahanSumInc implements kahan summation, see https://en.wikipedia.org/wiki/Kahan_summation_algorithm.
func KahanSumInc(inc, sum, c float64) (newSum, newC float64) {
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
