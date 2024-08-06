// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"gonum.org/v1/gonum/floats"
)

type ValueType int

const (
	NoValue ValueType = iota
	SingleTypeValue
	MixedTypeValue
)

type accumulator interface {
	Add(v float64, h *histogram.FloatHistogram) error
	Value() (float64, *histogram.FloatHistogram)
	ValueType() ValueType
	Reset(float64)
}

type vectorAccumulator interface {
	AddVector(vs []float64, hs []*histogram.FloatHistogram) error
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

func (s *sumAcc) AddVector(float64s []float64, histograms []*histogram.FloatHistogram) error {
	if len(float64s) > 0 {
		s.value += SumCompensated(float64s)
		s.hasFloatVal = true
	}

	var err error
	if len(histograms) > 0 {
		s.histSum, err = histogramSum(s.histSum, histograms)
	}
	return err
}

func (s *sumAcc) Add(v float64, h *histogram.FloatHistogram) error {
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
			return err
		}
	} else {
		t := h.Copy()
		if _, err = t.Add(s.histSum); err != nil {
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

func (c *maxAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if len(vs) == 0 {
		return nil
	}
	fst, rem := vs[0], vs[1:]
	if err := c.Add(fst, nil); err != nil {
		return err
	}
	if len(rem) == 0 {
		return nil
	}
	return c.Add(floats.Max(rem), nil)
}

func (c *maxAcc) Add(v float64, h *histogram.FloatHistogram) error {
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

func (c *minAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if len(vs) == 0 {
		return nil
	}
	fst, rem := vs[0], vs[1:]
	if err := c.Add(fst, nil); err != nil {
		return err
	}
	if len(rem) == 0 {
		return nil
	}
	return c.Add(floats.Min(rem), nil)
}

func (c *minAcc) Add(v float64, h *histogram.FloatHistogram) error {
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

func (c *groupAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if len(vs) == 0 && len(hs) == 0 {
		return nil
	}
	c.hasValue = true
	c.value = 1
	return nil
}

func (c *groupAcc) Add(v float64, h *histogram.FloatHistogram) error {
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

func (c *countAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if len(vs) > 0 || len(hs) > 0 {
		c.hasValue = true
		c.value += float64(len(vs)) + float64(len(hs))
	}
	return nil
}

func (c *countAcc) Add(v float64, h *histogram.FloatHistogram) error {
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
	avg      float64
	count    int64
	hasValue bool
}

func newAvgAcc() *avgAcc {
	return &avgAcc{}
}

func (a *avgAcc) Add(v float64, _ *histogram.FloatHistogram) error {
	a.count++
	if !a.hasValue {
		a.hasValue = true
		a.avg = v
		return nil
	}

	a.hasValue = true
	if math.IsInf(a.avg, 0) {
		if math.IsInf(v, 0) && (a.avg > 0) == (v > 0) {
			// The `avg` and `v` values are `Inf` of the same sign.  They
			// can't be subtracted, but the value of `avg` is correct
			// already.
			return nil
		}
		if !math.IsInf(v, 0) && !math.IsNaN(v) {
			// At this stage, the avg is an infinite. If the added
			// value is neither an Inf or a Nan, we can keep that avg
			// value.
			// This is required because our calculation below removes
			// the avg value, which would look like Inf += x - Inf and
			// end up as a NaN.
			return nil
		}
	}

	a.avg += v/float64(a.count) - a.avg/float64(a.count)
	return nil
}

func (a *avgAcc) AddVector(vs []float64, _ []*histogram.FloatHistogram) error {
	for _, v := range vs {
		if err := a.Add(v, nil); err != nil {
			return err
		}
	}
	return nil
}

func (a *avgAcc) Value() (float64, *histogram.FloatHistogram) {
	return a.avg, nil
}

func (c *avgAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}
func (a *avgAcc) Reset(_ float64) {
	a.hasValue = false
	a.count = 0
}

type statAcc struct {
	count    float64
	mean     float64
	value    float64
	hasValue bool
}

func (s *statAcc) Add(v float64, h *histogram.FloatHistogram) error {
	s.hasValue = true
	s.count++

	delta := v - s.mean
	s.mean += delta / s.count
	s.value += delta * (v - s.mean)
	return nil
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

func newStdDevAcc() accumulator {
	return &stdDevAcc{}
}

func (s *stdDevAcc) Value() (float64, *histogram.FloatHistogram) {
	if s.count == 1 {
		return 0, nil
	}
	return math.Sqrt(s.value / s.count), nil
}

type stdVarAcc struct {
	statAcc
}

func newStdVarAcc() accumulator {
	return &stdVarAcc{}
}

func (s *stdVarAcc) Value() (float64, *histogram.FloatHistogram) {
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

func (q *quantileAcc) Add(v float64, h *histogram.FloatHistogram) error {
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
