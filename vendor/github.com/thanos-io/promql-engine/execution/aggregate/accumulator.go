// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package aggregate

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"gonum.org/v1/gonum/floats"
)

type accumulator interface {
	Add(v float64, h *histogram.FloatHistogram)
	Value() (float64, *histogram.FloatHistogram)
	HasValue() bool
	Reset(float64)
}

type vectorAccumulator interface {
	AddVector(vs []float64, hs []*histogram.FloatHistogram)
	Value() (float64, *histogram.FloatHistogram)
	HasValue() bool
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

func (s *sumAcc) AddVector(float64s []float64, histograms []*histogram.FloatHistogram) {
	if len(float64s) > 0 {
		s.value += floats.Sum(float64s)
		s.hasFloatVal = true
	}

	if len(histograms) > 0 {
		s.histSum = histogramSum(s.histSum, histograms)
	}
}

func (s *sumAcc) Add(v float64, h *histogram.FloatHistogram) {
	if h == nil {
		s.hasFloatVal = true
		s.value += v
		return
	}
	if s.histSum == nil {
		s.histSum = h.Copy()
		return
	}
	// The histogram being added must have an equal or larger schema.
	// https://github.com/prometheus/prometheus/blob/57bcbf18880f7554ae34c5b341d52fc53f059a97/promql/engine.go#L2448-L2456
	if h.Schema >= s.histSum.Schema {
		s.histSum = s.histSum.Add(h)
	} else {
		t := h.Copy()
		t.Add(s.histSum)
		s.histSum = t
	}
}

func (s *sumAcc) Value() (float64, *histogram.FloatHistogram) {
	return s.value, s.histSum
}

// HasValue for sum returns an empty result when floats are histograms are aggregated.
func (s *sumAcc) HasValue() bool {
	return s.hasFloatVal != (s.histSum != nil)
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

func (c *maxAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) {
	if len(vs) == 0 {
		return
	}
	fst, rem := vs[0], vs[1:]
	c.Add(fst, nil)
	if len(rem) == 0 {
		return
	}
	c.Add(floats.Max(rem), nil)
}

func (c *maxAcc) Add(v float64, h *histogram.FloatHistogram) {
	if !c.hasValue {
		c.value = v
		c.hasValue = true
		return
	}
	if c.value < v || math.IsNaN(c.value) {
		c.value = v
	}
}

func (c *maxAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *maxAcc) HasValue() bool {
	return c.hasValue
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

func (c *minAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) {
	if len(vs) == 0 {
		return
	}
	fst, rem := vs[0], vs[1:]
	c.Add(fst, nil)
	if len(rem) == 0 {
		return
	}
	c.Add(floats.Min(rem), nil)
}

func (c *minAcc) Add(v float64, h *histogram.FloatHistogram) {
	if !c.hasValue {
		c.value = v
		c.hasValue = true
		return
	}
	if c.value > v || math.IsNaN(c.value) {
		c.value = v
	}
}

func (c *minAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *minAcc) HasValue() bool {
	return c.hasValue
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

func (c *groupAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) {
	if len(vs) == 0 && len(hs) == 0 {
		return
	}
	c.hasValue = true
	c.value = 1
}

func (c *groupAcc) Add(v float64, h *histogram.FloatHistogram) {
	c.hasValue = true
	c.value = 1
}

func (c *groupAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *groupAcc) HasValue() bool {
	return c.hasValue
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

func (c *countAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) {
	if len(vs) > 0 || len(hs) > 0 {
		c.hasValue = true
		c.value += float64(len(vs)) + float64(len(hs))
	}
}

func (c *countAcc) Add(v float64, h *histogram.FloatHistogram) {
	c.hasValue = true
	c.value += 1
}

func (c *countAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *countAcc) HasValue() bool {
	return c.hasValue
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

func (a *avgAcc) Add(v float64, _ *histogram.FloatHistogram) {
	a.count++
	if !a.hasValue {
		a.hasValue = true
		a.avg = v
		return
	}

	a.hasValue = true
	if math.IsInf(a.avg, 0) {
		if math.IsInf(v, 0) && (a.avg > 0) == (v > 0) {
			// The `avg` and `v` values are `Inf` of the same sign.  They
			// can't be subtracted, but the value of `avg` is correct
			// already.
			return
		}
		if !math.IsInf(v, 0) && !math.IsNaN(v) {
			// At this stage, the avg is an infinite. If the added
			// value is neither an Inf or a Nan, we can keep that avg
			// value.
			// This is required because our calculation below removes
			// the avg value, which would look like Inf += x - Inf and
			// end up as a NaN.
			return
		}
	}

	a.avg += v/float64(a.count) - a.avg/float64(a.count)
}

func (a *avgAcc) AddVector(vs []float64, _ []*histogram.FloatHistogram) {
	for _, v := range vs {
		a.Add(v, nil)
	}
}

func (a *avgAcc) Value() (float64, *histogram.FloatHistogram) {
	return a.avg, nil
}

func (a *avgAcc) HasValue() bool {
	return a.hasValue
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

func (s *statAcc) Add(v float64, h *histogram.FloatHistogram) {
	s.hasValue = true
	s.count++

	delta := v - s.mean
	s.mean += delta / s.count
	s.value += delta * (v - s.mean)
}

func (s *statAcc) HasValue() bool {
	return s.hasValue
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

func (q *quantileAcc) Add(v float64, h *histogram.FloatHistogram) {
	q.hasValue = true
	q.points = append(q.points, v)
}

func (q *quantileAcc) Value() (float64, *histogram.FloatHistogram) {
	return Quantile(q.arg, q.points), nil
}

func (q *quantileAcc) HasValue() bool {
	return q.hasValue
}

func (q *quantileAcc) Reset(f float64) {
	q.hasValue = false
	q.arg = f
	q.points = q.points[:0]
}
