// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package compute

import (
	"math"
	"sort"

	"github.com/thanos-io/promql-engine/warnings"

	"github.com/prometheus/prometheus/model/histogram"
	"gonum.org/v1/gonum/floats"
)

type ValueType int

const (
	NoValue ValueType = iota
	SingleTypeValue
	MixedTypeValue
)

// CounterResetState tracks which counter reset hints have been seen during aggregation.
// Used to detect collisions between CounterReset and NotCounterReset hints.
type CounterResetState uint8

const (
	SeenCounterReset    CounterResetState = 1 << iota // histogram with CounterReset hint was seen
	SeenNotCounterReset                               // histogram with NotCounterReset hint was seen
)

// HasCollision returns true if both CounterReset and NotCounterReset hints were seen.
func (s CounterResetState) HasCollision() bool {
	return s&SeenCounterReset != 0 && s&SeenNotCounterReset != 0
}

// Accumulators map prometheus behavior for aggregations, either operators or
// "[...]_over_time" functions. The caller is responsible to add all errors
// returned by Add as annotations.
// The Warnings method returns a bitset of warning conditions that occurred
// during accumulation (e.g., ignored histograms, mixed types).
type Accumulator interface {
	Add(v float64, h *histogram.FloatHistogram) error
	Value() (float64, *histogram.FloatHistogram)
	ValueType() ValueType
	Warnings() warnings.Warnings
	Reset(float64)
}

// VectorAccumulator is like Accumulator but accepts batches of values.
type VectorAccumulator interface {
	AddVector(vs []float64, hs []*histogram.FloatHistogram) error
	Value() (float64, *histogram.FloatHistogram)
	ValueType() ValueType
	Warnings() warnings.Warnings
	Reset(float64)
}

type SumAcc struct {
	value             float64
	compensation      float64
	histSum           *histogram.FloatHistogram
	hasFloatVal       bool
	hasError          bool // histogram error occurred; accumulator becomes no-op
	warn              warnings.Warnings
	counterResetState CounterResetState
}

func NewSumAcc() *SumAcc {
	return &SumAcc{}
}

func (s *SumAcc) AddVector(float64s []float64, histograms []*histogram.FloatHistogram) error {
	if s.hasError {
		return nil
	}
	if len(float64s) > 0 {
		s.value, s.compensation = KahanSumInc(compensatedSum(float64s), s.value, s.compensation)
		s.hasFloatVal = true
	}

	if len(histograms) > 0 {
		// Track counter reset hints for collision detection.
		for _, h := range histograms {
			switch h.CounterResetHint {
			case histogram.CounterReset:
				s.counterResetState |= SeenCounterReset
			case histogram.NotCounterReset:
				s.counterResetState |= SeenNotCounterReset
			}
		}

		var (
			err  error
			warn warnings.Warnings
		)
		s.histSum, warn, err = histogramSum(s.histSum, histograms)
		s.warn |= warn
		if err != nil {
			s.hasError = true
			return err
		}
	}
	return nil
}

func (s *SumAcc) Add(v float64, h *histogram.FloatHistogram) error {
	if s.hasError {
		return nil
	}
	if h == nil {
		s.hasFloatVal = true
		s.value, s.compensation = KahanSumInc(v, s.value, s.compensation)
		return nil
	}
	return s.addHistogram(h)
}

func (s *SumAcc) addHistogram(h *histogram.FloatHistogram) error {
	// Track counter reset hints for collision detection.
	switch h.CounterResetHint {
	case histogram.CounterReset:
		s.counterResetState |= SeenCounterReset
	case histogram.NotCounterReset:
		s.counterResetState |= SeenNotCounterReset
	}

	if s.histSum == nil {
		s.histSum = h.Copy()
		return nil
	}
	// The histogram being added must have an equal or larger schema.
	// https://github.com/prometheus/prometheus/blob/57bcbf18880f7554ae34c5b341d52fc53f059a97/promql/engine.go#L2448-L2456
	var (
		err                  error
		nhcbBoundsReconciled bool
	)
	if h.Schema >= s.histSum.Schema {
		s.histSum, _, nhcbBoundsReconciled, err = s.histSum.Add(h)
	} else {
		t := h.Copy()
		if s.histSum, _, nhcbBoundsReconciled, err = t.Add(s.histSum); err == nil {
			s.histSum = t
		}
	}
	if nhcbBoundsReconciled {
		s.warn |= warnings.WarnNHCBBoundsReconciledAgg
	}
	if err != nil {
		s.histSum = nil
		s.hasError = true
		return warnings.ConvertHistogramError(err)
	}
	return nil
}

func (s *SumAcc) Value() (float64, *histogram.FloatHistogram) {
	if s.histSum != nil {
		s.histSum.Compact(0)
	}
	return s.value + s.compensation, s.histSum
}

func (s *SumAcc) ValueType() ValueType {
	if s.hasFloatVal && s.histSum != nil {
		return MixedTypeValue
	}
	if s.hasFloatVal || s.histSum != nil {
		return SingleTypeValue
	}
	return NoValue
}

func (s *SumAcc) Warnings() warnings.Warnings {
	warn := s.warn
	if s.ValueType() == MixedTypeValue {
		warn |= warnings.WarnMixedFloatsHistograms
	}
	// Detect counter reset collision: if we've seen both CounterReset and NotCounterReset hints.
	if s.counterResetState.HasCollision() {
		warn |= warnings.WarnCounterResetCollision
	}
	return warn
}

func (s *SumAcc) Reset(_ float64) {
	s.histSum = nil
	s.hasFloatVal = false
	s.hasError = false
	s.warn = 0
	s.value = 0
	s.compensation = 0
	s.counterResetState = 0
}

func NewMaxAcc() *MaxAcc {
	return &MaxAcc{}
}

type MaxAcc struct {
	value    float64
	hasValue bool
	warn     warnings.Warnings
}

func (c *MaxAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if len(hs) > 0 {
		c.warn |= warnings.WarnHistogramIgnoredInAggregation
	}
	if len(vs) == 0 {
		return nil
	}

	fst, rem := vs[0], vs[1:]
	_ = c.Add(fst, nil)
	if len(rem) > 0 {
		_ = c.Add(floats.Max(rem), nil)
	}
	return nil
}

func (c *MaxAcc) Add(v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		c.warn |= warnings.WarnHistogramIgnoredInAggregation
		return nil
	}
	c.addFloat(v)
	return nil
}

func (c *MaxAcc) Warnings() warnings.Warnings {
	return c.warn
}

func (c *MaxAcc) addFloat(v float64) {
	if !c.hasValue {
		c.value = v
		c.hasValue = true
		return
	}
	if c.value < v || math.IsNaN(c.value) {
		c.value = v
	}
}

func (c *MaxAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *MaxAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}

func (c *MaxAcc) Reset(_ float64) {
	c.hasValue = false
	c.warn = 0
	c.value = 0
}

func NewMinAcc() *MinAcc {
	return &MinAcc{}
}

type MinAcc struct {
	value    float64
	hasValue bool
	warn     warnings.Warnings
}

func (c *MinAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if len(hs) > 0 {
		c.warn |= warnings.WarnHistogramIgnoredInAggregation
	}
	if len(vs) == 0 {
		return nil
	}

	fst, rem := vs[0], vs[1:]
	_ = c.Add(fst, nil)
	if len(rem) > 0 {
		_ = c.Add(floats.Min(rem), nil)
	}
	return nil
}

func (c *MinAcc) Add(v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		c.warn |= warnings.WarnHistogramIgnoredInAggregation
		return nil
	}
	c.addFloat(v)
	return nil
}

func (c *MinAcc) Warnings() warnings.Warnings {
	return c.warn
}

func (c *MinAcc) addFloat(v float64) {
	if !c.hasValue {
		c.value = v
		c.hasValue = true
		return
	}
	if c.value > v || math.IsNaN(c.value) {
		c.value = v
	}
}

func (c *MinAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *MinAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}

func (c *MinAcc) Reset(_ float64) {
	c.hasValue = false
	c.warn = 0
	c.value = 0
}

func NewGroupAcc() *GroupAcc {
	return &GroupAcc{}
}

type GroupAcc struct {
	value    float64
	hasValue bool
}

func (c *GroupAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if len(vs) == 0 && len(hs) == 0 {
		return nil
	}
	c.hasValue = true
	c.value = 1
	return nil
}

func (c *GroupAcc) Add(v float64, h *histogram.FloatHistogram) error {
	c.hasValue = true
	c.value = 1
	return nil
}

func (c *GroupAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *GroupAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}

func (c *GroupAcc) Warnings() warnings.Warnings {
	return 0
}

func (c *GroupAcc) Reset(_ float64) {
	c.hasValue = false
	c.value = 0
}

type CountAcc struct {
	value    float64
	hasValue bool
}

func NewCountAcc() *CountAcc {
	return &CountAcc{}
}

func (c *CountAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if len(vs) > 0 || len(hs) > 0 {
		c.hasValue = true
		c.value += float64(len(vs)) + float64(len(hs))
	}
	return nil
}

func (c *CountAcc) Add(v float64, h *histogram.FloatHistogram) error {
	c.hasValue = true
	c.value += 1
	return nil
}

func (c *CountAcc) Value() (float64, *histogram.FloatHistogram) {
	return c.value, nil
}

func (c *CountAcc) ValueType() ValueType {
	if c.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}
func (c *CountAcc) Warnings() warnings.Warnings {
	return 0
}

func (c *CountAcc) Reset(_ float64) {
	c.hasValue = false
	c.value = 0
}

type AvgAcc struct {
	kahanSum    float64
	kahanC      float64
	avg         float64
	incremental bool
	count       int64
	hasValue    bool
	hasError    bool // histogram error occurred; accumulator becomes no-op

	histSum           *histogram.FloatHistogram
	histScratch       *histogram.FloatHistogram
	histSumScratch    *histogram.FloatHistogram
	histCount         float64
	warn              warnings.Warnings
	counterResetState CounterResetState
}

func NewAvgAcc() *AvgAcc {
	return &AvgAcc{}
}

func (a *AvgAcc) Add(v float64, h *histogram.FloatHistogram) error {
	if a.hasError {
		return nil
	}
	if h == nil {
		return a.addFloat(v)
	}
	return a.addHistogram(h)
}

func (a *AvgAcc) addHistogram(h *histogram.FloatHistogram) error {
	// Track counter reset hints for collision detection.
	switch h.CounterResetHint {
	case histogram.CounterReset:
		a.counterResetState |= SeenCounterReset
	case histogram.NotCounterReset:
		a.counterResetState |= SeenNotCounterReset
	}

	a.histCount++
	if a.histSum == nil {
		a.histSum = h.Copy()
		a.histScratch = &histogram.FloatHistogram{}
		a.histSumScratch = &histogram.FloatHistogram{}
		return nil
	}

	var (
		err                  error
		nhcbBoundsReconciled bool
	)
	h.CopyTo(a.histScratch)
	left := a.histScratch.Div(a.histCount)
	a.histSum.CopyTo(a.histSumScratch)
	right := a.histSumScratch.Div(a.histCount)
	toAdd, _, nhcbBoundsReconciled, err := left.Sub(right)
	if nhcbBoundsReconciled {
		a.warn |= warnings.WarnNHCBBoundsReconciledAgg
	}
	if err == nil {
		var nbr bool
		a.histSum, _, nbr, err = a.histSum.Add(toAdd)
		if nbr {
			a.warn |= warnings.WarnNHCBBoundsReconciledAgg
		}
	}
	if err != nil {
		a.histSum = nil
		a.histCount = 0
		a.hasError = true
		return warnings.ConvertHistogramError(err)
	}
	return nil
}

func (a *AvgAcc) addFloat(v float64) error {
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

func (a *AvgAcc) AddVector(vs []float64, hs []*histogram.FloatHistogram) error {
	if a.hasError {
		return nil
	}
	for _, v := range vs {
		if err := a.Add(v, nil); err != nil {
			return err
		}
	}
	for _, h := range hs {
		if err := a.Add(0, h); err != nil {
			return err
		}
	}
	return nil
}

func (a *AvgAcc) Value() (float64, *histogram.FloatHistogram) {
	if a.histSum != nil {
		a.histSum.Compact(0)
	}
	if a.incremental {
		return a.avg + a.kahanC, a.histSum
	}
	return (a.kahanSum + a.kahanC) / float64(a.count), a.histSum
}

func (a *AvgAcc) ValueType() ValueType {
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

func (a *AvgAcc) Warnings() warnings.Warnings {
	warn := a.warn
	if a.ValueType() == MixedTypeValue {
		warn |= warnings.WarnMixedFloatsHistograms
	}
	// Detect counter reset collision: if we've seen both CounterReset and NotCounterReset hints.
	if a.counterResetState.HasCollision() {
		warn |= warnings.WarnCounterResetCollision
	}
	return warn
}

func (a *AvgAcc) Reset(_ float64) {
	a.hasValue = false
	a.hasError = false
	a.incremental = false
	a.kahanSum = 0
	a.kahanC = 0
	a.count = 0

	a.histCount = 0
	a.histSum = nil
	a.warn = 0
	a.counterResetState = 0
}

type statAcc struct {
	count    float64
	mean     float64
	cMean    float64
	value    float64
	cValue   float64
	hasValue bool
	hasNaN   bool
	warn     warnings.Warnings
}

func (s *statAcc) ValueType() ValueType {
	if s.hasValue {
		return SingleTypeValue
	}
	return NoValue
}

func (s *statAcc) Warnings() warnings.Warnings {
	return s.warn
}

func (s *statAcc) Reset(_ float64) {
	s.hasValue = false
	s.hasNaN = false
	s.warn = 0
	s.count = 0
	s.mean = 0
	s.cMean = 0
	s.value = 0
	s.cValue = 0
}

func (s *statAcc) add(v float64) {
	s.hasValue = true
	s.count++
	if math.IsNaN(v) || math.IsInf(v, 0) {
		s.hasNaN = true
		return
	}
	delta := v - (s.mean + s.cMean)
	s.mean, s.cMean = KahanSumInc(delta/s.count, s.mean, s.cMean)
	s.value, s.cValue = KahanSumInc(delta*(v-(s.mean+s.cMean)), s.value, s.cValue)
}

func (s *statAcc) variance() float64 {
	if s.hasNaN {
		return math.NaN()
	}
	return (s.value + s.cValue) / s.count
}

type StdDevAcc struct {
	statAcc
}

func NewStdDevAcc() *StdDevAcc {
	return &StdDevAcc{}
}

func (s *StdDevAcc) Add(v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		s.warn |= warnings.WarnHistogramIgnoredInAggregation
		return nil
	}
	s.add(v)
	return nil
}

func (s *StdDevAcc) Value() (float64, *histogram.FloatHistogram) {
	return math.Sqrt(s.variance()), nil
}

type StdVarAcc struct {
	statAcc
}

func NewStdVarAcc() *StdVarAcc {
	return &StdVarAcc{}
}

func (s *StdVarAcc) Add(v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		s.warn |= warnings.WarnHistogramIgnoredInAggregation
		return nil
	}
	s.add(v)
	return nil
}

func (s *StdVarAcc) Value() (float64, *histogram.FloatHistogram) {
	return s.variance(), nil
}

type QuantileAcc struct {
	arg      float64
	points   []float64
	hasValue bool
	warn     warnings.Warnings
}

func NewQuantileAcc() Accumulator {
	return &QuantileAcc{}
}

func (q *QuantileAcc) Add(v float64, h *histogram.FloatHistogram) error {
	if h != nil {
		q.warn |= warnings.WarnHistogramIgnoredInAggregation
		return nil
	}

	q.hasValue = true
	q.points = append(q.points, v)
	return nil
}

func (q *QuantileAcc) Value() (float64, *histogram.FloatHistogram) {
	return Quantile(q.arg, q.points), nil
}

func (q *QuantileAcc) ValueType() ValueType {
	if q.hasValue {
		return SingleTypeValue
	} else {
		return NoValue
	}
}

func (q *QuantileAcc) Warnings() warnings.Warnings {
	return q.warn
}

func (q *QuantileAcc) Reset(f float64) {
	q.hasValue = false
	q.warn = 0
	q.arg = f
	q.points = q.points[:0]
}

type HistogramAvgAcc struct {
	sum      *histogram.FloatHistogram
	count    int64
	hasFloat bool
}

func NewHistogramAvgAcc() *HistogramAvgAcc {
	return &HistogramAvgAcc{
		sum: &histogram.FloatHistogram{},
	}
}

func (acc *HistogramAvgAcc) Add(v float64, h *histogram.FloatHistogram) error {
	if h == nil {
		acc.hasFloat = true
	}
	if acc.count == 0 {
		h.CopyTo(acc.sum)
	}
	var err error
	if h.Schema >= acc.sum.Schema {
		if acc.sum, _, _, err = acc.sum.Add(h); err != nil {
			return err
		}
	} else {
		t := h.Copy()
		if _, _, _, err = t.Add(acc.sum); err != nil {
			return err
		}
		acc.sum = t
	}
	acc.count++
	return nil
}

func (acc *HistogramAvgAcc) Value() (float64, *histogram.FloatHistogram) {
	return 0, acc.sum.Mul(1 / float64(acc.count))
}

func (acc *HistogramAvgAcc) ValueType() ValueType {
	if acc.count > 0 && !acc.hasFloat {
		return SingleTypeValue
	}
	return NoValue
}

func (acc *HistogramAvgAcc) Warnings() warnings.Warnings {
	return 0
}

func (acc *HistogramAvgAcc) Reset(f float64) {
	acc.count = 0
}

// LastAcc tracks the last value seen. Used for last_over_time.
type LastAcc struct {
	value    float64
	hist     *histogram.FloatHistogram
	hasValue bool
}

func NewLastAcc() *LastAcc {
	return &LastAcc{}
}

func (l *LastAcc) Add(v float64, h *histogram.FloatHistogram) error {
	l.hasValue = true
	if h != nil {
		l.value = 0
		if l.hist == nil {
			l.hist = h.Copy()
		} else {
			h.CopyTo(l.hist)
		}
	} else {
		l.value = v
		l.hist = nil
	}
	return nil
}

func (l *LastAcc) Value() (float64, *histogram.FloatHistogram) {
	if l.hist != nil {
		return 0, l.hist.Copy()
	}
	return l.value, nil
}

func (l *LastAcc) ValueType() ValueType {
	if l.hasValue {
		return SingleTypeValue
	}
	return NoValue
}

func (l *LastAcc) Warnings() warnings.Warnings {
	return 0
}

func (l *LastAcc) Reset(_ float64) {
	l.hasValue = false
	l.value = 0
	l.hist = nil
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

func Quantile(q float64, points []float64) float64 {
	if len(points) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sort.Float64s(points)

	n := float64(len(points))
	// When the quantile lies between two samples,
	// we use a weighted average of the two samples.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	return points[int(lowerIndex)]*(1-weight) + points[int(upperIndex)]*weight
}

func histogramSum(current *histogram.FloatHistogram, histograms []*histogram.FloatHistogram) (*histogram.FloatHistogram, warnings.Warnings, error) {
	if len(histograms) == 0 {
		return current, 0, nil
	}
	if current == nil && len(histograms) == 1 {
		return histograms[0].Copy(), 0, nil
	}
	var histSum *histogram.FloatHistogram
	if current != nil {
		histSum = current.Copy()
	} else {
		histSum = histograms[0].Copy()
		histograms = histograms[1:]
	}

	var (
		err                  error
		warn                 warnings.Warnings
		nhcbBoundsReconciled bool
	)
	for i := range histograms {
		if histograms[i].Schema >= histSum.Schema {
			histSum, _, nhcbBoundsReconciled, err = histSum.Add(histograms[i])
		} else {
			t := histograms[i].Copy()
			histSum, _, nhcbBoundsReconciled, err = t.Add(histSum)
		}
		if nhcbBoundsReconciled {
			warn |= warnings.WarnNHCBBoundsReconciledAgg
		}
		if err != nil {
			return nil, warn, warnings.ConvertHistogramError(err)
		}
	}
	return histSum, warn, nil
}

// compensatedSum returns the sum of the elements of the slice calculated with greater
// accuracy than Sum at the expense of additional computation.
func compensatedSum(s []float64) float64 {
	// compensatedSum uses an improved version of Kahan's compensated
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
