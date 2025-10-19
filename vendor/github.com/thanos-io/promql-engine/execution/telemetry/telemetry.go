// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/stats"
)

type OperatorTelemetry interface {
	fmt.Stringer

	MaxSeriesCount() int
	SetMaxSeriesCount(count int)
	ExecutionTimeTaken() time.Duration
	AddSeriesExecutionTime(time.Duration)
	SeriesExecutionTime() time.Duration
	AddNextExecutionTime(time.Duration)
	NextExecutionTime() time.Duration
	IncrementSamplesAtTimestamp(samples int, t int64)
	Samples() *stats.QuerySamples
	LogicalNode() logicalplan.Node
	UpdatePeak(count int)
}

func NewTelemetry(operator fmt.Stringer, opts *query.Options) OperatorTelemetry {
	if opts.EnableAnalysis {
		return NewTrackedTelemetry(operator, opts, nil)
	}
	return NewNoopTelemetry(operator)
}

func NewSubqueryTelemetry(operator fmt.Stringer, opts *query.Options) OperatorTelemetry {
	if opts.EnableAnalysis {
		return NewTrackedTelemetry(operator, opts, &logicalplan.Subquery{})
	}
	return NewNoopTelemetry(operator)
}

func NewStepInvariantTelemetry(operator fmt.Stringer, opts *query.Options) OperatorTelemetry {
	if opts.EnableAnalysis {
		return NewTrackedTelemetry(operator, opts, &logicalplan.StepInvariantExpr{})
	}
	return NewNoopTelemetry(operator)
}

type NoopTelemetry struct {
	fmt.Stringer
}

func NewNoopTelemetry(operator fmt.Stringer) *NoopTelemetry {
	return &NoopTelemetry{Stringer: operator}
}

func (tm *NoopTelemetry) AddExecutionTimeTaken(t time.Duration) {}

func (tm *NoopTelemetry) ExecutionTimeTaken() time.Duration {
	return time.Duration(0)
}

func (tm *NoopTelemetry) AddSeriesExecutionTime(t time.Duration) {}

func (tm *NoopTelemetry) SeriesExecutionTime() time.Duration {
	return time.Duration(0)
}

func (tm *NoopTelemetry) AddNextExecutionTime(t time.Duration) {}

func (tm *NoopTelemetry) NextExecutionTime() time.Duration {
	return time.Duration(0)
}

func (tm *NoopTelemetry) IncrementSamplesAtTimestamp(_ int, _ int64) {}

func (tm *NoopTelemetry) Samples() *stats.QuerySamples { return nil }

func (tm *NoopTelemetry) MaxSeriesCount() int { return 0 }

func (tm *NoopTelemetry) SetMaxSeriesCount(_ int) {}

func (tm *NoopTelemetry) LogicalNode() logicalplan.Node {
	return nil
}

func (tm *NoopTelemetry) UpdatePeak(_ int) {}

type TrackedTelemetry struct {
	fmt.Stringer

	Series        int
	ExecutionTime time.Duration
	SeriesTime    time.Duration
	NextTime      time.Duration
	LoadedSamples *stats.QuerySamples
	logicalNode   logicalplan.Node
}

func NewTrackedTelemetry(operator fmt.Stringer, opts *query.Options, logicalPlanNode logicalplan.Node) *TrackedTelemetry {
	ss := stats.NewQuerySamples(opts.EnablePerStepStats)
	ss.InitStepTracking(opts.Start.UnixMilli(), opts.End.UnixMilli(), StepTrackingInterval(opts.Step))
	return &TrackedTelemetry{
		Stringer:      operator,
		LoadedSamples: ss,
		logicalNode:   logicalPlanNode,
	}
}

func StepTrackingInterval(step time.Duration) int64 {
	if step == 0 {
		return 1
	}
	return int64(step / (time.Millisecond / time.Nanosecond))
}

func (ti *TrackedTelemetry) AddExecutionTimeTaken(t time.Duration) { ti.ExecutionTime += t }

func (ti *TrackedTelemetry) ExecutionTimeTaken() time.Duration {
	return ti.ExecutionTime
}

func (ti *TrackedTelemetry) AddSeriesExecutionTime(t time.Duration) {
	ti.SeriesTime += t
	ti.ExecutionTime += t
}

func (ti *TrackedTelemetry) SeriesExecutionTime() time.Duration {
	return ti.SeriesTime
}

func (ti *TrackedTelemetry) AddNextExecutionTime(t time.Duration) {
	ti.NextTime += t
	ti.ExecutionTime += t
}

func (ti *TrackedTelemetry) NextExecutionTime() time.Duration {
	return ti.NextTime
}

func (ti *TrackedTelemetry) IncrementSamplesAtTimestamp(samples int, t int64) {
	ti.LoadedSamples.IncrementSamplesAtTimestamp(t, int64(samples))
}

func (ti *TrackedTelemetry) LogicalNode() logicalplan.Node {
	return ti.logicalNode
}

func (ti *TrackedTelemetry) Samples() *stats.QuerySamples { return ti.LoadedSamples }

func (ti *TrackedTelemetry) MaxSeriesCount() int { return ti.Series }

func (ti *TrackedTelemetry) SetMaxSeriesCount(count int) { ti.Series = count }

func (ti *TrackedTelemetry) UpdatePeak(count int) {
	ti.Samples().UpdatePeak(count)
}

type ObservableVectorOperator interface {
	model.VectorOperator
	OperatorTelemetry
}

// CalculateHistogramSampleCount returns the size of the FloatHistogram compared to the size of a Float.
// The total size is calculated considering the histogram timestamp (p.T - 8 bytes),
// and then a number of bytes in the histogram.
// This sum is divided by 16, as samples are 16 bytes.
// See: https://github.com/prometheus/prometheus/blob/2bf6f4c9dcbb1ad2e8fef70c6a48d8fc44a7f57c/promql/value.go#L178
func CalculateHistogramSampleCount(h *histogram.FloatHistogram) int {
	return (h.Size() + 8) / 16
}

func NewOperator(telemetry OperatorTelemetry, inner model.VectorOperator) model.VectorOperator {
	op := &Operator{
		inner: inner,
	}
	op.OperatorTelemetry = telemetry
	return op
}

// Operator wraps other inner operator to track its telemetry.
type Operator struct {
	OperatorTelemetry
	inner model.VectorOperator
}

func (t *Operator) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { t.OperatorTelemetry.AddSeriesExecutionTime(time.Since(start)) }()
	s, err := t.inner.Series(ctx)
	if err != nil {
		return nil, err
	}
	t.OperatorTelemetry.SetMaxSeriesCount(len(s))
	return s, err
}

func (t *Operator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	var totalSamplesBeforeCount int64
	totalSamplesBefore := t.OperatorTelemetry.Samples()
	if totalSamplesBefore != nil {
		totalSamplesBeforeCount = totalSamplesBefore.TotalSamples
	} else {
		totalSamplesBeforeCount = 0
	}

	defer func() { t.OperatorTelemetry.AddNextExecutionTime(time.Since(start)) }()
	out, err := t.inner.Next(ctx)
	if err != nil {
		return nil, err
	}

	var totalSamplesAfter int64
	totalSamplesAfterSamples := t.OperatorTelemetry.Samples()
	if totalSamplesAfterSamples != nil {
		totalSamplesAfter = totalSamplesAfterSamples.TotalSamples
	} else {
		totalSamplesAfter = 0
	}

	t.OperatorTelemetry.UpdatePeak(int(totalSamplesAfter) - int(totalSamplesBeforeCount))

	return out, err
}

func (t *Operator) GetPool() *model.VectorPool {
	return t.inner.GetPool()
}

func (t *Operator) Explain() []model.VectorOperator {
	return t.inner.Explain()
}

func (t *Operator) String() string {
	return t.inner.String()
}
