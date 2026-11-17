// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/prometheus/prometheus/promql"
)

type ExplainableQuery interface {
	promql.Query

	Explain() *ExplainOutputNode
	Analyze() *AnalyzeOutputNode
}

type AnalyzeOutputNode struct {
	OperatorTelemetry telemetry.OperatorTelemetry `json:"telemetry,omitempty"`
	OperatorID        *uint64                     `json:"operatorId,omitempty"`
	Children          []*AnalyzeOutputNode        `json:"children,omitempty"`

	// subqueryInner is the child evaluating the subquery expression, when this
	// node is a subquery.
	subqueryInner *AnalyzeOutputNode

	once                sync.Once
	totalSamples        int64
	peakSamples         int64
	totalSamplesPerStep []int64
}

type ExplainOutputNode struct {
	OperatorName string              `json:"name,omitempty"`
	Children     []ExplainOutputNode `json:"children,omitempty"`
}

var _ ExplainableQuery = &compatibilityQuery{}

func (a *AnalyzeOutputNode) TotalSamples() int64 {
	a.aggregateSamples()
	return a.totalSamples
}

func (a *AnalyzeOutputNode) TotalSamplesPerStep() []int64 {
	a.aggregateSamples()
	return a.totalSamplesPerStep
}

func (a *AnalyzeOutputNode) PeakSamples() int64 {
	a.aggregateSamples()
	return a.peakSamples
}

func (a *AnalyzeOutputNode) aggregateSamples() {
	a.once.Do(func() {
		if nodeSamples := a.OperatorTelemetry.Samples(); nodeSamples != nil {
			a.totalSamples += nodeSamples.TotalSamples
			a.peakSamples += int64(nodeSamples.PeakSamples)
			a.totalSamplesPerStep = nodeSamples.TotalSamplesPerStep
		}

		for _, child := range a.Children {
			childPeak := child.PeakSamples()
			a.peakSamples = max(a.peakSamples, childPeak)

			switch a.OperatorTelemetry.LogicalNode().(type) {
			case *logicalplan.Subquery:
				if child == a.subqueryInner {
					a.mergeSubqueryInner(child)
					continue
				}
				// Function parameters run on the parent's steps.
				a.totalSamples += child.TotalSamples()
				for i, s := range child.TotalSamplesPerStep() {
					a.totalSamplesPerStep[i] += s
				}
			case *logicalplan.StepInvariantExpr:
				childSamples := child.TotalSamples()
				for i := range a.totalSamplesPerStep {
					a.totalSamples += childSamples
					a.totalSamplesPerStep[i] += childSamples
				}
			default:
				a.totalSamples += child.TotalSamples()
				for i, s := range child.TotalSamplesPerStep() {
					a.totalSamplesPerStep[i] += s
				}
			}
		}
	})
}

// mergeSubqueryInner adds the subquery's inner samples to the parent steps
// whose window consumes them, like Prometheus (#19165): each parent step counts
// every inner step in its window, so overlapping windows count a step more
// than once. The window is applied for every parent step, including instant
// queries, because the inner query may evaluate steps outside the window.
func (a *AnalyzeOutputNode) mergeSubqueryInner(inner *AnalyzeOutputNode) {
	info := a.OperatorTelemetry.Subquery()
	innerSamples := inner.OperatorTelemetry.Samples()
	if info == nil || innerSamples == nil {
		return
	}
	perStep := inner.TotalSamplesPerStep()
	if perStep == nil {
		// No per-step data to window: attribute everything to the first step.
		a.addAtStep(0, inner.TotalSamples())
		return
	}
	c := info.Consumer
	innerTs := func(k int) int64 { return innerSamples.StartTimestamp + int64(k)*innerSamples.Interval + c.Offset }
	if c.AtTimestamp != nil {
		var n int64
		for k, s := range perStep {
			if t := innerTs(k); t > *c.AtTimestamp-c.Range && t <= *c.AtTimestamp {
				n += s
			}
		}
		for step := range c.NumSteps {
			a.addAtStep(step, n)
		}
		return
	}
	// Parent windows only move forward, so slide over the inner steps.
	var sum int64
	lo, hi := 0, 0
	for step := range c.NumSteps {
		maxt := c.Start + int64(step)*c.Interval
		for hi < len(perStep) && innerTs(hi) <= maxt {
			sum += perStep[hi]
			hi++
		}
		for lo < hi && innerTs(lo) <= maxt-c.Range {
			sum -= perStep[lo]
			lo++
		}
		a.addAtStep(step, sum)
	}
}

func (a *AnalyzeOutputNode) addAtStep(step int, n int64) {
	a.totalSamples += n
	if step < len(a.totalSamplesPerStep) {
		a.totalSamplesPerStep[step] += n
	}
}

func analyzeQuery(op model.VectorOperator) *AnalyzeOutputNode {
	var operatorID *uint64
	if ider, ok := op.(model.OperatorIDer); ok {
		id := ider.OperatorID()
		operatorID = &id
	}
	obsv, ok := model.Unwrap(op).(telemetry.ObservableVectorOperator)
	if !ok {
		return nil
	}

	children := obsv.Explain()
	var (
		childTelemetry []*AnalyzeOutputNode
		subqueryInner  *AnalyzeOutputNode
	)
	info := obsv.Subquery()
	for _, child := range children {
		if node := analyzeQuery(child); node != nil {
			childTelemetry = append(childTelemetry, node)
			if info != nil && child == info.Inner {
				subqueryInner = node
			}
		}
	}

	return &AnalyzeOutputNode{
		OperatorTelemetry: obsv,
		OperatorID:        operatorID,
		Children:          childTelemetry,
		subqueryInner:     subqueryInner,
	}
}

func explainVector(v model.VectorOperator) *ExplainOutputNode {
	vectors := v.Explain()

	var children []ExplainOutputNode
	for _, vector := range vectors {
		children = append(children, *explainVector(vector))
	}

	return &ExplainOutputNode{
		OperatorName: v.String(),
		Children:     children,
	}
}
