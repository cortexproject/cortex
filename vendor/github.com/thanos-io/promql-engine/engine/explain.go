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
				// Skip aggregating samples for subquery
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
	var childTelemetry []*AnalyzeOutputNode
	for _, child := range children {
		if node := analyzeQuery(child); node != nil {
			childTelemetry = append(childTelemetry, node)
		}
	}

	return &AnalyzeOutputNode{
		OperatorTelemetry: obsv,
		OperatorID:        operatorID,
		Children:          childTelemetry,
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
