// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"sync"

	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-io/promql-engine/execution/model"
)

type ExplainableQuery interface {
	promql.Query

	Explain() *ExplainOutputNode
	Analyze() *AnalyzeOutputNode
}

type AnalyzeOutputNode struct {
	OperatorTelemetry model.OperatorTelemetry `json:"telemetry,omitempty"`
	Children          []*AnalyzeOutputNode    `json:"children,omitempty"`

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
			for i, s := range child.TotalSamplesPerStep() {
				a.totalSamplesPerStep[i] += s
			}
			// Aggregate only if the node is not a subquery to avoid double counting samples from children.
			if !a.OperatorTelemetry.SubQuery() {
				a.totalSamples += child.TotalSamples()
			}
		}
	})
}

func analyzeQuery(obsv model.ObservableVectorOperator) *AnalyzeOutputNode {
	children := obsv.Explain()
	var childTelemetry []*AnalyzeOutputNode
	for _, child := range children {
		if obsChild, ok := child.(model.ObservableVectorOperator); ok {
			childTelemetry = append(childTelemetry, analyzeQuery(obsChild))
		}
	}

	return &AnalyzeOutputNode{
		OperatorTelemetry: obsv,
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
