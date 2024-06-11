// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
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
	Children          []AnalyzeOutputNode     `json:"children,omitempty"`
}

type ExplainOutputNode struct {
	OperatorName string              `json:"name,omitempty"`
	Children     []ExplainOutputNode `json:"children,omitempty"`
}

var _ ExplainableQuery = &compatibilityQuery{}

func (a *AnalyzeOutputNode) TotalSamples() int64 {
	var total int64
	for _, child := range a.Children {
		total += child.TotalSamples()
	}
	if a.OperatorTelemetry.Samples() != nil {
		total += a.OperatorTelemetry.Samples().TotalSamples
	}
	return total
}

func (a *AnalyzeOutputNode) PeakSamples() int64 {
	var peak int64
	if a.OperatorTelemetry.Samples() != nil {
		peak = int64(a.OperatorTelemetry.Samples().PeakSamples)
	}
	for _, child := range a.Children {
		childPeak := child.PeakSamples()
		if childPeak > peak {
			peak = childPeak
		}
	}
	return peak
}

func analyzeQuery(obsv model.ObservableVectorOperator) *AnalyzeOutputNode {
	children := obsv.Explain()
	var childTelemetry []AnalyzeOutputNode
	for _, child := range children {
		if obsChild, ok := child.(model.ObservableVectorOperator); ok {
			childTelemetry = append(childTelemetry, *analyzeQuery(obsChild))
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
