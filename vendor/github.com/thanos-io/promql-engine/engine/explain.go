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

func analyzeVector(obsv model.ObservableVectorOperator) *AnalyzeOutputNode {
	telemetry, obsVectors := obsv.Analyze()

	var children []AnalyzeOutputNode
	for _, vector := range obsVectors {
		children = append(children, *analyzeVector(vector))
	}

	return &AnalyzeOutputNode{
		OperatorTelemetry: telemetry,
		Children:          children,
	}
}

func explainVector(v model.VectorOperator) *ExplainOutputNode {
	name, vectors := v.Explain()

	var children []ExplainOutputNode
	for _, vector := range vectors {
		children = append(children, *explainVector(vector))
	}

	return &ExplainOutputNode{
		OperatorName: name,
		Children:     children,
	}
}
