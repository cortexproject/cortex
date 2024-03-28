// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package storage

import (
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

type Scanners interface {
	NewVectorSelector(opts *query.Options, hints storage.SelectHints, selector logicalplan.VectorSelector) (model.VectorOperator, error)
	NewMatrixSelector(opts *query.Options, hints storage.SelectHints, selector logicalplan.MatrixSelector, call parser.Call) (model.VectorOperator, error)
}
