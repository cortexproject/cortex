// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"context"

	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/query"
)

// UserDefinedExpr is an extension point which allows users to define their execution operators.
type UserDefinedExpr interface {
	Node
	MakeExecutionOperator(
		ctx context.Context,
		vectors *model.VectorPool,
		opts *query.Options,
		hints storage.SelectHints,
	) (model.VectorOperator, error)
}
