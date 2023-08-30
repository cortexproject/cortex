package logicalplan

import (
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/execution/model"
	engstore "github.com/thanos-io/promql-engine/execution/storage"
	"github.com/thanos-io/promql-engine/parser"
	"github.com/thanos-io/promql-engine/query"
)

// UserDefinedExpr is an extension point which allows users to define their execution operators.
type UserDefinedExpr interface {
	parser.Expr
	MakeExecutionOperator(
		vectors *model.VectorPool,
		selectors *engstore.SelectorPool,
		opts *query.Options,
		hints storage.SelectHints,
	) (model.VectorOperator, error)
}
