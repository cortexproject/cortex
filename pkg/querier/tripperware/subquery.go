package tripperware

import (
	"net/http"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"
)

var (
	ErrSubQueryStepTooSmall = "exceeded maximum resolution of %d points per timeseries in subquery. Try increasing the step size of your subquery"
)

const (
	MaxStep = 11000
)

// SubQueryStepSizeCheck ensures the query doesn't contain too small step size in subqueries.
func SubQueryStepSizeCheck(query string, defaultSubQueryInterval time.Duration, maxStep int64) error {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		// If query fails to parse, we don't throw step size error
		// but fail query later on querier.
		return nil
	}
	parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
		e, ok := node.(*parser.SubqueryExpr)
		if !ok {
			return nil
		}
		step := e.Step
		if e.Step == 0 {
			step = defaultSubQueryInterval
		}

		if e.Range/step > time.Duration(maxStep) {
			err = httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, maxStep)
		}
		return err
	})
	return err
}
