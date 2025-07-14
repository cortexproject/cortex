package parser

import (
	"maps"

	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/execution/parse"
)

var functions = buildFunctions()

func buildFunctions() map[string]*promqlparser.Function {
	fns := make(map[string]*promqlparser.Function, len(promqlparser.Functions))
	maps.Copy(fns, promqlparser.Functions)
	maps.Copy(fns, parse.XFunctions)
	return fns
}

func ParseExpr(qs string) (promqlparser.Expr, error) {
	return promqlparser.NewParser(qs, promqlparser.WithFunctions(functions)).ParseExpr()
}
