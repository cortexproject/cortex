package parser

import (
	"maps"

	"github.com/prometheus/prometheus/promql"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/execution/parse"
)

var functions = buildFunctions(true)

func Setup(enableExperimentalFunctions, enableHoltWinters bool) {
	promqlparser.EnableExperimentalFunctions = enableExperimentalFunctions
	buildFunctions(enableHoltWinters)
}

func buildFunctions(enableHoltWinters bool) map[string]*promqlparser.Function {
	fns := make(map[string]*promqlparser.Function, len(promqlparser.Functions))
	maps.Copy(fns, promqlparser.Functions)
	maps.Copy(fns, parse.XFunctions)

	// The holt_winters function was renamed to double_exponential_smoothing and marked experimental in Prometheus v3.
	// Register holt_winters as an alias to maintain backward compatibility.
	if enableHoltWinters {
		if des, ok := fns["double_exponential_smoothing"]; ok {
			holtWinters := *des
			holtWinters.Experimental = false
			holtWinters.Name = "holt_winters"
			fns["holt_winters"] = &holtWinters

			// Also register in global Prometheus parser for engine execution
			promqlparser.Functions["holt_winters"] = &holtWinters
			promql.FunctionCalls["holt_winters"] = promql.FunctionCalls["double_exponential_smoothing"]
		}
	} else {
		delete(promqlparser.Functions, "holt_winters")
		delete(promql.FunctionCalls, "holt_winters")
	}

	return fns
}

func ParseExpr(qs string) (promqlparser.Expr, error) {
	return promqlparser.NewParser(qs, promqlparser.WithFunctions(functions)).ParseExpr()
}
