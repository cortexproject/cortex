package parser

import (
	"maps"

	"github.com/prometheus/prometheus/promql"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/execution/parse"
)

var (
	functions                   = buildFunctions(true)
	enableExperimentalFunctions bool
)

func Setup(enableExperimental, enableHoltWinters bool) {
	enableExperimentalFunctions = enableExperimental
	buildFunctions(enableHoltWinters)
}

// EnableExperimentalFunctions reports whether experimental PromQL functions are enabled.
func EnableExperimentalFunctions() bool {
	return enableExperimentalFunctions
}

// NewEngineParser returns a parser for the PromQL engines: the global
// promqlparser.Functions (with the holt_winters alias when enabled) plus the
// Thanos extended functions, which promql-engine no longer registers globally.
func NewEngineParser() promqlparser.Parser {
	fns := maps.Clone(promqlparser.Functions)
	maps.Copy(fns, parse.XFunctions)
	return promqlparser.NewParser(promqlparser.Options{
		EnableExperimentalFunctions: enableExperimentalFunctions,
		Functions:                   fns,
	})
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
	return promqlparser.NewParser(promqlparser.Options{
		EnableExperimentalFunctions: enableExperimentalFunctions,
		Functions:                   functions,
	}).ParseExpr(qs)
}
