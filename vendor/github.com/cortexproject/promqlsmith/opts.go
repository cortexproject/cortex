package promqlsmith

import (
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

var (
	defaultSupportedExprs = []ExprType{
		VectorSelector,
		MatrixSelector,
		BinaryExpr,
		AggregateExpr,
		SubQueryExpr,
		CallExpr,
		NumberLiteral,
		UnaryExpr,
	}

	defaultSupportedAggrs = []parser.ItemType{
		parser.SUM,
		parser.MIN,
		parser.MAX,
		parser.AVG,
		parser.COUNT,
		parser.GROUP,
		parser.STDDEV,
		parser.STDVAR,
		parser.TOPK,
		parser.BOTTOMK,
		parser.QUANTILE,
		parser.COUNT_VALUES,
	}

	experimentalPromQLAggrs = []parser.ItemType{
		parser.LIMITK,
		parser.LIMIT_RATIO,
	}

	defaultSupportedBinOps = []parser.ItemType{
		parser.SUB,
		parser.ADD,
		parser.MUL,
		parser.MOD,
		parser.DIV,
		parser.EQLC,
		parser.NEQ,
		parser.LTE,
		parser.GTE,
		parser.LSS,
		parser.GTR,
		parser.POW,
		parser.ATAN2,
		parser.LAND,
		parser.LOR,
		parser.LUNLESS,
	}

	defaultSupportedFuncs      []*parser.Function
	experimentalSupportedFuncs []*parser.Function
)

func init() {
	// Prometheus 3.x replaced holt_winters with double_exponential_smoothing.
	// Register holt_winters as a copy so we still support it for query generation.
	if parser.Functions["holt_winters"] == nil && parser.Functions["double_exponential_smoothing"] != nil {
		des := parser.Functions["double_exponential_smoothing"]
		parser.Functions["holt_winters"] = &parser.Function{
			Name:         "holt_winters",
			ArgTypes:     append([]parser.ValueType{}, des.ArgTypes...),
			ReturnType:   des.ReturnType,
			Experimental: false, // treat as stable like the old holt_winters
		}
	}

	for _, f := range parser.Functions {
		// holt_winters is excluded by default; use WithEnableHoltWinters(true) for older backends.
		if f.Name == "holt_winters" {
			continue
		}
		// Ignore experimental functions for now.
		if !f.Experimental {
			defaultSupportedFuncs = append(defaultSupportedFuncs, f)
		} else {
			experimentalSupportedFuncs = append(experimentalSupportedFuncs, f)
		}
	}
}

type options struct {
	enabledExprs  []ExprType
	enabledAggrs  []parser.ItemType
	enabledFuncs  []*parser.Function
	enabledBinops []parser.ItemType

	enableOffset                      bool
	enableAtModifier                  bool
	enableVectorMatching              bool
	enableHoltWinters                 bool // holt_winters not in Prometheus 3.x; enable for older backends
	enableExperimentalPromQLFunctions bool
	atModifierMaxTimestamp            int64

	enforceLabelMatchers []*labels.Matcher

	maxDepth int // Maximum depth of the query expression tree
}

func (o *options) applyDefaults() {
	if len(o.enabledExprs) == 0 {
		o.enabledExprs = defaultSupportedExprs
	}

	if len(o.enabledBinops) == 0 {
		o.enabledBinops = defaultSupportedBinOps
	}

	if len(o.enabledAggrs) == 0 {
		o.enabledAggrs = defaultSupportedAggrs
	}

	if len(o.enabledFuncs) == 0 {
		o.enabledFuncs = defaultSupportedFuncs
	}

	if o.enableHoltWinters && parser.Functions["holt_winters"] != nil {
		o.enabledFuncs = append(o.enabledFuncs, parser.Functions["holt_winters"])
	}

	if o.enableExperimentalPromQLFunctions {
		o.enabledAggrs = append(o.enabledAggrs, experimentalPromQLAggrs...)
		o.enabledFuncs = append(o.enabledFuncs, experimentalSupportedFuncs...)
	}

	if o.atModifierMaxTimestamp == 0 {
		o.atModifierMaxTimestamp = time.Now().UnixMilli()
	}

	if o.maxDepth == 0 {
		o.maxDepth = 5 // Default max depth
	}
}

// Option specifies options when generating queries.
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

func WithEnableOffset(enableOffset bool) Option {
	return optionFunc(func(o *options) {
		o.enableOffset = enableOffset
	})
}

func WithAtModifierMaxTimestamp(atModifierMaxTimestamp int64) Option {
	return optionFunc(func(o *options) {
		o.atModifierMaxTimestamp = atModifierMaxTimestamp
	})
}

func WithEnableAtModifier(enableAtModifier bool) Option {
	return optionFunc(func(o *options) {
		o.enableAtModifier = enableAtModifier
	})
}

func WithEnableVectorMatching(enableVectorMatching bool) Option {
	return optionFunc(func(o *options) {
		o.enableVectorMatching = enableVectorMatching
	})
}

// WithEnableHoltWinters enables generation of holt_winters() in queries.
// Disabled by default because Prometheus 3.x replaced it with double_exponential_smoothing;
// enable only when fuzzing older backends that still support holt_winters.
func WithEnableHoltWinters(enable bool) Option {
	return optionFunc(func(o *options) {
		o.enableHoltWinters = enable
	})
}

func WithEnableExperimentalPromQLFunctions(enableExperimentalPromQLFunctions bool) Option {
	return optionFunc(func(o *options) {
		o.enableExperimentalPromQLFunctions = enableExperimentalPromQLFunctions
	})
}

func WithEnabledBinOps(enabledBinops []parser.ItemType) Option {
	return optionFunc(func(o *options) {
		o.enabledBinops = enabledBinops
	})
}

func WithEnabledAggrs(enabledAggrs []parser.ItemType) Option {
	return optionFunc(func(o *options) {
		o.enabledAggrs = enabledAggrs
	})
}

func WithEnabledFunctions(enabledFunctions []*parser.Function) Option {
	return optionFunc(func(o *options) {
		o.enabledFuncs = enabledFunctions
	})
}

func WithEnabledExprs(enabledExprs []ExprType) Option {
	return optionFunc(func(o *options) {
		o.enabledExprs = enabledExprs
	})
}

func WithEnforceLabelMatchers(matchers []*labels.Matcher) Option {
	return optionFunc(func(o *options) {
		o.enforceLabelMatchers = matchers
	})
}

// WithMaxDepth sets the maximum depth for generated query expressions
func WithMaxDepth(depth int) Option {
	return optionFunc(func(o *options) {
		o.maxDepth = depth
	})
}
