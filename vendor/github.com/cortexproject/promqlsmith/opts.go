package promqlsmith

import (
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"
)

var (
	defaultSupportedExprs = []ExprType{
		VectorSelector,
		MatrixSelector,
		BinaryExpr,
		AggregateExpr,
		SubQueryExpr,
		CallExpr,
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

	defaultSupportedFuncs []*parser.Function
)

func init() {
	for _, f := range parser.Functions {
		// We skip variadic functions for now.
		if f.Variadic != 0 {
			continue
		}
		if slices.Contains(f.ArgTypes, parser.ValueTypeString) {
			continue
		}
		defaultSupportedFuncs = append(defaultSupportedFuncs, f)
	}
}

type options struct {
	enabledExprs  []ExprType
	enabledAggrs  []parser.ItemType
	enabledFuncs  []*parser.Function
	enabledBinops []parser.ItemType

	enableOffset           bool
	enableAtModifier       bool
	enableVectorMatching   bool
	atModifierMaxTimestamp int64
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

	if o.atModifierMaxTimestamp == 0 {
		o.atModifierMaxTimestamp = time.Now().UnixMilli()
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
