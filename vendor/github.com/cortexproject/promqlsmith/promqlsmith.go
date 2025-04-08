package promqlsmith

import (
	"math/rand"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type ExprType int

const (
	VectorSelector ExprType = iota
	MatrixSelector
	AggregateExpr
	BinaryExpr
	SubQueryExpr
	CallExpr
	NumberLiteral
	UnaryExpr
)

// Add minimum depth requirements for each ExprType
var exprMinDepth = map[ExprType]int{
	VectorSelector: 1,
	MatrixSelector: 1, // Needs one child expression but we consider 1 depth
	AggregateExpr:  2, // Needs at least one child expression
	BinaryExpr:     2, // Needs two child expressions but whichever higher
	SubQueryExpr:   2, // Needs one child expression
	CallExpr:       2, // Needs at least one argument
	NumberLiteral:  1,
	UnaryExpr:      2, // Needs one child expression
}

var (
	valueTypeToExprsMap = map[parser.ValueType][]ExprType{
		parser.ValueTypeVector: {VectorSelector, BinaryExpr, AggregateExpr, CallExpr, UnaryExpr},
		parser.ValueTypeMatrix: {MatrixSelector, SubQueryExpr},
		parser.ValueTypeScalar: {NumberLiteral, BinaryExpr, CallExpr, UnaryExpr},
	}

	vectorAndScalarValueTypes = []parser.ValueType{parser.ValueTypeVector, parser.ValueTypeScalar}

	allValueTypes = []parser.ValueType{
		parser.ValueTypeVector,
		parser.ValueTypeScalar,
		parser.ValueTypeMatrix,
		parser.ValueTypeString,
	}
)

type PromQLSmith struct {
	rnd *rand.Rand

	enableOffset             bool
	enableAtModifier         bool
	enableVectorMatching     bool
	enableExperimentalPromQL bool
	atModifierMaxTimestamp   int64
	maxDepth                 int

	seriesSet       []labels.Labels
	labelNames      []string
	labelValues     map[string][]string
	enforceMatchers []*labels.Matcher

	supportedExprs  []ExprType
	supportedAggrs  []parser.ItemType
	supportedFuncs  []*parser.Function
	supportedBinops []parser.ItemType
}

// New creates a PromQLsmith instance.
func New(rnd *rand.Rand, seriesSet []labels.Labels, opts ...Option) *PromQLSmith {
	options := options{}
	for _, o := range opts {
		o.apply(&options)
	}
	options.applyDefaults()

	ps := &PromQLSmith{
		rnd:                      rnd,
		seriesSet:                filterEmptySeries(seriesSet),
		supportedExprs:           options.enabledExprs,
		supportedAggrs:           options.enabledAggrs,
		supportedBinops:          options.enabledBinops,
		supportedFuncs:           options.enabledFuncs,
		enableOffset:             options.enableOffset,
		enableAtModifier:         options.enableAtModifier,
		atModifierMaxTimestamp:   options.atModifierMaxTimestamp,
		enableVectorMatching:     options.enableVectorMatching,
		enableExperimentalPromQL: options.enableExperimentalPromQLFunctions,
		enforceMatchers:          options.enforceLabelMatchers,
		maxDepth:                 options.maxDepth,
	}
	ps.labelNames, ps.labelValues = labelNameAndValuesFromLabelSet(seriesSet)
	return ps
}

// WalkInstantQuery walks the ast and generate an expression that can be used in
// instant query. Instant query also supports string literal, but we skip it here.
func (s *PromQLSmith) WalkInstantQuery() parser.Expr {
	return s.Walk(parser.ValueTypeVector, parser.ValueTypeScalar, parser.ValueTypeMatrix)
}

// WalkRangeQuery walks the ast and generate an expression that can be used in range query.
func (s *PromQLSmith) WalkRangeQuery() parser.Expr {
	return s.Walk(vectorAndScalarValueTypes...)
}

// WalkSelectors generates random label matchers based on the input series labels.
func (s *PromQLSmith) WalkSelectors() []*labels.Matcher {
	return s.walkSelectors()
}

// intersectExprTypes returns the intersection of two ExprType slices
func intersectExprTypes(a, b []ExprType) []ExprType {
	result := make([]ExprType, 0)
	for _, expr := range a {
		for _, other := range b {
			if expr == other {
				result = append(result, expr)
				break
			}
		}
	}
	return result
}

// Walk will walk the ast tree using one of the randomly generated expr type.
func (s *PromQLSmith) Walk(valueTypes ...parser.ValueType) parser.Expr {
	return s.walk(s.maxDepth, valueTypes...)
}

// filterNumberLiteral removes NumberLiteral from validExprs unless it's the only option
func filterNumberLiteral(validExprs []ExprType) []ExprType {
	if len(validExprs) <= 1 {
		return validExprs
	}

	filtered := make([]ExprType, 0, len(validExprs))
	for _, expr := range validExprs {
		if expr != NumberLiteral {
			filtered = append(filtered, expr)
		}
	}
	return filtered
}

func (s *PromQLSmith) walk(depth int, valueTypes ...parser.ValueType) parser.Expr {
	supportedExprs := s.supportedExprs
	if len(valueTypes) > 0 {
		supportedExprs = intersectExprTypes(supportedExprs, exprsFromValueTypes(valueTypes))
	}

	// Filter expressions based on remaining depth
	validExprs := make([]ExprType, 0, len(supportedExprs))
	for _, expr := range supportedExprs {
		if minDepth := exprMinDepth[expr]; depth >= minDepth {
			validExprs = append(validExprs, expr)
		}
	}

	// Return nil if no valid expressions are available
	if len(validExprs) == 0 {
		return nil
	}

	validExprs = filterNumberLiteral(validExprs)
	e := validExprs[s.rnd.Intn(len(validExprs))]
	expr, _ := s.walkExpr(e, depth, valueTypes...)
	return expr
}

func filterEmptySeries(seriesSet []labels.Labels) []labels.Labels {
	output := make([]labels.Labels, 0, len(seriesSet))
	for _, lbls := range seriesSet {
		if lbls.IsEmpty() {
			continue
		}
		output = append(output, lbls)
	}
	return output
}

func labelNameAndValuesFromLabelSet(labelSet []labels.Labels) ([]string, map[string][]string) {
	labelValueSet := make(map[string]map[string]struct{})
	for _, lbls := range labelSet {
		lbls.Range(func(lbl labels.Label) {
			if _, ok := labelValueSet[lbl.Name]; !ok {
				labelValueSet[lbl.Name] = make(map[string]struct{})
			}
			labelValueSet[lbl.Name][lbl.Value] = struct{}{}
		})
	}
	labelNames := make([]string, 0, len(labelValueSet))
	labelValues := make(map[string][]string)
	for name, values := range labelValueSet {
		labelNames = append(labelNames, name)
		labelValues[name] = make([]string, 0, len(values))
		for val := range values {
			labelValues[name] = append(labelValues[name], val)
		}
	}
	return labelNames, labelValues
}
