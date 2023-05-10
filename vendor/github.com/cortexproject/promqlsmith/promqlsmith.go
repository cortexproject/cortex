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

	enableOffset         bool
	enableAtModifier     bool
	enableVectorMatching bool

	seriesSet  []labels.Labels
	labelNames []string

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
		rnd:                  rnd,
		seriesSet:            filterEmptySeries(seriesSet),
		labelNames:           labelNamesFromLabelSet(seriesSet),
		supportedExprs:       options.enabledExprs,
		supportedAggrs:       options.enabledAggrs,
		supportedBinops:      options.enabledBinops,
		supportedFuncs:       options.enabledFuncs,
		enableOffset:         options.enableOffset,
		enableAtModifier:     options.enableAtModifier,
		enableVectorMatching: options.enableVectorMatching,
	}
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

// Walk will walk the ast tree using one of the randomly generated expr type.
func (s *PromQLSmith) Walk(valueTypes ...parser.ValueType) parser.Expr {
	supportedExprs := s.supportedExprs
	if len(valueTypes) > 0 {
		supportedExprs = exprsFromValueTypes(valueTypes)
	}
	e := supportedExprs[s.rnd.Intn(len(supportedExprs))]
	expr, _ := s.walkExpr(e, valueTypes...)
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

func labelNamesFromLabelSet(labelSet []labels.Labels) []string {
	s := make(map[string]struct{})
	for _, lbls := range labelSet {
		for _, lbl := range lbls {
			s[lbl.Name] = struct{}{}
		}
	}
	output := make([]string, 0, len(s))
	for name := range s {
		output = append(output, name)
	}
	return output
}
