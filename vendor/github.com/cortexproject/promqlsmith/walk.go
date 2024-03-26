package promqlsmith

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/exp/slices"
)

const (
	// max number of grouping labels in either by or without clause.
	maxGroupingLabels = 5
)

// walkExpr generates the given expression type with one of the required value type.
// valueTypes is only used for expressions that could have multiple possible return value types.
func (s *PromQLSmith) walkExpr(e ExprType, valueTypes ...parser.ValueType) (parser.Expr, error) {
	switch e {
	case AggregateExpr:
		return s.walkAggregateExpr(), nil
	case BinaryExpr:
		// Wrap binary expression with paren for readability.
		return wrapParenExpr(s.walkBinaryExpr(valueTypes...)), nil
	case SubQueryExpr:
		return s.walkSubQueryExpr(), nil
	case MatrixSelector:
		return s.walkMatrixSelector(), nil
	case VectorSelector:
		return s.walkVectorSelector(), nil
	case CallExpr:
		return s.walkCall(valueTypes...), nil
	case NumberLiteral:
		return s.walkNumberLiteral(), nil
	case UnaryExpr:
		return s.walkUnaryExpr(valueTypes...), nil
	default:
		return nil, fmt.Errorf("unsupported ExprType %d", e)
	}
}

func (s *PromQLSmith) walkAggregateExpr() parser.Expr {
	expr := &parser.AggregateExpr{
		Op:       s.supportedAggrs[s.rnd.Intn(len(s.supportedAggrs))],
		Without:  s.rnd.Int()%2 == 0,
		Expr:     s.Walk(parser.ValueTypeVector),
		Grouping: s.walkGrouping(),
	}
	if expr.Op.IsAggregatorWithParam() {
		expr.Param = s.walkAggregateParam(expr.Op)
	}
	return expr
}

// walkGrouping randomly generates grouping labels by picking from series label names.
// TODO(yeya24): can we reduce the label sets by picking from labels of selected series?
func (s *PromQLSmith) walkGrouping() []string {
	if len(s.labelNames) == 0 {
		return nil
	}
	orders := s.rnd.Perm(len(s.labelNames))
	items := s.rnd.Intn(min(len(s.labelNames), maxGroupingLabels))
	grouping := make([]string, items)
	for i := 0; i < items; i++ {
		grouping[i] = s.labelNames[orders[i]]
	}
	return grouping
}

func (s *PromQLSmith) walkAggregateParam(op parser.ItemType) parser.Expr {
	switch op {
	case parser.TOPK, parser.BOTTOMK:
		return s.Walk(parser.ValueTypeScalar)
	case parser.QUANTILE:
		return s.Walk(parser.ValueTypeScalar)
	case parser.COUNT_VALUES:
		return &parser.StringLiteral{Val: "value"}
	}
	return nil
}

// Can only do binary expression between vector and scalar. So any expression
// that returns matrix doesn't work like matrix selector, subquery
// or function that returns matrix.
func (s *PromQLSmith) walkBinaryExpr(valueTypes ...parser.ValueType) parser.Expr {
	valueTypes = keepValueTypes(valueTypes, vectorAndScalarValueTypes)
	expr := &parser.BinaryExpr{
		Op: s.walkBinaryOp(!slices.Contains(valueTypes, parser.ValueTypeVector)),
		VectorMatching: &parser.VectorMatching{
			Card: parser.CardOneToOne,
		},
	}
	// If it is a set operator then only vectors are allowed.
	if expr.Op.IsSetOperator() {
		valueTypes = []parser.ValueType{parser.ValueTypeVector}
		expr.VectorMatching.Card = parser.CardManyToMany
	}
	expr.LHS = wrapParenExpr(s.Walk(valueTypes...))
	expr.RHS = wrapParenExpr(s.Walk(valueTypes...))
	lvt := expr.LHS.Type()
	rvt := expr.RHS.Type()
	// ReturnBool can only be set for comparison operator. It is
	// required to set to true if both expressions are scalar type.
	if expr.Op.IsComparisonOperator() {
		if lvt == parser.ValueTypeScalar && rvt == parser.ValueTypeScalar || s.rnd.Intn(2) == 0 {
			expr.ReturnBool = true
		}
	}

	if !expr.Op.IsSetOperator() && s.enableVectorMatching && lvt == parser.ValueTypeVector &&
		rvt == parser.ValueTypeVector && s.rnd.Intn(2) == 0 {
		leftSeriesSet, stop := getOutputSeries(expr.LHS)
		if stop {
			return expr
		}
		rightSeriesSet, stop := getOutputSeries(expr.RHS)
		if stop {
			return expr
		}
		s.walkVectorMatching(expr, leftSeriesSet, rightSeriesSet, s.rnd.Intn(4) == 0)
	}
	return expr
}

func (s *PromQLSmith) walkVectorMatching(expr *parser.BinaryExpr, seriesSetA []labels.Labels, seriesSetB []labels.Labels, includeLabels bool) {
	sa := make(map[string]struct{})
	for _, series := range seriesSetA {
		series.Range(func(lbl labels.Label) {
			if lbl.Name == labels.MetricName {
				return
			}
			sa[lbl.Name] = struct{}{}
		})
	}

	sb := make(map[string]struct{})
	for _, series := range seriesSetB {
		series.Range(func(lbl labels.Label) {
			if lbl.Name == labels.MetricName {
				return
			}
			sb[lbl.Name] = struct{}{}
		})
	}
	expr.VectorMatching.On = true
	matchedLabels := make([]string, 0)
	for key := range sb {
		if _, ok := sa[key]; ok {
			matchedLabels = append(matchedLabels, key)
		}
	}
	// We are doing a very naive approach of guessing side cardinalities
	// by checking number of series each side.
	oneSideLabelsSet := sa
	if len(seriesSetA) > len(seriesSetB) {
		expr.VectorMatching.MatchingLabels = matchedLabels
		expr.VectorMatching.Card = parser.CardManyToOne
		oneSideLabelsSet = sb
	} else if len(seriesSetA) < len(seriesSetB) {
		expr.VectorMatching.MatchingLabels = matchedLabels
		expr.VectorMatching.Card = parser.CardOneToMany
	}
	// Otherwise we do 1:1 match.

	// For simplicity, we always include all labels on the one side.
	if expr.VectorMatching.Card != parser.CardOneToOne && includeLabels {
		includeLabels := getIncludeLabels(oneSideLabelsSet, matchedLabels)
		expr.VectorMatching.Include = includeLabels
	}
}

func getIncludeLabels(labelNameSet map[string]struct{}, matchedLabels []string) []string {
	output := make([]string, 0)
OUTER:
	for lbl := range labelNameSet {
		for _, matchedLabel := range matchedLabels {
			if lbl == matchedLabel {
				continue OUTER
			}
		}
		output = append(output, lbl)
	}
	sort.Strings(output)
	return output
}

// Walk binary op based on whether vector value type is allowed or not.
// Since Set operator only works with vector so if vector is disallowed
// we will choose comparison operator that works both for scalar and vector.
func (s *PromQLSmith) walkBinaryOp(disallowVector bool) parser.ItemType {
	binops := s.supportedBinops
	if disallowVector {
		binops = make([]parser.ItemType, 0)
		for _, binop := range s.supportedBinops {
			// Set operator can only be used with vector operator.
			if binop.IsSetOperator() {
				continue
			}
			binops = append(binops, binop)
		}
	}
	return binops[s.rnd.Intn(len(binops))]
}

func (s *PromQLSmith) walkSubQueryExpr() parser.Expr {
	expr := &parser.SubqueryExpr{
		Range: time.Hour,
		Step:  time.Minute,
		Expr:  s.walkVectorSelector(),
	}
	if s.enableOffset && s.rnd.Int()%2 == 0 {
		negativeOffset := s.rnd.Intn(2) == 0
		expr.OriginalOffset = time.Duration(s.rnd.Intn(300)) * time.Second
		if negativeOffset {
			expr.OriginalOffset = -expr.OriginalOffset
		}
	}
	if s.enableAtModifier && s.rnd.Float64() > 0.7 {
		expr.Timestamp, expr.StartOrEnd = s.walkAtModifier()
	}
	return expr
}

func (s *PromQLSmith) walkCall(valueTypes ...parser.ValueType) parser.Expr {
	expr := &parser.Call{}

	funcs := s.supportedFuncs
	if len(valueTypes) > 0 {
		funcs = make([]*parser.Function, 0)
		valueTypeSet := make(map[parser.ValueType]struct{})
		for _, vt := range valueTypes {
			valueTypeSet[vt] = struct{}{}
		}
		for _, f := range s.supportedFuncs {
			if _, ok := valueTypeSet[f.ReturnType]; ok {
				funcs = append(funcs, f)
			}
		}
	}
	sort.Slice(funcs, func(i, j int) bool { return strings.Compare(funcs[i].Name, funcs[j].Name) < 0 })
	expr.Func = funcs[s.rnd.Intn(len(funcs))]
	s.walkFuncArgs(expr)
	return expr
}

func (s *PromQLSmith) walkFuncArgs(expr *parser.Call) {
	expr.Args = make([]parser.Expr, len(expr.Func.ArgTypes))
	if expr.Func.Name == "holt_winters" {
		s.walkHoltWinters(expr)
		return
	}
	for i, arg := range expr.Func.ArgTypes {
		expr.Args[i] = s.Walk(arg)
	}
}

func (s *PromQLSmith) walkHoltWinters(expr *parser.Call) {
	expr.Args[0] = s.Walk(expr.Func.ArgTypes[0])
	expr.Args[1] = &parser.NumberLiteral{Val: getNonZeroFloat64(s.rnd)}
	expr.Args[2] = &parser.NumberLiteral{Val: getNonZeroFloat64(s.rnd)}
}

func (s *PromQLSmith) walkVectorSelector() parser.Expr {
	expr := &parser.VectorSelector{}
	expr.LabelMatchers = s.walkLabelMatchers()
	s.populateSeries(expr)
	if s.enableOffset && s.rnd.Int()%2 == 0 {
		negativeOffset := s.rnd.Intn(2) == 0
		expr.OriginalOffset = time.Duration(s.rnd.Intn(300)) * time.Second
		if negativeOffset {
			expr.OriginalOffset = -expr.OriginalOffset
		}
	}
	if s.enableAtModifier && s.rnd.Float64() > 0.7 {
		expr.Timestamp, expr.StartOrEnd = s.walkAtModifier()
	}

	return expr
}

func (s *PromQLSmith) populateSeries(expr *parser.VectorSelector) {
	expr.Series = make([]storage.Series, 0)
OUTER:
	for _, series := range s.seriesSet {
		for _, matcher := range expr.LabelMatchers {
			m := matcher
			if !m.Matches(series.Get(m.Name)) {
				continue OUTER
			}
		}
		expr.Series = append(expr.Series, &storage.SeriesEntry{Lset: series})
	}
}

func (s *PromQLSmith) walkLabelMatchers() []*labels.Matcher {
	if len(s.seriesSet) == 0 {
		return nil
	}
	series := s.seriesSet[s.rnd.Intn(len(s.seriesSet))]
	orders := s.rnd.Perm(series.Len())
	items := s.rnd.Intn((series.Len() + 1) / 2)
	matchers := make([]*labels.Matcher, 0, items)
	containsName := false
	lbls := make([]labels.Label, 0, series.Len())
	series.Range(func(l labels.Label) {
		lbls = append(lbls, l)
	})

	for i := 0; i < items; i++ {

		if lbls[orders[i]].Name == labels.MetricName {
			containsName = true
		}
		matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, lbls[orders[i]].Name, lbls[orders[i]].Value))
	}

	if !containsName {
		// Metric name is always included in the matcher to avoid
		// too high cardinality and potential grouping errors.
		// Ignore if metric name label doesn't exist.
		metricName := series.Get(labels.MetricName)
		if metricName != "" {
			matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName))
		}
	}
	matchers = append(matchers, s.enforceMatchers...)

	return matchers
}

// walkSelectors is similar to walkLabelMatchers, but used for generating various
// types of matchers more than simple equal matcher.
func (s *PromQLSmith) walkSelectors() []*labels.Matcher {
	if len(s.seriesSet) == 0 {
		return nil
	}
	orders := s.rnd.Perm(len(s.labelNames))
	items := randRange((len(s.labelNames)+1)/2, len(s.labelNames))
	matchers := make([]*labels.Matcher, 0, items)

	var (
		value  string
		repeat bool
	)
	for i := 0; i < items; {
		res := s.rnd.Intn(4)
		name := s.labelNames[orders[i]]
		matchType := labels.MatchType(res)
		switch matchType {
		case labels.MatchEqual:
			val := s.rnd.Float64()
			if val > 0.95 {
				value = ""
			} else if val > 0.9 {
				value = "not_exist_value"
			} else {
				idx := s.rnd.Intn(len(s.labelValues[name]))
				value = s.labelValues[name][idx]
			}
		case labels.MatchNotEqual:
			switch s.rnd.Intn(3) {
			case 0:
				value = ""
			case 1:
				value = "not_exist_value"
			default:
				idx := s.rnd.Intn(len(s.labelValues[name]))
				value = s.labelValues[name][idx]
			}
		case labels.MatchRegexp:
			val := s.rnd.Float64()
			if val > 0.95 {
				value = ""
			} else if val > 0.9 {
				value = "not_exist_value"
			} else if val > 0.8 {
				value = ".*"
			} else if val > 0.7 {
				value = ".+"
			} else if val > 0.5 {
				// Prefix
				idx := s.rnd.Intn(len(s.labelValues[name]))
				value = s.labelValues[name][idx][:len(s.labelValues[name][idx])/2] + ".*"
			} else {
				valueOrders := s.rnd.Perm(len(s.labelValues[name]))
				valueItems := s.rnd.Intn(len(s.labelValues[name]))
				var sb strings.Builder
				for j := 0; j < valueItems; j++ {
					sb.WriteString(s.labelValues[name][valueOrders[j]])
					if j < valueItems-1 {
						sb.WriteString("|")
					}
				}
				// Randomly attach a non-existent value.
				if s.rnd.Intn(2) == 1 {
					sb.WriteString("|not_exist_value")
				}
			}
		case labels.MatchNotRegexp:
			val := s.rnd.Float64()
			if val > 0.8 {
				value = ""
			} else if val > 0.6 {
				value = "not_exist_value"
			} else if val > 0.4 {
				// Prefix
				idx := s.rnd.Intn(len(s.labelValues[name]))
				value = s.labelValues[name][idx][:len(s.labelValues[name][idx])/2] + ".*"
			} else {
				valueOrders := s.rnd.Perm(len(s.labelValues[name]))
				valueItems := s.rnd.Intn(len(s.labelValues[name]))
				var sb strings.Builder
				for j := 0; j < valueItems; j++ {
					sb.WriteString(s.labelValues[name][valueOrders[j]])
					if j < valueItems-1 {
						sb.WriteString("|")
					}
				}
				// Randomly attach a non-existent value.
				if s.rnd.Intn(2) == 1 {
					sb.WriteString("|not_exist_value")
				}
			}
		default:
			panic("unsupported label matcher type")
		}
		matchers = append(matchers, labels.MustNewMatcher(matchType, name, value))

		if !repeat && s.rnd.Intn(3) == 0 {
			repeat = true
		} else {
			i++
		}
	}
	matchers = append(matchers, s.enforceMatchers...)

	return matchers
}

func (s *PromQLSmith) walkAtModifier() (ts *int64, op parser.ItemType) {
	res := s.rnd.Intn(3)
	switch res {
	case 0:
		op = parser.START
	case 1:
		op = parser.END
	case 2:
		t := s.rnd.Int63n(s.atModifierMaxTimestamp)
		ts = &t
	}
	return
}

func (s *PromQLSmith) walkMatrixSelector() parser.Expr {
	return &parser.MatrixSelector{
		// Make sure the time range is > 0s.
		Range:          time.Duration(s.rnd.Intn(5)+1) * time.Minute,
		VectorSelector: s.walkVectorSelector(),
	}
}

// Only vector and scalar result is allowed.
func (s *PromQLSmith) walkUnaryExpr(valueTypes ...parser.ValueType) parser.Expr {
	expr := &parser.UnaryExpr{
		Op: parser.SUB,
	}
	valueTypes = keepValueTypes(valueTypes, vectorAndScalarValueTypes)
	expr.Expr = s.Walk(valueTypes...)
	return expr
}

func (s *PromQLSmith) walkNumberLiteral() parser.Expr {
	return &parser.NumberLiteral{Val: s.rnd.Float64()}
}

func exprsFromValueTypes(valueTypes []parser.ValueType) []ExprType {
	set := make(map[ExprType]struct{})
	res := make([]ExprType, 0)
	for _, vt := range valueTypes {
		exprs, ok := valueTypeToExprsMap[vt]
		if !ok {
			continue
		}
		for _, expr := range exprs {
			set[expr] = struct{}{}
		}
	}
	for expr := range set {
		res = append(res, expr)
	}
	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	return res
}

// wrapParenExpr makes binary expr in a paren expr for better readability.
func wrapParenExpr(expr parser.Expr) parser.Expr {
	if _, ok := expr.(*parser.BinaryExpr); ok {
		return &parser.ParenExpr{Expr: expr}
	}
	return expr
}

// keepValueTypes picks value types that we should keep from the input.
// input shouldn't contain duplicate value types.
// If no input value types are provided, use value types to keep as result.
func keepValueTypes(input []parser.ValueType, keep []parser.ValueType) []parser.ValueType {
	if len(input) == 0 {
		return keep
	}
	out := make([]parser.ValueType, 0, len(keep))
	s := make(map[parser.ValueType]struct{})
	for _, vt := range keep {
		s[vt] = struct{}{}
	}
	for _, vt := range input {
		if _, ok := s[vt]; ok {
			out = append(out, vt)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// generate a non-zero float64 value randomly.
func getNonZeroFloat64(rnd *rand.Rand) float64 {
	for {
		res := rnd.Float64()
		if res == 0 {
			continue
		}
		return res
	}
}

// Get output series for the expr using best-effort guess. This can be used in fuzzing
// vector matching. A bool value will also be returned alongside with the output series.
// This is used to determine whether the expression is suitable to do vector matching or not.
func getOutputSeries(expr parser.Expr) ([]labels.Labels, bool) {
	stop := false
	var lbls []labels.Labels
	switch node := (expr).(type) {
	case *parser.VectorSelector:
		lbls := make([]labels.Labels, len(node.Series))
		for i, s := range node.Series {
			lbls[i] = s.Labels()
		}
		return lbls, len(lbls) == 0
	case *parser.StepInvariantExpr:
		return getOutputSeries(node.Expr)
	case *parser.MatrixSelector:
		return getOutputSeries(node.VectorSelector)
	case *parser.ParenExpr:
		return getOutputSeries(node.Expr)
	case *parser.UnaryExpr:
		return getOutputSeries(node.Expr)
	case *parser.NumberLiteral:
		return nil, false
	case *parser.StringLiteral:
		return nil, false
	case *parser.AggregateExpr:
		lbls, stop = getOutputSeries(node.Expr)
		if stop {
			return nil, true
		}

		m := make(map[uint64]labels.Labels)
		b := make([]byte, 1024)
		output := make([]labels.Labels, 0)
		lb := labels.NewBuilder(labels.EmptyLabels())
		if !node.Without {
			for _, lbl := range lbls {
				for _, groupLabel := range node.Grouping {
					if val := lbl.Get(groupLabel); val == "" {
						continue
					} else {
						lb.Set(groupLabel, val)
					}
				}
				newLbl := lb.Labels()
				h, _ := newLbl.HashForLabels(b, node.Grouping...)
				if _, ok := m[h]; !ok {
					m[h] = newLbl
				}
			}
		} else {
			set := make(map[string]struct{})
			for _, g := range node.Grouping {
				set[g] = struct{}{}
			}
			for _, lbl := range lbls {
				lbl.Range(func(l labels.Label) {
					if l.Name == labels.MetricName {
						return
					}
					if _, ok := set[l.Name]; !ok {
						val := lbl.Get(l.Name)
						if val == "" {
							return
						}

						lb.Set(l.Name, val)
					}
				})

				newLbl := lb.Labels()
				h, _ := newLbl.HashWithoutLabels(b, node.Grouping...)
				if _, ok := m[h]; !ok {
					m[h] = newLbl
				}
			}
		}

		for _, v := range m {
			output = append(output, v)
		}
		sort.Slice(output, func(i, j int) bool {
			return labels.Compare(output[i], output[j]) < 0
		})
		return output, false
	case *parser.SubqueryExpr:
		return getOutputSeries(node.Expr)
	case *parser.BinaryExpr:
		// Stop introducing complexity if there is a binary expr already.
		return nil, true
	case *parser.Call:
		// For function, we ignore `absent` and `absent_over_time`. And we continue
		// traversal by checking the first matrix or vector argument.
		if node.Func.Name == "absent" || node.Func.Name == "absent_over_time" {
			return nil, true
		}
		for i, arg := range node.Func.ArgTypes {
			// Find first matrix or vector type parameter, and we only
			// check series from it.
			if arg == parser.ValueTypeMatrix || arg == parser.ValueTypeVector {
				return getOutputSeries(node.Args[i])
			}
		}
		return nil, false
	}
	return lbls, stop
}

func randRange(min, max int) int {
	return rand.Intn(max-min) + min
}
