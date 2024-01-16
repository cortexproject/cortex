// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/query"
)

var (
	RewrittenExternalLabelWarning = errors.Newf("%s: rewriting an external label with label_replace could lead to unpredictable results", annotations.PromQLWarning.Error())
)

type timeRange struct {
	start time.Time
	end   time.Time
}

type timeRanges []timeRange

// minOverlap returns the smallest overlap between consecutive time ranges.
func (trs timeRanges) minOverlap() time.Duration {
	var minEngineOverlap time.Duration = math.MaxInt64
	if len(trs) == 1 {
		return minEngineOverlap
	}

	for i := 1; i < len(trs); i++ {
		overlap := trs[i-1].end.Sub(trs[i].start)
		if overlap < minEngineOverlap {
			minEngineOverlap = overlap
		}
	}
	return minEngineOverlap
}

type labelSetRanges map[string]timeRanges

func (lrs labelSetRanges) addRange(key string, tr timeRange) {
	lrs[key] = append(lrs[key], tr)
}

// minOverlap returns the smallest overlap between all label set ranges.
func (lrs labelSetRanges) minOverlap() time.Duration {
	var minLabelsetOverlap time.Duration = math.MaxInt64
	for _, lr := range lrs {
		minRangeOverlap := lr.minOverlap()
		if minRangeOverlap < minLabelsetOverlap {
			minLabelsetOverlap = minRangeOverlap
		}
	}

	return minLabelsetOverlap
}

type RemoteExecutions []RemoteExecution

func (rs RemoteExecutions) String() string {
	parts := make([]string, len(rs))
	for i, r := range rs {
		parts[i] = r.String()
	}
	return strings.Join(parts, ", ")
}

// RemoteExecution is a logical plan that describes a
// remote execution of a Query against the given PromQL Engine.
type RemoteExecution struct {
	Engine          api.RemoteEngine
	Query           string
	QueryRangeStart time.Time

	valueType parser.ValueType
}

func (r RemoteExecution) String() string {
	if r.QueryRangeStart.UnixMilli() == 0 {
		return fmt.Sprintf("remote(%s)", r.Query)
	}
	return fmt.Sprintf("remote(%s) [%s]", r.Query, r.QueryRangeStart.UTC().String())
}

func (r RemoteExecution) Pretty(level int) string { return r.String() }

func (r RemoteExecution) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (r RemoteExecution) Type() parser.ValueType { return r.valueType }

func (r RemoteExecution) PromQLExpr() {}

// Deduplicate is a logical plan which deduplicates samples from multiple RemoteExecutions.
type Deduplicate struct {
	Expressions RemoteExecutions
}

func (r Deduplicate) String() string {
	return fmt.Sprintf("dedup(%s)", r.Expressions.String())
}

func (r Deduplicate) Pretty(level int) string { return r.String() }

func (r Deduplicate) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (r Deduplicate) Type() parser.ValueType { return r.Expressions[0].Type() }

func (r Deduplicate) PromQLExpr() {}

type Noop struct{}

func (r Noop) String() string { return "noop" }

func (r Noop) Pretty(level int) string { return r.String() }

func (r Noop) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (r Noop) Type() parser.ValueType { return parser.ValueTypeVector }

func (r Noop) PromQLExpr() {}

// distributiveAggregations are all PromQL aggregations which support
// distributed execution.
var distributiveAggregations = map[parser.ItemType]struct{}{
	parser.SUM:     {},
	parser.MIN:     {},
	parser.MAX:     {},
	parser.GROUP:   {},
	parser.COUNT:   {},
	parser.BOTTOMK: {},
	parser.TOPK:    {},
}

// DistributedExecutionOptimizer produces a logical plan suitable for
// distributed Query execution.
type DistributedExecutionOptimizer struct {
	Endpoints          api.RemoteEndpoints
	SkipBinaryPushdown bool
}

func (m DistributedExecutionOptimizer) Optimize(plan parser.Expr, opts *query.Options) (parser.Expr, annotations.Annotations) {
	engines := m.Endpoints.Engines()
	sort.Slice(engines, func(i, j int) bool {
		return engines[i].MinT() < engines[j].MinT()
	})

	labelRanges := make(labelSetRanges)
	engineLabels := make(map[string]struct{})
	for _, e := range engines {
		for _, lset := range e.LabelSets() {
			lsetKey := lset.String()
			labelRanges.addRange(lsetKey, timeRange{
				start: time.UnixMilli(e.MinT()),
				end:   time.UnixMilli(e.MaxT()),
			})
			lset.Range(func(lbl labels.Label) {
				engineLabels[lbl.Name] = struct{}{}
			})
		}
	}
	minEngineOverlap := labelRanges.minOverlap()
	if rewritesEngineLabels(plan, engineLabels) {
		return plan, annotations.New().Add(RewrittenExternalLabelWarning)
	}

	// TODO(fpetkovski): Consider changing TraverseBottomUp to pass in a list of parents in the transform function.
	parents := make(map[*parser.Expr]*parser.Expr)
	TraverseBottomUp(nil, &plan, func(parent, current *parser.Expr) (stop bool) {
		parents[current] = parent
		return false
	})
	TraverseBottomUp(nil, &plan, func(parent, current *parser.Expr) (stop bool) {
		// If the current operation is not distributive, stop the traversal.
		if !isDistributive(current, m.SkipBinaryPushdown) {
			return true
		}

		// If the current node is an aggregation, distribute the operation and
		// stop the traversal.
		if aggr, ok := (*current).(*parser.AggregateExpr); ok {
			localAggregation := aggr.Op
			if aggr.Op == parser.COUNT {
				localAggregation = parser.SUM
			}

			remoteAggregation := newRemoteAggregation(aggr, engines)
			subQueries := m.distributeQuery(&remoteAggregation, engines, m.subqueryOpts(parents, current, opts), minEngineOverlap)
			*current = &parser.AggregateExpr{
				Op:       localAggregation,
				Expr:     subQueries,
				Param:    aggr.Param,
				Grouping: aggr.Grouping,
				Without:  aggr.Without,
				PosRange: aggr.PosRange,
			}
			return true
		}
		if isAbsent(*current) {
			*current = m.distributeAbsent(*current, engines, calculateStartOffset(current, opts.LookbackDelta), m.subqueryOpts(parents, current, opts))
			return true
		}

		// If the parent operation is distributive, continue the traversal.
		if isDistributive(parent, m.SkipBinaryPushdown) {
			return false
		}

		*current = m.distributeQuery(current, engines, m.subqueryOpts(parents, current, opts), minEngineOverlap)
		return true
	})

	return plan, nil
}

func (m DistributedExecutionOptimizer) subqueryOpts(parents map[*parser.Expr]*parser.Expr, current *parser.Expr, opts *query.Options) *query.Options {
	subqueryParents := make([]*parser.SubqueryExpr, 0, len(parents))
	for p := parents[current]; p != nil; p = parents[p] {
		if subquery, ok := (*p).(*parser.SubqueryExpr); ok {
			subqueryParents = append(subqueryParents, subquery)
		}
	}
	for i := len(subqueryParents) - 1; i >= 0; i-- {
		opts = query.NestedOptionsForSubquery(opts, subqueryParents[i])
	}
	return opts
}

func newRemoteAggregation(rootAggregation *parser.AggregateExpr, engines []api.RemoteEngine) parser.Expr {
	groupingSet := make(map[string]struct{})
	for _, lbl := range rootAggregation.Grouping {
		groupingSet[lbl] = struct{}{}
	}

	for _, engine := range engines {
		for _, lbls := range engine.LabelSets() {
			lbls.Range(func(lbl labels.Label) {
				if rootAggregation.Without {
					delete(groupingSet, lbl.Name)
				} else {
					groupingSet[lbl.Name] = struct{}{}
				}
			})
		}
	}

	groupingLabels := make([]string, 0, len(groupingSet))
	for lbl := range groupingSet {
		groupingLabels = append(groupingLabels, lbl)
	}
	sort.Strings(groupingLabels)

	remoteAggregation := *rootAggregation
	remoteAggregation.Grouping = groupingLabels
	return &remoteAggregation
}

// distributeQuery takes a PromQL expression in the form of *parser.Expr and a set of remote engines.
// For each engine which matches the time range of the query, it creates a RemoteExecution scoped to the range of the engine.
// All remote executions are wrapped in a Deduplicate logical node to make sure that results from overlapping engines are deduplicated.
func (m DistributedExecutionOptimizer) distributeQuery(expr *parser.Expr, engines []api.RemoteEngine, opts *query.Options, allowedStartOffset time.Duration) parser.Expr {
	startOffset := calculateStartOffset(expr, opts.LookbackDelta)
	if allowedStartOffset < startOffset {
		return *expr
	}
	if isConstantExpr(*expr) {
		return *expr
	}

	var globalMinT int64 = math.MaxInt64
	for _, e := range engines {
		if e.MinT() < globalMinT {
			globalMinT = e.MinT()
		}
	}

	remoteQueries := make(RemoteExecutions, 0, len(engines))
	for _, e := range engines {
		if !matchesExternalLabelSet(*expr, e.LabelSets()) {
			continue
		}
		if e.MinT() > opts.End.UnixMilli() {
			continue
		}
		if e.MaxT() < opts.Start.UnixMilli()-startOffset.Milliseconds() {
			continue
		}

		start, keep := getStartTimeForEngine(e, opts, startOffset, globalMinT)
		if !keep {
			continue
		}

		remoteQueries = append(remoteQueries, RemoteExecution{
			Engine:          e,
			Query:           (*expr).String(),
			QueryRangeStart: start,
			valueType:       (*expr).Type(),
		})
	}

	if len(remoteQueries) == 0 {
		return Noop{}
	}

	return Deduplicate{
		Expressions: remoteQueries,
	}
}

func (m DistributedExecutionOptimizer) distributeAbsent(expr parser.Expr, engines []api.RemoteEngine, startOffset time.Duration, opts *query.Options) parser.Expr {
	queries := make(RemoteExecutions, 0, len(engines))
	for i, e := range engines {
		if e.MaxT() < opts.Start.UnixMilli()-startOffset.Milliseconds() {
			continue
		}
		if e.MinT() > opts.End.UnixMilli() {
			continue
		}
		queries = append(queries, RemoteExecution{
			Engine:          engines[i],
			Query:           expr.String(),
			QueryRangeStart: opts.Start,
			valueType:       expr.Type(),
		})
	}
	// We need to make sure that absent is at least evaluated against one engine.
	// Otherwise, we will end up with an empty result (not absent) when no engine matches the query.
	// For practicality, we choose the latest one since it likely has data in memory or on disk.
	// TODO(fpetkovski): This could also solved by a synthetic node which acts as a number literal but has specific labels.
	if len(queries) == 0 && len(engines) > 0 {
		return RemoteExecution{
			Engine:          engines[len(engines)-1],
			Query:           expr.String(),
			QueryRangeStart: opts.Start,
			valueType:       expr.Type(),
		}
	}

	var rootExpr parser.Expr = queries[0]
	for i := 1; i < len(queries); i++ {
		rootExpr = &parser.BinaryExpr{
			Op:             parser.MUL,
			LHS:            rootExpr,
			RHS:            queries[i],
			VectorMatching: &parser.VectorMatching{},
		}
	}

	return rootExpr
}

func isAbsent(expr parser.Expr) bool {
	call, ok := expr.(*parser.Call)
	if !ok {
		return false
	}
	return call.Func.Name == "absent" || call.Func.Name == "absent_over_time"
}

func getStartTimeForEngine(e api.RemoteEngine, opts *query.Options, offset time.Duration, globalMinT int64) (time.Time, bool) {
	if e.MinT() > opts.End.UnixMilli() {
		return time.Time{}, false
	}

	// Do not adjust start time for oldest engine since there is no engine to backfill from.
	if e.MinT() == globalMinT {
		return opts.Start, true
	}

	// A remote engine needs to have sufficient scope to do a look-back from the start of the query range.
	engineMinTime := time.UnixMilli(e.MinT())
	requiredMinTime := opts.Start.Add(-offset)

	// Do not adjust the start time for instant queries since it would lead to
	// changing the user-provided timestamp and sending a result for a different time.
	if opts.IsInstantQuery() {
		keep := engineMinTime.Before(requiredMinTime)
		return opts.Start, keep
	}

	// If an engine's min time is before the start time of the query,
	// scope the query for this engine to the start of the range + the required offset.
	if engineMinTime.After(requiredMinTime) {
		engineMinTime = calculateStepAlignedStart(opts, engineMinTime.Add(offset))
	}

	return calculateStepAlignedStart(opts, maxTime(engineMinTime, opts.Start)), true
}

// calculateStepAlignedStart returns a start time for the query based on the
// engine min time and the query step size.
// The purpose of this alignment is to make sure that the steps for the remote query
// have the same timestamps as the ones for the central query.
func calculateStepAlignedStart(opts *query.Options, engineMinTime time.Time) time.Time {
	originalSteps := numSteps(opts.Start, opts.End, opts.Step)
	remoteQuerySteps := numSteps(engineMinTime, opts.End, opts.Step)

	stepsToSkip := originalSteps - remoteQuerySteps
	stepAlignedStartTime := opts.Start.UnixMilli() + stepsToSkip*opts.Step.Milliseconds()

	return time.UnixMilli(stepAlignedStartTime)
}

// calculateStartOffset returns the offset that needs to be added to the start time
// for each remote query. It is calculated by taking the maximum between
// the range of a matrix selector (if present in a query) and the lookback configured
// in the query engine.
// Applying an offset is necessary to make sure that a remote engine has sufficient
// scope to calculate results for the first several steps of a range.
//
// For example, for a query like sum_over_time(metric[1h]), an engine with a time range of
// 6h can correctly evaluate only the last 5h of the range.
// The first 1 hour of data cannot be correctly calculated since the range selector in the engine
// will not be able to gather enough points.
func calculateStartOffset(expr *parser.Expr, lookbackDelta time.Duration) time.Duration {
	if expr == nil {
		return lookbackDelta
	}

	var selectRange time.Duration
	var offset time.Duration
	traverse(expr, func(node *parser.Expr) {
		switch n := (*node).(type) {
		case *parser.SubqueryExpr:
			selectRange += n.Range
		case *MatrixSelector:
			selectRange += n.Range
		case *VectorSelector:
			offset = n.Offset
		}
	})
	return maxDuration(offset+selectRange, lookbackDelta)
}

func numSteps(start, end time.Time, step time.Duration) int64 {
	return (end.UnixMilli()-start.UnixMilli())/step.Milliseconds() + 1
}

func isDistributive(expr *parser.Expr, skipBinaryPushdown bool) bool {
	if expr == nil {
		return false
	}

	switch e := (*expr).(type) {
	case *parser.BinaryExpr:
		return isBinaryExpressionWithOneConstantSide(e) || (!skipBinaryPushdown && isBinaryExpressionWithDistributableMatching(e))
	case *parser.AggregateExpr:
		// Certain aggregations are currently not supported.
		if _, ok := distributiveAggregations[e.Op]; !ok {
			return false
		}
	case *parser.Call:
		return len(e.Args) > 0
	}

	return true
}

func isBinaryExpressionWithOneConstantSide(expr *parser.BinaryExpr) bool {
	lhsConstant := isConstantExpr(expr.LHS)
	rhsConstant := isConstantExpr(expr.RHS)
	return (lhsConstant || rhsConstant)
}

func isBinaryExpressionWithDistributableMatching(expr *parser.BinaryExpr) bool {
	if expr.VectorMatching == nil {
		return false
	}

	// we can distribute if the vector matching contains the external labels so that
	// all potential matching partners are contained in one engine
	return !expr.VectorMatching.On && len(expr.VectorMatching.MatchingLabels) == 0
}

// matchesExternalLabels returns false if given matchers are not matching external labels.
func matchesExternalLabelSet(expr parser.Expr, externalLabelSet []labels.Labels) bool {
	if len(externalLabelSet) == 0 {
		return true
	}
	var selectorSet [][]*labels.Matcher
	traverse(&expr, func(current *parser.Expr) {
		vs, ok := (*current).(*VectorSelector)
		if ok {
			selectorSet = append(selectorSet, vs.LabelMatchers)
		}
	})

	for _, selectors := range selectorSet {
		hasMatch := false
		for _, externalLabels := range externalLabelSet {
			hasMatch = hasMatch || matchesExternalLabels(selectors, externalLabels)
		}
		if !hasMatch {
			return false
		}
	}

	return true
}

// matchesExternalLabels returns false if given matchers are not matching external labels.
func matchesExternalLabels(ms []*labels.Matcher, externalLabels labels.Labels) bool {
	if externalLabels.Len() == 0 {
		return true
	}

	for _, matcher := range ms {
		extValue := externalLabels.Get(matcher.Name)
		if extValue != "" && !matcher.Matches(extValue) {
			return false
		}
	}
	return true
}

func isConstantExpr(expr parser.Expr) bool {
	// TODO: there are more possibilities for constant expressions
	switch texpr := expr.(type) {
	case *parser.NumberLiteral:
		return true
	case *parser.StepInvariantExpr:
		return isConstantExpr(texpr.Expr)
	case *parser.ParenExpr:
		return isConstantExpr(texpr.Expr)
	case *parser.Call:
		constArgs := true
		for _, arg := range texpr.Args {
			constArgs = constArgs && isConstantExpr(arg)
		}
		return constArgs
	case *parser.BinaryExpr:
		return isConstantExpr(texpr.LHS) && isConstantExpr(texpr.RHS)
	default:
		return false
	}
}

func rewritesEngineLabels(e parser.Expr, engineLabels map[string]struct{}) bool {
	var result bool
	TraverseBottomUp(nil, &e, func(parent *parser.Expr, node *parser.Expr) bool {
		call, ok := (*node).(*parser.Call)
		if !ok || call.Func.Name != "label_replace" {
			return false
		}
		targetLabel := call.Args[1].(*parser.StringLiteral).Val
		if _, ok := engineLabels[targetLabel]; ok {
			result = true
			return true
		}
		return false
	})
	return result
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
