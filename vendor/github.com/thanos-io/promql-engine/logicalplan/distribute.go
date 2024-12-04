// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/query"
)

var (
	RewrittenExternalLabelWarning = errors.Newf("%s: rewriting an external label with label_replace can disable distributed query execution", annotations.PromQLWarning.Error())
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
	LeafNode
	Engine          api.RemoteEngine
	Query           Node
	QueryRangeStart time.Time
}

func (r RemoteExecution) Clone() Node {
	clone := r
	clone.Query = r.Query.Clone()
	return clone
}

func (r RemoteExecution) String() string {
	if r.QueryRangeStart.UnixMilli() == 0 {
		return fmt.Sprintf("remote(%s)", r.Query)
	}
	return fmt.Sprintf("remote(%s) [%s]", r.Query, r.QueryRangeStart.UTC().String())
}

func (r RemoteExecution) Type() NodeType { return RemoteExecutionNode }

func (r RemoteExecution) ReturnType() parser.ValueType { return r.Query.ReturnType() }

// Deduplicate is a logical plan which deduplicates samples from multiple RemoteExecutions.
type Deduplicate struct {
	LeafNode
	Expressions RemoteExecutions
}

func (r Deduplicate) Clone() Node {
	clone := r
	clone.Expressions = make(RemoteExecutions, len(r.Expressions))
	for i, e := range r.Expressions {
		clone.Expressions[i] = e.Clone().(RemoteExecution)
	}
	return clone
}

func (r Deduplicate) String() string {
	return fmt.Sprintf("dedup(%s)", r.Expressions.String())
}

func (r Deduplicate) ReturnType() parser.ValueType { return r.Expressions[0].ReturnType() }

func (r Deduplicate) Type() NodeType { return DeduplicateNode }

type Noop struct {
	LeafNode
}

func (r Noop) Clone() Node { return r }

func (r Noop) String() string { return "noop" }

func (r Noop) ReturnType() parser.ValueType { return parser.ValueTypeVector }

func (r Noop) Type() NodeType { return NoopNode }

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

func (m DistributedExecutionOptimizer) Optimize(plan Node, opts *query.Options) (Node, annotations.Annotations) {
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

	// Preprocess rewrite distributable averages as sum/count
	var warns = annotations.New()
	TraverseBottomUp(nil, &plan, func(parent, current *Node) (stop bool) {
		if !(isDistributive(current, m.SkipBinaryPushdown, engineLabels, warns) || isAvgAggregation(current)) {
			return true
		}
		// If the current node is avg(), distribute the operation and
		// stop the traversal.
		if aggr, ok := (*current).(*Aggregation); ok {
			if aggr.Op != parser.AVG {
				return true
			}

			sum := *(*current).(*Aggregation)
			sum.Op = parser.SUM
			count := *(*current).(*Aggregation)
			count.Op = parser.COUNT
			*current = &Binary{
				Op:  parser.DIV,
				LHS: &sum,
				RHS: &count,
				VectorMatching: &parser.VectorMatching{
					Include:        aggr.Grouping,
					MatchingLabels: aggr.Grouping,
					On:             true,
				},
			}
			return true
		}
		return !(isDistributive(parent, m.SkipBinaryPushdown, engineLabels, warns) || isAvgAggregation(parent))
	})

	// TODO(fpetkovski): Consider changing TraverseBottomUp to pass in a list of parents in the transform function.
	parents := make(map[*Node]*Node)
	TraverseBottomUp(nil, &plan, func(parent, current *Node) (stop bool) {
		parents[current] = parent
		return false
	})
	TraverseBottomUp(nil, &plan, func(parent, current *Node) (stop bool) {
		// If the current operation is not distributive, stop the traversal.
		if !isDistributive(current, m.SkipBinaryPushdown, engineLabels, warns) {
			return true
		}

		// If the current node is an aggregation, distribute the operation and
		// stop the traversal.
		if aggr, ok := (*current).(*Aggregation); ok {
			localAggregation := aggr.Op
			if aggr.Op == parser.COUNT {
				localAggregation = parser.SUM
			}

			remoteAggregation := newRemoteAggregation(aggr, engines)
			subQueries := m.distributeQuery(&remoteAggregation, engines, m.subqueryOpts(parents, current, opts), minEngineOverlap)
			*current = &Aggregation{
				Op:       localAggregation,
				Expr:     subQueries,
				Param:    aggr.Param,
				Grouping: aggr.Grouping,
				Without:  aggr.Without,
			}
			return true
		}
		if isAbsent(*current) {
			*current = m.distributeAbsent(*current, engines, calculateStartOffset(current, opts.LookbackDelta), m.subqueryOpts(parents, current, opts))
			return true
		}

		// If the parent operation is distributive, continue the traversal.
		if isDistributive(parent, m.SkipBinaryPushdown, engineLabels, warns) {
			return false
		}

		*current = m.distributeQuery(current, engines, m.subqueryOpts(parents, current, opts), minEngineOverlap)
		return true
	})

	return plan, *warns
}

func (m DistributedExecutionOptimizer) subqueryOpts(parents map[*Node]*Node, current *Node, opts *query.Options) *query.Options {
	subqueryParents := make([]*Subquery, 0, len(parents))
	for p := parents[current]; p != nil; p = parents[p] {
		if subquery, ok := (*p).(*Subquery); ok {
			subqueryParents = append(subqueryParents, subquery)
		}
	}
	for i := len(subqueryParents) - 1; i >= 0; i-- {
		opts = query.NestedOptionsForSubquery(
			opts,
			subqueryParents[i].Step,
			subqueryParents[i].Range,
			subqueryParents[i].Offset,
		)
	}
	return opts
}

func newRemoteAggregation(rootAggregation *Aggregation, engines []api.RemoteEngine) Node {
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
func (m DistributedExecutionOptimizer) distributeQuery(expr *Node, engines []api.RemoteEngine, opts *query.Options, allowedStartOffset time.Duration) Node {
	startOffset := calculateStartOffset(expr, opts.LookbackDelta)
	if allowedStartOffset < startOffset {
		return *expr
	}
	if IsConstantExpr(*expr) {
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
			Query:           (*expr).Clone(),
			QueryRangeStart: start,
		})
	}

	if len(remoteQueries) == 0 {
		return Noop{}
	}

	return Deduplicate{
		Expressions: remoteQueries,
	}
}

func (m DistributedExecutionOptimizer) distributeAbsent(expr Node, engines []api.RemoteEngine, startOffset time.Duration, opts *query.Options) Node {
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
			Query:           expr.Clone(),
			QueryRangeStart: opts.Start,
		})
	}
	// We need to make sure that absent is at least evaluated against one engine.
	// Otherwise, we will end up with an empty result (not absent) when no engine matches the query.
	// For practicality, we choose the latest one since it likely has data in memory or on disk.
	// TODO(fpetkovski): This could also solved by a synthetic node which acts as a number literal but has specific labels.
	if len(queries) == 0 && len(engines) > 0 {
		return RemoteExecution{
			Engine:          engines[len(engines)-1],
			Query:           expr,
			QueryRangeStart: opts.Start,
		}
	}

	var rootExpr Node = queries[0]
	for i := 1; i < len(queries); i++ {
		rootExpr = &Binary{
			Op:             parser.MUL,
			LHS:            rootExpr,
			RHS:            queries[i],
			VectorMatching: &parser.VectorMatching{},
		}
	}

	return rootExpr
}

func isAbsent(expr Node) bool {
	call, ok := expr.(*FunctionCall)
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
func calculateStartOffset(expr *Node, lookbackDelta time.Duration) time.Duration {
	if expr == nil {
		return lookbackDelta
	}

	var selectRange time.Duration
	var offset time.Duration
	Traverse(expr, func(node *Node) {
		switch n := (*node).(type) {
		case *Subquery:
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

func isDistributive(expr *Node, skipBinaryPushdown bool, engineLabels map[string]struct{}, warns *annotations.Annotations) bool {
	if expr == nil {
		return false
	}

	switch e := (*expr).(type) {
	case Deduplicate, RemoteExecution:
		return false
	case *Binary:
		if isBinaryExpressionWithOneScalarSide(e) {
			return true
		}
		return !skipBinaryPushdown &&
			isBinaryExpressionWithDistributableMatching(e, engineLabels) &&
			isDistributive(&e.LHS, skipBinaryPushdown, engineLabels, warns) &&
			isDistributive(&e.RHS, skipBinaryPushdown, engineLabels, warns)
	case *Aggregation:
		// Certain aggregations are currently not supported.
		if _, ok := distributiveAggregations[e.Op]; !ok {
			return false
		}
	case *FunctionCall:
		if e.Func.Name == "label_replace" {
			targetLabel := UnsafeUnwrapString(e.Args[1])
			if _, ok := engineLabels[targetLabel]; ok {
				warns.Add(RewrittenExternalLabelWarning)
				return false
			}
		}
	}

	return true
}

func isBinaryExpressionWithOneScalarSide(expr *Binary) bool {
	lhsConstant := IsConstantScalarExpr(expr.LHS)
	rhsConstant := IsConstantScalarExpr(expr.RHS)
	return lhsConstant || rhsConstant
}

func isBinaryExpressionWithDistributableMatching(expr *Binary, engineLabels map[string]struct{}) bool {
	if expr.VectorMatching == nil {
		return false
	}
	// TODO: think about "or" but for safety we dont push it down for now.
	if expr.Op == parser.LOR {
		return false
	}

	if expr.VectorMatching.On {
		// on (...) - if ... contains all partition labels we can distribute
		for lbl := range engineLabels {
			if !slices.Contains(expr.VectorMatching.MatchingLabels, lbl) {
				return false
			}
		}
		return true
	}
	// ignoring (...) - if ... does contain any engine labels we cannot distribute
	for lbl := range engineLabels {
		if slices.Contains(expr.VectorMatching.MatchingLabels, lbl) {
			return false
		}
	}
	return true
}

// matchesExternalLabels returns false if given matchers are not matching external labels.
func matchesExternalLabelSet(expr Node, externalLabelSet []labels.Labels) bool {
	if len(externalLabelSet) == 0 {
		return true
	}
	var selectorSet [][]*labels.Matcher
	Traverse(&expr, func(current *Node) {
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
