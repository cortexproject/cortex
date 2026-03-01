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

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

var (
	RewrittenExternalLabelWarning = errors.Newf("%s: rewriting an external label with label_replace can disable distributed query execution", annotations.PromQLWarning.Error())
)

type timeRange struct {
	start time.Time
	end   time.Time
}

type timeRanges []timeRange

// minOverlap returns the smallest overlap between consecutive time ranges that overlap the interval [mint, maxt].
func (trs timeRanges) minOverlap(mint, maxt int64) time.Duration {
	var minEngineOverlap time.Duration = math.MaxInt64
	if len(trs) == 1 {
		return minEngineOverlap
	}

	for i := 1; i < len(trs); i++ {
		if trs[i].end.UnixMilli() < mint || trs[i].start.UnixMilli() > maxt {
			continue
		}
		minEngineOverlap = min(minEngineOverlap, trs[i-1].end.Sub(trs[i].start))
	}
	return minEngineOverlap
}

type labelSetRanges map[string]timeRanges

func (lrs labelSetRanges) addRange(key string, tr timeRange) {
	lrs[key] = append(lrs[key], tr)
}

// minOverlap returns the smallest overlap between all label set ranges that overlap the interval [mint, maxt].
func (lrs labelSetRanges) minOverlap(mint, maxt int64) time.Duration {
	var minLabelsetOverlap time.Duration = math.MaxInt64
	for _, lr := range lrs {
		minLabelsetOverlap = min(minLabelsetOverlap, lr.minOverlap(mint, maxt))
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
	QueryRangeEnd   time.Time
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
	return fmt.Sprintf("remote(%s) [%s, %s]", r.Query, r.QueryRangeStart.UTC().String(), r.QueryRangeEnd.UTC().String())
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
	parser.SUM:         {},
	parser.MIN:         {},
	parser.MAX:         {},
	parser.GROUP:       {},
	parser.COUNT:       {},
	parser.BOTTOMK:     {},
	parser.TOPK:        {},
	parser.LIMITK:      {},
	parser.LIMIT_RATIO: {},
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
		for _, lset := range e.PartitionLabelSets() {
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

	var warns = annotations.New()

	// TODO(fpetkovski): Consider changing TraverseBottomUp to pass in a list of parents in the transform function.
	parents := make(map[*Node]*Node)
	TraverseBottomUp(nil, &plan, func(parent, current *Node) (stop bool) {
		parents[current] = parent
		return false
	})
	TraverseBottomUp(nil, &plan, func(parent, current *Node) (stop bool) {
		// Handle avg() specially - it's not distributive but can be distributed as sum/count.
		if isAvgAggregation(current) {
			*current = m.distributeAvg(*current, engines, m.subqueryOpts(parents, current, opts), labelRanges)
			return true
		}

		// If the current operation is not distributive, stop the traversal.
		if !isDistributive(current, m.SkipBinaryPushdown, engineLabels, warns) {
			return true
		}

		// Handle absent functions specially
		if isAbsent(current) {
			*current = m.distributeAbsent(*current, engines, calculateStartOffset(current, opts.LookbackDelta), m.subqueryOpts(parents, current, opts))
			return true
		}

		// If the current node is an aggregation, check if we should distribute here
		// or continue traversing up.
		if aggr, ok := (*current).(*Aggregation); ok {
			// If this aggregation preserves partition labels and there's a
			// distributive aggregation ancestor, continue up to let it handle distribution.
			// This enables patterns like:
			//   - topk(10, sum by (P, instance) (X))
			//   - sum(metric_a * group by (P) (metric_b))
			//   - max(sum by (P, instance) (X))
			// where P is a partition label - we can push the entire expression
			// to remote engines.
			//
			// We need to check ancestors (not just immediate parent) because the
			// aggregation might be nested inside a binary expression that is itself
			// inside another aggregation: sum(A * group by (P) (B))
			if preservesPartitionLabels(*current, engineLabels) {
				if hasDistributiveAncestor(parents, current, m.SkipBinaryPushdown, engineLabels, warns) {
					return false
				}
			}
			localAggregation := aggr.Op
			if aggr.Op == parser.COUNT {
				localAggregation = parser.SUM
			}

			remoteAggregation := newRemoteAggregation(aggr, engines)
			subQueries := m.distributeQuery(&remoteAggregation, engines, m.subqueryOpts(parents, current, opts), labelRanges)
			*current = &Aggregation{
				Op:       localAggregation,
				Expr:     subQueries,
				Param:    aggr.Param,
				Grouping: aggr.Grouping,
				Without:  aggr.Without,
			}
			return true
		}

		// If the parent operation is distributive or is an avg (which we handle specially),
		// continue the traversal.
		if isDistributive(parent, m.SkipBinaryPushdown, engineLabels, warns) || isAvgAggregation(parent) {
			return false
		}

		*current = m.distributeQuery(current, engines, m.subqueryOpts(parents, current, opts), labelRanges)
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
		for _, lbls := range engine.PartitionLabelSets() {
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
func (m DistributedExecutionOptimizer) distributeQuery(expr *Node, engines []api.RemoteEngine, opts *query.Options, labelRanges labelSetRanges) Node {
	startOffset := calculateStartOffset(expr, opts.LookbackDelta)
	allowedStartOffset := labelRanges.minOverlap(opts.Start.UnixMilli()-startOffset.Milliseconds(), opts.End.UnixMilli())

	if allowedStartOffset < startOffset {
		return *expr
	}
	if IsConstantExpr(*expr) {
		return *expr
	}

	// Selectors in queries can be scoped to a single timestamp. This case is hard to
	// distribute properly and can lead to flaky results.
	// We only do it if all engines have sufficient scope for the full range of the query,
	// adjusted for the timestamp.
	// Otherwise, we fall back to the default mode of not executing the query remotely.
	if timestamps := getQueryTimestamps(expr); len(timestamps) > 0 {
		for _, e := range engines {
			for _, ts := range timestamps {
				if e.MinT() > ts-startOffset.Milliseconds() || e.MaxT() < ts {
					return *expr
				}
			}
		}
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
			QueryRangeEnd:   opts.End,
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
			QueryRangeEnd:   opts.End,
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
			QueryRangeEnd:   opts.End,
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

func isAbsent(expr *Node) bool {
	if expr == nil {
		return false
	}
	call, ok := (*expr).(*FunctionCall)
	if !ok {
		return false
	}
	return call.Func.Name == "absent" || call.Func.Name == "absent_over_time"
}

// distributeAvg distributes an avg() aggregation by rewriting it as sum()/count()
// where each side is distributed independently. This is necessary because averaging
// averages gives incorrect results - we must sum all values and count all values
// separately, then divide.
func (m DistributedExecutionOptimizer) distributeAvg(expr Node, engines []api.RemoteEngine, opts *query.Options, labelRanges labelSetRanges) Node {
	aggr := expr.(*Aggregation)

	sumAggr := *aggr
	sumAggr.Op = parser.SUM
	sumRemote := newRemoteAggregation(&sumAggr, engines)
	sumSubQueries := m.distributeQuery(&sumRemote, engines, opts, labelRanges)
	distributedSum := &Aggregation{
		Op:       parser.SUM,
		Expr:     sumSubQueries,
		Param:    aggr.Param,
		Grouping: aggr.Grouping,
		Without:  aggr.Without,
	}

	countAggr := *aggr
	countAggr.Op = parser.COUNT
	countAggr.Expr = aggr.Expr.Clone()
	countRemote := newRemoteAggregation(&countAggr, engines)
	countSubQueries := m.distributeQuery(&countRemote, engines, opts, labelRanges)
	distributedCount := &Aggregation{
		Op:       parser.SUM,
		Expr:     countSubQueries,
		Param:    aggr.Param,
		Grouping: aggr.Grouping,
		Without:  aggr.Without,
	}

	return &Binary{
		Op:  parser.DIV,
		LHS: distributedSum,
		RHS: distributedCount,
		VectorMatching: &parser.VectorMatching{
			Include:        aggr.Grouping,
			MatchingLabels: aggr.Grouping,
			On:             !aggr.Without,
		},
	}
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

	var (
		selectRange time.Duration
		offset      time.Duration
	)
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

func getQueryTimestamps(expr *Node) []int64 {
	var timestamps []int64
	Traverse(expr, func(node *Node) {
		switch n := (*node).(type) {
		case *Subquery:
			if n.Timestamp != nil {
				timestamps = append(timestamps, *n.Timestamp)
				return
			}
		case *VectorSelector:
			if n.Timestamp != nil {
				timestamps = append(timestamps, *n.Timestamp)
				return
			}
		}
	})
	return timestamps
}

func numSteps(start, end time.Time, step time.Duration) int64 {
	return (end.UnixMilli()-start.UnixMilli())/step.Milliseconds() + 1
}

// preservesPartitionLabels checks if an expression preserves all partition labels.
// An expression preserves partition labels if the output series will still have
// those labels, meaning results from different engines won't overlap and can be
// coalesced without deduplication.
//
// This enables pushing more operations to remote engines. For example:
//
//	topk(10, sum by (P, instance) (X))
//
// If P is a partition label, the sum preserves P, so topk can also be pushed
// down since each engine's top 10 won't overlap with other engines' top 10.
func preservesPartitionLabels(expr Node, partitionLabels map[string]struct{}) bool {
	if len(partitionLabels) == 0 {
		return false
	}

	switch e := expr.(type) {
	case *VectorSelector, *MatrixSelector, *NumberLiteral, *StringLiteral:
		return true
	case *Aggregation:
		for lbl := range partitionLabels {
			if slices.Contains(e.Grouping, lbl) == e.Without {
				return false
			}
		}
		return true
	case *Binary:
		if e.VectorMatching != nil {
			for lbl := range partitionLabels {
				inMatching := slices.Contains(e.VectorMatching.MatchingLabels, lbl)
				inInclude := slices.Contains(e.VectorMatching.Include, lbl)
				if !inInclude && inMatching != e.VectorMatching.On {
					return false
				}
			}
		}
		return preservesPartitionLabels(e.LHS, partitionLabels) &&
			preservesPartitionLabels(e.RHS, partitionLabels)
	case *FunctionCall:
		if e.Func.Name == "label_replace" {
			if _, ok := partitionLabels[UnsafeUnwrapString(e.Args[1])]; ok {
				return false
			}
		}
		for _, arg := range e.Args {
			if arg.ReturnType() == parser.ValueTypeVector || arg.ReturnType() == parser.ValueTypeMatrix {
				if !preservesPartitionLabels(arg, partitionLabels) {
					return false
				}
			}
		}
		return true
	case *Unary:
		return preservesPartitionLabels(e.Expr, partitionLabels)
	case *Parens:
		return preservesPartitionLabels(e.Expr, partitionLabels)
	case *StepInvariantExpr:
		return preservesPartitionLabels(e.Expr, partitionLabels)
	case *CheckDuplicateLabels:
		return preservesPartitionLabels(e.Expr, partitionLabels)
	case *Subquery:
		return preservesPartitionLabels(e.Expr, partitionLabels)
	default:
		return false
	}
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
		// scalar() returns NaN if the vector selector returns nothing
		// so it's not possible to know which result is correct. Hence,
		// it is not distributive.
		if e.Func.Name == "scalar" {
			return false
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

	isSetOperation := expr.Op == parser.LOR || expr.Op == parser.LUNLESS

	// For set operations (or/unless) with a constant expression on either side,
	// distribution is not safe because the constant will be evaluated by each
	// engine and cause duplicates. For example, `bar or on () vector(0)` would
	// have vector(0) returned by every engine.
	if isSetOperation && (IsConstantExpr(expr.LHS) || IsConstantExpr(expr.RHS)) {
		return false
	}

	// Default matching (no explicit on() or ignoring()) matches on all labels.
	// For this to be safe, both sides must preserve partition labels so that the
	// matching will include them. If only one side has partition labels, the matching
	// behavior differs per partition.
	if len(expr.VectorMatching.MatchingLabels) == 0 && !expr.VectorMatching.On {
		// For or/unless with default matching, we can distribute if:
		// 1. Both sides preserve partition labels (matching will include them), OR
		// 2. Both sides have the same partition label scope (both global, or both
		//    filtered to the same partition values)
		//
		// Case 2 is important because it allows queries like:
		//   metric_a or metric_b  (both global)
		//   metric_a{zone="east"} or metric_b{zone="east"}  (same partition)
		if isSetOperation {
			lhsMatchers := getPartitionMatchers(expr.LHS, engineLabels)
			rhsMatchers := getPartitionMatchers(expr.RHS, engineLabels)
			return partitionMatchersEqual(lhsMatchers, rhsMatchers)
		}
		return true
	}

	for lbl := range engineLabels {
		inMatching := slices.Contains(expr.VectorMatching.MatchingLabels, lbl)
		inInclude := slices.Contains(expr.VectorMatching.Include, lbl)
		// If a partition label is in group_left/group_right (Include), distribution
		// changes match cardinality semantics. Each partition only sees one value for
		// that label, so what's many-to-many globally may become one-to-one per partition,
		// producing results instead of errors (or vice versa).
		return !inInclude && inMatching == expr.VectorMatching.On
	}
	// At this point, partition labels are in the matching set (either via on() or
	// by not being in ignoring()). This means or/unless can be safely distributed
	// because the matching ensures series are paired by partition.
	return true
}

// getPartitionMatchers extracts matchers for partition labels from all selectors in the expression.
// Returns a map of partition label name to a list of matchers found across all selectors.
// If a selector has no matcher for a partition label, it's considered "global" for that label.
func getPartitionMatchers(expr Node, partitionLabels map[string]struct{}) map[string][]*labels.Matcher {
	result := make(map[string][]*labels.Matcher)
	for lbl := range partitionLabels {
		result[lbl] = nil
	}

	Traverse(&expr, func(current *Node) {
		vs, ok := (*current).(*VectorSelector)
		if !ok {
			return
		}
		for _, m := range vs.LabelMatchers {
			if _, isPartition := partitionLabels[m.Name]; isPartition {
				result[m.Name] = append(result[m.Name], m)
			}
		}
	})

	return result
}

// partitionMatchersEqual checks if two sets of partition matchers are equivalent.
func partitionMatchersEqual(a, b map[string][]*labels.Matcher) bool {
	for lbl := range a {
		aMatchers := a[lbl]
		bMatchers := b[lbl]

		// Both global (no matchers) for this label
		if len(aMatchers) == 0 && len(bMatchers) == 0 {
			continue
		}

		// One has matchers, other doesn't - not equal
		if len(aMatchers) != len(bMatchers) {
			return false
		}

		// Compare matchers - they should be identical
		// Sort by name+type+value for comparison
		aSet := make(map[string]struct{})
		for _, m := range aMatchers {
			key := fmt.Sprintf("%s:%d:%s", m.Name, m.Type, m.Value)
			aSet[key] = struct{}{}
		}
		for _, m := range bMatchers {
			key := fmt.Sprintf("%s:%d:%s", m.Name, m.Type, m.Value)
			if _, ok := aSet[key]; !ok {
				return false
			}
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

// hasDistributiveAncestor checks if there's a distributive node somewhere up the
// parent chain from the current node that can handle distribution.
// We must have an unbroken chain of distributive nodes to the ancestor for it to
// be able to handle distribution on our behalf.
func hasDistributiveAncestor(parents map[*Node]*Node, current *Node, skipBinaryPushdown bool, engineLabels map[string]struct{}, warns *annotations.Annotations) bool {
	for p := parents[current]; p != nil; p = parents[p] {
		if !isDistributive(p, skipBinaryPushdown, engineLabels, warns) {
			// We hit a non-distributive node, so we can't push through it.
			// No ancestor can help us distribute.
			return false
		}
	}
	// All ancestors are distributive, so the root (or the point where we
	// stop traversing) can handle distribution.
	return parents[current] != nil
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
