// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/query"
)

// PropagateMatchersOptimizer implements matcher propagation between
// two vector selectors in a binary expression.
type PropagateMatchersOptimizer struct{}

func (m PropagateMatchersOptimizer) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	Traverse(&plan, func(expr *Node) {
		binOp, ok := (*expr).(*Binary)
		if !ok {
			return
		}

		// The optimizer cannot be applied to comparison operations or 'atan2'.
		if binOp.Op.IsComparisonOperator() || binOp.Op.String() == "atan2" {
			return
		}

		// TODO(fpetkovski): Investigate support for vector matching on a subset of labels.
		if binOp.VectorMatching != nil && len(binOp.VectorMatching.MatchingLabels) > 0 {
			return
		}

		// TODO(fpetkovski): Investigate support for one-to-many and many-to-one.
		if binOp.VectorMatching != nil && binOp.VectorMatching.Card != parser.CardOneToOne {
			return
		}

		propagateMatchers(binOp)
	})

	return plan, nil
}

func propagateMatchers(binOp *Binary) {
	lhSelector, ok := binOp.LHS.(*VectorSelector)
	if !ok {
		return
	}
	rhSelector, ok := binOp.RHS.(*VectorSelector)
	if !ok {
		return
	}
	// This case is handled by MergeSelectsOptimizer.
	if lhSelector.Name == rhSelector.Name {
		return
	}

	lhMatchers := toMatcherMap(lhSelector)
	rhMatchers := toMatcherMap(rhSelector)
	union, hasDuplicates := makeUnion(lhMatchers, rhMatchers)
	if hasDuplicates {
		return
	}

	finalMatchers := toSlice(union)
	lhSelector.LabelMatchers = finalMatchers
	rhSelector.LabelMatchers = finalMatchers
}

func toSlice(union map[string]*labels.Matcher) []*labels.Matcher {
	finalMatchers := make([]*labels.Matcher, 0, len(union))
	for _, m := range union {
		finalMatchers = append(finalMatchers, m)
	}

	sort.Slice(finalMatchers, func(i, j int) bool { return finalMatchers[i].Name < finalMatchers[j].Name })
	return finalMatchers
}

func makeUnion(lhMatchers map[string]*labels.Matcher, rhMatchers map[string]*labels.Matcher) (map[string]*labels.Matcher, bool) {
	union := make(map[string]*labels.Matcher)
	for _, m := range lhMatchers {
		if m.Name == labels.MetricName {
			continue
		}
		if duplicateExists(rhMatchers, m) {
			return nil, true
		}
		union[m.Name] = m
	}

	for _, m := range rhMatchers {
		if m.Name == labels.MetricName {
			continue
		}
		if duplicateExists(lhMatchers, m) {
			return nil, true
		}
		union[m.Name] = m
	}
	return union, false
}

func toMatcherMap(lhSelector *VectorSelector) map[string]*labels.Matcher {
	lhMatchers := make(map[string]*labels.Matcher)
	for _, m := range lhSelector.LabelMatchers {
		lhMatchers[m.Name] = m
	}
	return lhMatchers
}

func duplicateExists(matchers map[string]*labels.Matcher, matcher *labels.Matcher) bool {
	existing, ok := matchers[matcher.Name]
	if !ok {
		return false
	}

	return existing.String() == matcher.String()
}
