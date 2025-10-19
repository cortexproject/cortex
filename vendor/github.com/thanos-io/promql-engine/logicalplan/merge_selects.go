// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"slices"

	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
)

// MergeSelectsOptimizer optimizes a binary expression where
// one select is a superset of the other select.
// For example, the expression:
//
//	metric{a="b", c="d"} / scalar(metric{a="b"}) becomes:
//	Filter(c="d", metric{a="b"}) / scalar(metric{a="b"}).
//
// The engine can then cache the result of `metric{a="b"}`
// and apply an additional filter for {c="d"}.
type MergeSelectsOptimizer struct{}

func (m MergeSelectsOptimizer) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	heap := make(matcherHeap)
	extractSelectors(heap, plan)
	replaceMatchers(heap, &plan)

	return plan, nil
}

func extractSelectors(selectors matcherHeap, expr Node) {
	Traverse(&expr, func(node *Node) {
		e, ok := (*node).(*VectorSelector)
		if !ok {
			return
		}
		if !emptyProjection(e) {
			return
		}
		for _, l := range e.LabelMatchers {
			if l.Name == labels.MetricName {
				selectors.add(l.Value, e.LabelMatchers)
			}
		}
	})
}

func replaceMatchers(selectors matcherHeap, expr *Node) {
	Traverse(expr, func(node *Node) {
		var matchers []*labels.Matcher
		switch e := (*node).(type) {
		case *VectorSelector:
			if !emptyProjection(e) {
				return
			}
			matchers = e.LabelMatchers
		default:
			return
		}

		for _, l := range matchers {
			if l.Name != labels.MetricName {
				continue
			}
			replacement, found := selectors.findReplacement(l.Value, matchers)
			if !found {
				continue
			}

			// Make a copy of the original selectors to avoid modifying them while
			// trimming filters.
			filters := make([]*labels.Matcher, len(matchers))
			copy(filters, matchers)

			// Drop filters which are already present as matchers in the replacement selector including metric name selector.
			filters = dropMatcher(replacement, filters)

			switch e := (*node).(type) {
			case *VectorSelector:
				e.LabelMatchers = replacement
				e.Filters = filters
			}

			return
		}
	})
}

func dropMatcher(toDrop []*labels.Matcher, original []*labels.Matcher) []*labels.Matcher {
	res := slices.Clone(original)
	i := 0
	for i < len(res) {
		l := res[i]
		remove := false
		for _, m := range toDrop {
			if l.Name == m.Name && l.Type == m.Type && l.Value == m.Value {
				remove = true
				break
			}
		}
		if remove {
			res = slices.Delete(res, i, i+1)
		} else {
			i++
		}
	}
	return res
}

func matcherToMap(matchers []*labels.Matcher) map[string]*labels.Matcher {
	r := make(map[string]*labels.Matcher, len(matchers))
	for i := range matchers {
		r[matchers[i].Name] = matchers[i]
	}
	return r
}

// matcherHeap is a set of the most selective label matchers
// for each metrics discovered in a PromQL expression.
// The selectivity of a matcher is defined by how many series are
// matched by it. Since matchers in PromQL are open, selectors
// with the least amount of matchers are typically the most selective ones.
type matcherHeap map[string][]*labels.Matcher

func (m matcherHeap) add(metricName string, lessSelective []*labels.Matcher) {
	moreSelective, ok := m[metricName]
	if !ok {
		m[metricName] = lessSelective
		return
	}

	if len(lessSelective) < len(moreSelective) {
		moreSelective = lessSelective
	}

	m[metricName] = moreSelective
}

func (m matcherHeap) findReplacement(metricName string, matcher []*labels.Matcher) ([]*labels.Matcher, bool) {
	top, ok := m[metricName]
	if !ok {
		return nil, false
	}

	matcherSet := matcherToMap(matcher)
	topSet := matcherToMap(top)
	for k, v := range topSet {
		m, ok := matcherSet[k]
		if !ok {
			return nil, false
		}

		equals := v.Name == m.Name && v.Type == m.Type && v.Value == m.Value
		if !equals {
			return nil, false
		}
	}

	// The top matcher and input matcher are equal. No replacement needed.
	if len(topSet) == len(matcherSet) {
		return nil, false
	}

	return top, true
}

func emptyProjection(vs *VectorSelector) bool {
	if vs.Projection == nil {
		return true
	}
	return !vs.Projection.Include && len(vs.Projection.Labels) == 0
}
