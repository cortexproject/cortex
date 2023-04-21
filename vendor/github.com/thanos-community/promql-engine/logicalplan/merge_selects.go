// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"
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

func (m MergeSelectsOptimizer) Optimize(expr parser.Expr, _ *Opts) parser.Expr {
	heap := make(matcherHeap)
	extractSelectors(heap, expr)
	replaceMatchers(heap, &expr)

	return expr
}

func extractSelectors(selectors matcherHeap, expr parser.Expr) {
	parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
		e, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		for _, l := range e.LabelMatchers {
			if l.Name == labels.MetricName {
				selectors.add(l.Value, e.LabelMatchers)
			}
		}
		return nil
	})
}

func replaceMatchers(selectors matcherHeap, expr *parser.Expr) {
	traverse(expr, func(node *parser.Expr) {
		e, ok := (*node).(*parser.VectorSelector)
		if !ok {
			return
		}

		for _, l := range e.LabelMatchers {
			if l.Name != labels.MetricName {
				continue
			}
			replacement, found := selectors.findReplacement(l.Value, e.LabelMatchers)
			if !found {
				continue
			}

			// Make a copy of the original selectors to avoid modifying them while
			// trimming filters.
			filters := make([]*labels.Matcher, len(e.LabelMatchers))
			copy(filters, e.LabelMatchers)

			// All replacements are done on metrics name only,
			// so we can drop the explicit metric name selector.
			filters = dropMatcher(labels.MetricName, filters)

			// Drop filters which are already present as matchers in the replacement selector.
			for _, s := range replacement {
				for _, f := range filters {
					if s.Name == f.Name && s.Value == f.Value && s.Type == f.Type {
						filters = dropMatcher(f.Name, filters)
					}
				}
			}
			e.LabelMatchers = replacement
			*node = &FilteredSelector{
				Filters:        filters,
				VectorSelector: e,
			}
			return
		}
	})
}

func dropMatcher(matcherName string, originalMatchers []*labels.Matcher) []*labels.Matcher {
	i := 0
	for i < len(originalMatchers) {
		l := originalMatchers[i]
		if l.Name == matcherName {
			originalMatchers = append(originalMatchers[:i], originalMatchers[i+1:]...)
		} else {
			i++
		}
	}
	return originalMatchers
}

func matcherToMap(matchers []*labels.Matcher) map[string]*labels.Matcher {
	r := make(map[string]*labels.Matcher, len(matchers))
	for i := 0; i < len(matchers); i++ {
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
