// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type Filter interface {
	Matches(series storage.Series) bool
	Matchers() []*labels.Matcher
}

type nopFilter struct{}

func (n nopFilter) Matchers() []*labels.Matcher { return nil }

func (n nopFilter) Matches(storage.Series) bool { return true }

type filter struct {
	matchers   []*labels.Matcher
	matcherSet map[string][]*labels.Matcher
}

func NewFilter(matchers []*labels.Matcher) Filter {
	if len(matchers) == 0 {
		return &nopFilter{}
	}

	matcherSet := make(map[string][]*labels.Matcher)
	for _, m := range matchers {
		matcherSet[m.Name] = append(matcherSet[m.Name], m)
	}
	return &filter{
		matchers:   matchers,
		matcherSet: matcherSet,
	}
}

func (f filter) Matchers() []*labels.Matcher { return f.matchers }

func (f filter) Matches(series storage.Series) bool {
	if len(f.matcherSet) == 0 {
		return true
	}

	for name, matchers := range f.matcherSet {
		label := series.Labels().Get(name)

		for _, m := range matchers {
			if !m.Matches(label) {
				return false
			}
		}
	}

	return true
}
