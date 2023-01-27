// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package storage

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
	matcherSet map[string]*labels.Matcher
}

func NewFilter(matchers []*labels.Matcher) Filter {
	if len(matchers) == 0 {
		return &nopFilter{}
	}

	matcherSet := make(map[string]*labels.Matcher)
	for _, m := range matchers {
		matcherSet[m.Name] = m
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

	for _, l := range series.Labels() {
		m, ok := f.matcherSet[l.Name]
		if !ok {
			continue
		}
		if !m.Matches(l.Value) {
			return false
		}
	}

	return true
}
