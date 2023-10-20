package storegateway

import (
	"sort"

	"github.com/prometheus/prometheus/model/labels"
)

type sortedMatchers []*labels.Matcher

func newSortedMatchers(matchers []*labels.Matcher) sortedMatchers {
	sort.Slice(matchers, func(i, j int) bool {
		if matchers[i].Type == matchers[j].Type {
			if matchers[i].Name == matchers[j].Name {
				return matchers[i].Value < matchers[j].Value
			}
			return matchers[i].Name < matchers[j].Name
		}
		return matchers[i].Type < matchers[j].Type
	})

	return matchers
}
