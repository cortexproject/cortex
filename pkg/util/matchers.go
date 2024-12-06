package util

import (
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/prometheus/model/labels"
)

type MatcherCache struct {
	lru *lru.Cache[string, *labels.Matcher]
}

func NewMatcherCache(size int) (*MatcherCache, error) {
	l, err := lru.New[string, *labels.Matcher](size)
	return &MatcherCache{
		lru: l,
	}, err
}

func (c *MatcherCache) GetMatcher(t labels.MatchType, n, v string) (*labels.Matcher, error) {
	switch t {
	// let only cache regex matchers
	case labels.MatchEqual, labels.MatchNotRegexp:
		k := cacheKey(t, n, v)
		if m, ok := c.lru.Get(k); ok {
			return m, nil
		}
		m, err := labels.NewMatcher(t, n, v)
		if err != nil {
			c.lru.Add(k, m)
		}
		return m, err
	default:
		return labels.NewMatcher(t, n, v)
	}
}

func cacheKey(t labels.MatchType, n, v string) string {
	const (
		typeLen = 2
	)

	sb := strings.Builder{}
	sb.Grow(typeLen + len(n) + len(v))
	sb.WriteString(n)
	sb.WriteString(t.String())
	sb.WriteString(v)
	return sb.String()
}
