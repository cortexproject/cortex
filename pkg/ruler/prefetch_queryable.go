package ruler

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
)

type selectMergerCtxKey struct{}

// selectMergerState is injected into context by the iteration func.
// The QueryFunc wrapper lazily executes the prefetch on first access.
type selectMergerState struct {
	plan  []mergedSelect
	once  sync.Once
	cache *prefetchCache
}

func withSelectMergerPlan(ctx context.Context, plan []mergedSelect) context.Context {
	return context.WithValue(ctx, selectMergerCtxKey{}, &selectMergerState{plan: plan})
}

// selectMergerQueryFunc wraps a QueryFunc to check context for a merge plan.
// On first call, it lazily pre-fetches using the inner QueryFunc, then serves from cache.
func selectMergerQueryFunc(inner rules.QueryFunc) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		state, _ := ctx.Value(selectMergerCtxKey{}).(*selectMergerState)
		if state == nil {
			return inner(ctx, qs, t)
		}

		// Lazy prefetch: execute plan on first call.
		state.once.Do(func() {
			state.cache = executePrefetch(ctx, state.plan, inner, t)
		})

		if state.cache != nil {
			selectors := extractSelectorsFromExpr(qs)
			if len(selectors) == 1 {
				if vec, ok := state.cache.get(selectors[0]); ok {
					return vec, nil
				}
			}
		}
		return inner(ctx, qs, t)
	}
}

// prefetchEntry holds pre-fetched results for a merged selector.
type prefetchEntry struct {
	matchers []*labels.Matcher
	vector   promql.Vector
}

// prefetchCache holds all pre-fetched data for a single group evaluation.
type prefetchCache struct {
	entries []prefetchEntry
}

func (c *prefetchCache) get(queryMatchers []*labels.Matcher) (promql.Vector, bool) {
	for _, e := range c.entries {
		if isMatcherSetSuperset(e.matchers, queryMatchers) {
			extra := extraMatchers(e.matchers, queryMatchers)
			if len(extra) == 0 {
				return e.vector, true
			}
			return filterVector(e.vector, extra), true
		}
	}
	return nil, false
}

func filterVector(vec promql.Vector, filters []*labels.Matcher) promql.Vector {
	var result promql.Vector
	for _, s := range vec {
		if matchesAll(s.Metric, filters) {
			result = append(result, s)
		}
	}
	return result
}

// executePrefetch runs the merged selectors via QueryFunc and populates a cache.
// Called without cache in context, so inner falls through to the real query.
func executePrefetch(ctx context.Context, plan []mergedSelect, queryFunc rules.QueryFunc, ts time.Time) *prefetchCache {
	// Remove the state from context to prevent recursion during prefetch.
	ctx = context.WithValue(ctx, selectMergerCtxKey{}, (*selectMergerState)(nil))
	cache := &prefetchCache{}
	for _, ms := range plan {
		vec, err := queryFunc(ctx, ms.prefetchExpr, ts)
		if err != nil {
			continue
		}
		cache.entries = append(cache.entries, prefetchEntry{
			matchers: ms.mergedMatchers,
			vector:   vec,
		})
	}
	return cache
}

func isMatcherSetSuperset(superMatchers, subMatchers []*labels.Matcher) bool {
	for _, sup := range superMatchers {
		found := false
		for _, sub := range subMatchers {
			if sub.Name == sup.Name {
				found = true
				if !isMatcherSuperset(sup, sub) {
					return false
				}
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func extraMatchers(entryMatchers, queryMatchers []*labels.Matcher) []*labels.Matcher {
	var result []*labels.Matcher
	for _, qm := range queryMatchers {
		isExtra := true
		for _, em := range entryMatchers {
			if em.Name == qm.Name && em.Type == qm.Type && em.Value == qm.Value {
				isExtra = false
				break
			}
		}
		if isExtra {
			result = append(result, qm)
		}
	}
	return result
}

func matchesAll(lset labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lset.Get(m.Name)) {
			return false
		}
	}
	return true
}

func extractSelectorsFromExpr(qs string) [][]*labels.Matcher {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil
	}
	var result [][]*labels.Matcher
	extractSelectors(expr, func(vs *parser.VectorSelector) {
		result = append(result, vs.LabelMatchers)
	})
	return result
}
