package ruler

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
)

type mergedSelect struct {
	metricName      string
	mergedMatchers  []*labels.Matcher
	originalEntries [][]*labels.Matcher // per-rule matchers
	prefetchExpr    string              // expression to evaluate for pre-fetch
}

func planMergedSelects(rls []rules.Rule, minRules int) []mergedSelect {
	// Group selectors by metric name AND expression structure.
	// The "structure key" is the expression with matchers blanked out,
	// so rate(m{a="1"}[5m]) and rate(m{a="2"}[5m]) share a key but
	// sum(m{a="1"}) does not.
	type entry struct {
		matchers []*labels.Matcher
		exprStr  string
	}
	// Key: metricName + "\x00" + expression structure
	groups := map[string][]entry{}

	for _, r := range rls {
		exprStr := r.Query().String()
		extractSelectors(r.Query(), func(vs *parser.VectorSelector) {
			name := metricNameFromMatchers(vs.LabelMatchers)
			if name == "" {
				return
			}
			key := name + "\x00" + exprStructureKey(r.Query(), vs)
			groups[key] = append(groups[key], entry{matchers: vs.LabelMatchers, exprStr: exprStr})
		})
	}

	var result []mergedSelect
	for _, entries := range groups {
		if len(entries) < minRules {
			continue
		}
		originals := make([][]*labels.Matcher, len(entries))
		for i, e := range entries {
			originals[i] = e.matchers
		}
		merged := computeMergedMatchers(originals)

		// Find the expression whose matchers equal the merged set (the broadest rule).
		// If none found, skip — we can't safely pre-fetch without a full expression.
		prefetchExpr := ""
		for _, e := range entries {
			if matchersEqual(e.matchers, merged) {
				prefetchExpr = e.exprStr
				break
			}
		}
		if prefetchExpr == "" {
			continue
		}

		name := metricNameFromMatchers(merged)
		result = append(result, mergedSelect{
			metricName:      name,
			mergedMatchers:  merged,
			originalEntries: originals,
			prefetchExpr:    prefetchExpr,
		})
	}
	return result
}

// exprStructureKey returns a string representing the expression structure
// with the VectorSelector's matchers replaced by a placeholder.
// Two expressions with the same structure key differ only in their matchers.
func exprStructureKey(expr parser.Expr, vs *parser.VectorSelector) string {
	full := expr.String()
	selectorStr := vs.String()
	// Replace the specific selector with a placeholder.
	return strings.Replace(full, selectorStr, "{__PLACEHOLDER__}", 1)
}

// matchersEqual returns true if two matcher slices contain the same matchers (order-independent).
func matchersEqual(a, b []*labels.Matcher) bool {
	if len(a) != len(b) {
		return false
	}
	for _, am := range a {
		found := false
		for _, bm := range b {
			if am.Name == bm.Name && am.Type == bm.Type && am.Value == bm.Value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func extractSelectors(expr parser.Expr, fn func(*parser.VectorSelector)) {
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok {
			fn(vs)
		}
		return nil
	})
}

func metricNameFromMatchers(ms []*labels.Matcher) string {
	for _, m := range ms {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			return m.Value
		}
	}
	return ""
}

func computeMergedMatchers(entries [][]*labels.Matcher) []*labels.Matcher {
	// Collect all label names across entries, but only keep labels
	// that appear in ALL entries. A label missing from any entry means
	// that entry matches all values for that label — including it in the
	// merged set would make the pre-fetch too restrictive.
	labelMatchers := map[string][]*labels.Matcher{}
	for _, ms := range entries {
		for _, m := range ms {
			labelMatchers[m.Name] = append(labelMatchers[m.Name], m)
		}
	}

	var result []*labels.Matcher
	for _, ms := range labelMatchers {
		// Only include if present in ALL entries.
		if len(ms) != len(entries) {
			continue
		}
		if sup := findSuperset(ms); sup != nil {
			result = append(result, sup)
		}
	}
	return result
}

func findSuperset(ms []*labels.Matcher) *labels.Matcher {
	for _, candidate := range ms {
		coversAll := true
		for _, other := range ms {
			if !isMatcherSuperset(candidate, other) {
				coversAll = false
				break
			}
		}
		if coversAll {
			return candidate
		}
	}
	return nil
}

func isMatcherSuperset(a, b *labels.Matcher) bool {
	if a.Name != b.Name {
		return false
	}
	// =~".*" covers everything.
	if a.Type == labels.MatchRegexp && a.Value == ".*" {
		return true
	}
	// =~".+" covers any non-empty value.
	if a.Type == labels.MatchRegexp && a.Value == ".+" {
		if b.Type == labels.MatchEqual && b.Value == "" {
			return false
		}
		return true
	}
	// Same type and value covers itself.
	return a.Type == b.Type && a.Value == b.Value
}
