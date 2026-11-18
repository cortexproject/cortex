package querysharding

import (
	"slices"

	"github.com/prometheus/prometheus/model/labels"
)

// maxShardingValueCombinations caps the number of sharding label value combinations checked for a
// selector. Above it the selector is queried in every shard, as before.
const maxShardingValueCombinations = 256

// ShardCannotMatchSelector returns true when the matchers of a vertically sharded vector selector
// cannot select any series that belongs to the selector's shard.
//
// With `by` sharding, storepb.ShardMatcher hashes only the names and values of the sharding labels a
// series carries. When the matchers restrict every sharding label to a finite set of non-empty values,
// through an equality matcher or a regex matcher that resolves to a set of literals such as `a|b`,
// every series the selector selects carries one combination of those values. The shards of those
// combinations can be computed from the matchers alone. In every other shard the shard matcher would
// drop each selected series, so the selector can return an empty result without querying ingesters
// or store-gateways.
//
// It returns false, which keeps the current behaviour, when the matchers carry no sharding info, for
// `without` sharding, and when any sharding label is not restricted to a finite set of non-empty
// values, for example with no matcher, only negative matchers, a regex that is not a set of literals,
// a case-insensitive regex, or a set that contains the empty value (which also selects series without
// the label). Other matchers on a sharding label are applied to the candidate values, because they
// only narrow the selection.
func ShardCannotMatchSelector(matchers []*labels.Matcher) bool {
	_, shardInfo, err := ExtractShardingInfo(matchers)
	// With a single shard every series belongs to it, so there is nothing to skip.
	if err != nil || shardInfo == nil || shardInfo.TotalShards <= 1 || !shardInfo.By || len(shardInfo.Labels) == 0 {
		return false
	}

	names := slices.Clone(shardInfo.Labels)
	slices.Sort(names)
	names = slices.Compact(names)

	values := make([][]string, 0, len(names))
	combinations := 1
	for _, name := range names {
		candidates, ok := candidateValues(matchers, name)
		if !ok {
			return false
		}
		combinations *= len(candidates)
		if combinations > maxShardingValueCombinations {
			return false
		}
		values = append(values, candidates)
	}

	shardMatcher := shardInfo.Matcher(&Buffers)
	defer shardMatcher.Close()

	// Every series selected by the matchers carries one combination of the candidate values, so it
	// hashes the same way as the label set of that combination. Reuse the shard matcher so the hash is
	// the one storage applies.
	builder := labels.NewScratchBuilder(len(names))
	indexes := make([]int, len(names))
	for {
		builder.Reset()
		for i, name := range names {
			builder.Add(name, values[i][indexes[i]])
		}
		// names is sorted, so the label set is sorted too.
		if shardMatcher.MatchesLabels(builder.Labels()) {
			return false
		}

		// Advance to the next combination.
		i := len(indexes) - 1
		for ; i >= 0; i-- {
			indexes[i]++
			if indexes[i] < len(values[i]) {
				break
			}
			indexes[i] = 0
		}
		if i < 0 {
			return true
		}
	}
}

// candidateValues returns the finite set of values that the matchers allow for the label. It returns
// false when the matchers do not restrict the label to a finite set of literals, when the set contains
// the empty value (the matchers then also select series that do not have the label at all), or when
// the set is empty.
func candidateValues(matchers []*labels.Matcher, name string) ([]string, bool) {
	var candidates []string
	restricted := false
	for _, m := range matchers {
		if m.Name != name {
			continue
		}
		var set []string
		switch m.Type {
		case labels.MatchEqual:
			set = []string{m.Value}
		case labels.MatchRegexp:
			// SetMatches is only set when the regex matches exactly that set of case-sensitive literals.
			set = m.SetMatches()
		}
		if len(set) == 0 {
			continue
		}
		if !restricted {
			candidates = set
			restricted = true
			continue
		}
		candidates = slices.DeleteFunc(candidates, func(v string) bool { return !slices.Contains(set, v) })
	}
	if !restricted {
		return nil, false
	}

	// Apply every matcher on the label, including negative and non-literal regex matchers.
	candidates = slices.DeleteFunc(candidates, func(v string) bool {
		for _, m := range matchers {
			if m.Name == name && !m.Matches(v) {
				return true
			}
		}
		return false
	})
	slices.Sort(candidates)
	candidates = slices.Compact(candidates)

	// An empty result means the selector selects no series at all. Leave that to storage.
	if len(candidates) == 0 || slices.Contains(candidates, "") {
		return nil, false
	}
	return candidates, true
}
