package querysharding

import (
	"github.com/prometheus/prometheus/model/labels"
)

// ShardCannotMatchSelector returns true when the matchers of a vertically sharded vector selector
// cannot select any series that belongs to the selector's shard.
//
// With `by` sharding, storepb.ShardMatcher hashes only the names and values of the sharding labels a
// series carries. When the selector pins every sharding label to a non-empty value with an equality
// matcher, every series it selects carries exactly those sharding label values and therefore hashes
// to the same shard. That shard can be computed from the matchers alone. In every other shard the
// shard matcher would drop each selected series, so the selector can return an empty result without
// querying ingesters or store-gateways.
//
// It returns false, which keeps the current behaviour, when the matchers carry no sharding info, for
// `without` sharding, and when any sharding label has no equality matcher, an empty value, or
// conflicting equality matchers. Regex and negative matchers on a sharding label are allowed next to
// the pinning equality matcher, because they only narrow the selection.
func ShardCannotMatchSelector(matchers []*labels.Matcher) bool {
	_, shardInfo, err := ExtractShardingInfo(matchers)
	if err != nil || shardInfo == nil || shardInfo.TotalShards < 1 || !shardInfo.By || len(shardInfo.Labels) == 0 {
		return false
	}

	builder := labels.NewScratchBuilder(len(shardInfo.Labels))
	seen := make(map[string]struct{}, len(shardInfo.Labels))
	for _, name := range shardInfo.Labels {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		value, ok := pinnedValue(matchers, name)
		if !ok {
			return false
		}
		builder.Add(name, value)
	}
	builder.Sort()

	// Every series selected by the matchers carries exactly these sharding label values, so it hashes
	// the same way as this label set. Reuse the shard matcher so the hash is the one storage applies.
	shardMatcher := shardInfo.Matcher(&Buffers)
	defer shardMatcher.Close()
	return !shardMatcher.MatchesLabels(builder.Labels())
}

// pinnedValue returns the value that the matchers pin the label to with equality matchers. It returns
// false when there is no equality matcher for the label, when the value is empty (the matcher also
// selects series that do not have the label at all), or when the equality matchers disagree.
func pinnedValue(matchers []*labels.Matcher, name string) (string, bool) {
	value := ""
	found := false
	for _, m := range matchers {
		if m.Name != name || m.Type != labels.MatchEqual {
			continue
		}
		if m.Value == "" {
			return "", false
		}
		if found && m.Value != value {
			return "", false
		}
		value = m.Value
		found = true
	}
	return value, found
}
