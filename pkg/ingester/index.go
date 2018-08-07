package ingester

import (
	"sort"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

const indexShards = 32

type invertedIndex struct {
	shards []indexShard
}

type indexShard struct {
	mtx sync.RWMutex
	idx map[model.LabelName]map[model.LabelValue][]model.Fingerprint // entries are sorted in fp order?
}

func newInvertedIndex() *invertedIndex {
	shards := make([]indexShard, indexShards)
	for i := 0; i < indexShards; i++ {
		shards[i].idx = map[model.LabelName]map[model.LabelValue][]model.Fingerprint{}
	}
	return &invertedIndex{
		shards: shards,
	}
}

func (ii *invertedIndex) add(metric model.Metric, fp model.Fingerprint) {
	i := &ii.shards[hashFP(fp)%indexShards]
	i.mtx.Lock()
	defer i.mtx.Unlock()

	for name, value := range metric {
		values, ok := i.idx[name]
		if !ok {
			values = map[model.LabelValue][]model.Fingerprint{}
		}
		fingerprints := values[value]
		j := sort.Search(len(fingerprints), func(i int) bool {
			return fingerprints[i] >= fp
		})
		fingerprints = append(fingerprints, 0)
		copy(fingerprints[j+1:], fingerprints[j:])
		fingerprints[j] = fp
		values[value] = fingerprints
		i.idx[name] = values
	}
}

func (ii *invertedIndex) lookup(matchers []*labels.Matcher) []model.Fingerprint {
	if len(matchers) == 0 {
		return nil
	}

	result := []model.Fingerprint{}

outer:
	for i := range ii.shards {
		ii.shards[i].mtx.RLock()

		// per-shard intersection is initially nil, which is a special case.
		var intersection []model.Fingerprint
		for _, matcher := range matchers {
			values, ok := ii.shards[i].idx[model.LabelName(matcher.Name)]
			if !ok {
				continue outer
			}
			var toIntersect []model.Fingerprint
			if matcher.Type == labels.MatchEqual {
				fps := values[model.LabelValue(matcher.Value)]
				toIntersect = merge(toIntersect, fps)
			} else {
				for value, fps := range values {
					if matcher.Matches(string(value)) {
						toIntersect = merge(toIntersect, fps)
					}
				}
			}
			intersection = intersect(intersection, toIntersect)
			if len(intersection) == 0 {
				continue outer
			}
		}

		ii.shards[i].mtx.RUnlock()
		result = append(result, intersection...)
	}

	sort.Sort(fingerprints(result))
	return result
}

func (ii *invertedIndex) lookupLabelValues(name model.LabelName) model.LabelValues {
	results := make([]model.LabelValues, 0, indexShards)

	for i := range ii.shards {
		ii.shards[i].mtx.RLock()
		values, ok := ii.shards[i].idx[name]
		if !ok {
			continue
		}
		shardResult := make(model.LabelValues, 0, len(values))
		for val := range values {
			shardResult = append(shardResult, val)
		}
		ii.shards[i].mtx.RUnlock()

		sort.Sort(labelValues(shardResult))
		results = append(results, shardResult)
	}

	return mergeLabelValueLists(results)
}

func (ii *invertedIndex) delete(metric model.Metric, fp model.Fingerprint) {
	i := &ii.shards[hashFP(fp)%indexShards]
	i.mtx.Lock()
	defer i.mtx.Unlock()

	for name, value := range metric {
		values, ok := i.idx[name]
		if !ok {
			continue
		}
		fingerprints, ok := values[value]
		if !ok {
			continue
		}

		j := sort.Search(len(fingerprints), func(i int) bool {
			return fingerprints[i] >= fp
		})
		fingerprints = fingerprints[:j+copy(fingerprints[j:], fingerprints[j+1:])]

		if len(fingerprints) == 0 {
			delete(values, value)
		} else {
			values[value] = fingerprints
		}

		if len(values) == 0 {
			delete(i.idx, name)
		} else {
			i.idx[name] = values
		}
	}
}

// intersect two sorted lists of fingerprints.  Assumes there are no duplicate
// fingerprints within the input lists.
func intersect(a, b []model.Fingerprint) []model.Fingerprint {
	if a == nil {
		return b
	}
	result := []model.Fingerprint{}
	for i, j := 0, 0; i < len(a) && j < len(b); {
		if a[i] == b[j] {
			result = append(result, a[i])
		}
		if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// merge two sorted lists of fingerprints.  Assumes there are no duplicate
// fingerprints between or within the input lists.
func merge(a, b []model.Fingerprint) []model.Fingerprint {
	result := make([]model.Fingerprint, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

type labelValues model.LabelValues

func (a labelValues) Len() int           { return len(a) }
func (a labelValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a labelValues) Less(i, j int) bool { return a[i] < a[j] }

type fingerprints []model.Fingerprint

func (a fingerprints) Len() int           { return len(a) }
func (a fingerprints) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a fingerprints) Less(i, j int) bool { return a[i] < a[j] }

func mergeLabelValueLists(lvss []model.LabelValues) model.LabelValues {
	switch len(lvss) {
	case 0:
		return nil
	case 1:
		return lvss[0]
	case 2:
		return mergeTwoLabelValueLists(lvss[0], lvss[1])
	default:
		n := len(lvss) / 2
		left := mergeLabelValueLists(lvss[:n])
		right := mergeLabelValueLists(lvss[n:])
		return mergeTwoLabelValueLists(left, right)
	}
}

func mergeTwoLabelValueLists(a, b model.LabelValues) model.LabelValues {
	result := make(model.LabelValues, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}
