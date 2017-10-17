package ingester

import (
	"sort"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

type invertedIndex struct {
	mtx sync.RWMutex
	idx map[model.LabelName]map[model.LabelValue][]model.Fingerprint // entries are sorted in fp order?
}

func newInvertedIndex() *invertedIndex {
	return &invertedIndex{
		idx: map[model.LabelName]map[model.LabelValue][]model.Fingerprint{},
	}
}

func (i *invertedIndex) add(metric model.Metric, fp model.Fingerprint) {
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

func (i *invertedIndex) lookup(matchers []*labels.Matcher) []model.Fingerprint {
	if len(matchers) == 0 {
		return nil
	}
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	// intersection is initially nil, which is a special case.
	var intersection []model.Fingerprint
	for _, matcher := range matchers {
		values, ok := i.idx[model.LabelName(matcher.Name)]
		if !ok {
			return nil
		}
		var toIntersect []model.Fingerprint
		for value, fps := range values {
			if matcher.Matches(string(value)) {
				toIntersect = merge(toIntersect, fps)
			}
		}
		intersection = intersect(intersection, toIntersect)
		if len(intersection) == 0 {
			return nil
		}
	}

	return intersection
}

func (i *invertedIndex) lookupLabelValues(name model.LabelName) model.LabelValues {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	values, ok := i.idx[name]
	if !ok {
		return nil
	}
	res := make(model.LabelValues, 0, len(values))
	for val := range values {
		res = append(res, val)
	}
	return res
}

func (i *invertedIndex) delete(metric model.Metric, fp model.Fingerprint) {
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
	for ; i < len(a); i++ {
		result = append(result, a[i])
	}
	for ; j < len(b); j++ {
		result = append(result, b[j])
	}
	return result
}
