package util

import "github.com/prometheus/common/model"

// MergeSamples merges and dedupes two sets of already sorted sample pairs.
func MergeSamples(a, b []model.SamplePair) []model.SamplePair {
	result := make([]model.SamplePair, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Timestamp < b[j].Timestamp {
			result = append(result, a[i])
			i++
		} else if a[i].Timestamp > b[j].Timestamp {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
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

// MergeNSamples merges and dedupes n sets of already sorted sample pairs.
func MergeNSamples(sampleSets ...[]model.SamplePair) []model.SamplePair {
	l := len(sampleSets)
	switch l {
	case 0:
		return []model.SamplePair{}
	case 1:
		return sampleSets[0]
	}

	c := make(chan []model.SamplePair, l)
	for _, ss := range sampleSets {
		c <- ss
	}
	for ; l > 1; l-- {
		left, right := <-c, <-c
		go func() {
			c <- MergeSamples(left, right)
		}()
	}
	return <-c
}
