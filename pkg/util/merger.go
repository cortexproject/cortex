package util

import "github.com/prometheus/common/model"

// MergeSampleSets merges and dedupes two sets of already sorted sample pairs.
func MergeSampleSets(a, b []model.SamplePair) []model.SamplePair {
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

// MergeNSampleSets merges and dedupes n sets of already sorted sample pairs.
func MergeNSampleSets(sampleSets ...[]model.SamplePair) []model.SamplePair {
	l := len(sampleSets)
	switch l {
	case 0:
		return []model.SamplePair{}
	case 1:
		return sampleSets[0]
	}

	n := l / 2
	left := MergeNSampleSets(sampleSets[:n]...)
	right := MergeNSampleSets(sampleSets[n:]...)
	return MergeSampleSets(left, right)
}

func MergeSortedStringLists(lnss [][]string) []string {
	switch len(lnss) {
	case 0:
		return nil
	case 1:
		return lnss[0]
	case 2:
		return MergeTwoSortedStringLists(lnss[0], lnss[1])
	default:
		n := len(lnss) / 2
		left := MergeSortedStringLists(lnss[:n])
		right := MergeSortedStringLists(lnss[n:])
		return MergeTwoSortedStringLists(left, right)
	}
}

func MergeTwoSortedStringLists(a, b []string) []string {
	result := make([]string, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else if a[i] > b[j] {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, b[j])
			i++
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}
