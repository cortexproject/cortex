package chunk

import (
	"bytes"
	"sort"
)

// UniqueStrings keeps a slice of unique strings.
type UniqueStrings struct {
	values map[string]struct{}
	result []string
}

// Add adds a new string, dropping duplicates.
func (us *UniqueStrings) Add(strings ...string) {
	for _, s := range strings {
		if _, ok := us.values[s]; ok {
			continue
		}
		if us.values == nil {
			us.values = map[string]struct{}{}
		}
		us.values[s] = struct{}{}
		us.result = append(us.result, s)
	}
}

// Strings returns the sorted sliced of unique strings.
func (us UniqueStrings) Strings() []string {
	sort.Strings(us.result)
	return us.result
}

// implement `Interface` in sort package.
type sortByteArrays [][]byte

func (b sortByteArrays) Len() int           { return len(b) }
func (b sortByteArrays) Less(i, j int) bool { return bytes.Compare(b[i], b[j]) < 0 }
func (b sortByteArrays) Swap(i, j int)      { b[j], b[i] = b[i], b[j] }

// SortByteArrays sort a slice of []byte lexicographically.
func SortByteArrays(src [][]byte) [][]byte {
	sorted := sortByteArrays(src)
	sort.Sort(sorted)
	return sorted
}

func uniqueByteArrays(cs [][]byte) [][]byte {
	if len(cs) == 0 {
		return [][]byte{}
	}

	result := make([][]byte, 1, len(cs))
	result[0] = cs[0]
	i, j := 0, 1
	for j < len(cs) {
		if bytes.Equal(result[i], cs[j]) {
			j++
			continue
		}
		result = append(result, cs[j])
		i++
		j++
	}
	return result
}

func intersectByteArrays(left, right [][]byte) [][]byte {
	var (
		i, j   = 0, 0
		result = [][]byte{}
	)
	for i < len(left) && j < len(right) {
		if bytes.Equal(left[i], right[j]) {
			result = append(result, left[i])
		}

		if bytes.Compare(left[i], right[j]) == -1 {
			i++
		} else {
			j++
		}
	}
	return result
}
