package ring

import (
	"sort"
)

// Tokens is a simple list of tokens.
type Tokens []uint32

func (t Tokens) Len() int           { return len(t) }
func (t Tokens) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t Tokens) Less(i, j int) bool { return t[i] < t[j] }

// Equals returns whether the tokens are equal to the input ones.
func (t Tokens) Equals(other Tokens) bool {
	if len(t) != len(other) {
		return false
	}

	mine := t
	sort.Sort(mine)
	sort.Sort(other)

	for i := 0; i < len(mine); i++ {
		if mine[i] != other[i] {
			return false
		}
	}

	return true
}
