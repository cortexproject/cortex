package ring

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// PrintableRanges wraps a slice of TokenRanges and provides a String
// method so it can be printed and displayed to the user.
type PrintableRanges []TokenRange

func (r PrintableRanges) String() string {
	strs := make([]string, len(r))
	for i, rg := range r {
		strs[i] = fmt.Sprintf("(%d, %d)", rg.From, rg.To)
	}
	return strings.Join(strs, ", ")
}

// GenerateTokens make numTokens unique random tokens, none of which clash
// with takenTokens.
//
// GenerateTokens is the default implementation of TokenGeneratorFunc.
func GenerateTokens(numTokens int, takenTokens []uint32) []uint32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	used := make(map[uint32]bool)
	for _, v := range takenTokens {
		used[v] = true
	}

	tokens := []uint32{}

	for i := 0; i < numTokens; {
		candidate := r.Uint32()
		if used[candidate] {
			continue
		}
		used[candidate] = true
		tokens = append(tokens, candidate)
		i++
	}
	return tokens
}
