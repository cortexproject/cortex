package ring

import (
	"testing"
)

func TestGenerateTokens(t *testing.T) {
	tokens := GenerateTokens(1000000, nil)

	dups := make(map[uint32]int)

	for ix, v := range tokens {
		if ox, ok := dups[v]; ok {
			t.Errorf("Found duplicate token %d, tokens[%d]=%d, tokens[%d]=%d", v, ix, tokens[ix], ox, tokens[ox])
		} else {
			dups[v] = ix
		}
	}
}

func TestGenerateTokensIgnoresOldTokens(t *testing.T) {
	first := GenerateTokens(1000000, nil)
	second := GenerateTokens(1000000, first)

	dups := make(map[uint32]bool)

	for _, v := range first {
		dups[v] = true
	}

	for _, v := range second {
		if dups[v] {
			t.Fatal("GenerateTokens returned old token")
		}
	}
}
