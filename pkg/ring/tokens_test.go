package ring

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokens_Serialization(t *testing.T) {
	tokens := make(Tokens, 512)
	for i := 0; i < 512; i++ {
		tokens = append(tokens, uint32(rand.Int31()))
	}

	b, err := tokens.Marshal()
	require.NoError(t, err)

	var unmarshaledTokens Tokens
	require.NoError(t, unmarshaledTokens.Unmarshal(b))
	require.Equal(t, tokens, unmarshaledTokens)
}

func TestTokens_Equals(t *testing.T) {
	tests := []struct {
		first    Tokens
		second   Tokens
		expected bool
	}{
		{
			first:    Tokens{},
			second:   Tokens{},
			expected: true,
		},
		{
			first:    Tokens{1, 2, 3},
			second:   Tokens{1, 2, 3},
			expected: true,
		},
		{
			first:    Tokens{1, 2, 3},
			second:   Tokens{3, 2, 1},
			expected: true,
		},
		{
			first:    Tokens{1, 2},
			second:   Tokens{1, 2, 3},
			expected: false,
		},
	}

	for _, c := range tests {
		assert.Equal(t, c.expected, c.first.Equals(c.second))
		assert.Equal(t, c.expected, c.second.Equals(c.first))
	}
}
