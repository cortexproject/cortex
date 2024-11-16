package ring

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
