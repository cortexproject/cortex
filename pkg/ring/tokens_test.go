package ring

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenSerialization(t *testing.T) {
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
