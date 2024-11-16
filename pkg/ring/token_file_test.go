package ring

import (
	"encoding/json"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenFile_Serialization(t *testing.T) {
	tokens := make(Tokens, 0, 512)
	for i := 0; i < 512; i++ {
		tokens = append(tokens, uint32(rand.Int31()))
	}
	tokenFile := TokenFile{
		PreviousState: READONLY,
		Tokens:        tokens,
	}
	b, err := json.Marshal(tokenFile)
	require.NoError(t, err)

	unmarshaledTokenFile := TokenFile{}
	require.NoError(t, json.Unmarshal(b, &unmarshaledTokenFile))
	require.Equal(t, tokens, unmarshaledTokenFile.Tokens)
	require.Equal(t, READONLY, unmarshaledTokenFile.PreviousState)
}

func TestTokenFile_Serialization_ForwardCompatibility(t *testing.T) {
	tokens := make(Tokens, 0, 512)
	for i := 0; i < 512; i++ {
		tokens = append(tokens, uint32(rand.Int31()))
	}
	b, err := oldMarshal(tokens)
	require.NoError(t, err)

	unmarshaledTokenFile := TokenFile{}
	require.NoError(t, json.Unmarshal(b, &unmarshaledTokenFile))
	require.Equal(t, tokens, unmarshaledTokenFile.Tokens)
	require.Equal(t, ACTIVE, unmarshaledTokenFile.PreviousState)
}

func TestTokenFile_Serialization_BackwardCompatibility(t *testing.T) {
	tokens := make(Tokens, 0, 512)
	for i := 0; i < 512; i++ {
		tokens = append(tokens, uint32(rand.Int31()))
	}
	tokenFile := TokenFile{
		PreviousState: READONLY,
		Tokens:        tokens,
	}
	b, err := json.Marshal(tokenFile)
	require.NoError(t, err)

	unmarshaledTokens := Tokens{}
	require.NoError(t, oldUnmarshal(b, &unmarshaledTokens))
	require.Equal(t, tokens, unmarshaledTokens)
}

func TestLoadTokenFile_ShouldGuaranteeSortedTokens(t *testing.T) {
	tmpDir := t.TempDir()

	// Store tokens to file.
	origTokens := Tokens{1, 5, 3}
	orig := TokenFile{
		Tokens: origTokens,
	}

	require.NoError(t, orig.StoreToFile(filepath.Join(tmpDir, "tokens")))

	// Read back and ensure they're sorted.
	actual, err := LoadTokenFile(filepath.Join(tmpDir, "tokens"))
	require.NoError(t, err)
	assert.Equal(t, Tokens{1, 3, 5}, actual.Tokens)
}

// Copied from removed code for compatibility test
func oldMarshal(t Tokens) ([]byte, error) {
	return json.Marshal(tokensJSON{Tokens: t})
}

// Copied from removed code for compatibility test
func oldUnmarshal(b []byte, t *Tokens) error {
	tj := tokensJSON{}
	if err := json.Unmarshal(b, &tj); err != nil {
		return err
	}
	*t = tj.Tokens
	return nil
}

// Copied from removed code for compatibility test
type tokensJSON struct {
	Tokens []uint32 `json:"tokens"`
}
