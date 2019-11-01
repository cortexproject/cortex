package ring

import (
	"encoding/binary"
	"errors"
)

// Tokens is the interface to store Token.
type Tokens interface {
	// Encoding return the type of the token implementation.
	Encoding() TokenType
	// Add adds a new token.
	Add(...uint32)
	// Tokens returns a slice of uint32 tokens.
	Tokens() []uint32
	// Len returns the number of tokens.
	Len() int
	// Marshal marshalls all the tokens into byte stream.
	Marshal([]byte) []byte
	// Unmarshal reads the tokens from the byte stream.
	Unmarshal([]byte) error
}

// TokenType is the type identifier for the token.
type TokenType byte

const (
	// SimpleList is the type for Type1Tokens.
	SimpleList TokenType = 1
)

// SimpleListTokens is a simple list of tokens.
type SimpleListTokens []uint32

// Encoding implements the Tokens interface.
func (slt SimpleListTokens) Encoding() TokenType {
	return SimpleList
}

// Tokens implements the Tokens interface.
func (slt SimpleListTokens) Tokens() []uint32 {
	return slt
}

// Len implements the Tokens interface.
func (slt SimpleListTokens) Len() int {
	return len(slt)
}

// Add implements the Tokens interface.
func (slt *SimpleListTokens) Add(tokens ...uint32) {
	*slt = append(*slt, tokens...)
}

// Marshal implements the Tokens interface.
func (slt SimpleListTokens) Marshal(b []byte) []byte {
	if cap(b) < 1+(4*len(slt)) {
		b = make([]byte, 1+(4*len(slt)))
	}
	b = b[:1+(4*len(slt))]
	b[0] = byte(SimpleList)
	for idx, token := range slt {
		binary.BigEndian.PutUint32(b[1+(idx*4):], uint32(token))
	}
	return b
}

// Unmarshal implements the Tokens interface.
func (slt *SimpleListTokens) Unmarshal(b []byte) error {
	*slt = (*slt)[:0]
	if len(b) == 0 {
		return nil
	} else if len(b)%4 != 0 {
		return errors.New("token data is not 4 byte aligned")
	}

	numTokens := len(b) >> 2
	for i := 0; i < numTokens; i++ {
		*slt = append(*slt, binary.BigEndian.Uint32(b[i<<2:]))
	}

	return nil
}

// UnmarshalTokens converts the byte stream into Tokens.
// The first byte should be the token type follwed by the
// actual token data.
func UnmarshalTokens(b []byte) (Tokens, error) {
	if len(b) == 0 {
		return nil, errors.New("empty bytes")
	}
	switch TokenType(b[0]) {
	case SimpleList:
		tokens := &SimpleListTokens{}
		err := tokens.Unmarshal(b[1:])
		return tokens, err
	default:
		return nil, errors.New("invalid token type")
	}
}
