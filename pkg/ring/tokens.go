package ring

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
)

const (
	// TokenVersion1 is the version is a simple list of tokens.
	TokenVersion1 = 1

	// Name of the file in which tokens is stored.
	tokensFileName = "tokens"
)

// Tokens is a simple list of tokens.
type Tokens []uint32

// StoreToFile stores the tokens in the given directory.
func (t Tokens) StoreToFile(dir string) error {
	if dir == "" {
		return errors.New("directory is empty")
	}
	tokenFilePath := path.Join(dir, tokensFileName)
	f, err := os.OpenFile(tokenFilePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	b, err := t.Marshal()
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		return err
	}
	return nil
}

// LoadFromFile loads tokens from given directory.
func (t *Tokens) LoadFromFile(dir string) error {
	tokenFilePath := path.Join(dir, tokensFileName)
	b, err := ioutil.ReadFile(tokenFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return t.Unmarshal(b)
}

// Marshal encodes the tokens into JSON.
func (t Tokens) Marshal() ([]byte, error) {
	data := tokensJSON{
		Version: TokenVersion1,
		Tokens:  t,
	}
	return json.Marshal(data)
}

// Unmarshal reads the tokens from JSON byte stream.
func (t *Tokens) Unmarshal(b []byte) error {
	tj := tokensJSON{}
	if err := json.Unmarshal(b, &tj); err != nil {
		return err
	}
	switch tj.Version {
	case TokenVersion1:
		*t = Tokens(tj.Tokens)
		return nil
	default:
		return errors.New("invalid token type")
	}
}

type tokensJSON struct {
	Version int      `json:"version"`
	Tokens  []uint32 `json:"tokens"`
}
