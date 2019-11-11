package ring

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

const (
	// TokenVersion1 is the version is a simple list of tokens.
	TokenVersion1 = 1
)

// Tokens is a simple list of tokens.
type Tokens []uint32

// StoreToFile stores the tokens in the given directory.
func (t Tokens) StoreToFile(tokenFilePath string) error {
	if tokenFilePath == "" {
		return errors.New("path is empty")
	}
	tmp := tokenFilePath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	defer func() {
		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(util.Logger).Log("msg", "error deleting temporary file", "err", err)
		}
	}()

	b, err := t.Marshal()
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	// Block successfully written, make visible and remove old ones.
	return fileutil.Replace(tmp, tokenFilePath)
}

// LoadFromFile loads tokens from given directory.
func (t *Tokens) LoadFromFile(tokenFilePath string) error {
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
