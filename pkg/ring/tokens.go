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
	// TokensVersion1 is the version is a simple list of tokens.
	TokensVersion1 = 1
)

// Tokens is a simple list of tokens.
type Tokens []uint32

// StoreToFile stores the tokens in the given directory.
func (t Tokens) StoreToFile(tokenFilePath string) error {
	if tokenFilePath == "" {
		return errors.New("path is empty")
	}

	f, err := ioutil.TempFile(os.TempDir(), "tokens")
	if err != nil {
		return err
	}

	defer func() {
		// If the file was not closed, then there must already be an error, hence ignore
		// the error (if any) from f.Close(). If the file was already closed, then
		// we would ignore the error in that case too.
		f.Close()
		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		if err := os.RemoveAll(f.Name()); err != nil {
			level.Warn(util.Logger).Log("msg", "error deleting temporary file", "err", err)
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
	return fileutil.Replace(f.Name(), tokenFilePath)
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
		Version: TokensVersion1,
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
	case TokensVersion1:
		*t = Tokens(tj.Tokens)
		return nil
	default:
		return errors.New("invalid token version")
	}
}

type tokensJSON struct {
	Version int      `json:"version"`
	Tokens  []uint32 `json:"tokens"`
}
