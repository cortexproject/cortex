package ring

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
)

type TokenFile struct {
	PreviousState InstanceState `json:"previousState,omitempty"`
	Tokens        Tokens        `json:"tokens"`
}

// StoreToFile stores the tokens in the given directory.
func (l TokenFile) StoreToFile(tokenFilePath string) error {
	if tokenFilePath == "" {
		return errors.New("path is empty")
	}

	// If any operations failed further in the function, we keep the temporary
	// file hanging around for debugging.
	f, err := os.Create(tokenFilePath + ".tmp")
	if err != nil {
		return err
	}

	defer func() {
		// If the file was not closed, then there must already be an error, hence ignore
		// the error (if any) from f.Close(). If the file was already closed, then
		// we would ignore the error in that case too.
		_ = f.Close()
	}()

	b, err := json.Marshal(l)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	// Tokens successfully written, replace the temporary file with the actual file path.
	return os.Rename(f.Name(), tokenFilePath)
}

func LoadTokenFile(tokenFilePath string) (*TokenFile, error) {
	b, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return nil, err
	}
	t := TokenFile{}
	err = json.Unmarshal(b, &t)

	// Tokens may have been written to file by an older version which
	// doesn't guarantee sorted tokens, so we enforce sorting here.
	if !sort.IsSorted(t.Tokens) {
		sort.Sort(t.Tokens)
	}

	return &t, err
}

func (p InstanceState) MarshalJSON() ([]byte, error) {
	ss := InstanceState_name[int32(p)]
	return json.Marshal(ss)
}
func (p *InstanceState) UnmarshalJSON(data []byte) error {
	res := ""
	if err := json.Unmarshal(data, &res); err != nil {
		return err
	}
	*p = InstanceState(InstanceState_value[res])
	return nil
}
