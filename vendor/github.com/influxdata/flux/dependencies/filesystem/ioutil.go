package filesystem

import "io/ioutil"

// ReadFile will open the file from the service and read
// the entire contents.
func ReadFile(fs Service, filename string) ([]byte, error) {
	f, err := fs.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return ioutil.ReadAll(f)
}
