package filesystem

import "os"

// SystemFS implements the filesystem.Service by proxying all requests
// to the filesystem.
var SystemFS Service = systemFS{}

type systemFS struct{}

func (systemFS) Open(fpath string) (File, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (systemFS) Create(fpath string) (File, error) {
	f, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (systemFS) Stat(fpath string) (os.FileInfo, error) {
	return os.Stat(fpath)
}
