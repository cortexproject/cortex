package filesystem

import (
	"io"
	"os"
)

// File is an interface for interacting with a file.
type File interface {
	io.ReadWriteCloser
	io.Seeker
	Stat() (os.FileInfo, error)
}

// Service is the service for accessing the filesystem.
type Service interface {
	Open(fpath string) (File, error)
	Create(fpath string) (File, error)
	Stat(fpath string) (os.FileInfo, error)
}
