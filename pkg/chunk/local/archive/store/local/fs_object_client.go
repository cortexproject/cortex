package local

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/util"
)

// ObjectClient for managing objects in a gcs bucket
type ObjectClient struct {
	cfg Config
}

// Config is config for the local object Client.
type Config struct {
	Directory string `yaml:"directory"`
}

// NewLocalObjectObjectClient makes a new chunk.ObjectClient that writes objects to local FS.
func NewLocalObjectObjectClient(cfg Config) (*ObjectClient, error) {
	return &ObjectClient{cfg: cfg}, nil
}

// Get object from the store
func (s *ObjectClient) Get(ctx context.Context, objectName string) ([]byte, error) {
	fullPath := filepath.Join(s.cfg.Directory, objectName)
	return ioutil.ReadFile(fullPath)
}

// Put object into the store
func (s *ObjectClient) Put(ctx context.Context, objectName string, object io.ReadSeeker) error {
	buf, err := ioutil.ReadAll(object)
	if err != nil {
		return err
	}

	fullPath := filepath.Join(s.cfg.Directory, objectName)
	err = util.EnsureDirectory(filepath.Dir(fullPath))
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(fullPath, buf, 0644); err != nil {
		return err
	}
	return err
}

// List objects from the store
func (s *ObjectClient) List(ctx context.Context, prefix string) (map[string]time.Time, error) {
	folderPath := filepath.Join(s.cfg.Directory, prefix)

	_, err := os.Stat(folderPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	filesInfo, err := ioutil.ReadDir(folderPath)
	if err != nil {
		return nil, err
	}

	files := map[string]time.Time{}
	for _, fileInfo := range filesInfo {
		if fileInfo.IsDir() {
			continue
		}
		files[fileInfo.Name()] = fileInfo.ModTime()
	}

	return files, nil
}
