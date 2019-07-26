package store

import (
	"context"
	"io"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/local/archive/store/gcs"
)

// Config for stores
type Config struct {
	Store     string     `yaml:"store"`
	GCSConfig gcs.Config `yaml:"gcs"`
}

// NewArchiveStoreClient creates a client for store that is configured to be used
func NewArchiveStoreClient(cfg Config) (ArchiveStoreClient, error) {
	switch cfg.Store {
	case "gcs":
		return gcs.NewGCSObjectClient(context.Background(), cfg.GCSConfig)
	}

	return nil, nil
}

// ArchiveStoreClient define all the methods that a store needs to implement for managing objects
type ArchiveStoreClient interface {
	Get(ctx context.Context, objectName string) ([]byte, error)
	Put(ctx context.Context, objectName string, object io.Reader) error
	List(ctx context.Context, prefix string) (map[string]time.Time, error)
}
