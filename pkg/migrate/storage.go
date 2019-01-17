package migrate

import (
	"context"
	"fmt"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cassandra"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
)

// NewStorage makes a new table client based on the configuration.
func NewStorage(name string, cfg storage.Config) (chunk.ObjectClient, error) {
	switch name {
	case "inmemory":
		return nil, fmt.Errorf("inmemory reader storage client not implemented currently")
	case "aws", "aws-dynamo":
		return nil, fmt.Errorf("inmemory reader storage client not implemented currently")
	case "gcp", "gcp-columnkey":
		return gcp.NewBigtableObjectClient(context.Background(), cfg.GCPStorageConfig, chunk.SchemaConfig{})
	case "cassandra":
		return cassandra.NewStorageClient(cfg.CassandraStorageConfig, chunk.SchemaConfig{})
	default:
		return nil, fmt.Errorf("Unrecognized storage client %v, choose one of: aws, gcp, inmemory", name)
	}
}
