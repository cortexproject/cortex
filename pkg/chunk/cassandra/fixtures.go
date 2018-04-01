package cassandra

import (
	"context"
	"os"

	"github.com/weaveworks/cortex/pkg/chunk"
)

// GOCQL doesn't provide nice mocks, so we use a real Cassandra instance.
// To enable these tests:
// $ docker run --name cassandra --rm -p 9042:9042 cassandra:3.11
// $ CASSANDRA_TEST_ADDRESSES=localhost:9042 go test ./pkg/chunk/storage

type fixture struct {
	name          string
	storageClient chunk.StorageClient
	tableClient   chunk.TableClient
	schemaConfig  chunk.SchemaConfig
}

func (f fixture) Name() string {
	return f.name
}

func (f fixture) Clients() (chunk.StorageClient, chunk.TableClient, chunk.SchemaConfig, error) {
	return f.storageClient, f.tableClient, f.schemaConfig, nil
}

func (f fixture) Teardown() error {
	return nil
}

func Fixtures() ([]chunk.Fixture, error) {
	addresses := os.Getenv("CASSANDRA_TEST_ADDRESSES")
	if addresses == "" {
		return nil, nil
	}

	cfg := Config{
		addresses:         addresses,
		keyspace:          "test-keyspace",
		consistency:       "QUORUM",
		replicationFactor: 1,
	}
	schemaConfig := chunk.SchemaConfig{}

	storageClient, err := NewStorageClient(cfg, schemaConfig)
	if err != nil {
		return nil, err
	}

	tableClient, err := NewTableClient(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	return []chunk.Fixture{
		fixture{
			name:          "Cassandra",
			storageClient: storageClient,
			tableClient:   tableClient,
			schemaConfig:  schemaConfig,
		},
	}, nil
}
