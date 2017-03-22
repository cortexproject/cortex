package chunk

import (
	"flag"
	"fmt"
	"strings"

	"github.com/prometheus/common/log"
	"golang.org/x/net/context"
)

// StorageClient is a client for the persistent storage for Cortex. (e.g. DynamoDB + S3).
type StorageClient interface {
	// For the write path.
	NewWriteBatch() WriteBatch
	BatchWrite(context.Context, WriteBatch) error

	// For the read path.
	QueryPages(ctx context.Context, entry IndexEntry, callback func(result ReadBatch, lastPage bool) (shouldContinue bool)) error

	// For storing and retrieving chunks.
	PutChunk(ctx context.Context, key string, data []byte) error
	GetChunk(ctx context.Context, key string) ([]byte, error)
}

// WriteBatch represents a batch of writes.
type WriteBatch interface {
	Add(tableName, hashValue string, rangeValue []byte, value []byte)
}

// ReadBatch represents the results of a QueryPages.
type ReadBatch interface {
	Len() int
	RangeValue(index int) []byte
	Value(index int) []byte
}

// StorageClientConfig chooses which storage client to use.
type StorageClientConfig struct {
	StorageClient string
	AWSStorageConfig
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *StorageClientConfig) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.StorageClient, "chunk.storage-client", "aws", "Which storage client to use (aws, inmemory).")
	cfg.AWSStorageConfig.RegisterFlags(f)
}

// NewStorageClient makes a storage client based on the configuration.
func NewStorageClient(cfg StorageClientConfig) (StorageClient, string, error) {
	switch cfg.StorageClient {
	case "inmemory":
		return NewMockStorage(), "", nil
	case "aws":
		// TODO(jml): Remove this once deprecation period expires - 2017-03-21.
		tableName := strings.TrimPrefix(cfg.DynamoDB.URL.Path, "/")
		if len(tableName) > 0 {
			log.Warnf("Specifying fallback table name in DynamoDB URL is deprecated.")
		}
		client, err := NewAWSStorageClient(cfg.AWSStorageConfig)
		return client, tableName, err
	default:
		return nil, "", fmt.Errorf("Unrecognized storage client %v, choose one of: aws, inmemory", cfg.StorageClient)
	}
}
