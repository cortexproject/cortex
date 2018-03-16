package chunk

import "context"

// StorageClient is a client for the persistent storage for Cortex. (e.g. DynamoDB + S3).
type StorageClient interface {
	// For the write path.
	NewWriteBatch() WriteBatch
	BatchWrite(context.Context, WriteBatch) error

	// This retrieves the query result in batches and passes it to the callback. The return value of the callback would determine if more data should be retrieved.
	QueryPages(ctx context.Context, query IndexQuery, callback func(result ReadBatch, lastPage bool) (shouldContinue bool)) error

	// For storing and retrieving chunks.
	PutChunks(ctx context.Context, chunks []Chunk) error
	GetChunks(ctx context.Context, chunks []Chunk) ([]Chunk, error)
}

// WriteBatch represents a batch of writes.
type WriteBatch interface {
	Add(tableName, hashValue string, rangeValue []byte, value []byte)
}

// ReadBatch represents the results of a QueryPages.
type ReadBatch interface {
	// The total number of entries in this batch.
	Len() int
	// The RangeValue key and Value for the index.
	RangeValue(index int) []byte
	Value(index int) []byte
}

// Fixture type for per-backend testing.
type Fixture interface {
	Name() string
	Clients() (StorageClient, TableClient, SchemaConfig, error)
	Teardown() error
}
