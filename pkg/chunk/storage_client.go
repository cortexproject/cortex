package chunk

import "context"

// StorageClient is a client for the persistent storage for Cortex. (e.g. DynamoDB + S3).
type StorageClient interface {
	Stop()

	// For the write path.
	NewWriteBatch() WriteBatch
	BatchWrite(context.Context, WriteBatch) error
	BatchWriteNoRetry(context.Context, WriteBatch) (retry WriteBatch, err error)

	// For the read path.
	QueryPages(ctx context.Context, queries []IndexQuery, callback func(IndexQuery, ReadBatch) (shouldContinue bool)) error

	// Iterate through every row in a table, for batch jobs
	ScanTable(ctx context.Context, tableName string, callbacks []func(result ReadBatch)) error

	// For storing and retrieving chunks.
	PutChunks(ctx context.Context, chunks []Chunk) error
	GetChunks(ctx context.Context, chunks []Chunk) ([]Chunk, error)
}

// WriteBatch represents a batch of writes.
type WriteBatch interface {
	Add(tableName, hashValue string, rangeValue []byte, value []byte)
	AddDelete(tableName, hashValue string, rangeValue []byte)
	AddBatch(WriteBatch)
	Len() int
	Take(undersizedOK bool) WriteBatch
}

// ReadBatch represents the results of a QueryPages.
type ReadBatch interface {
	Iterator() ReadBatchIterator
}

// ReadBatchIterator is an iterator over a ReadBatch.
type ReadBatchIterator interface {
	Next() bool
	HashValue() string
	RangeValue() []byte
	Value() []byte
}
