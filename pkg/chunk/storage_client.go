package chunk

import (
	"context"

	"github.com/prometheus/common/model"
)

// IndexClient is a client for the storage of the index (e.g. DynamoDB or Bigtable).
type IndexClient interface {
	Stop()

	// For the write path.
	NewWriteBatch() WriteBatch
	BatchWrite(context.Context, WriteBatch) error
	BatchWriteNoRetry(context.Context, WriteBatch) (retry WriteBatch, err error)

	// For the read path.
	QueryPages(ctx context.Context, queries []IndexQuery, callback func(IndexQuery, ReadBatch) (shouldContinue bool)) error
}

// ObjectClient is for storing and retrieving chunks.
type ObjectClient interface {
	Stop()

	// Iterate through every row in the tables in time range, for batch jobs
	Scan(ctx context.Context, from, through model.Time, withValue bool, callbacks []func(result ReadBatch)) error

	PutChunks(ctx context.Context, chunks []Chunk) error
	GetChunks(ctx context.Context, chunks []Chunk) ([]Chunk, error)
}

// WriteBatch represents a batch of writes.
type WriteBatch interface {
	Add(tableName, hashValue string, rangeValue []byte, value []byte)
	AddChunk(ObjectClient, Chunk, []byte)
	AddDelete(tableName, hashValue string, rangeValue []byte)
	AddBatch(WriteBatch)
	Len() int
	Take(undersizedOK bool) (WriteBatch, int)
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
