package chunk

import (
	"golang.org/x/net/context"
)

// StorageClient is a client for DynamoDB
type StorageClient interface {
	// For the write path
	NewWriteBatch() WriteBatch
	BatchWrite(context.Context, WriteBatch) error

	// For the read path
	QueryPages(ctx context.Context, entry IndexEntry, callback func(result ReadBatch, lastPage bool) (shouldContinue bool)) error

	// For table management
	ListTables() ([]string, error)
	CreateTable(name string, readCapacity, writeCapacity int64) error
	DescribeTable(name string) (readCapacity, writeCapacity int64, status string, err error)
	UpdateTable(name string, readCapacity, writeCapacity int64) error
}

// WriteBatch represents a batch of writes
type WriteBatch interface {
	Add(tableName, hashValue string, rangeValue []byte)
}

// ReadBatch represents the results of a QueryPages
type ReadBatch interface {
	Len() int
	RangeValue(index int) []byte

	// Value is deprecated - it exists to support an old schema where the chunk
	// metadata was written to the chunk index.
	Value(index int) []byte
}
