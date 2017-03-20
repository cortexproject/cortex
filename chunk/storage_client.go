package chunk

import (
	"golang.org/x/net/context"
)

// StorageClient is a client for the persistent storage for Cortex. (e.g. DynamoDB + S3).
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

	// For storing and retrieving objects
	PutObject(ctx context.Context, key string, data []byte) error
	GetObject(ctx context.Context, key string) ([]byte, error)
}

// WriteBatch represents a batch of writes
type WriteBatch interface {
	Add(tableName, hashValue string, rangeValue []byte, value []byte)
}

// ReadBatch represents the results of a QueryPages
type ReadBatch interface {
	Len() int
	RangeValue(index int) []byte
	Value(index int) []byte
}
