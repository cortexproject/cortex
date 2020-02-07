package local

import (
	"context"
	"fmt"

	"go.etcd.io/bbolt"
)

type BoltDBDeleteRequestsClient struct {
	*BoltIndexClient
	deleteRequestsTableName string
}

func NewBoltDBDeleteRequestsClient(cfg BoltDBConfig, deleteRequestsTableName string) (*BoltDBDeleteRequestsClient, error) {
	boltIndexClient, err := NewBoltDBIndexClient(cfg)
	if err != nil {
		return nil, err
	}

	return &BoltDBDeleteRequestsClient{
		BoltIndexClient:         boltIndexClient,
		deleteRequestsTableName: deleteRequestsTableName,
	}, nil
}

// Update value of an index entry
func (b *BoltDBDeleteRequestsClient) Update(ctx context.Context, tableName, hashValue string, rangeValue []byte, value []byte) error {
	db, err := b.GetDB(b.deleteRequestsTableName, DBOperationWrite)
	if err != nil {
		return err
	}

	key := hashValue + separator + string(rangeValue)

	if err := db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found in table %s", bucketName, b.deleteRequestsTableName)
		}

		bv := bucket.Get([]byte(key))

		if bv == nil {
			return fmt.Errorf("key %s not found for updating its value", key)
		}
		if err := bucket.Put([]byte(key), value); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}
